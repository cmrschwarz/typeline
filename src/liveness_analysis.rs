use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
    iter,
    ops::Range,
};

use bitvec::{prelude::LocalBits, slice::BitSlice, vec::BitVec};
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    context::Session,
    operators::{
        call::OpCall,
        call_concurrent::OpCallConcurrent,
        operator::{OperatorData, OperatorOffsetInChain},
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        string_store::{StringStoreEntry, INVALID_STRING_STORE_ENTRY},
    },
};

pub type BasicBlockId = usize;

pub const READS_OFFSET: usize = 0;
pub const WRITES_OFFSET: usize = 1; // 'writes' means dup/drop in our case
pub const STRINGIFIED_READS_ONLY_OFFSET: usize = 2;
pub const REGULAR_FIELD_OFFSETS: [usize; 3] =
    [READS_OFFSET, WRITES_OFFSET, STRINGIFIED_READS_ONLY_OFFSET];
pub const SURVIVES_OFFSET: usize = 3;

pub const SLOTS_PER_OP_OUTPUT: usize = 3; // output ops don't care about survives
pub const LOCAL_SLOTS_PER_BASIC_BLOCK: usize = 4;
pub const SLOTS_PER_BASIC_BLOCK: usize = LOCAL_SLOTS_PER_BASIC_BLOCK * 2;

pub type VarId = u32;
pub type OpOutputIdx = u32;
pub const INVALID_VAR_ID: VarId = VarId::MAX;
pub const INVALID_OP_OUTPUT_ID: OpOutputIdx = OpOutputIdx::MAX;
pub const INVALID_FIELD_NAME: StringStoreEntry = INVALID_STRING_STORE_ENTRY;

pub struct BasicBlock {
    chain_id: ChainId,
    operators_start: OperatorOffsetInChain,
    operators_end: OperatorOffsetInChain,
    var_data_start: usize,
    calls: SmallVec<[BasicBlockId; 2]>,
    successors: SmallVec<[BasicBlockId; 2]>,
    predecessors: SmallVec<[BasicBlockId; 2]>,
    // var name after the bb -> original var name
    // if the 'original name' was locally generated, there is no entry
    renames: HashMap<VarId, VarId, BuildIdentityHasher>,
    // vars available at the start that may be accessed through
    // data behind vars available at the end that contain field references
    field_references: HashMap<VarId, SmallVec<[VarId; 4]>>,
    input_var_handed_to_calls: Option<VarId>,
    updates_required: bool,
}
pub enum Var {
    Named(StringStoreEntry),
    Anonymous(ChainId),
}
#[derive(Clone, Default)]
pub struct OpOutput {
    bound_vars_after_bb: SmallVec<[VarId; 4]>,
    // vars available at the start that this output may contain field
    // references to
    field_references: SmallVec<[VarId; 4]>,
}

#[derive(Clone, Default)]
pub struct OperatorLivenessData {
    pub outputs_start: OpOutputIdx,
    pub outputs_end: OpOutputIdx,
}

#[derive(Default)]
pub struct LivenessData {
    pub var_data: BitVec<Cell<usize>, LocalBits>,
    pub op_outputs_data: BitVec<Cell<usize>, LocalBits>,
    pub op_outputs: Vec<OpOutput>,
    pub vars: Vec<Var>,
    pub var_names: HashMap<StringStoreEntry, VarId, BuildIdentityHasher>,
    pub basic_blocks: Vec<BasicBlock>,
    // for each var id, either an op output id or INVALID_OP_OUTPUT_ID
    pub operator_output_map: Vec<OpOutputIdx>,
    pub operator_data: Vec<OperatorLivenessData>,
    updates_required: Vec<BasicBlockId>,
}

impl LivenessData {
    pub fn add_var_name(&mut self, name: Option<StringStoreEntry>) {
        if let Some(name) = name {
            match self.var_names.entry(name) {
                Entry::Occupied(_) => (),
                Entry::Vacant(e) => {
                    e.insert(self.vars.len() as VarId);
                    self.vars.push(Var::Named(name));
                }
            }
        }
    }
    pub fn setup_vars(&mut self, sess: &Session) {
        self.vars.extend(
            (0..sess.chains.len()).map(|i| Var::Anonymous(i as ChainId)),
        );
        for c in &sess.chains {
            for op in &c.operators {
                self.add_var_name(sess.operator_bases[*op as usize].label);
                match &sess.operator_data[*op as usize] {
                    OperatorData::Regex(re) => {
                        for n in &re.capture_group_names {
                            self.add_var_name(*n);
                        }
                    }
                    OperatorData::Key(k) => {
                        self.add_var_name(Some(k.key_interned));
                    }
                    OperatorData::Call(_) => (),
                    OperatorData::CallConcurrent(_) => (),
                    OperatorData::Cast(_) => (),
                    OperatorData::Count(_) => (),
                    OperatorData::Print(_) => (),
                    OperatorData::Join(_) => (),
                    OperatorData::Fork(_) => (),
                    OperatorData::Next(_) => (),
                    OperatorData::Up(_) => (),
                    OperatorData::Select(_) => (),
                    OperatorData::Format(_) => (),
                    OperatorData::StringSink(_) => (),
                    OperatorData::FileReader(_) => (),
                    OperatorData::Literal(_) => (),
                    OperatorData::Sequence(_) => (),
                }
            }
        }
    }
    fn split_bb_at_call(
        &mut self,
        bb_id: BasicBlockId,
        op_n: OperatorOffsetInChain,
        called_chain: ChainId,
        data_size: &mut usize,
        bits_per_bb: usize,
    ) {
        let succ_bb_id = self.basic_blocks.len();
        let bb = &mut self.basic_blocks[bb_id];
        let end = bb.operators_end;
        let chain_id = bb.chain_id;
        bb.operators_end = op_n + 1;
        if op_n < end {
            bb.successors.push(succ_bb_id);
            bb.operators_end = op_n;
            bb.calls.push(called_chain as BasicBlockId);
            self.basic_blocks.push(BasicBlock {
                chain_id: chain_id,
                operators_start: op_n + 1,
                operators_end: end,
                var_data_start: *data_size,
                calls: Default::default(),
                successors: Default::default(),
                renames: Default::default(),
                updates_required: true,
                input_var_handed_to_calls: None,
                field_references: Default::default(),
                predecessors: Default::default(),
            });
            *data_size += bits_per_bb;
            self.updates_required.push(bb_id);
        }
    }
    fn setup_bbs(&mut self, sess: &Session) {
        let var_count = self.vars.len();
        let bits_per_bb = var_count * SLOTS_PER_BASIC_BLOCK;
        let mut data_size = 0;
        for (i, c) in sess.chains.iter().enumerate() {
            self.basic_blocks.push(BasicBlock {
                chain_id: i as ChainId,
                operators_start: 0,
                operators_end: c.operators.len() as u32,
                var_data_start: data_size,
                calls: Default::default(),
                successors: Default::default(),
                renames: Default::default(),
                updates_required: true,
                input_var_handed_to_calls: None,
                field_references: Default::default(),
                predecessors: Default::default(),
            });
            data_size += bits_per_bb;
        }
        self.updates_required.extend(1..sess.chains.len() + 1);
        while let Some(i) = self.updates_required.pop() {
            let bb = &mut self.basic_blocks[i];
            let cn = &sess.chains[bb.chain_id as usize];
            for (op_n, op) in cn.operators
                [bb.operators_start as usize..bb.operators_end as usize]
                .iter()
                .enumerate()
            {
                let op_n = op_n as OperatorOffsetInChain;
                let op_id = *op as usize;
                match &sess.operator_data[op_id] {
                    OperatorData::CallConcurrent(OpCallConcurrent {
                        target_resolved,
                        ..
                    })
                    | OperatorData::Call(OpCall {
                        target_resolved, ..
                    }) => {
                        self.split_bb_at_call(
                            i,
                            op_n,
                            *target_resolved,
                            &mut data_size,
                            bits_per_bb,
                        );
                        break;
                    }
                    OperatorData::Fork(f) => {
                        debug_assert!(op_n + 1 == bb.operators_end);
                        for sc in &cn.subchains[f.subchain_count_before
                            as usize
                            ..f.subchain_count_after as usize]
                        {
                            bb.successors.push(*sc as BasicBlockId);
                        }
                    }
                    OperatorData::Cast(_) => (),
                    OperatorData::Count(_) => (),
                    OperatorData::Print(_) => (),
                    OperatorData::Join(_) => (),
                    OperatorData::Next(_) => unreachable!(),
                    OperatorData::Up(_) => unreachable!(),
                    OperatorData::Key(_) => {}
                    OperatorData::Select(_) => {}
                    OperatorData::Regex(_) => {}
                    OperatorData::Format(_) => (),
                    OperatorData::StringSink(_) => (),
                    OperatorData::FileReader(_) => (),
                    OperatorData::Literal(_) => (),
                    OperatorData::Sequence(_) => (),
                }
            }
        }
        self.var_data.resize(data_size, false);
    }
    fn setup_bb_predecessors(&mut self) {
        for bb_id in 0..self.basic_blocks.len() {
            for succ_n in 0..self.basic_blocks[bb_id].successors.len() {
                let succ_id = self.basic_blocks[bb_id].successors[succ_n];
                self.basic_blocks[succ_id].predecessors.push(bb_id);
            }
        }
    }
    fn access_field(
        &self,
        bb_id: BasicBlockId,
        var_id: VarId,
        write: bool,
        stringified: bool,
    ) {
        let bb = &self.basic_blocks[bb_id];
        let mut vid = var_id as usize;
        let ooid = self.operator_output_map[vid] as usize;
        if ooid != INVALID_OP_OUTPUT_ID as usize {
            let ooc = self.op_outputs.len();
            self.op_outputs_data
                .set_aliased(READS_OFFSET * ooc + ooid, true);
            if !stringified {
                self.op_outputs_data.set_aliased(
                    STRINGIFIED_READS_ONLY_OFFSET * ooc + ooid,
                    false,
                );
            }
            if write {
                self.op_outputs_data
                    .set_aliased(WRITES_OFFSET * ooc + ooid, true);
            }
            for fr in &self.op_outputs[ooid].field_references {
                self.access_field(bb_id, *fr, write, stringified);
            }
            return; // var was killed, no need to record data on it
        }
        if let Some(original_name) = bb.renames.get(&var_id) {
            vid = *original_name as usize;
        }
        let vc = self.vars.len();
        let vds = bb.var_data_start;
        self.var_data
            .set_aliased(vds + READS_OFFSET * vc + vid, true);
        if !stringified {
            self.var_data.set_aliased(
                vds + STRINGIFIED_READS_ONLY_OFFSET * vc + vid,
                false,
            );
        }
        if write {
            self.var_data
                .set_aliased(vds + WRITES_OFFSET * vc + vid, true);
        }
    }
    fn compute_local_liveness_for_bb(
        &mut self,
        sess: &Session,
        bb_id: BasicBlockId,
    ) {
        let mut bb = &mut self.basic_blocks[bb_id];
        let varcount = self.vars.len();
        let reads_start = bb.var_data_start + READS_OFFSET * varcount;
        let writes_start = bb.var_data_start + WRITES_OFFSET * varcount;
        let survives = bb.var_data_start + SURVIVES_OFFSET * varcount;
        self.var_data[reads_start..reads_start + varcount].fill(false);
        self.var_data[writes_start..writes_start + varcount].fill(false);
        let cn = &sess.chains[bb.chain_id as usize];
        let mut input_field = bb.chain_id as VarId;
        let mut last_output_field = input_field;
        let mut any_writes_so_far = false;
        self.operator_output_map
            .iter_mut()
            .for_each(|v| *v = INVALID_OP_OUTPUT_ID);
        for op_n in bb.operators_start..bb.operators_end {
            let op_id = cn.operators[op_n as usize] as usize;
            let op_base = &sess.operator_bases[op_id];
            let output_field = if op_base.append_mode {
                input_field
            } else {
                if let Some(label) = op_base.label {
                    self.var_names[&label]
                } else if op_base.append_mode {
                    last_output_field
                } else {
                    INVALID_VAR_ID
                }
            };
            let used_input_field =
                if op_base.append_mode && last_output_field == bb.chain_id {
                    INVALID_VAR_ID
                } else {
                    input_field
                };
            let mut input_accessed = true;
            let mut input_access_stringified = true;
            let mut may_dup_or_drop = false;
            match &sess.operator_data[op_id as usize] {
                OperatorData::Fork(_) => {
                    debug_assert!(op_n + 1 == bb.operators_end);
                    break;
                }
                OperatorData::Call(_) | OperatorData::CallConcurrent(_) => {
                    let op =
                        self.operator_output_map[used_input_field as usize];
                    if op == INVALID_OP_OUTPUT_ID {
                        bb.input_var_handed_to_calls = Some(used_input_field);
                    }
                    debug_assert!(op_n + 1 == bb.operators_end);
                    break;
                }
                OperatorData::Key(key) => {
                    debug_assert!(!op_base.append_mode);
                    let tgt_var_name = self.var_names[&key.key_interned];
                    let op_output =
                        self.operator_output_map[input_field as usize];
                    self.operator_output_map[tgt_var_name as usize] =
                        op_output;
                    if op_output == INVALID_OP_OUTPUT_ID {
                        bb.renames.insert(tgt_var_name, input_field);
                    } else {
                        bb.renames.remove(&tgt_var_name);
                    }
                    continue;
                }
                OperatorData::Select(select) => {
                    input_field = self.var_names[&select.key_interned];
                    if let Some(original_var) = bb.renames.get(&input_field) {
                        input_field = *original_var;
                    }
                    continue;
                }
                OperatorData::Regex(re) => {
                    may_dup_or_drop =
                        !re.opts.non_mandatory || re.opts.multimatch;
                    if used_input_field != INVALID_VAR_ID {
                        if self.operator_output_map[used_input_field as usize]
                            == INVALID_OP_OUTPUT_ID
                        {
                            let tgt = *bb
                                .renames
                                .get(&used_input_field)
                                .unwrap_or(&used_input_field);
                            for i in 0..re.capture_group_names.len() {
                                self.op_outputs[self.operator_data[op_id]
                                    .outputs_start
                                    as usize
                                    + i]
                                    .field_references
                                    .push(tgt);
                            }
                        }
                    }
                    for (cgi, cgn) in re.capture_group_names.iter().enumerate()
                    {
                        if let Some(name) = cgn {
                            let tgt_var_name = self.var_names[name];
                            self.operator_output_map[tgt_var_name as usize] =
                                self.operator_data[op_id].outputs_start
                                    + cgi as OpOutputIdx;
                            bb.renames.remove(&tgt_var_name);
                        }
                    }
                }
                OperatorData::Format(fmt) => {
                    // TODO: check the format keys to detemine this
                    input_access_stringified = false;
                    // might be set to true again in the loop below
                    input_accessed = false;
                    for ref_name in &fmt.refs_idx {
                        if let Some(name) = ref_name {
                            let var_id = self.var_names[name];
                            self.access_field(
                                bb_id,
                                var_id,
                                any_writes_so_far,
                                false, // TODO
                            );
                            bb = &mut self.basic_blocks[bb_id];
                        } else {
                            input_accessed = true;
                        }
                    }
                }
                OperatorData::FileReader(_) => {
                    // this only inserts if input is done, so no write flag
                    // neccessary
                    input_accessed = false;
                }
                OperatorData::Literal(di) => {
                    may_dup_or_drop = di.insert_count.is_some();
                    input_accessed = false;
                }
                OperatorData::Join(_) => {
                    may_dup_or_drop = true;
                }
                OperatorData::Sequence(seq) => {
                    input_accessed = false;
                    may_dup_or_drop = !seq.stop_after_input;
                }
                OperatorData::Count(_) => {
                    may_dup_or_drop = true;
                    input_accessed = false;
                }
                OperatorData::Cast(_) => (),
                OperatorData::Print(_) => (),
                OperatorData::StringSink(_) => (),
                OperatorData::Next(_) => unreachable!(),
                OperatorData::Up(_) => unreachable!(),
            }
            if input_accessed {
                self.access_field(
                    bb_id,
                    input_field,
                    any_writes_so_far,
                    input_access_stringified,
                );
                bb = &mut self.basic_blocks[bb_id];
            }
            if let Some(label) = sess.operator_bases[op_id as usize].label {
                let var_id = self.var_names[&label];
                self.operator_output_map[var_id as usize] =
                    self.operator_data[op_id].outputs_start;
                bb.renames.remove(&var_id);
                self.var_data.set_aliased(survives + var_id as usize, false);
            }
            any_writes_so_far |= may_dup_or_drop;
            last_output_field = output_field;
            if !op_base.append_mode && !op_base.transparent_mode {
                input_field = output_field;
            }
        }
        for (var_idx, op_output_id) in
            self.operator_output_map.iter().enumerate()
        {
            if *op_output_id == INVALID_OP_OUTPUT_ID {
                continue;
            }
            let var_id = var_idx as VarId;
            self.var_data.set_aliased(
                bb.var_data_start
                    + SURVIVES_OFFSET * self.vars.len()
                    + var_idx,
                false,
            );
            let op_output = &mut self.op_outputs[*op_output_id as usize];
            op_output.bound_vars_after_bb.push(var_id);
            if !op_output.field_references.is_empty() {
                bb.field_references
                    .insert(var_id, op_output.field_references.clone());
            }
            self.op_outputs[*op_output_id as usize]
                .bound_vars_after_bb
                .push(var_id);
        }
    }
    fn compute_local_liveness(&mut self, sess: &Session) {
        for i in 0..self.basic_blocks.len() {
            self.compute_local_liveness_for_bb(sess, i);
        }
    }
    fn get_global_var_data_bounds(&self, bb_id: BasicBlockId) -> Range<usize> {
        let local_bits_per_bb = self.vars.len() * LOCAL_SLOTS_PER_BASIC_BLOCK;
        let start =
            self.basic_blocks[bb_id].var_data_start + local_bits_per_bb;
        start..start + local_bits_per_bb
    }
    fn get_local_var_data_bounds(&self, bb_id: BasicBlockId) -> Range<usize> {
        let local_bits_per_bb = self.vars.len() * LOCAL_SLOTS_PER_BASIC_BLOCK;
        let start = self.basic_blocks[bb_id].var_data_start;
        start..start + local_bits_per_bb
    }
    fn get_global_var_data(
        &self,
        bb_id: BasicBlockId,
    ) -> &BitSlice<Cell<usize>> {
        &self.var_data[self.get_global_var_data_bounds(bb_id)]
    }
    fn get_to_global_var_data_ored<'b>(
        &self,
        tgt: &mut BitSlice<Cell<usize>>,
        mut bbs: impl Iterator<Item = &'b BasicBlockId>,
    ) {
        if let Some(bb_id) = bbs.next() {
            tgt.copy_from_bitslice(self.get_global_var_data(*bb_id));
            for bb_id in bbs {
                *tgt |= self.get_global_var_data(*bb_id);
            }
        } else {
            tgt.fill(false);
        }
    }
    fn kill_non_survivors(
        &self,
        tgt: &mut BitSlice<Cell<usize>>,
        survivors: &BitSlice<Cell<usize>>,
    ) {
        let var_count = self.vars.len();
        let survivors_start = SURVIVES_OFFSET * var_count;
        let survivors_slice =
            &survivors[survivors_start..survivors_start + var_count];
        for i in &REGULAR_FIELD_OFFSETS {
            let start = var_count * i;
            tgt[start..start + var_count] &= survivors_slice;
        }
    }
    fn apply_alias(
        &self,
        tgt: &BitSlice<Cell<usize>>,
        src: &BitSlice<Cell<usize>>,
        original_var: VarId,
        alias_var: VarId,
    ) {
        let var_count = self.vars.len();
        for i in REGULAR_FIELD_OFFSETS {
            tgt.set_aliased(
                i * var_count + original_var as usize,
                src[i * var_count + alias_var as usize],
            );
        }
    }
    fn apply_bb_aliases(
        &self,
        tgt: &BitSlice<Cell<usize>>,
        src: &BitSlice<Cell<usize>>,
        bb: &BasicBlock,
    ) {
        for (alias_var, original_var) in &bb.renames {
            self.apply_alias(tgt, src, *original_var, *alias_var);
        }
        for (alias_var, field_refs) in &bb.field_references {
            for original_var in field_refs {
                self.apply_alias(tgt, src, *original_var, *alias_var);
            }
        }
    }
    fn update_bb_predecessors(&mut self, bb_id: BasicBlockId) {
        for pred_n in 0..self.basic_blocks[bb_id].predecessors.len() {
            let pred_id = self.basic_blocks[bb_id].predecessors[pred_n];
            let pred = &mut self.basic_blocks[pred_id];
            if !pred.updates_required {
                pred.updates_required = true;
                self.updates_required.push(pred_id);
            }
        }
    }
    fn compute_global_liveness(&mut self) {
        let var_count = self.vars.len();
        let local_bits_per_bb = var_count * LOCAL_SLOTS_PER_BASIC_BLOCK;
        let mut successors = BitVec::<Cell<usize>, LocalBits>::new();
        let mut calls = BitVec::<Cell<usize>, LocalBits>::new();
        let mut global = BitVec::<Cell<usize>, LocalBits>::new();
        successors.resize(local_bits_per_bb, false);
        calls.resize(local_bits_per_bb, false);
        global.resize(local_bits_per_bb, false);
        self.updates_required.extend(0..self.basic_blocks.len());
        while let Some(bb_id) = self.updates_required.pop() {
            self.basic_blocks[bb_id].updates_required = false;
            let bb = &self.basic_blocks[bb_id];

            let bb_local_range = self.get_local_var_data_bounds(bb_id);
            let bb_global_range = self.get_global_var_data_bounds(bb_id);

            if bb.successors.is_empty() && bb.calls.is_empty() {
                self.var_data.copy_within(
                    bb_local_range.clone(),
                    bb_global_range.start,
                );
                self.update_bb_predecessors(bb_id);
                continue;
            }
            global.copy_from_bitslice(&self.var_data[bb_local_range.clone()]);
            if bb.calls.is_empty() {
                self.get_to_global_var_data_ored(
                    &mut calls,
                    bb.successors.iter(),
                );
            } else {
                self.get_to_global_var_data_ored(&mut calls, bb.calls.iter());
                if bb.successors.is_empty() {
                    self.get_to_global_var_data_ored(
                        &mut successors,
                        bb.successors.iter(),
                    );
                    for call_bb_id in &bb.calls {
                        let cbb = &self.basic_blocks[*call_bb_id];
                        self.apply_bb_aliases(&calls, &successors, cbb);
                    }
                    self.kill_non_survivors(&mut successors, &calls);
                    calls |= &successors;
                }
            }
            self.apply_bb_aliases(&calls, &global, bb);
            self.kill_non_survivors(&mut calls, &global);
            global |= &calls;
            if global != self.var_data[bb_global_range.clone()] {
                self.var_data[bb_global_range.clone()]
                    .copy_from_bitslice(&global);
                self.update_bb_predecessors(bb_id);
            }
        }
    }
    fn compute_op_output_liveness(&mut self) {
        todo!();
    }
    pub fn setup_operator_outputs(&mut self, sess: &mut Session) {
        self.operator_data.extend(
            iter::repeat(Default::default()).take(sess.operator_data.len()),
        );
        let mut total_outputs_count = sess.chains.len();
        for op_id in 0..sess.operator_data.len() {
            let op_base = &mut sess.operator_bases[op_id];
            let op_liveness_data = &mut self.operator_data[op_id];

            op_liveness_data.outputs_start =
                total_outputs_count as OpOutputIdx;
            let app = if op_base.append_mode { 0 } else { 1 };
            let outputs_count = match &sess.operator_data[op_id] {
                OperatorData::Call(_) => app,
                OperatorData::CallConcurrent(_) => app,
                OperatorData::Cast(_) => app,
                OperatorData::Count(_) => app,
                OperatorData::Print(_) => app,
                OperatorData::Join(_) => app,
                OperatorData::Fork(_) => 0,
                OperatorData::Next(_) => app,
                OperatorData::Up(_) => 0,
                OperatorData::Key(_) => 0,
                OperatorData::Select(_) => 0,
                OperatorData::Regex(re) => {
                    app + re.capture_group_names.len() - 1
                }
                OperatorData::Format(_) => app,
                OperatorData::StringSink(_) => app,
                OperatorData::FileReader(_) => app,
                OperatorData::Literal(_) => app,
                OperatorData::Sequence(_) => app,
            };
            total_outputs_count += outputs_count;
            op_liveness_data.outputs_end = total_outputs_count as OpOutputIdx;
        }
        self.op_outputs.extend(
            iter::repeat(Default::default()).take(total_outputs_count),
        );
        self.operator_output_map.extend(
            iter::repeat(INVALID_OP_OUTPUT_ID).take(total_outputs_count),
        );
        self.op_outputs_data
            .resize(total_outputs_count * SLOTS_PER_OP_OUTPUT, false);
    }
}

pub fn compute_liveness(sess: &mut Session) -> LivenessData {
    let mut ld = LivenessData::default();
    ld.setup_operator_outputs(sess);
    ld.setup_vars(sess);
    ld.setup_bbs(sess);
    ld.setup_bb_predecessors();
    ld.compute_local_liveness(sess);
    ld.compute_global_liveness();
    ld.compute_op_output_liveness();
    ld
}
