use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
    iter,
    ops::Range,
};

use bitvec::{slice::BitSlice, vec::BitVec};
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    context::Session,
    operators::{
        call::OpCall,
        call_concurrent::OpCallConcurrent,
        fork::OpFork,
        forkcat::OpForkCat,
        operator::{OperatorData, OperatorOffsetInChain},
    },
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

pub type BasicBlockId = usize;

pub const READS_OFFSET: usize = 0;
pub const HEADER_WRITES_OFFSET: usize = 1;
pub const DATA_WRITES_OFFSET: usize = 2;
pub const STRINGIFIED_READS_ONLY_OFFSET: usize = 3;
pub const REGULAR_FIELD_OFFSETS: [usize; 4] = [
    READS_OFFSET,
    HEADER_WRITES_OFFSET,
    DATA_WRITES_OFFSET,
    STRINGIFIED_READS_ONLY_OFFSET,
];

// used in reset_op_outputs_data_for_vars to prepare op output data
pub const LOCALLY_ZERO_INITIALIZED_FIELDS_OFFSET: [usize; 3] =
    [READS_OFFSET, HEADER_WRITES_OFFSET, DATA_WRITES_OFFSET];
pub const LOCALLY_ONE_INITIALIZED_FIELDS_OFFSET: [usize; 1] =
    [STRINGIFIED_READS_ONLY_OFFSET];

pub const SURVIVES_OFFSET: usize = 4;

pub const SLOTS_PER_OP_OUTPUT: usize = 4; // output ops don't care about survives
pub const LOCAL_SLOTS_PER_BASIC_BLOCK: usize = 5;
pub const LOCAL_SLOTS_OFFSET: usize = 0;
pub const GLOBAL_SLOTS_OFFSET: usize = LOCAL_SLOTS_PER_BASIC_BLOCK;
pub const SUCCESSION_SLOTS_OFFSET: usize = 2 * LOCAL_SLOTS_PER_BASIC_BLOCK;
// local slots + global slots + succession data
pub const SLOTS_PER_BASIC_BLOCK: usize = LOCAL_SLOTS_PER_BASIC_BLOCK * 3;

pub type VarId = u32;
pub const BB_INPUT_VAR: VarId = 0;
pub const UNREACHABLE_DUMMY_VAR: VarId = 1;

pub type OpOutputIdx = u32;
pub const BB_INPUT_OPERATOR_OUTPUT_IDX: OpOutputIdx = BB_INPUT_VAR;
pub const DUMMY_FIELD_OUTPUT_IDX: OpOutputIdx = UNREACHABLE_DUMMY_VAR;

pub struct BasicBlock {
    pub chain_id: ChainId,
    pub operators_start: OperatorOffsetInChain,
    pub operators_end: OperatorOffsetInChain,
    pub var_data_start: usize,
    pub calls: SmallVec<[BasicBlockId; 2]>,
    pub successors: SmallVec<[BasicBlockId; 2]>,
    pub predecessors: SmallVec<[BasicBlockId; 2]>,
    // var name after the bb -> original var name
    // if the 'original name' was locally generated, there is no entry
    pub renames: HashMap<VarId, VarId, BuildIdentityHasher>,
    // vars available at the start that may be accessed through
    // data behind vars available at the end that contain field references
    pub field_references: HashMap<VarId, SmallVec<[VarId; 4]>>,
    updates_required: bool,
}
pub enum Var {
    Named(StringStoreEntry),
    BBInput,
    UnreachableDummyVar,
}
#[derive(Clone, Default)]
pub struct OpOutput {
    bound_vars_after_bb: SmallVec<[VarId; 4]>,
    // other outputs referenced in the data of this output, e.g. for regex
    field_references: SmallVec<[OpOutputIdx; 4]>,
}

#[derive(Clone, Default)]
pub struct OperatorLivenessData {
    pub basic_block_id: BasicBlockId,
    pub outputs_start: OpOutputIdx,
    pub outputs_end: OpOutputIdx,
}

#[derive(Default)]
pub struct LivenessData {
    pub var_data: BitVec<Cell<usize>>,
    pub op_outputs_data: BitVec<Cell<usize>>,
    pub op_outputs: Vec<OpOutput>,
    pub vars: Vec<Var>,
    pub var_names: HashMap<StringStoreEntry, VarId, BuildIdentityHasher>,
    pub basic_blocks: Vec<BasicBlock>,
    pub vars_to_op_outputs_map: Vec<OpOutputIdx>,
    pub operator_liveness_data: Vec<OperatorLivenessData>,
    updates_required: Vec<BasicBlockId>,
}

impl LivenessData {
    pub fn setup_operator_outputs(&mut self, sess: &mut Session) {
        self.operator_liveness_data.extend(
            iter::repeat(Default::default()).take(sess.operator_data.len()),
        );
        let mut total_outputs_count = self.vars.len();
        for op_id in 0..sess.operator_data.len() {
            let op_base = &mut sess.operator_bases[op_id];
            let op_liveness_data = &mut self.operator_liveness_data[op_id];

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
                OperatorData::Nop(_) => 0,
                // technically this has output, but it always introduces a
                // separate BB so we don't want to allocate slots for that
                OperatorData::ForkCat(_) => 0,
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
        self.vars_to_op_outputs_map
            .extend(0..self.vars.len() as VarId);

        self.op_outputs_data
            .resize(total_outputs_count * SLOTS_PER_OP_OUTPUT, false);
        for i in LOCALLY_ONE_INITIALIZED_FIELDS_OFFSET {
            self.op_outputs_data
                [i * total_outputs_count..(i + 1) * total_outputs_count]
                .fill(true);
        }
    }
    pub fn add_var_name(&mut self, name: StringStoreEntry) {
        match self.var_names.entry(name) {
            Entry::Occupied(_) => (),
            Entry::Vacant(e) => {
                e.insert(self.vars.len() as VarId);
                self.vars.push(Var::Named(name));
            }
        }
    }
    pub fn add_var_name_opt(&mut self, name: Option<StringStoreEntry>) {
        if let Some(name) = name {
            self.add_var_name(name);
        }
    }
    pub fn setup_vars(&mut self, sess: &Session) {
        self.vars.push(Var::BBInput);
        self.vars.push(Var::UnreachableDummyVar);
        for c in &sess.chains {
            for op in &c.operators {
                self.add_var_name_opt(sess.operator_bases[*op as usize].label);
                match &sess.operator_data[*op as usize] {
                    OperatorData::Regex(re) => {
                        for n in &re.capture_group_names {
                            self.add_var_name_opt(*n);
                        }
                    }
                    OperatorData::Key(k) => {
                        self.add_var_name(k.key_interned.unwrap());
                    }
                    OperatorData::Call(_) => (),
                    OperatorData::CallConcurrent(_) => (),
                    OperatorData::Cast(_) => (),
                    OperatorData::Count(_) => (),
                    OperatorData::Print(_) => (),
                    OperatorData::Join(_) => (),
                    OperatorData::Fork(_) => (),
                    OperatorData::ForkCat(_) => (),
                    OperatorData::Next(_) => (),
                    OperatorData::Up(_) => (),
                    OperatorData::Nop(_) => (),
                    OperatorData::Select(s) => {
                        self.add_var_name(s.key_interned.unwrap());
                    }
                    OperatorData::Format(fmt) => {
                        for r in &fmt.refs_idx {
                            self.add_var_name_opt(*r);
                        }
                    }
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
                chain_id,
                operators_start: op_n + 1,
                operators_end: end,
                var_data_start: *data_size,
                calls: Default::default(),
                successors: Default::default(),
                renames: Default::default(),
                updates_required: true,
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
                field_references: Default::default(),
                predecessors: Default::default(),
            });
            data_size += bits_per_bb;
        }
        self.updates_required.extend(0..sess.chains.len());
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
                            target_resolved.unwrap(),
                            &mut data_size,
                            bits_per_bb,
                        );
                        break;
                    }
                    OperatorData::Fork(OpFork {
                        subchains_start,
                        subchains_end,
                        ..
                    })
                    | OperatorData::ForkCat(OpForkCat {
                        subchains_start,
                        subchains_end,
                        ..
                    }) => {
                        for sc in &cn.subchains[*subchains_start as usize
                            ..*subchains_end as usize]
                        {
                            bb.successors.push(*sc as BasicBlockId);
                        }
                        break;
                    }
                    OperatorData::Cast(_) => (),
                    OperatorData::Nop(_) => (),
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
    fn setup_bb_linkage_data(&mut self, sess: &Session) {
        for bb_id in 0..self.basic_blocks.len() {
            for succ_n in 0..self.basic_blocks[bb_id].successors.len() {
                let succ_id = self.basic_blocks[bb_id].successors[succ_n];
                self.basic_blocks[succ_id].predecessors.push(bb_id);
            }
            let bb = &self.basic_blocks[bb_id];
            let chain = &sess.chains[bb.chain_id as usize];
            for op_id in &chain.operators
                [bb.operators_start as usize..bb.operators_end as usize]
            {
                self.operator_liveness_data[*op_id as usize].basic_block_id =
                    bb_id;
            }
        }
    }
    fn access_field(
        &self,
        op_output_idx: OpOutputIdx,
        write: bool,
        stringified: bool,
    ) {
        if op_output_idx == DUMMY_FIELD_OUTPUT_IDX {
            // we don't want this field to be forwarded by fork and friends
            // so we just pretend it is never accessed.
            return;
        }
        let oo_idx = op_output_idx as usize;
        let ooc = self.op_outputs.len();
        self.op_outputs_data
            .set_aliased(READS_OFFSET * ooc + oo_idx, true);
        if !stringified {
            self.op_outputs_data.set_aliased(
                STRINGIFIED_READS_ONLY_OFFSET * ooc + oo_idx,
                false,
            );
        }
        if write {
            self.op_outputs_data
                .set_aliased(HEADER_WRITES_OFFSET * ooc + oo_idx, true);
        }
        for fr in &self.op_outputs[oo_idx].field_references {
            self.access_field(*fr, write, stringified);
        }
    }
    fn append_to_field(&self, op_output_idx: OpOutputIdx) {
        debug_assert!(op_output_idx != DUMMY_FIELD_OUTPUT_IDX);
        let oo_idx = op_output_idx as usize;
        let ooc = self.op_outputs.len();
        self.op_outputs_data
            .set_aliased(DATA_WRITES_OFFSET * ooc + oo_idx, true);
    }
    fn reset_op_outputs_data_for_vars(&mut self) {
        let vc = self.vars.len();
        let ooc = self.op_outputs.len();
        for i in &LOCALLY_ZERO_INITIALIZED_FIELDS_OFFSET {
            self.op_outputs_data[i * ooc..i * ooc + vc].fill(false);
        }
        for i in &LOCALLY_ONE_INITIALIZED_FIELDS_OFFSET {
            self.op_outputs_data[i * ooc..i * ooc + vc].fill(true);
        }
        for i in 0..vc {
            // initially each var points to it's input
            self.vars_to_op_outputs_map[i] = i as OpOutputIdx;
            self.op_outputs[i].field_references.clear();
        }
    }
    fn compute_local_liveness_for_bb(
        &mut self,
        sess: &Session,
        bb_id: BasicBlockId,
    ) {
        self.reset_op_outputs_data_for_vars();
        let mut bb = &mut self.basic_blocks[bb_id];
        let cn = &sess.chains[bb.chain_id as usize];
        let mut input_field = BB_INPUT_OPERATOR_OUTPUT_IDX;
        let mut last_output_field = input_field;
        let mut any_writes_so_far = false;
        for op_n in bb.operators_start..bb.operators_end {
            bb = &mut self.basic_blocks[bb_id]; // reborrow for lifetime
            let op_id = cn.operators[op_n as usize] as usize;
            let op_base = &sess.operator_bases[op_id];
            let output_field = if op_base.append_mode {
                last_output_field
            } else {
                self.operator_liveness_data[op_id].outputs_start
            };
            let used_input_field = if op_base.append_mode
                && last_output_field == BB_INPUT_OPERATOR_OUTPUT_IDX
            {
                DUMMY_FIELD_OUTPUT_IDX
            } else {
                input_field
            };
            let mut input_accessed = true;
            let mut input_access_stringified = true;
            let mut may_dup_or_drop = false;
            match &sess.operator_data[op_id] {
                OperatorData::Fork(_) | OperatorData::ForkCat(_) => {
                    debug_assert!(op_n + 1 == bb.operators_end);
                    break;
                }
                OperatorData::Call(_) | OperatorData::CallConcurrent(_) => {
                    debug_assert!(!op_base.append_mode);
                    debug_assert!(op_n + 1 == bb.operators_end);
                    break;
                }
                OperatorData::Key(key) => {
                    let tgt_var = self.var_names[&key.key_interned.unwrap()];
                    debug_assert!(!op_base.append_mode);
                    self.vars_to_op_outputs_map[tgt_var as usize] =
                        used_input_field;
                    continue;
                }
                OperatorData::Select(select) => {
                    let mut var =
                        self.var_names[&select.key_interned.unwrap()];
                    // resolve renames
                    loop {
                        input_field =
                            self.vars_to_op_outputs_map[var as usize];
                        if input_field >= self.vars.len() as OpOutputIdx {
                            break;
                        }
                        // OpOutput indices below vars.len() are the vars
                        var = input_field as VarId;
                    }
                    continue;
                }
                OperatorData::Regex(re) => {
                    may_dup_or_drop =
                        !re.opts.non_mandatory || re.opts.multimatch;
                    for i in 0..re.capture_group_names.len() {
                        self.op_outputs[self.operator_liveness_data[op_id]
                            .outputs_start
                            as usize
                            + i]
                            .field_references
                            .push(used_input_field);
                    }

                    for (cgi, cgn) in re.capture_group_names.iter().enumerate()
                    {
                        if let Some(name) = cgn {
                            let tgt_var_name = self.var_names[name];
                            self.vars_to_op_outputs_map
                                [tgt_var_name as usize] = self
                                .operator_liveness_data[op_id]
                                .outputs_start
                                + cgi as OpOutputIdx;
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
                                var_id,
                                any_writes_so_far,
                                false, // TODO
                            );
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
                OperatorData::Nop(_) => {
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
                    used_input_field,
                    any_writes_so_far,
                    input_access_stringified,
                );
            }
            if op_base.append_mode {
                self.append_to_field(output_field);
            }
            if let Some(label) = sess.operator_bases[op_id].label {
                let var_id = self.var_names[&label];
                self.vars_to_op_outputs_map[var_id as usize] =
                    self.operator_liveness_data[op_id].outputs_start;
            }
            any_writes_so_far |= may_dup_or_drop;
            last_output_field = output_field;
            if !op_base.append_mode && !op_base.transparent_mode {
                input_field = output_field;
            }
        }
        bb = &mut self.basic_blocks[bb_id];
        let vc = self.vars.len();
        let ooc = self.op_outputs.len();
        for i in REGULAR_FIELD_OFFSETS {
            self.var_data
                [bb.var_data_start + i * vc..bb.var_data_start + (i + 1) * vc]
                .copy_from_bitslice(
                    &self.op_outputs_data[i * ooc..i * ooc + vc],
                );
        }
        for (var_idx, op_output_id) in
            self.vars_to_op_outputs_map.iter().enumerate()
        {
            if *op_output_id >= self.vars.len() as OpOutputIdx {
                self.var_data.set_aliased(
                    bb.var_data_start
                        + SURVIVES_OFFSET * self.vars.len()
                        + var_idx,
                    false,
                );
                let op_output = &mut self.op_outputs[*op_output_id as usize];
                let mut field_refs = None;
                for fr in &op_output.field_references {
                    if *fr < self.vars.len() as OpOutputIdx {
                        let frs = if let Some(ref mut frs) = field_refs {
                            frs
                        } else {
                            let frs = bb
                                .field_references
                                .entry(var_idx as VarId)
                                .or_default();
                            field_refs = Some(frs);
                            field_refs.as_mut().unwrap()
                        };
                        frs.push(*fr);
                    }
                }
                op_output.bound_vars_after_bb.push(var_idx as VarId);
            } else if *op_output_id != var_idx as VarId {
                bb.renames.insert(var_idx as VarId, *op_output_id as VarId);
            }
        }
    }
    fn compute_local_liveness(&mut self, sess: &Session) {
        for i in 0..self.basic_blocks.len() {
            self.compute_local_liveness_for_bb(sess, i);
        }
    }
    fn get_slot_group_var_data_bounds(
        &self,
        bb_id: BasicBlockId,
        slot_group_offset: usize,
    ) -> Range<usize> {
        let local_bits_per_bb = self.vars.len() * LOCAL_SLOTS_PER_BASIC_BLOCK;
        let start = self.basic_blocks[bb_id].var_data_start
            + slot_group_offset * self.vars.len();
        start..start + local_bits_per_bb
    }
    pub fn get_succession_var_data_bounds(
        &self,
        bb_id: BasicBlockId,
    ) -> Range<usize> {
        self.get_slot_group_var_data_bounds(bb_id, SUCCESSION_SLOTS_OFFSET)
    }
    pub fn get_global_var_data_bounds(
        &self,
        bb_id: BasicBlockId,
    ) -> Range<usize> {
        self.get_slot_group_var_data_bounds(bb_id, GLOBAL_SLOTS_OFFSET)
    }
    pub fn get_local_var_data_bounds(
        &self,
        bb_id: BasicBlockId,
    ) -> Range<usize> {
        self.get_slot_group_var_data_bounds(bb_id, LOCAL_SLOTS_OFFSET)
    }
    pub fn get_global_var_data(
        &self,
        bb_id: BasicBlockId,
    ) -> &BitSlice<Cell<usize>> {
        &self.var_data[self.get_global_var_data_bounds(bb_id)]
    }
    pub fn get_global_var_data_ored<'b>(
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
    pub fn kill_non_survivors(
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
    pub fn apply_alias(
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
    pub fn apply_bb_aliases(
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
    pub fn merge_calls_with_successors<'a>(
        &self,
        calls: &mut BitVec<Cell<usize>>,
        successors: &mut BitVec<Cell<usize>>,
        calls_iter: impl Iterator<Item = &'a BasicBlockId> + Clone,
        calls_empty: bool,
        successors_iter: impl Iterator<Item = &'a BasicBlockId>,
        successors_empty: bool,
    ) {
        if calls_empty {
            self.get_global_var_data_ored(calls, successors_iter);
        } else {
            self.get_global_var_data_ored(calls, calls_iter.clone());
            if !successors_empty {
                self.get_global_var_data_ored(successors, successors_iter);

                for call_bb_id in calls_iter {
                    let cbb = &self.basic_blocks[*call_bb_id];
                    self.apply_bb_aliases(calls, successors, cbb);
                }
                self.kill_non_survivors(successors, calls);
                *calls |= &*successors;
            }
        }
    }
    fn build_bb_succession_data(
        &self,
        bb: &BasicBlock,
        calls: &mut BitVec<Cell<usize>>,
        successors: &mut BitVec<Cell<usize>>,
    ) {
        self.merge_calls_with_successors(
            calls,
            successors,
            bb.calls.iter(),
            bb.calls.is_empty(),
            bb.successors.iter(),
            bb.successors.is_empty(),
        );
    }
    fn compute_global_liveness(&mut self) {
        let var_count = self.vars.len();
        let local_bits_per_bb = var_count * LOCAL_SLOTS_PER_BASIC_BLOCK;
        let mut successors = BitVec::<Cell<usize>>::new();
        let mut calls = BitVec::<Cell<usize>>::new();
        let mut global = BitVec::<Cell<usize>>::new();
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
                if !bb_local_range.is_empty() {
                    self.var_data.copy_within(
                        bb_local_range.clone(),
                        bb_global_range.start,
                    );
                    self.update_bb_predecessors(bb_id);
                }
                continue;
            }
            global.copy_from_bitslice(&self.var_data[bb_local_range.clone()]);
            self.build_bb_succession_data(bb, &mut calls, &mut successors);
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
    fn compute_bb_succession_data(&mut self) {
        let mut calls = BitVec::<Cell<usize>>::new();
        let mut successors = BitVec::<Cell<usize>>::new();
        let var_bits = LOCAL_SLOTS_PER_BASIC_BLOCK * self.vars.len();
        calls.resize(var_bits, false);
        successors.resize(var_bits, false);
        for bb in &self.basic_blocks {
            self.build_bb_succession_data(bb, &mut calls, &mut successors);
            self.var_data[bb.var_data_start + 2 * var_bits
                ..bb.var_data_start + 3 * var_bits]
                .copy_from_bitslice(&calls);
        }
    }
    fn compute_op_output_liveness(&mut self, sess: &Session) {
        let var_count = self.vars.len();
        let op_output_count = self.op_outputs.len();
        for bb_id in 0..self.basic_blocks.len() {
            let succ_var_data =
                &self.var_data[self.get_succession_var_data_bounds(bb_id)];
            let bb = &self.basic_blocks[bb_id];
            let chain = &sess.chains[bb.chain_id as usize];
            if bb.successors.is_empty() && bb.calls.is_empty() {
                continue;
            }
            for op_id in &chain.operators
                [bb.operators_start as usize..bb.operators_end as usize]
            {
                let op_ld = &self.operator_liveness_data[*op_id as usize];
                for op_output_id in op_ld.outputs_start..op_ld.outputs_start {
                    let oo = &self.op_outputs[op_output_id as usize];
                    for var_id in &oo.bound_vars_after_bb {
                        for i in REGULAR_FIELD_OFFSETS {
                            let oo_idx =
                                i * op_output_count + op_output_id as usize;
                            let v_idx = i * var_count + *var_id as usize;
                            let v = self.op_outputs_data[oo_idx]
                                || succ_var_data[v_idx];
                            self.op_outputs_data.set(oo_idx, v);
                        }
                    }
                }
            }
        }
    }
}

pub fn compute_liveness_data(sess: &mut Session) -> LivenessData {
    let mut ld = LivenessData::default();
    ld.setup_vars(sess);
    ld.setup_operator_outputs(sess);
    ld.setup_bbs(sess);
    ld.setup_bb_linkage_data(sess);
    ld.compute_local_liveness(sess);
    ld.compute_global_liveness();
    ld.compute_bb_succession_data();
    ld.compute_op_output_liveness(sess);
    ld
}
