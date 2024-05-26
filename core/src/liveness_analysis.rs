use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
    iter,
    ops::{Not, Range},
};

use bitvec::{slice::BitSlice, vec::BitVec};
use smallvec::SmallVec;

use crate::{
    chain::{Chain, ChainId},
    context::SessionData,
    operators::{
        call::OpCall,
        call_concurrent::OpCallConcurrent,
        foreach::OpForeach,
        fork::OpFork,
        forkcat::OpForkCat,
        operator::{OperatorData, OperatorId, OperatorOffsetInChain},
    },
    utils::{
        get_two_distinct_mut,
        identity_hasher::BuildIdentityHasher,
        string_store::{StringStore, StringStoreEntry},
    },
};

pub type BasicBlockId = usize;

pub const READS_OFFSET: usize = 0;
pub const NON_STRING_READS_OFFSET: usize = 1;
pub const HEADER_WRITES_OFFSET: usize = 2;

// all fields except survives
pub const REGULAR_FIELD_OFFSETS: [usize; 3] =
    [READS_OFFSET, NON_STRING_READS_OFFSET, HEADER_WRITES_OFFSET];

// survives is special because
// a) it does not make sense for output_ops, as they are valid across bbs
// b) it does not get affected by aliases (survives means the value bound to
// the var at the start of a bb remains bound to that same var after the bb)
pub const SURVIVES_OFFSET: usize = 3;

pub const SLOTS_PER_OP_OUTPUT: usize = 3; // output ops don't care about survives
pub const LOCAL_SLOTS_PER_BASIC_BLOCK: usize = 4;
pub const LOCAL_SLOTS_OFFSET: usize = 0;
pub const GLOBAL_SLOTS_OFFSET: usize = LOCAL_SLOTS_PER_BASIC_BLOCK;

/// 'Succession' combines the effect of callees and successors.
///
/// For blocks that *are* callees, it also include the effects of the
/// successors of the *callers*.
pub const SUCCESSION_SLOTS_OFFSET: usize = 2 * LOCAL_SLOTS_PER_BASIC_BLOCK;

// local slots + global slots + succession data
pub const SLOTS_PER_BASIC_BLOCK: usize = LOCAL_SLOTS_PER_BASIC_BLOCK * 3;

pub type VarId = u32;

// the order and number of special variables has to be consistent
// with the initialization in `LivenessData::setup_vars`
pub const BB_INPUT_VAR_ID: VarId = 0;
pub const VOID_VAR_ID: VarId = 1;
pub const ANY_VAR_ID: VarId = 2;
pub const DYN_VAR_ID: VarId = 3;
pub const SPECIAL_VAR_COUNT: VarId = 4;

pub type OpOutputIdx = u32;
pub const BB_INPUT_VAR_OUTPUT_IDX: OpOutputIdx = BB_INPUT_VAR_ID;
pub const VOID_VAR_OUTPUT_IDX: OpOutputIdx = VOID_VAR_ID;

pub struct BasicBlock {
    pub chain_id: ChainId,
    pub operators_start: OperatorOffsetInChain,
    pub operators_end: OperatorOffsetInChain,
    pub calls: SmallVec<[BasicBlockId; 2]>,
    pub successors: SmallVec<[BasicBlockId; 2]>,
    // if this block is a callee, this contains the bb ids of all blocks
    // that any caller of this block has as successors
    // this is used to calculate 'succession' var data, which is in turn
    // used to calculate the liveness of operator outputs created by this
    // block
    pub caller_successors: SmallVec<[BasicBlockId; 2]>,
    // used to propagate changes during global liveness analysis
    pub predecessors: SmallVec<[BasicBlockId; 2]>,
    // Vars available at the start that may be accessed through
    // data behind vars available at the end that contain field references.
    // All field references are flattened out, so there's no need to recurse.
    pub field_references: HashMap<VarId, SmallVec<[VarId; 4]>>,

    // var name after the bb -> original var name
    // if the 'original name' was locally generated, there is no entry
    pub key_aliases: HashMap<VarId, VarId, BuildIdentityHasher>,

    updates_required: bool,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Var {
    BBInput,
    VoidVar,
    // any var of a dynamically generated name (e.g. explode)
    DynVar,
    Named(StringStoreEntry),
}
#[derive(Clone)]
pub struct OpOutput {
    // after liveness analysis concludes, this contains the vars that
    // are bound to the output at the end of the BB that contains it's
    // originating operator. During (local) liveness analysis, it contains
    // the vars bound to the output at the current point of 'execution'
    pub bound_vars: SmallVec<[VarId; 4]>,
    // other outputs referenced in the data of this output, e.g. for regex
    // this is not flattened out, so recursion is required to get all refs
    pub field_references: SmallVec<[OpOutputIdx; 4]>,
    pub producing_op: Option<OperatorId>,
}

// counts the number of different fields that an operator accesses *directly*
// (directly meaning not through following field references)
// it is mainly used by the can_consume_nth_access method where the operator
// can check after liveness analysis whether he can steal the data from the
// field that he accessed nth during liveness analysis
type DirectOperatorAccessIndex = u32;

#[derive(Clone)]
pub struct OutputAcccess {
    header_write: bool,
    non_stringified: bool,
    direct_access: bool,
    direct_access_index: DirectOperatorAccessIndex,
}

#[derive(Clone, Default)]
pub struct OperatorLivenessData {
    pub basic_block_id: BasicBlockId,
    pub direct_access_count: DirectOperatorAccessIndex,
    pub accessed_vars: SmallVec<[VarId; 4]>,
    // TODO: this is not populated correctly yet
    pub killed_vars: SmallVec<[VarId; 4]>,
    pub accessed_outputs: HashMap<OpOutputIdx, OutputAcccess>,
    pub killed_outputs: SmallVec<[OpOutputIdx; 4]>,
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
    pub key_aliases_map: HashMap<VarId, OpOutputIdx, BuildIdentityHasher>,
    pub operator_liveness_data: Vec<OperatorLivenessData>,
    updates_required: Vec<BasicBlockId>,
}

pub struct AccessFlags {
    pub input_accessed: bool,
    pub non_stringified_input_access: bool,
    pub may_dup_or_drop: bool,
}

impl AccessFlags {
    pub fn or(&self, other: &Self) -> Self {
        Self {
            input_accessed: self.input_accessed | other.input_accessed,
            non_stringified_input_access: self.non_stringified_input_access
                | other.non_stringified_input_access,
            may_dup_or_drop: self.may_dup_or_drop | other.may_dup_or_drop,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorCallEffect {
    Basic,   // normal operators
    NoCall,  // 'meta' operators like key or select
    Diverge, // diverging operators like call
}

impl Var {
    pub fn get_name(&self) -> Option<StringStoreEntry> {
        match self {
            Var::Named(name) => Some(*name),
            _ => None,
        }
    }
    pub fn debug_name<'a>(&self, string_store: &'a StringStore) -> &'a str {
        match self {
            Var::Named(n) => string_store.lookup(*n),
            Var::BBInput => "<bb input>",
            Var::VoidVar => "<void>",
            Var::DynVar => "<dyn>",
        }
    }
}

impl LivenessData {
    pub fn setup_operator_outputs(&mut self, sess: &mut SessionData) {
        self.operator_liveness_data.extend(
            iter::repeat(OperatorLivenessData::default())
                .take(sess.operator_data.len()),
        );
        let mut total_outputs_count = self.vars.len();
        self.op_outputs.extend(
            iter::repeat(OpOutput {
                bound_vars: SmallVec::new(),
                field_references: SmallVec::new(),
                producing_op: None,
            })
            .take(self.vars.len()),
        );
        for op_id in 0..sess.operator_data.len() {
            let op_output_count = sess.operator_data[op_id]
                .output_count(sess, op_id as OperatorId);
            let op_base = &mut sess.operator_bases[op_id];
            if op_output_count == 0 {
                op_base.transparent_mode = true;
            }

            op_base.outputs_start = total_outputs_count as OpOutputIdx;
            total_outputs_count += op_output_count;
            op_base.outputs_end = total_outputs_count as OpOutputIdx;
            self.op_outputs.extend(
                iter::repeat(OpOutput {
                    bound_vars: SmallVec::new(),
                    field_references: SmallVec::new(),
                    producing_op: Some(op_id as OperatorId),
                })
                .take(op_output_count),
            );
        }
        debug_assert!(self.op_outputs.len() == total_outputs_count);
        self.vars_to_op_outputs_map
            .extend(0..self.vars.len() as VarId);

        self.op_outputs_data
            .resize(total_outputs_count * SLOTS_PER_OP_OUTPUT, false);
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
    pub fn setup_op_vars(&mut self, sess: &SessionData, op_id: OperatorId) {
        self.add_var_name_opt(sess.operator_bases[op_id as usize].label);
        sess.operator_data[op_id as usize]
            .register_output_var_names(self, sess, op_id);
    }
    pub fn setup_vars(&mut self, sess: &SessionData) {
        // the order of these special vars has to match the IDs specified
        // at the top of this file for BB_INPUT_VAR_ID etc.
        self.vars.push(Var::BBInput);
        self.vars.push(Var::VoidVar);
        self.vars.push(Var::DynVar);
        for c in &sess.chains {
            for &op_id in &c.operators {
                self.setup_op_vars(sess, op_id);
            }
        }
    }
    fn split_bb_at_call(
        &mut self,
        bb_id: BasicBlockId,
        op_n: OperatorOffsetInChain,
    ) {
        let curr_bb_count = self.basic_blocks.len();
        let bb = &mut self.basic_blocks[bb_id];
        let end = bb.operators_end;
        let chain_id = bb.chain_id;
        bb.operators_end = op_n + 1;
        if op_n + 1 != end {
            bb.operators_end = op_n;
            bb.successors.push(curr_bb_count);
            self.basic_blocks.push(BasicBlock {
                chain_id,
                operators_start: op_n + 1,
                operators_end: end,
                calls: SmallVec::new(),
                successors: SmallVec::new(),
                caller_successors: SmallVec::new(),
                updates_required: true,
                field_references: HashMap::new(),
                predecessors: SmallVec::new(),
                key_aliases: HashMap::default(),
            });
            self.updates_required.push(bb_id);
        }
    }
    // returns true if the op ends the block
    fn update_bb_for_op(
        &mut self,
        sess: &SessionData,
        op_id: OperatorId,
        op_n: OperatorOffsetInChain,
        cn: &Chain,
        bb_id: BasicBlockId,
    ) -> bool {
        let bb = &mut self.basic_blocks[bb_id];
        match &sess.operator_data[op_id as usize] {
            OperatorData::CallConcurrent(OpCallConcurrent {
                target_resolved,
                ..
            })
            | OperatorData::Call(OpCall {
                target_resolved, ..
            }) => {
                bb.calls.push(target_resolved.unwrap() as BasicBlockId);
                self.split_bb_at_call(bb_id, op_n);
                return true;
            }
            OperatorData::Fork(OpFork {
                subchains_start,
                subchains_end,
                ..
            }) => {
                for sc in &cn.subchains
                    [*subchains_start as usize..*subchains_end as usize]
                {
                    bb.successors.push(*sc as BasicBlockId);
                }
                return true;
            }
            OperatorData::ForkCat(OpForkCat {
                subchains_start,
                subchains_end,
                ..
            }) => {
                for sc in &cn.subchains
                    [*subchains_start as usize..*subchains_end as usize]
                {
                    bb.calls.push(*sc as BasicBlockId);
                }
                self.split_bb_at_call(bb_id, op_n);
                return true;
            }
            OperatorData::Foreach(OpForeach {
                subchains_start, ..
            }) => {
                bb.calls.push(
                    cn.subchains[*subchains_start as usize] as BasicBlockId,
                );
                self.split_bb_at_call(bb_id, op_n);
            }
            OperatorData::Next(_) | OperatorData::End(_) => unreachable!(),
            OperatorData::ToStr(_)
            | OperatorData::Nop(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Count(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::Sequence(_) => (),
            OperatorData::Custom(_) | OperatorData::MultiOp(_) => {
                // TODO: maybe support this
            }
            OperatorData::Aggregator(agg) => {
                for &sub_op in &agg.sub_ops {
                    self.update_bb_for_op(sess, sub_op, op_n, cn, bb_id);
                }
            }
        };
        false
    }
    fn setup_bbs(&mut self, sess: &SessionData) {
        let var_count = self.vars.len();
        let bits_per_bb = var_count * SLOTS_PER_BASIC_BLOCK;
        for (i, c) in sess.chains.iter().enumerate() {
            self.basic_blocks.push(BasicBlock {
                chain_id: i as ChainId,
                operators_start: 0,
                operators_end: c.operators.len() as u32,
                calls: SmallVec::new(),
                successors: SmallVec::new(),
                caller_successors: SmallVec::new(),
                updates_required: true,
                field_references: HashMap::new(),
                predecessors: SmallVec::new(),
                key_aliases: HashMap::default(),
            });
        }
        self.updates_required.extend(0..sess.chains.len());
        while let Some(bb_id) = self.updates_required.pop() {
            let bb = &mut self.basic_blocks[bb_id];
            let cn = &sess.chains[bb.chain_id as usize];
            for (op_n, &op) in cn.operators
                [bb.operators_start as usize..bb.operators_end as usize]
                .iter()
                .enumerate()
            {
                let op_n = op_n as OperatorOffsetInChain;
                if self.update_bb_for_op(sess, op, op_n, cn, bb_id) {
                    break;
                }
            }
        }
        self.var_data
            .resize(bits_per_bb * self.basic_blocks.len(), false);
    }
    fn setup_bb_linkage_data(&mut self, sess: &SessionData) {
        for bb_id in 0..self.basic_blocks.len() {
            for succ_n in 0..self.basic_blocks[bb_id].successors.len() {
                let succ_id = self.basic_blocks[bb_id].successors[succ_n];
                self.basic_blocks[succ_id].predecessors.push(bb_id);
            }
            for callee_n in 0..self.basic_blocks[bb_id].calls.len() {
                let callee_id = self.basic_blocks[bb_id].calls[callee_n];
                let (bb, callee) = get_two_distinct_mut(
                    &mut self.basic_blocks,
                    bb_id,
                    callee_id,
                );
                callee.caller_successors.extend_from_slice(&bb.successors);
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
    pub fn access_var(
        &mut self,
        sess: &SessionData,
        op_id: OperatorId,
        var_id: VarId,
        op_offset_after_last_write: OperatorOffsetInChain,
        non_stringified: bool,
    ) {
        self.operator_liveness_data[op_id as usize]
            .accessed_vars
            .push(var_id);
        let output_id = self.vars_to_op_outputs_map[var_id as usize];
        self.access_output(
            sess,
            op_id,
            output_id,
            op_offset_after_last_write,
            non_stringified,
            true,
        );
    }
    pub fn access_output(
        &mut self,
        sess: &SessionData,
        op_id: OperatorId,
        op_output_idx: OpOutputIdx,
        op_offset_after_last_write: OperatorOffsetInChain,
        non_stringified: bool,
        direct_access: bool,
    ) {
        if op_output_idx == VOID_VAR_OUTPUT_IDX {
            // we don't want this field to be forwarded by fork and friends
            // so we just pretend it is never accessed.
            return;
        }
        let oo_idx = op_output_idx as usize;
        let ooc = self.op_outputs.len();
        let ld = &mut self.operator_liveness_data[op_id as usize];

        let header_write = if let Some(oo_producing_op) =
            self.op_outputs[oo_idx].producing_op
        {
            sess.operator_bases[oo_producing_op as usize].offset_in_chain + 1
                < op_offset_after_last_write
        } else {
            op_offset_after_last_write != 0
        };
        match ld.accessed_outputs.entry(op_output_idx) {
            Entry::Occupied(mut e) => {
                let acc = e.get_mut();
                if direct_access && !acc.direct_access {
                    acc.direct_access = true;
                    acc.direct_access_index = ld.direct_access_count;
                }
                acc.header_write |= header_write;
                acc.non_stringified |= non_stringified;
            }
            Entry::Vacant(e) => {
                e.insert(OutputAcccess {
                    header_write,
                    non_stringified,
                    direct_access,
                    direct_access_index: ld.direct_access_count,
                });
            }
        }
        ld.direct_access_count +=
            DirectOperatorAccessIndex::from(direct_access);
        self.op_outputs_data
            .set_aliased(READS_OFFSET * ooc + oo_idx, true);
        if non_stringified {
            self.op_outputs_data
                .set_aliased(NON_STRING_READS_OFFSET * ooc + oo_idx, true);
        }
        if header_write {
            self.op_outputs_data
                .set_aliased(HEADER_WRITES_OFFSET * ooc + oo_idx, true);
        }
        for fri in 0..self.op_outputs[oo_idx].field_references.len() {
            self.access_output(
                sess,
                op_id,
                self.op_outputs[oo_idx].field_references[fri],
                op_offset_after_last_write,
                non_stringified,
                false,
            );
        }
    }
    fn reset_op_outputs_data_for_vars(&mut self) {
        let vc = self.vars.len();
        let ooc = self.op_outputs.len();
        for i in &REGULAR_FIELD_OFFSETS {
            self.op_outputs_data[i * ooc..i * ooc + vc].fill(false);
        }
        for i in 0..vc {
            // initially each var points to it's input
            self.vars_to_op_outputs_map[i] = i as OpOutputIdx;
            self.op_outputs[i].field_references.clear();
        }
    }
    pub fn apply_var_remapping(&self, var_id: VarId, target: OpOutputIdx) {
        let offset = NON_STRING_READS_OFFSET * self.op_outputs.len();
        let tgt_idx = offset + target as usize;
        let src_idx = offset + var_id as usize;
        self.op_outputs_data.set_aliased(
            tgt_idx,
            self.op_outputs_data[tgt_idx] || self.op_outputs_data[src_idx],
        );
    }
    fn compute_local_liveness_for_bb(
        &mut self,
        sess: &SessionData,
        bb_id: BasicBlockId,
    ) {
        self.key_aliases_map.clear();
        self.reset_op_outputs_data_for_vars();
        let mut bb = &mut self.basic_blocks[bb_id];
        let cn = &sess.chains[bb.chain_id as usize];
        let mut input_field = BB_INPUT_VAR_OUTPUT_IDX;
        let mut op_offset_after_last_write: OperatorOffsetInChain = 0;
        for op_n in bb.operators_start..bb.operators_end {
            let op_id = cn.operators[op_n as usize];
            let op_idx = op_id as usize;
            let op_base = &sess.operator_bases[op_idx];
            let mut flags = AccessFlags {
                input_accessed: true,
                non_stringified_input_access: true,
                may_dup_or_drop: true,
            };
            let (output_field, ce) = sess.operator_data[op_idx]
                .update_liveness_for_op(
                    sess,
                    self,
                    &mut flags,
                    op_offset_after_last_write,
                    op_id,
                    bb_id,
                    input_field,
                    0,
                );
            match ce {
                OperatorCallEffect::Basic => (),
                OperatorCallEffect::NoCall => {
                    input_field = output_field;
                    continue;
                }
                OperatorCallEffect::Diverge => break,
            }
            if flags.input_accessed {
                self.access_var(
                    sess,
                    op_id,
                    BB_INPUT_VAR_ID,
                    op_offset_after_last_write,
                    flags.non_stringified_input_access,
                );
            }
            if flags.may_dup_or_drop {
                op_offset_after_last_write = op_n + 1;
            }
            if let Some(label) = sess.operator_bases[op_idx].label {
                let var_id = self.var_names[&label];
                self.vars_to_op_outputs_map[var_id as usize] =
                    sess.operator_bases[op_idx].outputs_start;
            }
            if !op_base.transparent_mode {
                input_field = output_field;
                self.vars_to_op_outputs_map[BB_INPUT_VAR_ID as usize] =
                    sess.operator_bases[op_idx].outputs_start;
            }
        }
        let vc = self.vars.len();
        let ooc = self.op_outputs.len();
        let var_data_start = bb_id * SLOTS_PER_BASIC_BLOCK * vc;
        if op_offset_after_last_write != 0 {
            // TODO: maybe do something more sophisticated than this
            // for now we just assume that any op_output that is bound
            // after the bb might be accessed by someone and therefore needs
            // header writes applied
            for &op_output_id in &self.vars_to_op_outputs_map {
                if let Some(producing_op) =
                    self.op_outputs[op_output_id as usize].producing_op
                {
                    if producing_op >= op_offset_after_last_write - 1 {
                        continue;
                    }
                }
                self.op_outputs_data.set_aliased(
                    HEADER_WRITES_OFFSET * ooc + op_output_id as usize,
                    true,
                );
            }
        }
        for i in REGULAR_FIELD_OFFSETS {
            self.var_data
                [var_data_start + i * vc..var_data_start + (i + 1) * vc]
                .copy_from_bitslice(
                    &self.op_outputs_data[i * ooc..i * ooc + vc],
                );
        }
        self.var_data[var_data_start + SURVIVES_OFFSET * vc
            ..var_data_start + (SURVIVES_OFFSET + 1) * vc]
            .fill(true);
        for var_idx in 0..self.vars_to_op_outputs_map.len() {
            let op_output_id = self.vars_to_op_outputs_map[var_idx];
            if op_output_id >= self.vars.len() as OpOutputIdx {
                self.var_data.set_aliased(
                    var_data_start
                        + SURVIVES_OFFSET * self.vars.len()
                        + var_idx,
                    false,
                );
                self.insert_var_field_references(op_output_id, bb_id, var_idx);
                self.op_outputs[op_output_id as usize]
                    .bound_vars
                    .push(var_idx as VarId);
            } else {
                debug_assert!(op_output_id == var_idx as VarId);
            }
        }
        for (&var_id, &op_output) in &self.key_aliases_map {
            self.apply_var_remapping(var_id, op_output);
            bb = &mut self.basic_blocks[bb_id];
            if op_output < self.vars.len() as OpOutputIdx {
                bb.key_aliases.insert(op_output as VarId, var_id);
            } else {
                for &bv in &self.op_outputs[op_output as usize].bound_vars {
                    bb.key_aliases.insert(bv, op_output as VarId);
                }
            }
        }
    }

    fn insert_var_field_references(
        &mut self,
        op_output_idx: OpOutputIdx,
        bb_id: BasicBlockId,
        var_idx: usize,
    ) {
        debug_assert!(op_output_idx as usize >= self.vars.len());
        for fr_i in 0..self.op_outputs[op_output_idx as usize]
            .field_references
            .len()
        {
            let fr =
                self.op_outputs[op_output_idx as usize].field_references[fr_i];
            if fr < self.vars.len() as OpOutputIdx {
                self.basic_blocks[bb_id]
                    .field_references
                    .entry(var_idx as VarId)
                    .or_default()
                    .push(fr);
            } else {
                self.insert_var_field_references(fr, bb_id, var_idx);
            }
        }
    }
    fn compute_local_liveness(&mut self, sess: &SessionData) {
        for i in 0..self.basic_blocks.len() {
            self.compute_local_liveness_for_bb(sess, i);
        }
    }
    pub fn get_slot_group_var_data_bounds(
        &self,
        bb_id: BasicBlockId,
        slot_group_offset: usize,
    ) -> Range<usize> {
        let vc = self.vars.len();
        let start =
            bb_id * SLOTS_PER_BASIC_BLOCK * vc + slot_group_offset * vc;
        start..start + vc * LOCAL_SLOTS_PER_BASIC_BLOCK
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
    pub fn get_var_slot_range(&self, slot_offset: usize) -> Range<usize> {
        let vc = self.vars.len();
        vc * slot_offset..vc * (slot_offset + 1)
    }
    pub fn get_var_data_field(
        &self,
        bb_id: BasicBlockId,
        slot_group_offset: usize,
        slot_offset: usize,
    ) -> &BitSlice<Cell<usize>> {
        &self.var_data
            [self.get_slot_group_var_data_bounds(bb_id, slot_group_offset)]
            [self.get_var_slot_range(slot_offset)]
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
        survivors: &BitSlice<Cell<usize>>,
        tgt: &mut BitSlice<Cell<usize>>,
    ) {
        let var_count = self.vars.len();
        let survivors_start = SURVIVES_OFFSET * var_count;
        let survivors_slice =
            &survivors[survivors_start..survivors_start + var_count];
        for i in &REGULAR_FIELD_OFFSETS {
            // the default field
            let start = var_count * i;
            tgt[start..start + var_count] &= survivors_slice;
        }
    }
    pub fn apply_alias(
        &self,
        bb_data: &BitSlice<Cell<usize>>,
        succ_data: &BitSlice<Cell<usize>>,
        original_var: VarId,
        alias_var: VarId, // var that after the bb refers to the original var
    ) {
        let var_count = self.vars.len();
        for i in REGULAR_FIELD_OFFSETS {
            let tgt_idx = i * var_count + original_var as usize;
            let src_idx = i * var_count + alias_var as usize;
            bb_data
                .set_aliased(tgt_idx, bb_data[tgt_idx] || succ_data[src_idx]);
        }
    }
    pub fn apply_bb_aliases(
        &self,
        bb_data: &BitSlice<Cell<usize>>,
        successor_data: &BitSlice<Cell<usize>>,
        bb: &BasicBlock,
    ) {
        for (&alias_var, &original_var) in &bb.key_aliases {
            self.apply_alias(bb_data, successor_data, original_var, alias_var);
        }
        for (alias_var, field_refs) in &bb.field_references {
            for original_var in field_refs {
                self.apply_alias(
                    bb_data,
                    successor_data,
                    *original_var,
                    *alias_var,
                );
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
                self.kill_non_survivors(calls, successors);
                *calls |= &*successors;
            }
        }
    }
    // 'continuation' data describes the effects of calls and successors
    // unlike 'succession' it does not include the effects of successors of
    // callers in case the bb is a callee
    fn build_bb_continuation_data(
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
                self.var_data.copy_within(
                    bb_local_range.clone(),
                    bb_global_range.start,
                );
                self.update_bb_predecessors(bb_id);
                continue;
            }
            global.copy_from_bitslice(&self.var_data[bb_local_range.clone()]);
            self.build_bb_continuation_data(bb, &mut calls, &mut successors);
            self.apply_bb_aliases(&calls, &global, bb);
            self.kill_non_survivors(&global, &mut calls);
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
        let vc = self.vars.len();
        let var_bits = LOCAL_SLOTS_PER_BASIC_BLOCK * vc;
        calls.resize(var_bits, false);
        successors.resize(var_bits, false);
        for (bb_id, bb) in self.basic_blocks.iter().enumerate() {
            self.merge_calls_with_successors(
                &mut successors,
                &mut calls,
                bb.successors.iter(),
                bb.successors.is_empty(),
                bb.caller_successors.iter(),
                bb.caller_successors.is_empty(),
            );
            let vd_range = self.get_succession_var_data_bounds(bb_id);
            if bb.calls.is_empty() {
                self.var_data[vd_range].copy_from_bitslice(&successors);
                continue;
            }
            self.get_global_var_data_ored(&mut calls, bb.calls.iter());

            for &call_bb_id in &bb.calls {
                let cbb = &self.basic_blocks[call_bb_id];
                self.apply_bb_aliases(&calls, &successors, cbb);
            }
            self.kill_non_survivors(&calls, &mut successors);
            *calls |= &*successors;
            self.var_data[vd_range].copy_from_bitslice(&calls);
        }
    }
    fn compute_op_output_liveness(&mut self, sess: &SessionData) {
        let var_count = self.vars.len();
        let op_output_count = self.op_outputs.len();
        for bb_id in 0..self.basic_blocks.len() {
            let succ_var_data =
                &self.var_data[self.get_succession_var_data_bounds(bb_id)];
            let bb = &self.basic_blocks[bb_id];
            let chain = &sess.chains[bb.chain_id as usize];
            if bb.successors.is_empty()
                && bb.calls.is_empty()
                && bb.caller_successors.is_empty()
            {
                continue;
            }
            for op_id in &chain.operators
                [bb.operators_start as usize..bb.operators_end as usize]
            {
                let op_base = &sess.operator_bases[*op_id as usize];
                for op_output_id in op_base.outputs_start..op_base.outputs_end
                {
                    let oo = &self.op_outputs[op_output_id as usize];
                    for var_id in &oo.bound_vars {
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
    fn compute_operator_kills(&mut self, sess: &SessionData) {
        let var_count = self.vars.len();
        let output_count = self.op_outputs.len();
        let mut live_outputs = BitVec::<Cell<usize>>::new();
        live_outputs.resize(output_count, false);
        for bb_id in 0..self.basic_blocks.len() {
            let succession_reads = &self.var_data
                [self.get_succession_var_data_bounds(bb_id)]
                [READS_OFFSET * var_count..(READS_OFFSET + 1) * var_count];
            let local_survives = &self.var_data
                [self.get_local_var_data_bounds(bb_id)]
                [SURVIVES_OFFSET * var_count
                    ..(SURVIVES_OFFSET + 1) * var_count];
            let mut vars = &mut live_outputs[0..var_count];
            vars.copy_from_bitslice(local_survives);
            vars = vars.not();
            *vars |= succession_reads;

            let bb = &self.basic_blocks[bb_id];

            let chain = &sess.chains[bb.chain_id as usize];
            let operators = &chain.operators
                [bb.operators_start as usize..bb.operators_end as usize];
            for &op_id in operators {
                let op_base = &sess.operator_bases[op_id as usize];
                for output_id in op_base.outputs_start..op_base.outputs_end {
                    let mut output =
                        live_outputs.get_mut(output_id as usize).unwrap();
                    for &v_id in
                        &self.op_outputs[output_id as usize].bound_vars
                    {
                        *output |= succession_reads[v_id as usize];
                    }
                }
            }
            for op_id in operators {
                let op_idx = *op_id as usize;
                let op_ld = &mut self.operator_liveness_data[op_idx];
                for &output_idx in op_ld.accessed_outputs.keys() {
                    if !live_outputs[output_idx as usize] {
                        op_ld.killed_outputs.push(output_idx);
                        live_outputs.set(output_idx as usize, true);
                    }
                }
                op_ld.killed_outputs.sort_unstable();
            }
        }
    }
    pub fn log_liveness_data(&mut self, sess: &SessionData) {
        fn print_bits(
            label: &str,
            padding: usize,
            count: usize,
            offset: usize,
            livness_data: &BitSlice<Cell<usize>>,
        ) {
            eprint!("{label:>padding$}: ");
            for i in offset..offset + count {
                eprint!(" {} ", if livness_data[i] { "X" } else { "-" });
            }
            eprintln!();
        }
        let string_store = sess.string_store.read().unwrap();
        eprintln!("{:-^80}", " <liveness analysis> ");
        eprintln!("chains:");
        for (bb_id, c) in sess.chains.iter().enumerate() {
            eprint!("chain {bb_id:02}");
            if let Some(l) = c.label {
                eprint!(" '{}'", string_store.lookup(l));
            }
            eprint!(": ");
            for &op_id in &c.operators {
                eprint!(
                    "(op {op_id} `{}`) ",
                    sess.operator_data[op_id as usize].default_op_name()
                );
            }
            eprintln!();
        }
        eprintln!();
        eprintln!("bbs:");
        for (bb_id, bb) in self.basic_blocks.iter().enumerate() {
            eprint!("bb {bb_id:02}: ");
            for op_n in bb.operators_start..bb.operators_end {
                let op_id =
                    sess.chains[bb.chain_id as usize].operators[op_n as usize];
                eprint!(
                    "(op {op_id} `{}`) ",
                    sess.operator_data[op_id as usize].debug_op_name()
                );
            }
            if !bb.calls.is_empty() {
                eprint!("[calls: ");
                for c in &bb.calls {
                    eprint!("{c:02},");
                }
                eprint!("] ");
            }
            if !bb.predecessors.is_empty() {
                eprint!("{{predecessors: ");
                for s in &bb.predecessors {
                    eprint!("{s:02},");
                }
                eprint!("}}");
            }
            if !bb.successors.is_empty() {
                eprint!("{{successors: ");
                for s in &bb.successors {
                    eprint!("{s:02},");
                }
                eprint!("}}");
            }
            if !bb.caller_successors.is_empty() {
                eprint!("{{caller successors: ");
                for s in &bb.caller_successors {
                    eprint!("{s:02},");
                }
                eprint!("}}");
            }
            eprintln!();
        }
        eprintln!();
        eprintln!("vars:");
        for (v_id, v) in self.vars.iter().enumerate() {
            eprintln!("var {v_id:02}: {}", v.debug_name(&string_store));
        }
        eprintln!();
        eprintln!("operators:");
        eprintln!("# I: only through field refs");
        eprintln!("# W: headers modified");
        eprintln!("# S: stringified only");
        eprintln!();
        let flag = |c: bool, t: char| if c { t } else { '-' };
        for (op_id, old) in self.operator_liveness_data.iter().enumerate() {
            eprint!(
                "op {op_id:02} ({}):",
                sess.operator_data[op_id].debug_op_name(),
            );
            if !old.accessed_outputs.is_empty() {
                eprint!(" [acc:");
                for (&idx, acc) in &old.accessed_outputs {
                    eprint!(
                        " {idx}::{}{}{}",
                        flag(!acc.direct_access, 'I'),
                        flag(acc.header_write, 'W'),
                        flag(!acc.non_stringified, 'S')
                    )
                }
                eprint!("]");
            }
            if !old.killed_outputs.is_empty() {
                eprint!(" (kills:");
                for idx in &old.killed_outputs {
                    eprint!(" {idx}")
                }
                eprint!(")");
            }
            eprintln!();
        }
        eprintln!();
        eprintln!("op_outputs:");
        for (i, v) in self.vars.iter().enumerate() {
            eprintln!("op_output {i:02}: {}", v.debug_name(&string_store));
        }
        for (op_id, op_base) in sess.operator_bases.iter().enumerate() {
            for oo_n in op_base.outputs_start..op_base.outputs_end {
                eprint!(
                    "op_output {oo_n:02}: chain {:02}, bb {:02}, op_id {op_id:02} `{}`",
                    op_base.chain_id.map_or(-1, i64::from),
                    self.operator_liveness_data[op_id].basic_block_id,
                    sess.operator_data[op_id].default_op_name()
                );
                let oo = &self.op_outputs[oo_n as usize];
                if !oo.bound_vars.is_empty() {
                    eprint!(" (bound vars: ");
                    for &v_id in &oo.bound_vars {
                        eprint!(
                            "{} ",
                            self.vars[v_id as usize].debug_name(&string_store)
                        );
                    }
                    eprint!(")");
                }
                if !oo.field_references.is_empty() {
                    eprint!(" (field refs: ");
                    for &fr in &oo.field_references {
                        eprint!("{fr} ");
                    }
                    eprint!(")");
                }
                eprintln!();
            }
        }
        eprintln!();
        let vc = self.vars.len();
        let ooc = self.op_outputs.len();
        const PADDING_OP_OUTPUTS: usize = 16;
        eprint!("{:>PADDING_OP_OUTPUTS$}: ", "op_output id");
        for oo_n in 0..ooc {
            eprint!("{oo_n:02} ");
        }
        eprintln!();
        for (name, offs) in [
            ("reads", READS_OFFSET),
            ("non string reads", NON_STRING_READS_OFFSET),
            ("header writes", HEADER_WRITES_OFFSET),
        ] {
            print_bits(
                name,
                PADDING_OP_OUTPUTS,
                ooc,
                offs * ooc,
                &self.op_outputs_data,
            );
        }
        const PADDING_VARS: usize = 32;
        eprint!("\n{:>PADDING_VARS$}: ", "var id");

        for vid in 0..vc {
            eprint!("{vid:02} ");
        }
        eprintln!();
        let vars_print_len = 3 * vc + 1;
        for bb_id in 0..self.basic_blocks.len() {
            eprintln!("{:->PADDING_VARS$}{:-^vars_print_len$}", "", "");
            eprintln!("bb {bb_id:02}:");
            let vars_start = bb_id * SLOTS_PER_BASIC_BLOCK * vc;
            for (category_offs, category) in
                ["local", "global", "succession"].iter().enumerate()
            {
                if category_offs != 0 {
                    eprintln!();
                }
                for (name, offs) in [
                    ("reads", READS_OFFSET),
                    ("non str reads", NON_STRING_READS_OFFSET),
                    ("header writes", HEADER_WRITES_OFFSET),
                    ("survives", SURVIVES_OFFSET),
                ] {
                    print_bits(
                        &format!("{category} {name} bb {bb_id:02}"),
                        PADDING_VARS,
                        vc,
                        vars_start
                            + ((category_offs * LOCAL_SLOTS_PER_BASIC_BLOCK)
                                + offs)
                                * vc,
                        &self.var_data,
                    );
                }
            }
            let bb = &self.basic_blocks[bb_id];

            let r: [(&str, &mut dyn Iterator<Item = (&VarId, &[VarId])>); 2] = [
                (
                    "key aliases",
                    &mut bb
                        .key_aliases
                        .iter()
                        .map(|(src, tgt)| (tgt, std::slice::from_ref(src))),
                ),
                (
                    "field refs",
                    &mut bb
                        .field_references
                        .iter()
                        .map(|(tgt, srcs)| (tgt, srcs.as_slice())),
                ),
            ];
            for (name, elements) in r {
                let mut elements = elements.peekable();
                if elements.peek().is_none() {
                    continue;
                }
                eprint!("\n{name}: ");
                for (&tgt, srcs) in elements {
                    for &src in srcs {
                        eprint!(
                            "[{} <- {}] ",
                            self.vars[src as usize].debug_name(&string_store),
                            self.vars[tgt as usize].debug_name(&string_store),
                        )
                    }
                }
            }

            eprintln!();
        }
        eprintln!("{:-^80}", " </liveness analysis> ");
    }
    pub fn can_consume_output(
        &self,
        op_id: OperatorId,
        output_idx: OpOutputIdx,
    ) -> bool {
        self.operator_liveness_data[op_id as usize]
            .killed_outputs
            .contains(&output_idx)
    }
    pub fn can_consume_nth_access(
        &self,
        op_id: OperatorId,
        n: DirectOperatorAccessIndex,
    ) -> bool {
        let old = &self.operator_liveness_data[op_id as usize];
        for (idx, acc) in &old.accessed_outputs {
            if acc.direct_access_index == n {
                return old.killed_outputs.binary_search(idx).ok().is_some();
            }
        }
        false
    }
    pub fn accessed_names_afterwards(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> BitVec<usize> {
        let bb_id = self.operator_liveness_data[op_id as usize].basic_block_id;
        let bb = &self.basic_blocks[bb_id];
        let chain = &sess.chains[bb.chain_id as usize];
        let vc = self.vars.len();
        let succ_range = self.get_succession_var_data_bounds(bb_id);
        let mut reads = BitVec::<usize>::new();
        reads.extend_from_bitslice(
            &self.var_data[succ_range]
                [vc * READS_OFFSET..vc * (READS_OFFSET + 1)],
        );
        for bb_op_offset in (bb.operators_start..bb.operators_end).rev() {
            let bb_op_id = chain.operators[bb_op_offset as usize];
            if op_id == bb_op_id {
                break;
            }
            for &var_id in
                &self.operator_liveness_data[bb_op_id as usize].killed_vars
            {
                reads.set(var_id as usize, false);
            }
            for &var_id in
                &self.operator_liveness_data[bb_op_id as usize].accessed_vars
            {
                reads.set(var_id as usize, true);
            }
        }
        reads
    }
}

pub fn compute_liveness_data(sess: &mut SessionData) -> LivenessData {
    let mut ld = LivenessData::default();
    ld.setup_vars(sess);
    ld.setup_operator_outputs(sess);
    ld.setup_bbs(sess);
    ld.setup_bb_linkage_data(sess);
    ld.compute_local_liveness(sess);
    ld.compute_global_liveness();
    ld.compute_bb_succession_data();
    ld.compute_op_output_liveness(sess);
    ld.compute_operator_kills(sess);
    #[cfg(feature = "liveness_analysis_logging")]
    ld.log_liveness_data(sess);
    ld
}
