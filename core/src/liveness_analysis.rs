use std::{
    borrow::Borrow,
    cell::Cell,
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
    iter,
    ops::{Not, Range},
};

use bitvec::{slice::BitSlice, vec::BitVec};
use ref_cast::RefCast;
use smallvec::SmallVec;
use subenum::subenum;

use crate::{
    chain::{Chain, ChainId},
    context::SessionData,
    index_newtype,
    operators::{
        call::OpCall,
        call_concurrent::OpCallConcurrent,
        chunks::OpChunks,
        fork::OpFork,
        forkcat::OpForkCat,
        operator::{OffsetInChain, OperatorData, OperatorId},
        utils::nested_op::NestedOp,
    },
    utils::{
        get_two_distinct_mut,
        identity_hasher::BuildIdentityHasher,
        index_vec::IndexVec,
        indexing_type::{IndexingType, IndexingTypeRange},
        string_store::{StringStore, StringStoreEntry},
    },
};

use derive_more::{Deref, DerefMut};

index_newtype! {
    pub struct BasicBlockId(usize);
    pub struct VarId(u32);
    pub struct OpOutputIdx(u32);
}

#[subenum(OpOutputLivenessSlotKind)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum VarLivenessSlotKind {
    #[subenum(OpOutputLivenessSlotKind)]
    Reads = 0,
    #[subenum(OpOutputLivenessSlotKind)]
    NonStringReads = 1,
    #[subenum(OpOutputLivenessSlotKind)]
    HeaderWrites = 2,
    // survives is special because
    // a) it does not make sense for output_ops, as they are valid across bbs
    // b) it does not get affected by aliases (survives means the value bound
    // to the var at the start of a bb remains bound to that same var
    // after the bb)
    Survives = 3,
}

// all fields except survives
pub const VAR_LIVENESS_SLOT_KINDS_WITHOUT_SURVIVES: [usize; 3] = [
    VarLivenessSlotKind::Reads as usize,
    VarLivenessSlotKind::NonStringReads as usize,
    VarLivenessSlotKind::HeaderWrites as usize,
];

pub const OP_OUTPUT_LIVENESS_SLOT_COUNT: usize = 3; // output ops don't care about survives
pub const VAR_LIVENESS_SLOTS_COUNT: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum VarLivenessSlotGroup {
    Local = 0,
    Global = 1,
    Succession = 2,
}

/// 'Succession' combines the effect of callees and successors.
///
/// For blocks that *are* callees, it also include the effects of the
/// successors of the *callers*.
pub const SUCCESSION_SLOTS_OFFSET: usize = 2 * VAR_LIVENESS_SLOTS_COUNT;

// local slots + global slots + succession data
pub const SLOTS_PER_BASIC_BLOCK: usize = VAR_LIVENESS_SLOTS_COUNT * 3;

// the order and number of special variables has to be consistent
// with the initialization in `LivenessData::setup_vars`
pub const BB_INPUT_VAR_ID: VarId = VarId(0);
pub const VOID_VAR_ID: VarId = VarId(1);
pub const DYN_VAR_ID: VarId = VarId(2);
pub const SPECIAL_VAR_COUNT: VarId = VarId(3);

pub const BB_INPUT_VAR_OUTPUT_IDX: OpOutputIdx =
    OpOutputIdx(BB_INPUT_VAR_ID.0);
pub const VOID_VAR_OUTPUT_IDX: OpOutputIdx = OpOutputIdx(VOID_VAR_ID.0);

pub struct BasicBlock {
    pub chain_id: ChainId,
    pub operators_start: OffsetInChain,
    pub operators_end: OffsetInChain,
    pub calls: SmallVec<[BasicBlockId; 2]>,
    pub successors: SmallVec<[BasicBlockId; 2]>,
    // if this block is a callee, this contains the bb ids of all blocks
    // that any caller of this block has as successors
    // this is used to calculate 'succession' var data, which is in turn
    // used to calculate the liveness of operator outputs created by this
    // block
    pub caller_successors: HashSet<BasicBlockId, BuildIdentityHasher>,
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
    pub op_outputs_data: OpOutputLivenessOwned,
    pub op_outputs: IndexVec<OpOutputIdx, OpOutput>,
    pub vars: IndexVec<VarId, Var>,
    pub var_names: HashMap<StringStoreEntry, VarId, BuildIdentityHasher>,
    pub basic_blocks: IndexVec<BasicBlockId, BasicBlock>,
    pub vars_to_op_outputs_map: IndexVec<VarId, OpOutputIdx>,
    pub key_aliases_map: HashMap<VarId, OpOutputIdx, BuildIdentityHasher>,
    pub operator_liveness_data: IndexVec<OperatorId, OperatorLivenessData>,
    updates_stack: Vec<BasicBlockId>,
}

#[derive(Deref, DerefMut, RefCast)]
#[repr(transparent)]
pub struct VarLivenessSlot {
    data: BitSlice<Cell<usize>>,
}

pub struct VarLivenessSlotOwned {
    data: BitVec<Cell<usize>>,
}

#[derive(Deref, DerefMut, RefCast)]
#[repr(transparent)]
pub struct VarLiveness {
    data: BitSlice<Cell<usize>>,
}

pub struct VarLivenessOwned {
    data: BitVec<Cell<usize>>,
}

#[derive(Deref, DerefMut, RefCast)]
#[repr(transparent)]
pub struct OpOutputLivenessSlot {
    data: BitSlice<Cell<usize>>,
}

#[derive(Deref, DerefMut, RefCast)]
#[repr(transparent)]
pub struct OpOutputLiveness {
    data: BitSlice<Cell<usize>>,
}

#[derive(Default)]
pub struct OpOutputLivenessOwned {
    data: BitVec<Cell<usize>>,
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

impl ToOwned for VarLivenessSlot {
    type Owned = VarLivenessSlotOwned;

    fn to_owned(&self) -> Self::Owned {
        VarLivenessSlotOwned {
            data: self.data.to_bitvec(),
        }
    }
}
impl Borrow<VarLivenessSlot> for VarLivenessSlotOwned {
    fn borrow(&self) -> &VarLivenessSlot {
        self
    }
}

impl std::ops::Deref for VarLivenessSlotOwned {
    type Target = VarLivenessSlot;

    fn deref(&self) -> &Self::Target {
        VarLivenessSlot::ref_cast(&self.data)
    }
}

impl std::ops::DerefMut for VarLivenessSlotOwned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        VarLivenessSlot::ref_cast_mut(&mut self.data)
    }
}

impl ToOwned for VarLiveness {
    type Owned = VarLivenessOwned;

    fn to_owned(&self) -> Self::Owned {
        VarLivenessOwned {
            data: self.data.to_bitvec(),
        }
    }
}
impl Borrow<VarLiveness> for VarLivenessOwned {
    fn borrow(&self) -> &VarLiveness {
        self
    }
}

impl std::ops::Deref for VarLivenessOwned {
    type Target = VarLiveness;

    fn deref(&self) -> &Self::Target {
        VarLiveness::ref_cast(&self.data)
    }
}

impl std::ops::DerefMut for VarLivenessOwned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        VarLiveness::ref_cast_mut(&mut self.data)
    }
}

impl VarLiveness {
    pub fn var_count(&self) -> usize {
        self.data.len() / VAR_LIVENESS_SLOTS_COUNT
    }
    pub fn get_slot(&self, slot: VarLivenessSlotKind) -> &VarLivenessSlot {
        let var_count = self.var_count();
        let slot_idx = slot as usize;
        VarLivenessSlot::ref_cast(
            &self.data[var_count * slot_idx..var_count * (slot_idx + 1)],
        )
    }
}

impl VarLivenessOwned {
    pub fn new(var_count: usize) -> Self {
        let mut data = BitVec::new();
        data.resize(var_count * VAR_LIVENESS_SLOTS_COUNT, false);
        VarLivenessOwned { data }
    }
}

impl std::ops::Deref for OpOutputLivenessOwned {
    type Target = OpOutputLiveness;

    fn deref(&self) -> &Self::Target {
        OpOutputLiveness::ref_cast(&self.data)
    }
}

impl std::ops::DerefMut for OpOutputLivenessOwned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        OpOutputLiveness::ref_cast_mut(&mut self.data)
    }
}

impl ToOwned for OpOutputLiveness {
    type Owned = OpOutputLivenessOwned;

    fn to_owned(&self) -> Self::Owned {
        OpOutputLivenessOwned {
            data: self.data.to_bitvec(),
        }
    }
}
impl Borrow<OpOutputLiveness> for OpOutputLivenessOwned {
    fn borrow(&self) -> &OpOutputLiveness {
        self
    }
}

impl OpOutputLiveness {
    pub fn op_count(&self) -> usize {
        self.data.len() / OP_OUTPUT_LIVENESS_SLOT_COUNT
    }
    pub fn get_slot(&self, slot: VarLivenessSlotKind) -> &VarLivenessSlot {
        debug_assert!(slot != VarLivenessSlotKind::Survives);
        let op_output_count = self.op_count();
        let slot_idx = slot as usize;
        VarLivenessSlot::ref_cast(
            &self.data
                [op_output_count * slot_idx..op_output_count * (slot_idx + 1)],
        )
    }
}

impl OpOutputLivenessOwned {
    pub fn new(op_output_count: usize) -> Self {
        let mut data = BitVec::new();
        data.resize(op_output_count * OP_OUTPUT_LIVENESS_SLOT_COUNT, false);
        OpOutputLivenessOwned { data }
    }
}

impl VarLivenessSlotKind {
    pub fn index(self) -> usize {
        self as usize
    }
}

impl VarLivenessSlotGroup {
    pub fn offset(self) -> usize {
        self as usize * VAR_LIVENESS_SLOTS_COUNT
    }
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

impl ChainId {
    pub fn into_bb_id(self) -> BasicBlockId {
        // we guarantee in `setup_bbs` that
        // chain_id == <id of first bb in chain>
        BasicBlockId::from_usize(self.into_usize())
    }
}

impl VarId {
    pub fn natural_output_idx(self) -> OpOutputIdx {
        // the lowest <var_count> op outputs are reserved for variables
        OpOutputIdx::from_usize(self.into_usize())
    }
}

impl OpOutputIdx {
    pub fn is_var(self, var_count: usize) -> bool {
        self.into_usize() < var_count
    }
    pub fn into_var_id(self, var_count: usize) -> Option<VarId> {
        if self.into_usize() >= var_count {
            return None;
        }
        Some(VarId::from_usize(self.into_usize()))
    }
}

pub struct OperatorLivenessOutput {
    pub flags: AccessFlags,
    pub call_effect: OperatorCallEffect,
    pub primary_output: OpOutputIdx,
}
impl OperatorLivenessOutput {
    pub(crate) fn with_defaults(primary_output: OpOutputIdx) -> Self {
        Self {
            flags: AccessFlags {
                input_accessed: true,
                non_stringified_input_access: true,
                may_dup_or_drop: true,
            },
            call_effect: OperatorCallEffect::Basic,
            primary_output,
        }
    }
}

impl LivenessData {
    pub fn append_op_outputs(&mut self, count: usize, op_id: OperatorId) {
        self.op_outputs.extend(
            std::iter::repeat(OpOutput {
                bound_vars: SmallVec::new(),
                field_references: SmallVec::new(),
                producing_op: Some(op_id),
            })
            .take(count),
        );
    }
    pub fn setup_operator_outputs(&mut self, sess: &mut SessionData) {
        self.operator_liveness_data.extend(
            iter::repeat(OperatorLivenessData::default())
                .take(sess.operator_data.len()),
        );
        let mut total_outputs_count = OpOutputIdx::from_usize(self.vars.len());
        self.op_outputs.extend(
            iter::repeat(OpOutput {
                bound_vars: SmallVec::new(),
                field_references: SmallVec::new(),
                producing_op: None,
            })
            .take(self.vars.len()),
        );

        for bb_id in IndexingTypeRange::from_zero(self.basic_blocks.next_idx())
        {
            let bb = &self.basic_blocks[bb_id];
            let chain_id = bb.chain_id;

            for op_offset in
                IndexingTypeRange::new(bb.operators_start..bb.operators_end)
            {
                let op_id = sess.chains[chain_id].operators[op_offset];
                sess.with_mut_op_data(op_id, |sess, op_data| {
                    op_data.assign_op_outputs(
                        sess,
                        self,
                        op_id,
                        &mut total_outputs_count,
                    )
                });
            }
        }

        debug_assert!(
            self.op_outputs.len() == total_outputs_count.into_usize()
        );
        self.vars_to_op_outputs_map
            .extend((0..self.vars.len()).map(OpOutputIdx::from_usize));

        self.op_outputs_data.data.resize(
            total_outputs_count.into_usize() * OP_OUTPUT_LIVENESS_SLOT_COUNT,
            false,
        );
    }
    pub fn add_var_name(&mut self, name: StringStoreEntry) {
        match self.var_names.entry(name) {
            Entry::Occupied(_) => (),
            Entry::Vacant(e) => {
                e.insert(VarId::from_usize(self.vars.len()));
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
        sess.operator_data[sess.op_data_id(op_id)]
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
    pub fn split_bb_at_call(
        &mut self,
        sess: &SessionData,
        bb_id: BasicBlockId,
        op_n: OffsetInChain,
    ) {
        let curr_bb_count = self.basic_blocks.len();
        let bb = &mut self.basic_blocks[bb_id];
        let chain_id = bb.chain_id;
        let end = sess.chains[chain_id].operators.next_idx();
        bb.operators_end = op_n + OffsetInChain::one();
        if op_n + OffsetInChain::one() != end {
            bb.successors.push(BasicBlockId::from(curr_bb_count));
            let bb_new = self.basic_blocks.push_get_id(BasicBlock {
                chain_id,
                operators_start: op_n + OffsetInChain::one(),
                operators_end: end,
                calls: SmallVec::new(),
                successors: SmallVec::new(),
                caller_successors: HashSet::default(),
                updates_required: true,
                field_references: HashMap::new(),
                predecessors: SmallVec::new(),
                key_aliases: HashMap::default(),
            });
            self.updates_stack.push(bb_new);
        }
    }
    // returns true if the op ends the block
    pub fn update_bb_for_op(
        &mut self,
        sess: &SessionData,
        op_id: OperatorId,
        op_n: OffsetInChain,
        cn: &Chain,
        bb_id: BasicBlockId,
    ) -> bool {
        let bb = &mut self.basic_blocks[bb_id];
        let op_base = &sess.operator_bases[op_id];
        match &sess.operator_data[op_base.op_data_id] {
            OperatorData::CallConcurrent(OpCallConcurrent {
                target_resolved,
                ..
            })
            | OperatorData::Call(OpCall {
                target_resolved, ..
            }) => {
                bb.calls.push(target_resolved.unwrap().into_bb_id());
                self.split_bb_at_call(sess, bb_id, op_n);
                return true;
            }
            OperatorData::Fork(OpFork {
                subchains_start,
                subchains_end,
                ..
            }) => {
                for sc in &cn.subchains[*subchains_start..*subchains_end] {
                    bb.successors.push(sc.into_bb_id());
                }
                return true;
            }
            OperatorData::ForkCat(OpForkCat {
                subchains_start,
                subchains_end,
                ..
            }) => {
                for sc in &cn.subchains[*subchains_start..*subchains_end] {
                    bb.calls.push(sc.into_bb_id());
                }
                self.split_bb_at_call(sess, bb_id, op_n);
                return true;
            }
            OperatorData::Chunks(OpChunks { subchain_idx, .. }) => {
                bb.calls.push(cn.subchains[*subchain_idx].into_bb_id());
                self.split_bb_at_call(sess, bb_id, op_n);
            }
            OperatorData::Key(op) => {
                let Some(nested_op) = &op.nested_op else {
                    return false;
                };
                let &NestedOp::SetUp(sub_op_id) = nested_op else {
                    unreachable!()
                };
                return self
                    .update_bb_for_op(sess, sub_op_id, op_n, cn, bb_id);
            }
            OperatorData::Atom(_)
            | OperatorData::Nop(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Select(_)
            | OperatorData::Literal(_) => (),

            OperatorData::Custom(op) => {
                return op
                    .update_bb_for_op(sess, self, op_id, op_n, cn, bb_id);
            }
        };
        false
    }
    fn setup_bbs(&mut self, sess: &SessionData) {
        let var_count = self.vars.len();
        let bits_per_bb = var_count * SLOTS_PER_BASIC_BLOCK;
        for (chain_id, c) in sess.chains.iter_enumerated() {
            self.basic_blocks.push(BasicBlock {
                chain_id,
                operators_start: OffsetInChain::zero(),
                operators_end: c.operators.next_idx(),
                calls: SmallVec::new(),
                successors: SmallVec::new(),
                caller_successors: HashSet::default(),
                updates_required: true,
                field_references: HashMap::new(),
                predecessors: SmallVec::new(),
                key_aliases: HashMap::default(),
            });
        }
        self.updates_stack.extend(IndexingTypeRange::new(
            BasicBlockId::zero()..sess.chains.next_idx().into_bb_id(),
        ));
        while let Some(bb_id) = self.updates_stack.pop() {
            let bb = &mut self.basic_blocks[bb_id];
            let cn = &sess.chains[bb.chain_id];
            for (op_n, &op) in cn.operators
                [bb.operators_start..bb.operators_end]
                .iter_enumerated(bb.operators_start)
            {
                let op_n = op_n as OffsetInChain;
                if self.update_bb_for_op(sess, op, op_n, cn, bb_id) {
                    break;
                }
            }
        }
        self.var_data
            .resize(bits_per_bb * self.basic_blocks.len(), false);
    }
    // returns true if any change occured
    fn try_update_callee_successors(
        &mut self,
        bb_id: BasicBlockId,
        callee_id: BasicBlockId,
    ) -> bool {
        let (bb, callee) = get_two_distinct_mut(
            self.basic_blocks.as_slice_mut(),
            bb_id.into_usize(),
            callee_id.into_usize(),
        );
        let len_before = callee.caller_successors.len();
        callee.caller_successors.extend(&bb.successors);
        len_before != callee.caller_successors.len()
    }
    fn setup_bb_linkage_data(&mut self, sess: &SessionData) {
        for bb_id in IndexingTypeRange::new(
            BasicBlockId::zero()..self.basic_blocks.next_idx(),
        ) {
            for succ_n in 0..self.basic_blocks[bb_id].successors.len() {
                let succ_id = self.basic_blocks[bb_id].successors[succ_n];
                self.basic_blocks[succ_id].predecessors.push(bb_id);
            }
            let bb = &self.basic_blocks[bb_id];
            let chain = &sess.chains[bb.chain_id];
            for op_id in &chain.operators[bb.operators_start..bb.operators_end]
            {
                self.operator_liveness_data[*op_id].basic_block_id = bb_id;
            }
            for callee_n in 0..bb.calls.len() {
                let callee_id = self.basic_blocks[bb_id].calls[callee_n];
                self.try_update_callee_successors(bb_id, callee_id);
            }
        }
        self.updates_stack.extend(IndexingTypeRange::from_zero(
            self.basic_blocks.next_idx(),
        ));
        while let Some(bb_id) = self.updates_stack.pop() {
            for callee_n in 0..self.basic_blocks[bb_id].calls.len() {
                let callee_id = self.basic_blocks[bb_id].calls[callee_n];
                let progress =
                    self.try_update_callee_successors(bb_id, callee_id);
                if progress && !self.basic_blocks[callee_id].updates_required {
                    self.updates_stack.push(callee_id);
                }
            }
        }
    }

    pub fn access_var(
        &mut self,
        sess: &SessionData,
        op_id: OperatorId,
        var_id: VarId,
        op_offset_after_last_write: OffsetInChain,
        non_stringified: bool,
    ) {
        self.operator_liveness_data[op_id]
            .accessed_vars
            .push(var_id);
        let output_id = self.vars_to_op_outputs_map[var_id];
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
        op_offset_after_last_write: OffsetInChain,
        non_stringified: bool,
        direct_access: bool,
    ) {
        if op_output_idx == VOID_VAR_OUTPUT_IDX {
            // we don't want this field to be forwarded by fork and friends
            // so we just pretend it is never accessed.
            return;
        }

        let mut propagate_change = false;

        let ooc = self.op_outputs.len();
        let ld = &mut self.operator_liveness_data[op_id];

        let header_write = if let Some(oo_producing_op) =
            self.op_outputs[op_output_idx].producing_op
        {
            sess.operator_bases[oo_producing_op]
                .offset_in_chain
                .base_chain_offset(sess)
                + OffsetInChain::from_usize(1)
                < op_offset_after_last_write
        } else {
            op_offset_after_last_write != OffsetInChain::zero()
        };

        match ld.accessed_outputs.entry(op_output_idx) {
            Entry::Occupied(mut e) => {
                let acc = e.get_mut();
                if direct_access && !acc.direct_access {
                    propagate_change |= !acc.direct_access;
                    acc.direct_access = true;

                    propagate_change |=
                        acc.direct_access_index != ld.direct_access_count;
                    acc.direct_access_index = ld.direct_access_count;
                }
                propagate_change |= !acc.header_write && header_write;
                acc.header_write |= header_write;
                propagate_change |= !acc.non_stringified && non_stringified;
                acc.non_stringified |= non_stringified;
            }
            Entry::Vacant(e) => {
                propagate_change = true;
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
        let oo_idx = op_output_idx.into_usize();

        let reads_bit = VarLivenessSlotKind::Reads.index() * ooc + oo_idx;
        propagate_change |= !self.op_outputs_data[reads_bit];
        self.op_outputs_data.set_aliased(reads_bit, true);
        if non_stringified {
            let non_string_reads_bit =
                VarLivenessSlotKind::NonStringReads.index() * ooc + oo_idx;
            propagate_change |= !self.op_outputs_data[non_string_reads_bit];
            self.op_outputs_data.set_aliased(non_string_reads_bit, true);
        }
        if header_write {
            let header_writes_bit =
                VarLivenessSlotKind::HeaderWrites.index() * ooc + oo_idx;
            propagate_change |= !self.op_outputs_data[header_writes_bit];
            self.op_outputs_data.set_aliased(header_writes_bit, true);
        }

        if propagate_change {
            for fri in 0..self.op_outputs[op_output_idx].field_references.len()
            {
                self.access_output(
                    sess,
                    op_id,
                    self.op_outputs[op_output_idx].field_references[fri],
                    op_offset_after_last_write,
                    non_stringified,
                    false,
                );
            }
        }
    }
    fn reset_op_outputs_data_for_vars(&mut self) {
        let var_count = self.vars.len();
        let ooc = self.op_outputs.len();
        for i in &VAR_LIVENESS_SLOT_KINDS_WITHOUT_SURVIVES {
            self.op_outputs_data[i * ooc..i * ooc + var_count].fill(false);
        }
        for i in 0..var_count {
            let var_id = VarId::from_usize(i);
            // initially each var points to it's input
            let op_output_idx = OpOutputIdx::from_usize(i);
            self.vars_to_op_outputs_map[var_id] = op_output_idx;
            self.op_outputs[op_output_idx].field_references.clear();
        }
    }
    pub fn apply_var_remapping(&self, var_id: VarId, target: OpOutputIdx) {
        let offset = VarLivenessSlotKind::NonStringReads.index()
            * self.op_outputs.len();
        let tgt_idx = offset + target.into_usize();
        let src_idx = offset + var_id.natural_output_idx().into_usize();
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
        let cn = &sess.chains[bb.chain_id];
        let mut input_field = BB_INPUT_VAR_OUTPUT_IDX;
        let mut op_offset_after_last_write = OffsetInChain::zero();
        for op_n in
            IndexingTypeRange::new(bb.operators_start..bb.operators_end)
        {
            let op_id = cn.operators[op_n];
            let op_base = &sess.operator_bases[op_id];
            let op_data_id = op_base.op_data_id;
            let mut output =
                OperatorLivenessOutput::with_defaults(op_base.outputs_start);
            sess.operator_data[op_data_id].update_liveness_for_op(
                sess,
                self,
                op_offset_after_last_write,
                op_id,
                bb_id,
                input_field,
                &mut output,
            );
            match output.call_effect {
                OperatorCallEffect::Basic => (),
                OperatorCallEffect::NoCall => {
                    input_field = output.primary_output;
                    continue;
                }
                OperatorCallEffect::Diverge => break,
            }
            if output.flags.input_accessed {
                self.access_var(
                    sess,
                    op_id,
                    BB_INPUT_VAR_ID,
                    op_offset_after_last_write,
                    output.flags.non_stringified_input_access,
                );
            }
            if output.flags.may_dup_or_drop {
                op_offset_after_last_write =
                    op_n + OffsetInChain::from_usize(1);
            }

            let op_base = &sess.operator_bases[op_id];
            if op_base.outputs_start != op_base.outputs_end {
                input_field = output.primary_output;
                self.vars_to_op_outputs_map[BB_INPUT_VAR_ID] =
                    sess.operator_bases[op_id].outputs_start;
            }
        }
        let vc = self.vars.len();
        let ooc = self.op_outputs.len();
        let var_data_start = bb_id.into_usize() * SLOTS_PER_BASIC_BLOCK * vc;
        if op_offset_after_last_write != OffsetInChain::zero() {
            // TODO: maybe do something more sophisticated than this
            // for now we just assume that any op_output that is bound
            // after the bb might be accessed by someone and therefore needs
            // header writes applied
            for &op_output_id in &self.vars_to_op_outputs_map {
                if let Some(producing_op) =
                    self.op_outputs[op_output_id].producing_op
                {
                    let producing_op_offset =
                        sess.operator_bases[producing_op].offset_in_chain;
                    if producing_op_offset.base_chain_offset(sess)
                        >= op_offset_after_last_write - OffsetInChain::one()
                    {
                        continue;
                    }
                }
                self.op_outputs_data.set_aliased(
                    VarLivenessSlotKind::HeaderWrites.index() * ooc
                        + op_output_id.into_usize(),
                    true,
                );
            }
        }
        for i in VAR_LIVENESS_SLOT_KINDS_WITHOUT_SURVIVES {
            self.var_data
                [var_data_start + i * vc..var_data_start + (i + 1) * vc]
                .copy_from_bitslice(
                    &self.op_outputs_data[i * ooc..i * ooc + vc],
                );
        }
        self.var_data[var_data_start
            + VarLivenessSlotKind::Survives.index() * vc
            ..var_data_start
                + (VarLivenessSlotKind::Survives.index() + 1) * vc]
            .fill(true);
        for var_id in 0..self.vars_to_op_outputs_map.len() {
            let var_id = VarId::from_usize(var_id);
            let op_output_id = self.vars_to_op_outputs_map[var_id];
            if op_output_id.into_usize() >= self.vars.len() {
                self.var_data.set_aliased(
                    var_data_start
                        + VarLivenessSlotKind::Survives.index()
                            * self.vars.len()
                        + var_id.into_usize(),
                    false,
                );
                self.insert_var_field_references(op_output_id, bb_id, var_id);
                self.op_outputs[op_output_id].bound_vars.push(var_id);
            } else {
                debug_assert!(op_output_id == var_id.natural_output_idx());
            }
        }
        for (&var_id, &op_output) in &self.key_aliases_map {
            self.apply_var_remapping(var_id, op_output);
            bb = &mut self.basic_blocks[bb_id];
            if let Some(op_output_var_id) =
                op_output.into_var_id(self.vars.len())
            {
                bb.key_aliases.insert(op_output_var_id, var_id);
            } else {
                for &bv in &self.op_outputs[op_output].bound_vars {
                    bb.key_aliases.insert(bv, var_id as VarId);
                }
            }
        }
    }

    fn insert_var_field_references(
        &mut self,
        op_output_idx: OpOutputIdx,
        bb_id: BasicBlockId,
        var_id: VarId,
    ) {
        debug_assert!(!op_output_idx.is_var(self.vars.len()));
        for fr_i in 0..self.op_outputs[op_output_idx].field_references.len() {
            let fr = self.op_outputs[op_output_idx].field_references[fr_i];
            if let Some(fr_var) = fr.into_var_id(self.vars.len()) {
                self.basic_blocks[bb_id]
                    .field_references
                    .entry(var_id)
                    .or_default()
                    .push(fr_var);
            } else {
                self.insert_var_field_references(fr, bb_id, var_id);
            }
        }
    }
    fn compute_local_liveness(&mut self, sess: &SessionData) {
        for i in IndexingTypeRange::new(
            BasicBlockId::zero()..self.basic_blocks.next_idx(),
        ) {
            self.compute_local_liveness_for_bb(sess, i);
        }
    }
    pub fn get_var_liveness_bounds(
        &self,
        bb_id: BasicBlockId,
        slot_group: VarLivenessSlotGroup,
    ) -> Range<usize> {
        let vc = self.vars.len();
        let start = bb_id.into_usize() * SLOTS_PER_BASIC_BLOCK * vc
            + slot_group.offset() * vc;
        start..start + vc * VAR_LIVENESS_SLOTS_COUNT
    }
    pub fn get_var_liveness(
        &self,
        bb_id: BasicBlockId,
        slot_group: VarLivenessSlotGroup,
    ) -> &VarLiveness {
        VarLiveness::ref_cast(
            &self.var_data[self.get_var_liveness_bounds(bb_id, slot_group)],
        )
    }
    pub fn get_var_liveness_slot(
        &self,
        bb_id: BasicBlockId,
        slot_group: VarLivenessSlotGroup,
        slot: VarLivenessSlotKind,
    ) -> &VarLivenessSlot {
        self.get_var_liveness(bb_id, slot_group).get_slot(slot)
    }
    pub fn set_var_liveness_ored<'b>(
        &self,
        slot_group: VarLivenessSlotGroup,
        tgt: &mut VarLiveness,
        bbs: impl IntoIterator<Item = &'b BasicBlockId>,
    ) {
        let mut bbs = bbs.into_iter();
        if let Some(&bb_id) = bbs.next() {
            tgt.copy_from_bitslice(self.get_var_liveness(bb_id, slot_group));
            for &bb_id in bbs {
                **tgt |= &**self.get_var_liveness(bb_id, slot_group);
            }
        } else {
            tgt.fill(false);
        }
    }
    pub fn get_var_liveness_ored<'b>(
        &self,
        bbs: impl IntoIterator<Item = &'b BasicBlockId>,
        slot_group: VarLivenessSlotGroup,
    ) -> VarLivenessOwned {
        let mut tgt = VarLivenessOwned::new(self.vars.len());
        self.set_var_liveness_ored(slot_group, &mut tgt, bbs);
        tgt
    }
    pub fn kill_non_survivors(
        &self,
        survivors: &VarLiveness,
        tgt: &mut VarLiveness,
    ) {
        let var_count = self.vars.len();
        let survivors_start =
            VarLivenessSlotKind::Survives.index() * var_count;
        let survivors_slice =
            &survivors.data[survivors_start..survivors_start + var_count];
        for i in &VAR_LIVENESS_SLOT_KINDS_WITHOUT_SURVIVES {
            // the default field
            let start = var_count * i;
            tgt.data[start..start + var_count] &= survivors_slice;
        }
    }
    pub fn apply_alias(
        &self,
        bb_data: &VarLiveness,
        succ_data: &VarLiveness,
        original_var: VarId,
        alias_var: VarId, // var that after the bb refers to the original var
    ) {
        let var_count = self.vars.len();
        for i in VAR_LIVENESS_SLOT_KINDS_WITHOUT_SURVIVES {
            let tgt_idx = i * var_count + original_var.into_usize();
            let src_idx = i * var_count + alias_var.into_usize();
            bb_data
                .set_aliased(tgt_idx, bb_data[tgt_idx] || succ_data[src_idx]);
        }
    }
    pub fn apply_bb_aliases(
        &self,
        bb_data: &VarLiveness,
        successor_data: &VarLiveness,
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
                self.updates_stack.push(pred_id);
            }
        }
    }
    pub fn merge_calls_with_successors<'a>(
        &self,
        calls: &mut VarLiveness,
        successors: &mut VarLiveness,
        calls_iter: impl Iterator<Item = &'a BasicBlockId> + Clone,
        calls_empty: bool,
        successors_iter: impl Iterator<Item = &'a BasicBlockId>,
        successors_empty: bool,
    ) {
        if calls_empty {
            self.set_var_liveness_ored(
                VarLivenessSlotGroup::Global,
                calls,
                successors_iter,
            );
        } else {
            self.set_var_liveness_ored(
                VarLivenessSlotGroup::Global,
                calls,
                calls_iter.clone(),
            );
            if !successors_empty {
                self.set_var_liveness_ored(
                    VarLivenessSlotGroup::Global,
                    successors,
                    successors_iter,
                );

                for call_bb_id in calls_iter {
                    let cbb = &self.basic_blocks[*call_bb_id];
                    self.apply_bb_aliases(calls, successors, cbb);
                }
                self.kill_non_survivors(calls, successors);
                **calls |= &**successors;
            }
        }
    }
    // 'continuation' data describes the effects of calls and successors
    // unlike 'succession' it does not include the effects of successors of
    // callers in case the bb is a callee
    fn build_bb_continuation_data(
        &self,
        bb: &BasicBlock,
        calls: &mut VarLiveness,
        successors: &mut VarLiveness,
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
        let mut successors = VarLivenessOwned::new(var_count);
        let mut calls = VarLivenessOwned::new(var_count);
        let mut global = VarLivenessOwned::new(var_count);

        self.updates_stack.extend(IndexingTypeRange::new(
            BasicBlockId::zero()..self.basic_blocks.next_idx(),
        ));
        while let Some(bb_id) = self.updates_stack.pop() {
            self.basic_blocks[bb_id].updates_required = false;
            let bb = &self.basic_blocks[bb_id];

            let bb_local_range = self
                .get_var_liveness_bounds(bb_id, VarLivenessSlotGroup::Local);
            let bb_global_range = self
                .get_var_liveness_bounds(bb_id, VarLivenessSlotGroup::Global);

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
            **global |= &**calls;
            if **global != self.var_data[bb_global_range.clone()] {
                self.var_data[bb_global_range.clone()]
                    .copy_from_bitslice(&global);
                self.update_bb_predecessors(bb_id);
            }
        }
    }
    fn compute_bb_succession_data(&mut self) {
        let var_count = self.vars.len();
        let mut caller_successors = VarLivenessOwned::new(var_count);
        let mut successors = VarLivenessOwned::new(var_count);
        for (bb_id, bb) in self.basic_blocks.iter_enumerated() {
            self.merge_calls_with_successors(
                &mut successors,
                &mut caller_successors,
                bb.successors.iter(),
                bb.successors.is_empty(),
                bb.caller_successors.iter(),
                bb.caller_successors.is_empty(),
            );
            // can't use `get_var_liveness` because of the borrow checker
            let succession_bounds = self.get_var_liveness_bounds(
                bb_id,
                VarLivenessSlotGroup::Succession,
            );
            if bb.calls.is_empty() {
                self.var_data[succession_bounds]
                    .copy_from_bitslice(&successors);
                continue;
            }
            // reuse allocation
            let calls = &mut caller_successors;
            self.set_var_liveness_ored(
                VarLivenessSlotGroup::Global,
                calls,
                bb.calls.iter(),
            );

            for &call_bb_id in &bb.calls {
                let cbb = &self.basic_blocks[call_bb_id];
                self.apply_bb_aliases(calls, &successors, cbb);
            }
            self.kill_non_survivors(calls, &mut successors);
            ***calls |= &**successors;
            self.var_data[succession_bounds].copy_from_bitslice(calls);
        }
    }
    fn compute_op_output_liveness(&mut self, sess: &SessionData) {
        let var_count = self.vars.len();
        let op_output_count = self.op_outputs.len();
        for bb_id in IndexingTypeRange::new(
            BasicBlockId::zero()..self.basic_blocks.next_idx(),
        ) {
            // can't use `get_var_liveness` due to brrwck
            let succ_var_data = &self.var_data[self.get_var_liveness_bounds(
                bb_id,
                VarLivenessSlotGroup::Succession,
            )];
            let bb = &self.basic_blocks[bb_id];
            let chain = &sess.chains[bb.chain_id];
            if bb.successors.is_empty()
                && bb.calls.is_empty()
                && bb.caller_successors.is_empty()
            {
                continue;
            }
            for op_id in &chain.operators[bb.operators_start..bb.operators_end]
            {
                let op_base = &sess.operator_bases[*op_id];
                for op_output_id in IndexingTypeRange::new(
                    op_base.outputs_start..op_base.outputs_end,
                ) {
                    let oo = &self.op_outputs[op_output_id];
                    for var_id in &oo.bound_vars {
                        for i in VAR_LIVENESS_SLOT_KINDS_WITHOUT_SURVIVES {
                            let oo_idx = i * op_output_count
                                + op_output_id.into_usize();
                            let v_idx = i * var_count + var_id.into_usize();

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
            let bb_id = BasicBlockId::from_usize(bb_id);
            let succession_reads = self.get_var_liveness_slot(
                bb_id,
                VarLivenessSlotGroup::Succession,
                VarLivenessSlotKind::Reads,
            );
            let local_survives = self.get_var_liveness_slot(
                bb_id,
                VarLivenessSlotGroup::Local,
                VarLivenessSlotKind::Survives,
            );
            let mut vars = &mut live_outputs[0..var_count];
            vars.copy_from_bitslice(local_survives);
            vars = vars.not();
            *vars |= &**succession_reads;

            let bb = &self.basic_blocks[bb_id];

            let chain = &sess.chains[bb.chain_id];
            let operators =
                &chain.operators[bb.operators_start..bb.operators_end];
            for &op_id in operators {
                let op_base = &sess.operator_bases[op_id];
                for output_id in IndexingTypeRange::new(
                    op_base.outputs_start..op_base.outputs_end,
                ) {
                    let mut output =
                        live_outputs.get_mut(output_id.into_usize()).unwrap();
                    for &v_id in &self.op_outputs[output_id].bound_vars {
                        *output |= succession_reads[v_id.into_usize()];
                    }
                }
            }
            for &op_id in operators {
                let op_ld = &mut self.operator_liveness_data[op_id];
                for &output_idx in op_ld.accessed_outputs.keys() {
                    if !live_outputs[output_idx.into_usize()] {
                        op_ld.killed_outputs.push(output_idx);
                        live_outputs.set(output_idx.into_usize(), true);
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
            data: &BitSlice<Cell<usize>>,
        ) {
            eprint!("{label:>padding$}: ");
            for bit in data {
                eprint!(" {} ", if *bit { "X" } else { "-" });
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
                let op_data_id = sess.operator_bases[op_id].op_data_id;
                eprint!(
                    "(op {op_id} `{}`) ",
                    sess.operator_data[op_data_id].debug_op_name()
                );
            }
            eprintln!();
        }
        eprintln!();
        eprintln!("bbs:");
        for (bb_id, bb) in self.basic_blocks.iter().enumerate() {
            eprint!("bb {bb_id:02}: ");
            for op_n in
                IndexingTypeRange::new(bb.operators_start..bb.operators_end)
            {
                let op_id = sess.chains[bb.chain_id].operators[op_n];
                let op_data_id = sess.operator_bases[op_id].op_data_id;
                eprint!(
                    "(op {op_id} `{}`) ",
                    sess.operator_data[op_data_id].debug_op_name()
                );
            }
            if !bb.calls.is_empty() {
                eprint!("[calls: ");
                for (i, c) in bb.calls.iter().enumerate() {
                    if i > 0 {
                        eprint!(", ");
                    }
                    eprint!("{c:02}");
                }
                eprint!("] ");
            }
            if !bb.predecessors.is_empty() {
                eprint!("{{predecessors: ");
                for (i, s) in bb.predecessors.iter().enumerate() {
                    if i > 0 {
                        eprint!(", ");
                    }
                    eprint!("{s:02}");
                }
                eprint!("}}");
            }
            if !bb.successors.is_empty() {
                eprint!("{{successors: ");
                for (i, s) in bb.successors.iter().enumerate() {
                    if i > 0 {
                        eprint!(", ");
                    }
                    eprint!("{s:02}");
                }
                eprint!("}}");
            }
            if !bb.caller_successors.is_empty() {
                eprint!("{{caller successors: ");
                for (i, s) in bb.caller_successors.iter().enumerate() {
                    if i > 0 {
                        eprint!(", ");
                    }
                    eprint!("{s:02}");
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
        for (op_id, old) in self.operator_liveness_data.iter_enumerated() {
            let op_data_id = sess.operator_bases[op_id].op_data_id;
            eprint!(
                "op {op_id:02} (bb {:02}) ({}):",
                self.operator_liveness_data[op_id].basic_block_id,
                sess.operator_data[op_data_id].debug_op_name(),
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
        for (op_id, op_base) in sess.operator_bases.iter_enumerated() {
            for op_output_idx in op_base.outputs_start.into_usize()
                ..op_base.outputs_end.into_usize()
            {
                let op_output_idx = OpOutputIdx::from_usize(op_output_idx);
                let op_data_id = sess.operator_bases[op_id].op_data_id;
                eprint!(
                    "op_output {op_output_idx:02}: chain {:02}, bb {:02}, op_id {op_id:02} `{}`",
                    op_base.chain_id,
                    self.operator_liveness_data[op_id].basic_block_id,
                    sess.operator_data[op_data_id].default_op_name()
                );
                let oo = &self.op_outputs[op_output_idx];
                if !oo.bound_vars.is_empty() {
                    eprint!(" (bound vars: ");
                    for &v_id in &oo.bound_vars {
                        eprint!(
                            "{} ",
                            self.vars[v_id].debug_name(&string_store)
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
        for (slot_name, slot) in [
            ("reads", VarLivenessSlotKind::Reads),
            ("non string reads", VarLivenessSlotKind::NonStringReads),
            ("header writes", VarLivenessSlotKind::HeaderWrites),
        ] {
            print_bits(
                slot_name,
                PADDING_OP_OUTPUTS,
                self.op_outputs_data.get_slot(slot),
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
            let bb_id = BasicBlockId::from_usize(bb_id);
            eprintln!("{:->PADDING_VARS$}{:-^vars_print_len$}", "", "");
            eprintln!("bb {bb_id:02}:");
            for (slot_group_name, slot_group) in [
                ("local", VarLivenessSlotGroup::Local),
                ("global", VarLivenessSlotGroup::Global),
                ("succession", VarLivenessSlotGroup::Succession),
            ] {
                if slot_group != VarLivenessSlotGroup::Local {
                    eprintln!();
                }
                for (slot_name, slot) in [
                    ("reads", VarLivenessSlotKind::Reads),
                    ("non str reads", VarLivenessSlotKind::NonStringReads),
                    ("header writes", VarLivenessSlotKind::HeaderWrites),
                    ("survives", VarLivenessSlotKind::Survives),
                ] {
                    print_bits(
                        &format!(
                            "{slot_group_name} {slot_name} bb {bb_id:02}"
                        ),
                        PADDING_VARS,
                        self.get_var_liveness_slot(bb_id, slot_group, slot),
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
                            self.vars[src].debug_name(&string_store),
                            self.vars[tgt].debug_name(&string_store),
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
        self.operator_liveness_data[op_id]
            .killed_outputs
            .contains(&output_idx)
    }
    pub fn can_consume_nth_access(
        &self,
        op_id: OperatorId,
        n: DirectOperatorAccessIndex,
    ) -> bool {
        let old = &self.operator_liveness_data[op_id];
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
    ) -> VarLivenessSlotOwned {
        let bb_id = self.operator_liveness_data[op_id].basic_block_id;
        let bb = &self.basic_blocks[bb_id];
        let chain = &sess.chains[bb.chain_id];
        let mut reads = self
            .get_var_liveness_slot(
                bb_id,
                VarLivenessSlotGroup::Succession,
                VarLivenessSlotKind::Reads,
            )
            .to_owned();
        for bb_op_offset in
            IndexingTypeRange::new(bb.operators_start..bb.operators_end).rev()
        {
            let bb_op_id = chain.operators[bb_op_offset];
            if op_id == bb_op_id {
                break;
            }
            for &var_id in &self.operator_liveness_data[bb_op_id].killed_vars {
                reads.set(var_id.into_usize(), false);
            }
            for &var_id in &self.operator_liveness_data[bb_op_id].accessed_vars
            {
                reads.set(var_id.into_usize(), true);
            }
        }
        reads
    }
}

pub fn compute_liveness_data(sess: &mut SessionData) -> LivenessData {
    let mut ld = LivenessData::default();
    ld.setup_vars(sess);
    ld.setup_bbs(sess);
    ld.setup_operator_outputs(sess);
    ld.setup_bb_linkage_data(sess);
    ld.compute_local_liveness(sess);
    ld.compute_global_liveness();
    ld.compute_bb_succession_data();
    ld.compute_op_output_liveness(sess);
    ld.compute_operator_kills(sess);
    #[cfg(feature = "debug_logging_liveness_analysis")]
    ld.log_liveness_data(sess);
    ld
}
