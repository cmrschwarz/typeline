use std::collections::HashMap;

use smallstr::SmallString;

use crate::{
    chain::{Chain, ChainId, SubchainIndex},
    cli::call_expr::Span,
    context::SessionData,
    index_newtype,
    job::{add_transform_to_job, Job},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorCallEffect,
        OperatorLivenessOutput, VarId,
    },
    options::session_setup::SessionSetupData,
    record_data::{
        field::FieldId, group_track::GroupTrackId, match_set::MatchSetId,
    },
    scr_error::ScrError,
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher, indexing_type::IndexingType,
        small_box::SmallBox,
    },
};

use super::{
    atom::{setup_op_atom, OpAtom},
    call::{build_tf_call, setup_op_call, OpCall},
    call_concurrent::{
        build_tf_call_concurrent, setup_op_call_concurrent,
        setup_op_call_concurrent_liveness_data, OpCallConcurrent,
    },
    chunks::{insert_tf_chunks, setup_op_chunks, OpChunks},
    fork::{
        build_tf_fork, setup_op_fork, setup_op_fork_liveness_data, OpFork,
    },
    key::{setup_op_key, OpKey},
    nop::OpNop,
    select::{setup_op_select, OpSelect},
    transform::{TransformData, TransformId, TransformState},
    utils::nested_op::{setup_op_outputs_for_nested_op, NestedOp},
};

index_newtype! {
    pub struct OperatorId(u32);
    pub struct OperatorDataId(u32);
    pub struct OffsetInChain(u32);
    pub struct OffsetInAggregation(u32);
    pub struct OffsetInChainOptions(u32);
}

pub type PreboundOutputsMap =
    HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>;

pub enum OperatorData {
    Call(OpCall),
    CallConcurrent(OpCallConcurrent),
    Fork(OpFork),
    Key(OpKey),
    Atom(OpAtom),
    Select(OpSelect),
    Chunks(OpChunks),
    Custom(SmallBox<dyn Operator, 96>),
}

#[derive(Clone, Copy)]
pub enum OperatorOffsetInChain {
    Direct(OffsetInChain),
    AggregationMember(OperatorId, OffsetInAggregation),
}

pub struct OperatorBase {
    pub op_data_id: OperatorDataId,

    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
    pub desired_batch_size: usize,

    pub span: Span,

    // these two are not part of the OperatorLivenessData struct because it is
    // used in the `prebound_outputs` mechanism that is used during
    // operators -> transforms expansion long after liveness analysis
    // has concluded
    pub outputs_start: OpOutputIdx,
    pub outputs_end: OpOutputIdx,
}

pub struct OperatorInstantiation {
    pub tfs_begin: TransformId,
    pub tfs_end: TransformId,
    pub next_match_set: MatchSetId,
    pub next_input_field: FieldId,
    pub next_group_track: GroupTrackId,
}

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub enum OutputFieldKind {
    #[default]
    Unique,
    Dummy,
    SameAsInput,
    Unconfigured,
}

pub type OperatorName = SmallString<[u8; 32]>;

impl Default for OperatorData {
    fn default() -> Self {
        OperatorData::from_custom(OpNop {})
    }
}

impl OperatorBase {
    pub fn new(
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        desired_batch_size: usize,
        span: Span,
    ) -> Self {
        Self {
            chain_id,
            offset_in_chain,
            desired_batch_size,
            span,
            op_data_id: OperatorDataId::MAX_VALUE,
            outputs_start: OpOutputIdx::MAX_VALUE,
            outputs_end: OpOutputIdx::MAX_VALUE,
        }
    }
}

impl OperatorOffsetInChain {
    pub fn base_chain_offset(&self, sess: &SessionData) -> OffsetInChain {
        match self {
            OperatorOffsetInChain::Direct(chain_offset) => *chain_offset,
            OperatorOffsetInChain::AggregationMember(op_id, _agg_offset) => {
                sess.operator_bases[*op_id]
                    .offset_in_chain
                    .base_chain_offset(sess)
            }
        }
    }
}

impl OperatorData {
    pub fn from_custom(op: impl Operator + 'static) -> Self {
        Self::Custom(smallbox!(op))
    }
    pub fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        match self {
            OperatorData::Key(op) => setup_op_key(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Select(op) => setup_op_select(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Fork(op) => setup_op_fork(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),

            OperatorData::Chunks(op) => setup_op_chunks(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Call(op) => setup_op_call(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::CallConcurrent(op) => setup_op_call_concurrent(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Custom(op) => Operator::setup(
                &mut **op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Atom(op) => setup_op_atom(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
        }
    }
    pub fn has_dynamic_outputs(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> bool {
        match self {
            OperatorData::Atom(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Fork(_)
            | OperatorData::Select(_)
            | OperatorData::Chunks(_) => false,
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return false;
                };
                let &NestedOp::SetUp(op_id) = nested else {
                    unreachable!()
                };
                self.has_dynamic_outputs(sess, op_id)
            }
            OperatorData::Custom(op) => {
                Operator::has_dynamic_outputs(&**op, sess, op_id)
            }
        }
    }
    pub fn output_count(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> usize {
        #[allow(clippy::match_same_arms)]
        match &self {
            OperatorData::Call(_) => 1,
            OperatorData::CallConcurrent(_) => 1,
            OperatorData::Fork(_) => 0,
            OperatorData::Atom(_) => 0,
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return 0;
                };
                let &NestedOp::SetUp(op_id) = nested else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_count(sess, op_id)
            }
            OperatorData::Select(_) => 0,
            OperatorData::Chunks(_) => 0, // last sc output is output
            OperatorData::Custom(op) => {
                Operator::output_count(&**op, sess, op_id)
            }
        }
    }

    pub fn assign_op_outputs(
        &mut self,
        sess: &mut SessionData,
        ld: &mut LivenessData,
        op_id: OperatorId,
        output_count: &mut OpOutputIdx,
    ) {
        match self {
            OperatorData::Key(op) => {
                if let Some(nested_op) = &op.nested_op {
                    setup_op_outputs_for_nested_op(
                        nested_op,
                        sess,
                        ld,
                        op_id,
                        output_count,
                    );
                    return;
                }
            }
            OperatorData::Atom(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Fork(_)
            | OperatorData::Select(_)
            | OperatorData::Chunks(_) => (),
            OperatorData::Custom(op) => {
                op.assign_op_outputs(sess, ld, op_id, output_count)
            }
        }

        let op_output_count = self.output_count(sess, op_id);
        let op_base = &mut sess.operator_bases[op_id];
        op_base.outputs_start = *output_count;
        *output_count += OpOutputIdx::from_usize(op_output_count);
        op_base.outputs_end = *output_count;
        ld.append_op_outputs(op_output_count, op_id);
    }

    pub fn default_op_name(&self) -> OperatorName {
        match self {
            OperatorData::Atom(_) => "atom".into(),
            OperatorData::Fork(_) => "fork".into(),
            OperatorData::Chunks(_) => "chunks".into(),
            OperatorData::Key(_) => "key".into(),
            OperatorData::Select(_) => "select".into(),
            OperatorData::Call(_) => "call".into(),
            OperatorData::CallConcurrent(_) => "callcc".into(),
            OperatorData::Custom(op) => op.default_name(),
        }
    }
    pub fn debug_op_name(&self) -> OperatorName {
        match self {
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return self.default_op_name();
                };
                match nested {
                    NestedOp::Operator(nested_op) => {
                        format!(
                            "[ key '{}' {} ]",
                            op.key,
                            nested_op.0.debug_op_name()
                        )
                    }
                    NestedOp::SetUp(op_id) => {
                        format!("[ key '{}' <op {op_id:02}> ]", op.key)
                    }
                }
                .into()
            }

            OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Fork(_)
            | OperatorData::Atom(_)
            | OperatorData::Select(_)
            | OperatorData::Chunks(_) => self.default_op_name(),
            OperatorData::Custom(op) => op.debug_op_name(),
        }
    }
    pub fn output_field_kind(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> OutputFieldKind {
        match self {
            OperatorData::Call(_) | OperatorData::CallConcurrent(_) => {
                OutputFieldKind::Unique
            }
            OperatorData::Chunks(_) | OperatorData::Fork(_) => {
                OutputFieldKind::SameAsInput
            }
            OperatorData::Select(_) | OperatorData::Atom(_) => {
                OutputFieldKind::Unconfigured
            }
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return OutputFieldKind::SameAsInput;
                };
                let &NestedOp::SetUp(op_id) = nested else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_field_kind(sess, op_id)
            }
            OperatorData::Custom(op) => {
                Operator::output_field_kind(&**op, sess, op_id)
            }
        }
    }
    pub fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        sess: &SessionData,
        op_id: OperatorId,
    ) {
        match &self {
            OperatorData::Key(k) => {
                ld.add_var_name(k.key_interned.unwrap());
                if let Some(NestedOp::SetUp(op_id)) = k.nested_op {
                    sess.operator_data[sess.op_data_id(op_id)]
                        .register_output_var_names(ld, sess, op_id);
                }
            }
            OperatorData::Select(s) => {
                ld.add_var_name(s.key_interned.unwrap());
            }
            OperatorData::Custom(op) => {
                op.register_output_var_names(ld, sess, op_id)
            }
            OperatorData::Call(_)
            | OperatorData::Atom(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Fork(_)
            | OperatorData::Chunks(_) => (),
        }
    }
    pub fn update_liveness_for_op(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        op_offset_after_last_write: OffsetInChain,
        op_id: OperatorId,
        bb_id: BasicBlockId,
        input_field: OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        match &self {
            OperatorData::Atom(_) => {
                output.flags.may_dup_or_drop = false;
                output.flags.non_stringified_input_access = false;
                output.flags.input_accessed = false;
            }
            OperatorData::Fork(_)
            | OperatorData::Chunks(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_) => {
                output.call_effect = OperatorCallEffect::Diverge;
            }
            OperatorData::Key(key) => {
                if let Some(NestedOp::SetUp(nested_op_id)) = key.nested_op {
                    sess.operator_data[sess.op_data_id(nested_op_id)]
                        .update_liveness_for_op(
                            sess,
                            ld,
                            op_offset_after_last_write,
                            nested_op_id,
                            bb_id,
                            input_field,
                            output,
                        );
                }

                let var_id = ld.var_names[&key.key_interned.unwrap()];
                ld.vars_to_op_outputs_map[var_id] = output.primary_output;
                ld.op_outputs[output.primary_output]
                    .field_references
                    .push(input_field);
                if let Some(prev_tgt) =
                    ld.key_aliases_map.insert(var_id, input_field)
                {
                    ld.apply_var_remapping(var_id, prev_tgt);
                }
                output.primary_output = input_field;
                output.call_effect = OperatorCallEffect::NoCall;
            }
            OperatorData::Select(select) => {
                let mut var = ld.var_names[&select.key_interned.unwrap()];
                // resolve rebinds
                loop {
                    let field = ld.vars_to_op_outputs_map[var];
                    if field.into_usize() >= ld.vars.len() {
                        break;
                    }
                    // var points to itself
                    if field.into_usize() == var.into_usize() {
                        break;
                    }
                    // OpOutput indices below vars.len() are the vars
                    var = VarId::from_usize(field.into_usize());
                }
                output.primary_output = var.natural_output_idx();
                output.call_effect = OperatorCallEffect::NoCall;
            }

            OperatorData::Custom(op) => Operator::update_variable_liveness(
                &**op,
                sess,
                ld,
                op_offset_after_last_write,
                op_id,
                bb_id,
                input_field,
                output,
            ),
        }
    }
    pub fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        match self {
            OperatorData::CallConcurrent(op) => {
                setup_op_call_concurrent_liveness_data(op, op_id, ld)
            }
            OperatorData::Fork(op) => {
                setup_op_fork_liveness_data(op, op_id, ld)
            }
            OperatorData::Custom(op) => {
                op.on_liveness_computed(sess, ld, op_id)
            }
            OperatorData::Key(op) => {
                if let Some(NestedOp::SetUp(op_id)) = op.nested_op {
                    let op_data_id = sess.op_data_id(op_id);
                    let mut op_data =
                        std::mem::take(&mut sess.operator_data[op_data_id]);
                    op_data.on_liveness_computed(sess, ld, op_id);
                    sess.operator_data[op_data_id] = op_data;
                }
            }
            OperatorData::Call(_)
            | OperatorData::Atom(_)
            | OperatorData::Chunks(_)
            | OperatorData::Select(_) => (),
        }
    }

    pub fn operator_build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        mut tf_state: TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> Option<OperatorInstantiation> {
        let tfs = &mut tf_state;
        let jd = &mut job.job_data;
        let op_base = &jd.session_data.operator_bases[op_id];
        let data: TransformData<'a> = match self {
            OperatorData::Key(_)
            | OperatorData::Atom(_)
            | OperatorData::Select(_) => unreachable!(),
            OperatorData::Chunks(op) => {
                return Some(insert_tf_chunks(
                    job,
                    op,
                    tf_state,
                    op_base.chain_id,
                    op_id,
                    prebound_outputs,
                ));
            }
            OperatorData::Fork(op) => build_tf_fork(jd, op_base, op, tfs),
            OperatorData::Call(op) => build_tf_call(jd, op_base, op, tfs),
            OperatorData::CallConcurrent(op) => {
                build_tf_call_concurrent(jd, op_base, op, tfs)
            }

            OperatorData::Custom(op) => {
                match Operator::build_transforms(
                    &**op,
                    job,
                    tfs,
                    op_id,
                    prebound_outputs,
                ) {
                    TransformInstatiation::None => return None,
                    TransformInstatiation::Single(tf) => tf,
                    TransformInstatiation::Multiple(instantiation) => {
                        return Some(instantiation)
                    }
                }
            }
        };

        let next_input_field = tf_state.output_field;
        let next_group_track = tf_state.output_group_track_id;
        let next_match_set = tf_state.match_set_id;
        let tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            tf_state,
            data,
        );
        Some(OperatorInstantiation {
            tfs_begin: tf_id,
            tfs_end: tf_id,
            next_match_set,
            next_input_field,
            next_group_track,
        })
    }

    pub fn aggregation_member(
        &self,
        agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        match self {
            OperatorData::Key(op) => {
                if let Some(NestedOp::SetUp(op_id)) = op.nested_op {
                    return Some(op_id);
                }
                None
            }
            OperatorData::Custom(op) => op.aggregation_member(agg_offset),

            OperatorData::Atom(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Fork(_)
            | OperatorData::Select(_)
            | OperatorData::Chunks(_) => None,
        }
    }
}

pub enum TransformInstatiation<'a> {
    Single(TransformData<'a>),
    Multiple(OperatorInstantiation),
    None,
}

pub trait Operator: Send + Sync {
    fn default_name(&self) -> OperatorName;
    fn debug_op_name(&self) -> super::operator::OperatorName {
        self.default_name()
    }
    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
        OutputFieldKind::Unique
    }
    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize;
    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool;
    fn on_op_added(
        &mut self,
        _so: &mut SessionSetupData,
        _op_id: OperatorId,
        _add_to_chain: bool,
    ) {
    }
    fn on_subchains_added(&mut self, _curr_subchains_end: SubchainIndex) {}
    fn register_output_var_names(
        &self,
        _ld: &mut LivenessData,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) {
    }
    fn update_bb_for_op(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _op_id: OperatorId,
        _op_n: OffsetInChain,
        _cn: &Chain,
        _bb_id: BasicBlockId,
    ) -> bool {
        false
    }
    fn assign_op_outputs(
        &mut self,
        _sess: &mut SessionData,
        _ld: &mut LivenessData,
        _op_id: OperatorId,
        _output_count: &mut OpOutputIdx,
    ) {
    }
    fn aggregation_member(
        &self,
        _agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        None
    }

    // all of the &mut bool flags default to true
    // turning them to false allows for some pipeline optimizations
    // but may cause incorrect behavior if the promises made are broken later
    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        _output: &mut OperatorLivenessOutput,
    ) {
    }
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
    }
    fn on_liveness_computed(
        &mut self,
        _sess: &mut SessionData,
        _ld: &LivenessData,
        _op_id: OperatorId,
    ) {
    }
    // While lifetimes can be elided here, which is nice for simple TFs,
    // it's good to remember that `TransformData<'a>` comes from `&'a self`
    // and can take full advantage of that for sharing state between instances
    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a>;
}
