use std::{any::Any, collections::HashMap};

use smallstr::SmallString;

use indexland::{index_newtype, Idx};

use crate::{
    chain::{Chain, ChainId},
    cli::call_expr::Span,
    context::SessionData,
    job::{add_transform_to_job, Job},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
    },
    options::session_setup::SessionSetupData,
    record_data::{
        field::FieldId, group_track::GroupTrackId, match_set::MatchSetId,
    },
    typeline_error::TypelineError,
    utils::identity_hasher::BuildIdentityHasher,
};

use super::transform::{TransformData, TransformId, TransformState};

index_newtype! {
    pub struct OperatorId(u32);
    pub struct OperatorDataId(u32);
    pub struct OffsetInChain(u32);
    pub struct OffsetInAggregation(u32);
    pub struct OffsetInChainOptions(u32);
}

pub type PreboundOutputsMap =
    HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>;

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

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub enum InputFieldKind {
    #[default]
    LastOutput,
    Dummy,
    Unconfigured,
}

pub type OperatorName = SmallString<[u8; 32]>;

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
            op_data_id: OperatorDataId::MAX,
            outputs_start: OpOutputIdx::MAX,
            outputs_end: OpOutputIdx::MAX,
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
    fn input_field_kind(&self) -> InputFieldKind {
        InputFieldKind::LastOutput
    }
    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }
    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

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
        sess: &mut SessionData,
        ld: &mut LivenessData,
        op_id: OperatorId,
        output_count: &mut OpOutputIdx,
    ) {
        let op_output_count = self.output_count(sess, op_id);
        let op_base = &mut sess.operator_bases[op_id];
        op_base.outputs_start = *output_count;
        *output_count += OpOutputIdx::from_usize(op_output_count);
        op_base.outputs_end = *output_count;
        ld.append_op_outputs(op_output_count, op_id);
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
        output: &mut OperatorLivenessOutput,
    ) {
        match self.input_field_kind() {
            InputFieldKind::LastOutput => (),
            InputFieldKind::Dummy => output.flags.input_accessed = false,
            InputFieldKind::Unconfigured => {
                unimplemented!()
            }
        }
    }
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
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

    fn as_any(&self) -> Option<&dyn Any> {
        None
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        None
    }
}

impl dyn Operator {
    pub fn build_transforms_expand_single<'a>(
        &'a self,
        job: &mut Job<'a>,
        mut tf_state: TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> Option<OperatorInstantiation> {
        match self.build_transforms(
            job,
            &mut tf_state,
            op_id,
            prebound_outputs,
        ) {
            TransformInstatiation::Single(data) => {
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
            TransformInstatiation::Multiple(inst) => Some(inst),
            TransformInstatiation::None => None,
        }
    }
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().and_then(|t| t.downcast_ref::<T>())
    }

    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.as_any_mut().and_then(|t| t.downcast_mut::<T>())
    }
}
