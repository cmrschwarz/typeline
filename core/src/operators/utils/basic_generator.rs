use crate::{
    job::JobData,
    liveness_analysis::OperatorLivenessOutput,
    operators::{
        operator::{
            Operator, OperatorData, OperatorName, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
        },
    },
    record_data::{
        action_buffer::ActorId, group_track::GroupTrackIterRef,
        iter_hall::IterId,
    },
    smallbox,
};

use super::generator_transform_update::{
    handle_generator_transform_update, GeneratorMode, GeneratorSequence,
};

pub trait BasicGenerator: Send + Sync {
    type Gen: GeneratorSequence + Send;
    fn default_name(&self) -> OperatorName;
    fn debug_op_name(&self) -> crate::operators::operator::OperatorName {
        self.default_name()
    }
    fn generator_mode(&self) -> GeneratorMode;
    fn create_generator(&self) -> Self::Gen;
    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: crate::operators::operator::OffsetInChain,
        _op_id: crate::operators::operator::OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        _outputs_offset: usize,
        output: &mut OperatorLivenessOutput,
    ) {
        output.flags.non_stringified_input_access = false;
        output.flags.may_dup_or_drop = true;
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut crate::context::SessionData,
        _ld: &crate::liveness_analysis::LivenessData,
        _op_id: crate::operators::operator::OperatorId,
    ) {
    }
}

pub struct BasicGeneratorWrapper<T> {
    base: T,
}

pub struct BasicGeneratorTransform<'a, Op: BasicGenerator> {
    op: &'a BasicGeneratorWrapper<Op>,
    input_iter: IterId,
    group_track_iter: GroupTrackIterRef,
    actor_id: ActorId,
    generator: Op::Gen,
}

impl<'a, Op: BasicGenerator> Transform<'a>
    for BasicGeneratorTransform<'a, Op>
{
    fn display_name(&self) -> DefaultTransformName {
        self.op.debug_op_name()
    }

    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        handle_generator_transform_update(
            jd,
            tf_id,
            self.input_iter,
            self.actor_id,
            self.group_track_iter,
            &mut self.generator,
            self.op.base.generator_mode(),
        )
    }
}

impl<T: BasicGenerator> Operator for BasicGeneratorWrapper<T> {
    fn default_name(&self) -> OperatorName {
        BasicGenerator::default_name(&self.base)
    }

    fn debug_op_name(&self) -> crate::operators::operator::OperatorName {
        BasicGenerator::default_name(&self.base)
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: crate::operators::operator::OperatorId,
    ) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: crate::operators::operator::OperatorId,
    ) -> bool {
        false
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut crate::operators::transform::TransformState,
        _op_id: crate::operators::operator::OperatorId,
        _prebound_outputs: &crate::operators::operator::PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;
        let actor_id = jd.add_actor_for_tf_state(tf_state);
        let input_iter = jd.claim_iter_for_tf_state(tf_state);
        let group_track_iter =
            jd.claim_group_track_iter_for_tf_state(tf_state);
        TransformInstatiation::Single(TransformData::Custom(smallbox!(
            BasicGeneratorTransform {
                op: self,
                input_iter,
                group_track_iter,
                actor_id,
                generator: self.base.create_generator()
            }
        )))
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::operator::OutputFieldKind {
        crate::operators::operator::OutputFieldKind::Unique
    }

    fn update_variable_liveness(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_offset_after_last_write: crate::operators::operator::OffsetInChain,
        op_id: crate::operators::operator::OperatorId,
        bb_id: crate::liveness_analysis::BasicBlockId,
        input_field: crate::liveness_analysis::OpOutputIdx,
        outputs_offset: usize,
        output: &mut OperatorLivenessOutput,
    ) {
        self.base.update_variable_liveness(
            sess,
            ld,
            op_offset_after_last_write,
            op_id,
            bb_id,
            input_field,
            outputs_offset,
            output,
        )
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &crate::liveness_analysis::LivenessData,
        op_id: crate::operators::operator::OperatorId,
    ) {
        self.base.on_liveness_computed(sess, ld, op_id)
    }
}

impl<T: BasicGenerator + 'static> BasicGeneratorWrapper<T> {
    pub fn new(base: T) -> Self {
        Self { base }
    }
    pub fn new_operator(base: T) -> OperatorData {
        OperatorData::Custom(smallbox!(Self::new(base)))
    }
}
