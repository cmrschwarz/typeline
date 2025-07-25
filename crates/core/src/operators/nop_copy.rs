use crate::{
    job::JobData,
    liveness_analysis::LivenessData,
    record_data::{
        field::FieldRefOffset, iter_hall::FieldIterId,
        push_interface::PushInterface,
    },
};

use super::{
    operator::{Operator, OperatorId},
    transform::{Transform, TransformId, TransformState},
    utils::basic_transform_update::{basic_transform_update, BasicUpdateData},
};

// the main purpose of this op is as a helper operation for an aggregation
// at the start of a chain, e.g  `tl seqn=10 fork +int=11 p`

#[derive(Clone, Default)]
pub struct OpNopCopy {
    may_consume_input: bool,
}
pub struct TfNopCopy {
    #[allow(unused)] // TODO
    may_consume_input: bool,
    input_iter_id: FieldIterId,
    input_field_ref_offset: FieldRefOffset,
}

pub fn create_op_nop_copy() -> Box<dyn Operator> {
    Box::new(OpNopCopy::default())
}

impl Operator for OpNopCopy {
    fn default_name(&self) -> super::operator::OperatorName {
        "nop-c".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        1
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        job.job_data.field_mgr.setup_field_refs(
            &mut job.job_data.match_set_mgr,
            tf_state.input_field,
        );
        let input_field_ref_offset =
            job.job_data.field_mgr.register_field_reference(
                tf_state.output_field,
                tf_state.input_field,
            );
        let tfc = TfNopCopy {
            may_consume_input: self.may_consume_input,
            input_iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
            input_field_ref_offset,
        };

        super::operator::TransformInstatiation::Single(Box::new(tfc))
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        ld: &mut LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
        ld.op_outputs[output.primary_output]
            .field_references
            .push(input_field);
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut crate::context::SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        self.may_consume_input = ld.can_consume_nth_access(op_id, 0);
    }
}

impl TfNopCopy {
    fn basic_update(&self, bud: &mut BasicUpdateData) -> (usize, bool) {
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            inserter.extend_from_ref_aware_range_smart_ref(
                range,
                true,
                false,
                true,
                self.input_field_ref_offset,
            );
        }
        (bud.batch_size, bud.ps.input_done)
    }
}

impl<'a> Transform<'a> for TfNopCopy {
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        basic_transform_update(
            jd,
            tf_id,
            [],
            self.input_iter_id,
            |mut bud| self.basic_update(&mut bud),
        );
    }
}
