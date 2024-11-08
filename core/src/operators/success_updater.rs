#[derive(Clone, Default)]
pub struct OpSuccessUpdator {}
pub struct TfSuccessUpdator {
    iter_id: IterId,
    success: bool,
}

use crate::{
    cli::call_expr::CallExpr,
    job::JobData,
    record_data::{
        field_value_ref::FieldValueSlice, iter_hall::IterId,
        iters::FieldIterOpts,
    },
};

use super::{
    errors::OperatorCreationError,
    operator::{
        Operator, OperatorData, OperatorId, OutputFieldKind,
        TransformInstatiation,
    },
    transform::{Transform, TransformData, TransformId, TransformState},
};

impl Operator for OpSuccessUpdator {
    fn default_name(&self) -> super::operator::OperatorName {
        "success_updator".into()
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
        OutputFieldKind::SameAsInput
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        _outputs_offset: usize,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let su = TfSuccessUpdator {
            iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
            success: true,
        };
        TransformInstatiation::Single(TransformData::from_custom(su))
    }
}

impl Transform<'_> for TfSuccessUpdator {
    fn display_name(&self) -> super::transform::DefaultTransformName {
        "success_updator".into()
    }

    fn update(&mut self, jd: &mut JobData<'_>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        let tf = &mut jd.tf_mgr.transforms[tf_id];

        let field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);
        let mut iter = jd.field_mgr.get_auto_deref_iter(
            tf.input_field,
            &field,
            self.iter_id,
        );

        // PERF: we could optimize this
        let mut rem = batch_size;
        while let Some(range) = iter.typed_range_fwd(
            &jd.match_set_mgr,
            rem,
            FieldIterOpts::default(),
        ) {
            if matches!(range.base.data, FieldValueSlice::Error(_)) {
                self.success = false;
            }
            rem -= range.base.field_count;
        }

        jd.tf_mgr.submit_batch(
            tf_id,
            batch_size,
            ps.group_to_truncate,
            ps.input_done,
        );
        if ps.input_done && !self.success {
            jd.session_data.set_success(false)
        }
    }
}

pub fn parse_op_success_updator(
    arg: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    arg.reject_args()?;
    Ok(create_op_success_updator())
}
pub fn create_op_success_updator() -> OperatorData {
    OperatorData::from_custom(OpSuccessUpdator::default())
}
