use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    job::JobData,
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
};

use super::{
    errors::OperatorCreationError,
    nop_copy::create_op_nop_copy,
    operator::{Operator, OperatorDataId, OperatorId, OperatorOffsetInChain},
    transform::{Transform, TransformData, TransformId, TransformState},
};

#[derive(Clone, Default)]
pub struct OpNop {}

#[derive(Clone, Default)]
pub struct TfNop {}

pub fn parse_op_nop(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    if expr.require_at_most_one_plaintext_arg()? == Some(b"-c") {
        Ok(create_op_nop_copy())
    } else {
        Ok(create_op_nop())
    }
}
pub fn create_op_nop() -> Box<dyn Operator> {
    Box::new(OpNop::default())
}

pub fn setup_op_nop(
    _op: &mut OpNop,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
}

pub fn build_tf_nop<'a>(
    _op: &OpNop,
    _tf_state: &TransformState,
) -> TransformData<'a> {
    create_tf_nop()
}

pub fn create_tf_nop<'a>() -> TransformData<'a> {
    Box::new(TfNop {})
}

impl Operator for OpNop {
    fn default_name(&self) -> super::operator::OperatorName {
        "nop".into()
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

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        super::operator::OutputFieldKind::SameAsInput
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut crate::job::Job<'a>,
        _tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        super::operator::TransformInstatiation::Single(Box::new(TfNop {}))
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.input_accessed = false;
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
    }
}

impl<'a> Transform<'a> for TfNop {
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        jd.tf_mgr.submit_batch(
            tf_id,
            batch_size,
            ps.group_to_truncate,
            ps.input_done,
        );
    }
}
