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
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorOffsetInChain,
    },
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Default)]
pub struct OpNop {}
pub struct TfNop {}

pub fn parse_op_nop(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    if expr.require_at_most_one_plaintext_arg()? == Some(b"-c") {
        Ok(create_op_nop_copy())
    } else {
        Ok(create_op_nop())
    }
}
pub fn create_op_nop() -> OperatorData {
    OperatorData::Nop(OpNop::default())
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
    tf_state: &TransformState,
) -> TransformData<'a> {
    debug_assert!(tf_state.is_transparent);
    create_tf_nop()
}

pub fn create_tf_nop<'a>() -> TransformData<'a> {
    TransformData::Nop(TfNop {})
}

pub fn handle_tf_nop(jd: &mut JobData, tf_id: TransformId, _nop: &TfNop) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
