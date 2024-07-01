use crate::{
    chain::ChainId, cli::call_expr::CallExpr, context::SessionSetupData,
    job::JobData, options::operator_base_options::OperatorBaseOptionsInterned,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
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
    if expr
        .require_at_most_one_arg()?
        .map(|v| &**v == b"-c")
        .unwrap_or(false)
    {
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
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    mut op_base_opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    op_base_opts_interned.transparent_mode = true;
    Ok(sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        op_base_opts_interned,
        op_data_id,
    ))
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
