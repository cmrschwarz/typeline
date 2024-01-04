use crate::{chain::Chain, job::JobData, options::argument::CliArgIdx};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Default)]
pub struct OpNop {}
pub struct TfNop {}

pub fn parse_op_nop(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(create_op_nop())
}
pub fn create_op_nop() -> OperatorData {
    OperatorData::Nop(OpNop::default())
}

pub fn setup_op_nop(
    _chain: &Chain,
    op_base: &mut OperatorBase,
    _op: &mut OpNop,
    _op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    op_base.transparent_mode = true;
    Ok(())
}

pub fn build_tf_nop(
    _op: &OpNop,
    tf_state: &TransformState,
) -> TransformData<'static> {
    debug_assert!(tf_state.is_transparent);
    create_tf_nop()
}

pub fn create_tf_nop() -> TransformData<'static> {
    TransformData::Nop(TfNop {})
}

pub fn handle_tf_nop(jd: &mut JobData, tf_id: TransformId, _nop: &TfNop) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
