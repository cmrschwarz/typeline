use crate::{
    chain::Chain, job_session::JobData, options::argument::CliArgIdx,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
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
    Ok(OperatorData::Nop(OpNop {}))
}
pub fn create_op_nop() -> OperatorData {
    OperatorData::Nop(OpNop {})
}

pub fn setup_op_nop(
    _chain: &Chain,
    op_base: &mut OperatorBase,
    _op: &mut OpNop,
    _op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if !op_base.append_mode {
        op_base.transparent_mode = true;
    }
    Ok(())
}

pub fn setup_tf_nop(tf_state: &mut TransformState) -> TransformData<'static> {
    assert!(tf_state.is_transparent);
    TransformData::Nop(TfNop {})
}

pub fn handle_tf_nop(
    sess: &mut JobData,
    tf_id: TransformId,
    _sel: &mut TfNop,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}
