use crate::{
    chain::Chain, job_session::JobData, options::argument::CliArgIdx,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

//TODO: get rid of manual unlink once we have the aggregator
#[derive(Clone, Default)]
pub struct OpNop {
    pub manual_unlink: bool,
}
pub struct TfNop {
    pub manual_unlink: bool,
}

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
    if !op_base.append_mode {
        op_base.transparent_mode = true;
    }
    Ok(())
}

pub fn build_tf_nop(
    op: &OpNop,
    tf_state: &TransformState,
) -> TransformData<'static> {
    assert!(tf_state.is_transparent);
    create_tf_nop(op.manual_unlink)
}

pub fn create_tf_nop(manual_unlink: bool) -> TransformData<'static> {
    TransformData::Nop(TfNop { manual_unlink })
}

pub fn handle_tf_nop(sess: &mut JobData, tf_id: TransformId, nop: &TfNop) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    if input_done && !nop.manual_unlink {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}
