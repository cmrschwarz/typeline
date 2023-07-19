use crate::{
    field_data::push_interface::PushInterface, options::argument::CliArgIdx,
    worker_thread_session::JobSession,
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCount {}
pub struct TfCount {
    count: usize,
}

pub fn setup_tf_count(
    _sess: &mut JobSession,
    _op_base: &OperatorBase,
    _op: &OpCount,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Count(TfCount { count: 0 })
}

pub fn handle_tf_count(sess: &mut JobSession, tf_id: TransformId, count: &mut TfCount) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    count.count += batch_size; //TODO: maybe handle overflow?
    if input_done {
        let output_field_id = sess.tf_mgr.transforms[tf_id].output_field;
        sess.field_mgr.fields[output_field_id]
            .borrow_mut()
            .field_data
            .push_int(count.count as i64, 1, false, false);
        sess.unlink_transform(tf_id, 1);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
    }
}

pub fn parse_op_count(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(OperatorData::Count(OpCount {}))
}
