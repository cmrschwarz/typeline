use crate::{
    job::JobData, options::argument::CliArgIdx,
    record_data::push_interface::PushInterface,
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

pub fn build_tf_count(
    _jd: &mut JobData,
    _op_base: &OperatorBase,
    _op: &OpCount,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Count(TfCount { count: 0 })
}

pub fn handle_tf_count(
    jd: &mut JobData,
    tf_id: TransformId,
    count: &mut TfCount,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    count.count += batch_size; // TODO: maybe handle overflow?
    if !ps.input_done {
        return;
    }
    let output_field_id = jd.tf_mgr.transforms[tf_id].output_field;
    jd.field_mgr.fields[output_field_id]
        .borrow_mut()
        .iter_hall
        .push_int(count.count as i64, 1, false, false);

    jd.tf_mgr.submit_batch(tf_id, 1, true);
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
