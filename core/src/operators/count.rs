use crate::{
    cli::call_expr::CallExpr, job::JobData,
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

pub fn build_tf_count<'a>(
    _jd: &mut JobData,
    _op_base: &OperatorBase,
    _op: &OpCount,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
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
        .push_int(i64::try_from(count.count).unwrap(), 1, false, false);

    jd.tf_mgr.submit_batch(tf_id, 1, true);
}

pub fn parse_op_count(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(OperatorData::Count(OpCount {}))
}
