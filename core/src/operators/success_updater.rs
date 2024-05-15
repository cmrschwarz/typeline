#[derive(Clone, Default)]
pub struct OpSuccessUpdator {}
pub struct TfSuccessUpdator {
    iter_id: IterId,
    success: bool,
}

use crate::{
    chain::Chain,
    cli::reject_operator_argument,
    job::JobData,
    options::argument::CliArgIdx,
    record_data::{
        field_data::field_value_flags, field_value_ref::FieldValueSlice,
        iter_hall::IterId,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

pub fn parse_op_success_updator(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_argument("success_updator", value, arg_idx)?;
    Ok(create_op_success_updator())
}
pub fn create_op_success_updator() -> OperatorData {
    OperatorData::SuccessUpdator(OpSuccessUpdator::default())
}

pub fn setup_op_success_updator(
    _chain: &Chain,
    op_base: &mut OperatorBase,
    _op: &mut OpSuccessUpdator,
    _op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    op_base.transparent_mode = true;
    Ok(())
}

pub fn build_tf_success_updator<'a>(
    jd: &mut JobData,
    _op: &OpSuccessUpdator,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let su = TfSuccessUpdator {
        iter_id: jd.add_iter_for_tf_state(tf_state),
        success: true,
    };
    TransformData::SuccessUpdator(su)
}

pub fn handle_tf_success_updator(
    jd: &mut JobData,
    tf_id: TransformId,
    su: &mut TfSuccessUpdator,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];

    let field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);
    let mut iter =
        jd.field_mgr
            .get_auto_deref_iter(tf.input_field, &field, su.iter_id);

    // PERF: we could optimize this
    let mut rem = batch_size;
    while let Some(range) = iter.typed_range_fwd(
        &jd.match_set_mgr,
        rem,
        field_value_flags::DEFAULT,
    ) {
        if matches!(range.base.data, FieldValueSlice::Error(_)) {
            su.success = false;
        }
        rem -= range.base.field_count;
    }

    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
    if ps.input_done && !su.success {
        jd.session_data.set_success(false)
    }
}
