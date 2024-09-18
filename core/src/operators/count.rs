use crate::{
    cli::call_expr::CallExpr,
    job::JobData,
    record_data::{group_track::GroupTrackIterRef, iter_hall::IterKind},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCount {}
pub struct TfCount {
    count: i64,
    iter: GroupTrackIterRef,
}

pub fn build_tf_count<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    _op: &OpCount,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Count(TfCount {
        count: 0,
        iter: jd.group_track_manager.claim_group_track_iter_ref(
            tf_state.input_group_track_id,
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
        ),
    })
}

pub fn handle_tf_count(
    jd: &mut JobData,
    tf_id: TransformId,
    tfc: &mut TfCount,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);

    let mut iter = jd
        .group_track_manager
        .lookup_group_track_iter(tfc.iter, &jd.match_set_mgr);

    let mut groups_emitted = 0;

    if iter.is_invalid() {
        jd.tf_mgr.submit_batch(
            tf_id,
            groups_emitted,
            ps.group_to_truncate,
            ps.input_done,
        );
        return;
    }

    let output_field_id = jd.tf_mgr.transforms[tf_id].output_field;
    let mut output_field = jd.field_mgr.fields[output_field_id].borrow_mut();
    let mut inserter =
        output_field.iter_hall.fixed_size_type_inserter::<i64>();

    let mut batch_size_rem = batch_size;
    let mut count = tfc.count;
    loop {
        let gl_rem = iter.group_len_rem();
        let consumed = iter.next_n_fields(gl_rem.min(batch_size_rem));
        count += consumed as i64;
        batch_size_rem -= consumed;

        if !iter.is_end_of_group(ps.input_done) {
            break;
        }
        inserter.push(count);
        groups_emitted += 1;
        count = 0;
        if !iter.try_next_group() {
            break;
        }
        let zero_count = iter.skip_empty_groups();
        if zero_count > 0 {
            inserter.push_with_rl(0, zero_count);
            groups_emitted += zero_count;
            if !iter.is_end_of_group(ps.input_done) {
                break;
            }
        }
    }
    tfc.count = count;
    iter.store_iter(tfc.iter.iter_id);
    jd.tf_mgr.submit_batch(
        tf_id,
        groups_emitted,
        ps.group_to_truncate,
        ps.input_done,
    );
}

pub fn parse_op_count(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(OperatorData::Count(OpCount {}))
}

pub fn create_op_count() -> OperatorData {
    OperatorData::Count(OpCount {})
}
