use crate::{
    job::{add_transform_to_job, Job, JobData},
    record_data::{
        action_buffer::ActorId, field::VOID_FIELD_ID,
        field_action::FieldActionKind, match_set::MatchSetId,
    },
};

use super::transform::{TransformData, TransformId, TransformState};

pub struct TfTerminator {
    actor_id: ActorId,
    delayed_deletion_row_count: usize,
}

pub fn setup_tf_terminator(
    jd: &mut JobData,
    tf_state: &TransformState,
) -> TransformData<'static> {
    let cb =
        &mut jd.match_set_mgr.match_sets[tf_state.match_set_id].action_buffer;
    TransformData::Terminator(TfTerminator {
        actor_id: cb.add_actor(),
        delayed_deletion_row_count: 0,
    })
}

pub fn handle_tf_terminator(
    jd: &mut JobData,
    tf_id: TransformId,
    tft: &mut TfTerminator,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let done = tf.done;
    let ab = &mut jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
    ab.begin_action_group(tft.actor_id);
    let rows_to_drop;
    if tf.successor.is_some() {
        rows_to_drop = tft.delayed_deletion_row_count;
        tft.delayed_deletion_row_count = batch_size;
    } else {
        rows_to_drop = tft.delayed_deletion_row_count + batch_size;
        tft.delayed_deletion_row_count = 0;
    }
    ab.push_action(FieldActionKind::Drop, 0, rows_to_drop);
    ab.end_action_group();
    if tft.delayed_deletion_row_count > 0 {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    if !done {
        jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
    }
}

pub fn add_terminator(
    sess: &mut Job,
    ms_id: MatchSetId,
    last_tf: TransformId,
) -> TransformId {
    let bs = sess.job_data.tf_mgr.transforms[last_tf].desired_batch_size;
    let tf_state =
        TransformState::new(VOID_FIELD_ID, VOID_FIELD_ID, ms_id, bs, None);
    sess.job_data.field_mgr.inc_field_refcount(VOID_FIELD_ID, 2);
    let tf_data = setup_tf_terminator(&mut sess.job_data, &tf_state);
    let tf_id = add_transform_to_job(
        &mut sess.job_data,
        &mut sess.transform_data,
        tf_state,
        tf_data,
    );
    let pred = &mut sess.job_data.tf_mgr.transforms[last_tf];
    debug_assert!(pred.successor.is_none());
    pred.successor = Some(tf_id);
    tf_id
}
