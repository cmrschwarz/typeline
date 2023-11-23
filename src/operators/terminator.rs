use crate::{
    job_session::JobData,
    record_data::{command_buffer::ActorId, field_action::FieldActionKind},
};

use super::transform::{TransformData, TransformId, TransformState};

pub struct TfTerminator {
    actor_id: ActorId,
    delayed_deletion_row_count: usize,
}

pub fn setup_tf_terminator(
    sess: &mut JobData,
    tf_state: &TransformState,
) -> TransformData<'static> {
    let cb = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
        .action_buffer;
    TransformData::Terminator(TfTerminator {
        actor_id: cb.add_actor(),
        delayed_deletion_row_count: 0,
    })
}

pub fn handle_tf_terminator(
    sess: &mut JobData,
    tf_id: TransformId,
    tft: &mut TfTerminator,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let ab = &mut sess.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
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
    if input_done && tft.delayed_deletion_row_count == 0 {
        sess.unlink_transform(tf_id, batch_size);
        return;
    }
    if tft.delayed_deletion_row_count > 0 {
        sess.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, batch_size);
}
