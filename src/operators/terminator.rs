use crate::{
    job_session::JobData,
    record_data::{command_buffer::ActorId, field_action::FieldActionKind},
};

use super::transform::{TransformData, TransformId, TransformState};

pub struct TfTerminator {
    actor_id: ActorId,
}

pub fn setup_tf_terminator(
    sess: &mut JobData,
    tf_state: &mut TransformState,
) -> TransformData<'static> {
    let cb = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
        .action_buffer;
    TransformData::Terminator(TfTerminator {
        actor_id: cb.add_actor(),
    })
}

pub fn handle_tf_terminator(
    sess: &mut JobData,
    tf_id: TransformId,
    tt: &TfTerminator,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let ab = &mut sess.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
    ab.begin_action_group(tt.actor_id);
    ab.push_action(FieldActionKind::Drop, 0, batch_size);
    ab.end_action_group();
    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}
