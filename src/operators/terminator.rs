use crate::{
    field_data::command_buffer::{ActionProducingFieldIndex, FieldActionKind},
    job_session::JobData,
};

use super::transform::{TransformData, TransformId, TransformState};

pub struct OpTerminator {}
pub struct TfTerminator {
    apf_idx: ActionProducingFieldIndex,
}

pub fn setup_tf_terminator(
    sess: &mut JobData,
    tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Terminator(TfTerminator {
        apf_idx: sess.match_set_mgr.match_sets[tf_state.match_set_id]
            .command_buffer
            .claim_apf(tf_state.ordering_id),
    })
}

pub fn handle_tf_terminator(
    sess: &mut JobData,
    tf_id: TransformId,
    t1000: &mut TfTerminator,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    debug_assert!(tf.successor.is_none());
    let cb =
        &mut sess.match_set_mgr.match_sets[tf.match_set_id].command_buffer;
    cb.begin_action_list(t1000.apf_idx);
    cb.push_action_with_usize_rl(
        t1000.apf_idx,
        FieldActionKind::Drop,
        0,
        batch_size,
    );
    cb.end_action_list(t1000.apf_idx);
    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
    }
}
