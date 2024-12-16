use crate::{
    job::{add_transform_to_job, Job, JobData},
    record_data::{action_buffer::ActorId, field_action::FieldActionKind},
};

use super::transform::{
    Transform, TransformData, TransformId, TransformState,
};

pub struct TfTerminator {
    actor_id: ActorId,
    delayed_deletion_row_count: usize,
}

pub fn setup_tf_terminator<'a>(
    jd: &mut JobData<'a>,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let mut ab = jd.match_set_mgr.match_sets[tf_state.match_set_id]
        .action_buffer
        .borrow_mut();
    TransformData::from_custom(TfTerminator {
        actor_id: ab.add_actor(),
        delayed_deletion_row_count: 0,
    })
}

pub fn add_terminator(sess: &mut Job, last_tf_id: TransformId) -> TransformId {
    let term_tf_id = sess.job_data.tf_mgr.transforms.peek_claim_id();
    let last_tf = &mut sess.job_data.tf_mgr.transforms[last_tf_id];
    debug_assert!(last_tf.successor.is_none());
    last_tf.successor = Some(term_tf_id);
    let dummy_field = sess
        .job_data
        .match_set_mgr
        .get_dummy_field(last_tf.match_set_id);
    let tf_state = TransformState::new(
        dummy_field,
        dummy_field,
        last_tf.match_set_id,
        last_tf.desired_batch_size,
        None,
        last_tf.output_group_track_id,
    );
    sess.job_data.field_mgr.inc_field_refcount(dummy_field, 2);
    let tf_data = setup_tf_terminator(&mut sess.job_data, &tf_state);
    add_transform_to_job(
        &mut sess.job_data,
        &mut sess.transform_data,
        tf_state,
        tf_data,
    )
}

impl<'a> Transform<'a> for TfTerminator {
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let done = tf.done;
        let mut ab = jd.match_set_mgr.match_sets[tf.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        let rows_to_drop;
        if tf.successor.is_some() {
            rows_to_drop = self.delayed_deletion_row_count;
            self.delayed_deletion_row_count = batch_size;
        } else {
            rows_to_drop = self.delayed_deletion_row_count + batch_size;
            self.delayed_deletion_row_count = 0;
        }
        ab.push_action(FieldActionKind::Drop, 0, rows_to_drop);
        ab.end_action_group();
        if self.delayed_deletion_row_count > 0 {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        if !done {
            jd.tf_mgr.submit_batch(
                tf_id,
                batch_size,
                ps.group_to_truncate,
                ps.input_done,
            );
        }
    }
}
