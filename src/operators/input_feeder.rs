use crate::{
    job_session::{JobData, JobSession},
    record_data::{
        command_buffer::ActorId, field::FieldId,
        field_action::FieldActionKind, iter_hall::IterId,
        iters::FieldIterator, match_set::MatchSetId,
    },
};

use super::{
    operator::OperatorId,
    transform::{TransformData, TransformId, TransformState},
};

pub struct TfInputFeeder {
    actor_id: ActorId,
    iter_id: IterId,
}

pub fn setup_tf_input_feeder(
    sess: &mut JobData,
    tf_state: &TransformState,
) -> TransformData<'static> {
    let iter_id = sess.field_mgr.fields[tf_state.input_field]
        .borrow_mut()
        .iter_hall
        .claim_iter();
    let actor_id = sess.match_set_mgr.match_sets[tf_state.match_set_id]
        .action_buffer
        .add_actor();
    TransformData::InputFeeder(TfInputFeeder { actor_id, iter_id })
}

pub fn setup_tf_input_feeder_as_input(
    sess: &mut JobSession<'_>,
    ms_id: MatchSetId,
    start_op: OperatorId,
    input_field_id: FieldId,
) -> TransformId {
    let tf_state = TransformState::new(
        input_field_id,
        input_field_id,
        ms_id,
        sess.get_op_debault_batch_size(start_op),
        None,
        None,
    );
    sess.job_data
        .field_mgr
        .inc_field_refcount(input_field_id, 2);
    let tf_data = setup_tf_input_feeder(&mut sess.job_data, &tf_state);
    sess.add_transform(tf_state, tf_data)
}

pub fn handle_tf_input_feeder(
    sess: &mut JobData,
    tf_id: TransformId,
    feeder: &TfInputFeeder,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_field = sess.field_mgr.get_cow_field_ref(
        &mut sess.match_set_mgr,
        tf.input_field,
        tf.has_unconsumed_input(),
    );
    let mut iter = sess.field_mgr.lookup_iter(
        tf.input_field,
        &input_field,
        feeder.iter_id,
    );

    let rem = iter.get_next_field_pos();
    if rem > 0 {
        let cb =
            &mut sess.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
        cb.begin_action_group(feeder.actor_id);
        cb.push_action(FieldActionKind::Drop, 0, rem);
        cb.end_action_group();
    }

    iter.next_n_fields(batch_size);
    sess.field_mgr
        .store_iter(tf.input_field, feeder.iter_id, iter);
    drop(input_field);
    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
        sess.tf_mgr.update_ready_state(tf_id);
    }
}
