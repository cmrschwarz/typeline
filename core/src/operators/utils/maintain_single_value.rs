use crate::{
    job::{JobData, PipelineState},
    operators::transform::TransformId,
    record_data::{
        action_buffer::ActorId, field_action::FieldActionKind,
        iter::field_iterator::FieldIterator, iter_hall::FieldIterId,
    },
};

pub struct ExplicitCount {
    pub count: usize,
    pub actor_id: ActorId,
}

pub fn maintain_single_value(
    jd: &mut JobData,
    tf_id: TransformId,
    explicit_count: Option<&ExplicitCount>,
    iter_id: FieldIterId,
) -> (usize, PipelineState) {
    let (batch_size, mut ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let output_field = tf.output_field;

    let mut count = batch_size;
    if let Some(ec) = explicit_count {
        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);
        let iter =
            jd.field_mgr
                .lookup_iter(tf.input_field, &input_field, iter_id);
        let mut pos = iter.get_next_field_pos();
        let mut ab = jd.match_set_mgr.match_sets[tf.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(ec.actor_id);
        for _ in 0..batch_size {
            ab.push_action(FieldActionKind::Dup, pos, ec.count - 1);
            pos += ec.count;
        }
        ab.end_action_group();
        jd.field_mgr.store_iter(tf.input_field, iter_id, iter);
        count *= ec.count;
    } else if tf.is_split && batch_size >= 1 {
        count = 1;
        jd.tf_mgr.unclaim_batch_size(tf_id, batch_size - 1);
        ps.input_done = true;
    }
    let mut output_field = jd.field_mgr.fields[output_field].borrow_mut();
    if ps.input_done && count == 0 {
        output_field.iter_hall.drop_last_value(1);
    } else {
        output_field
            .iter_hall
            .dup_last_value(count - usize::from(ps.input_done));
    }
    (count, ps)
}
