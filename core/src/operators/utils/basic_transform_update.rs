use crate::{
    context::SessionData,
    job::{JobData, PipelineState, TransformManager},
    operators::transform::TransformId,
    record_data::{
        field::{FieldId, FieldManager},
        group_track::GroupTrackManager,
        iter_hall::IterId,
        iters::{BoundedIter, DestructuredFieldDataRef, Iter},
        match_set::{MatchSetId, MatchSetManager},
        ref_iter::AutoDerefIter,
        stream_value::StreamValueManager,
    },
};

// a helper type around JobData that works around the fact that
// TransformUtils::basic_update needs
// to borrow the field manager in order to produce the iterator
// that it forwards into its closure
pub struct BasicUpdateData<'a, 'b, 'c> {
    pub session_data: &'a SessionData,
    pub tf_mgr: &'a mut TransformManager,
    pub match_set_mgr: &'a mut MatchSetManager,
    pub group_tracker: &'a mut GroupTrackManager,
    pub field_mgr: &'a FieldManager,
    pub sv_mgr: &'a mut StreamValueManager<'b>,
    pub temp_vec: &'a mut Vec<u8>,
    pub batch_size: usize,
    pub ps: PipelineState,
    pub input_field_id: FieldId,
    pub output_field_id: FieldId,
    pub match_set_id: MatchSetId,
    pub tf_id: TransformId,
    pub iter: &'a mut AutoDerefIter<
        'c,
        BoundedIter<'c, Iter<'c, DestructuredFieldDataRef<'c>>>,
    >,
}

fn basic_transform_update_with_batch(
    jd: &mut JobData,
    tf_id: TransformId,
    extra_output_fields: impl IntoIterator<Item = u32>,
    input_iter_id: IterId,
    mut f: impl FnMut(BasicUpdateData) -> (usize, bool),
    batch_size: usize,
    ps: PipelineState,
) {
    let tf = &jd.tf_mgr.transforms[tf_id];
    let output_field_id = tf.output_field;
    let match_set_id = tf.match_set_id;
    jd.tf_mgr.prepare_for_output(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
        std::iter::once(output_field_id).chain(extra_output_fields),
    );

    let input_field_id = jd.tf_mgr.get_input_field_id(&jd.field_mgr, tf_id);
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
    let mut iter = jd.field_mgr.get_bounded_auto_deref_iter(
        input_field_id,
        &input_field,
        input_iter_id,
        batch_size,
    );
    let (produced_fields, done) = f(BasicUpdateData {
        field_mgr: &jd.field_mgr,
        session_data: jd.session_data,
        tf_mgr: &mut jd.tf_mgr,
        match_set_mgr: &mut jd.match_set_mgr,
        group_tracker: &mut jd.group_track_manager,
        sv_mgr: &mut jd.sv_mgr,
        temp_vec: &mut jd.temp_vec,
        batch_size,
        ps,
        input_field_id,
        output_field_id,
        match_set_id,
        tf_id,
        iter: &mut iter,
    });
    jd.field_mgr.store_iter(input_field_id, input_iter_id, iter);
    if ps.next_batch_ready && !done {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr.submit_batch(tf_id, produced_fields, done);
}

pub fn basic_transform_update_claim_all(
    jd: &mut JobData,
    tf_id: TransformId,
    // if this is None, assume the output field of the transform
    // is the only output
    extra_output_fields: impl IntoIterator<Item = FieldId>,
    input_iter_id: IterId,
    f: impl for<'c> FnMut(BasicUpdateData<'_, '_, 'c>) -> (usize, bool),
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    basic_transform_update_with_batch(
        jd,
        tf_id,
        extra_output_fields,
        input_iter_id,
        f,
        batch_size,
        ps,
    );
}

pub fn basic_transform_update(
    jd: &mut JobData,
    tf_id: TransformId,
    // if this is None, assume the output field of the transform
    // is the only output
    extra_output_fields: impl IntoIterator<Item = FieldId>,
    input_iter_id: IterId,
    f: impl for<'c> FnMut(BasicUpdateData<'_, '_, 'c>) -> (usize, bool),
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    basic_transform_update_with_batch(
        jd,
        tf_id,
        extra_output_fields,
        input_iter_id,
        f,
        batch_size,
        ps,
    );
}
