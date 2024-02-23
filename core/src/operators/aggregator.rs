use std::collections::HashMap;

use crate::{
    context::SessionData,
    job::{add_transform_to_job, Job, JobData, TransformContinuationKind},
    liveness_analysis::OpOutputIdx,
    options::{
        operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions,
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        field_action::FieldActionKind,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
    },
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    errors::OperatorSetupError,
    nop_copy::create_op_nop_copy,
    operator::{OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpAggregator {
    pub sub_ops: Vec<OperatorId>,
}

pub const AGGREGATOR_DEFAULT_NAME: &str = "aggregator";

pub struct TfAggregatorHeader {
    curr_sub_tf_idx: usize,
    sub_tfs: Vec<TransformId>,
    elem_buffered: bool,
    last_elem_multiplied: bool,
    actor_id: ActorId,
    iter_id: IterId,
}

pub struct TfAggregatorTrailer {
    header_tf_id: TransformId,
}

pub fn create_op_aggregate(sub_ops: Vec<OperatorId>) -> OperatorData {
    OperatorData::Aggregator(OpAggregator { sub_ops })
}

pub fn create_op_aggregator_append_leader(
    ctx_opts: &mut SessionOptions,
) -> (OperatorBaseOptions, OperatorData) {
    let op_data = create_op_nop_copy();
    let op_base_opts = OperatorBaseOptions::from_name(
        ctx_opts
            .string_store
            .intern_cloned(op_data.default_op_name().as_str()),
    );
    (op_base_opts, op_data)
}

pub fn setup_op_aggregator(
    sess: &mut SessionData,
    chain_id: u32,
    // we can't take the operator because the borrow checker hates us
    op_id: u32,
) -> Result<(), OperatorSetupError> {
    let OperatorData::Aggregator(agg) = &sess.operator_data[op_id as usize]
    else {
        unreachable!()
    };
    for i in 0..agg.sub_ops.len() {
        let OperatorData::Aggregator(agg) =
            &sess.operator_data[op_id as usize]
        else {
            unreachable!()
        };
        let sub_op_id = agg.sub_ops[i];
        SessionOptions::setup_operator(sess, chain_id, sub_op_id)?;
    }
    Ok(())
}

pub fn add_aggregate_to_sess_opts_uninit(
    sess: &mut SessionOptions,
    op_aggregate_base: OperatorBaseOptions,
    aggregate_starter_is_appending: bool,
    ops: impl IntoIterator<Item = OperatorData>,
) -> OperatorId {
    let mut sub_ops = Vec::new();
    if aggregate_starter_is_appending {
        let op_data = create_op_nop_copy();
        let op_base = OperatorBaseOptions::from_name(
            sess.string_store
                .intern_cloned(op_data.default_op_name().as_str()),
        );
        sub_ops.push(sess.add_op_uninit(op_base, op_data));
    }
    for op_data in ops {
        let op_base = OperatorBaseOptions::from_name(
            sess.string_store
                .intern_cloned(op_data.default_op_name().as_str()),
        );
        sub_ops.push(sess.add_op_uninit(op_base, op_data));
    }
    let op_data = create_op_aggregate(sub_ops);
    sess.add_op_uninit(op_aggregate_base, op_data)
}

pub fn insert_tf_aggregator(
    job: &mut Job,
    op: &OpAggregator,
    mut tf_state: TransformState,
    op_id: u32,
    prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
) -> (TransformId, TransformId, FieldId, TransformContinuationKind) {
    let op_count = op.sub_ops.len();
    let in_fid = tf_state.input_field;
    let out_fid = tf_state.output_field;
    let ms_id = tf_state.match_set_id;
    let desired_batch_size = tf_state.desired_batch_size;
    job.job_data
        .field_mgr
        .inc_field_refcount(in_fid, op_count + 1);
    job.job_data
        .field_mgr
        .inc_field_refcount(out_fid, op_count + 1);
    tf_state.output_field = in_fid;
    let mut ab = job.job_data.match_set_mgr.match_sets[ms_id]
        .action_buffer
        .borrow_mut();
    let actor_id = ab.add_actor();
    job.job_data.field_mgr.fields[out_fid]
        .borrow_mut()
        .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
    drop(ab);
    let iter_id = job.job_data.field_mgr.fields[in_fid]
        .borrow_mut()
        .iter_hall
        .claim_iter(IterKind::Transform(
            job.job_data.tf_mgr.transforms.peek_claim_id(),
        ));
    let header_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        TransformData::AggregatorHeader(TfAggregatorHeader {
            sub_tfs: Vec::new(),
            curr_sub_tf_idx: 0,
            elem_buffered: false,
            last_elem_multiplied: false,
            actor_id,
            iter_id,
        }),
    );

    let mut sub_tfs = Vec::with_capacity(op_count);
    for (i, &sub_op_id) in op.sub_ops.iter().enumerate() {
        let mut sub_tf_state = TransformState::new(
            in_fid,
            out_fid,
            ms_id,
            desired_batch_size,
            Some(sub_op_id),
        );
        sub_tf_state.is_split = i + 1 != op.sub_ops.len();
        let (sub_tf_start, _sub_tf_end, _next_input_field, _diverged) = job
            .insert_transform_from_op(
                sub_tf_state,
                sub_op_id,
                prebound_outputs,
            );
        sub_tfs.push(sub_tf_start);
    }
    let trailer_tf_state = TransformState::new(
        out_fid,
        out_fid,
        ms_id,
        desired_batch_size,
        Some(op_id),
    );
    let trailer_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        trailer_tf_state,
        TransformData::AggregatorTrailer(TfAggregatorTrailer { header_tf_id }),
    );
    for &sub_tf_id in &sub_tfs {
        job.job_data.tf_mgr.transforms[sub_tf_id].successor =
            Some(trailer_tf_id);
    }
    let TransformData::AggregatorHeader(header) =
        &mut job.transform_data[header_tf_id.get()]
    else {
        unreachable!()
    };
    header.sub_tfs = sub_tfs;
    job.job_data.tf_mgr.transforms[header_tf_id].successor =
        Some(header.sub_tfs.first().copied().unwrap_or(trailer_tf_id));
    (
        header_tf_id,
        trailer_tf_id,
        out_fid,
        TransformContinuationKind::Regular,
    )
}

pub fn handle_tf_aggregator_header(
    jd: &mut JobData,
    tf_id: nonmax::NonMaxUsize,
    agg_h: &mut TfAggregatorHeader,
) {
    let (mut batch_size, mut ps) = jd.tf_mgr.claim_all(tf_id);

    let sub_tf_count = agg_h.sub_tfs.len();
    if sub_tf_count == agg_h.curr_sub_tf_idx
        || (!ps.next_batch_ready && !ps.input_done && batch_size == 0)
    {
        return;
    }

    let tf = &mut jd.tf_mgr.transforms[tf_id];
    let prev_sub_tf_id = tf.successor.unwrap();
    let sub_tf_id = agg_h.sub_tfs[agg_h.curr_sub_tf_idx];
    let iter_id = agg_h.iter_id;
    let input_field_id = tf.input_field;
    let ms_id = tf.match_set_id;
    let actor_id = agg_h.actor_id;
    let sub_tfs_after = sub_tf_count - agg_h.curr_sub_tf_idx - 1;

    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&mut jd.match_set_mgr, input_field_id);

    let mut iter =
        jd.field_mgr
            .lookup_iter(input_field_id, &input_field, agg_h.iter_id);

    if prev_sub_tf_id != sub_tf_id {
        tf.successor = Some(sub_tf_id);
        // in case the previous op left behind unclaimed records,
        // we reclaim them and give them to the next
        let prev_tf = &mut jd.tf_mgr.transforms[prev_sub_tf_id];
        batch_size += prev_tf.available_batch_size;
        prev_tf.available_batch_size = 0;
    }

    batch_size += usize::from(ps.input_done && agg_h.elem_buffered);
    if !agg_h.elem_buffered
        && sub_tfs_after != 0
        && batch_size > 0
        && (!ps.input_done || batch_size > 1)
    {
        agg_h.elem_buffered = true;
        batch_size -= 1;
        iter.next_n_fields(batch_size, true);
        ps.input_done = false;
    } else if ps.input_done
        && (agg_h.elem_buffered || batch_size > 0)
        && sub_tfs_after != 0
        && !agg_h.last_elem_multiplied
    {
        let pos = iter.get_next_field_pos() + batch_size - 1;
        let mut ab = jd.match_set_mgr.match_sets[ms_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(actor_id);
        ab.push_action(FieldActionKind::Dup, pos, sub_tfs_after);
        ab.end_action_group();
        agg_h.elem_buffered = true;
        agg_h.last_elem_multiplied = true;
        iter.next_n_fields(batch_size - 1, true);
    } else {
        iter.next_n_fields(batch_size, true);
    }
    let sub_tf = &mut jd.tf_mgr.transforms[sub_tf_id];
    jd.field_mgr.store_iter(input_field_id, iter_id, iter);
    sub_tf.available_batch_size += batch_size;
    sub_tf.predecessor_done |= ps.input_done;
    if batch_size > 0 || ps.input_done {
        if !ps.input_done || agg_h.curr_sub_tf_idx + 1 != sub_tf_count {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr.push_tf_in_ready_stack(sub_tf_id);
    }
}

pub fn handle_tf_aggregator_trailer(
    job: &mut Job,
    tf_id: nonmax::NonMaxUsize,
) {
    let (batch_size, mut ps) = job.job_data.tf_mgr.claim_all(tf_id);

    if ps.input_done {
        let TransformData::AggregatorTrailer(agg_t) =
            &mut job.transform_data[tf_id.get()]
        else {
            unreachable!()
        };
        let header_tf_id = agg_t.header_tf_id;
        let TransformData::AggregatorHeader(agg_h) =
            &mut job.transform_data[header_tf_id.get()]
        else {
            unreachable!()
        };
        agg_h.curr_sub_tf_idx += 1;
        job.job_data.tf_mgr.transforms[tf_id].predecessor_done = false;
        ps.input_done = agg_h.curr_sub_tf_idx == agg_h.sub_tfs.len();
    }
    job.job_data
        .tf_mgr
        .submit_batch(tf_id, batch_size, ps.input_done);
}
