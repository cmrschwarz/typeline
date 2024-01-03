use std::collections::HashMap;

use crate::{
    chain::Chain,
    context::SessionSettings,
    job_session::{add_transform_to_job, JobData, JobSession},
    liveness_analysis::OpOutputIdx,
    options::{
        operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions,
    },
    record_data::field::FieldId,
    utils::{identity_hasher::BuildIdentityHasher, string_store::StringStore},
};

use super::{
    errors::OperatorSetupError,
    nop_copy::create_op_nop_copy,
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpAggregator {
    pub sub_ops: Vec<OperatorId>,
}

pub const AGGREGATOR_DEFAULT_NAME: &str = "aggregator";

pub struct TfAggregatorHeader {
    trailer_tf_id: TransformId,
    sub_tfs: Vec<TransformId>,
    prev_sub_tf_idx: usize,
}

pub struct TfAggregatorTrailer {
    curr_sub_tf_idx: usize,
    sub_tf_count: usize,
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
    op_id: u32, //we can't take the operator because the borrow checker hates us
    sess_operator_data: &mut Vec<OperatorData>,
    sess_operator_bases: &mut Vec<OperatorBase>,
    sess_chain_labels: &mut HashMap<
        std::num::NonZeroU32,
        u32,
        BuildIdentityHasher,
    >,
    sess_chains: &mut Vec<Chain>,
    string_store: &mut StringStore,
    sess_settings: &mut SessionSettings,
    chain_id: u32,
) -> Result<(), OperatorSetupError> {
    let OperatorData::Aggregator(agg) = &sess_operator_data[op_id as usize]
    else {
        unreachable!()
    };
    for i in 0..agg.sub_ops.len() {
        let OperatorData::Aggregator(agg) =
            &sess_operator_data[op_id as usize]
        else {
            unreachable!()
        };
        let sub_op_id = agg.sub_ops[i];
        SessionOptions::setup_operator(
            sess_operator_bases,
            sess_operator_data,
            sess_chain_labels,
            sess_chains,
            string_store,
            sess_settings,
            sub_op_id,
            chain_id,
        )?;
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

pub fn build_tf_aggregator<'a>(
    sess: &mut JobSession,
    op: &'a OpAggregator,
    tf_state: &mut TransformState,
    op_id: u32,
    prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
) -> TransformData<'a> {
    let op_count = op.sub_ops.len();
    sess.job_data
        .field_mgr
        .inc_field_refcount(tf_state.input_field, op_count + 1);
    sess.job_data
        .field_mgr
        .inc_field_refcount(tf_state.output_field, op_count + 1);
    let mut sub_tfs = Vec::with_capacity(op_count);
    let trailer_tf_state = TransformState::new(
        tf_state.output_field,
        tf_state.output_field,
        tf_state.match_set_id,
        tf_state.desired_batch_size,
        None,
        Some(op_id),
    );
    let trailer_tf_id = add_transform_to_job(
        &mut sess.job_data,
        &mut sess.transform_data,
        trailer_tf_state,
        TransformData::AggregatorTrailer(TfAggregatorTrailer {
            curr_sub_tf_idx: 0,
            sub_tf_count: op_count,
        }),
    );
    for (i, &sub_op_id) in op.sub_ops.iter().enumerate() {
        let mut sub_tf_state = TransformState::new(
            tf_state.input_field,
            tf_state.output_field,
            tf_state.match_set_id,
            tf_state.desired_batch_size,
            None,
            Some(sub_op_id),
        );
        sub_tf_state.successor = Some(trailer_tf_id);
        sub_tf_state.has_appender = i + 1 < op_count;
        let sub_tf_data = sess.build_transform_data(
            &mut sub_tf_state,
            sub_op_id,
            prebound_outputs,
        );
        sub_tfs.push(add_transform_to_job(
            &mut sess.job_data,
            &mut sess.transform_data,
            sub_tf_state,
            sub_tf_data,
        ));
    }
    TransformData::AggregatorHeader(TfAggregatorHeader {
        sub_tfs,
        trailer_tf_id,
        prev_sub_tf_idx: 0,
    })
}

pub fn handle_tf_aggregator_header(
    sess: &mut JobSession,
    tf_id: nonmax::NonMaxUsize,
) {
    let TransformData::AggregatorHeader(header) =
        &sess.transform_data[tf_id.get()]
    else {
        unreachable!()
    };
    let trailer_id = header.trailer_tf_id;
    let sub_tf_idx = if let TransformData::AggregatorTrailer(trailer) =
        &sess.transform_data[trailer_id.get()]
    {
        trailer.curr_sub_tf_idx
    } else {
        unreachable!()
    };
    let TransformData::AggregatorHeader(header) =
        &mut sess.transform_data[tf_id.get()]
    else {
        unreachable!()
    };
    let sub_tf_count = header.sub_tfs.len();
    let (mut batch_size, ps) = sess.job_data.tf_mgr.claim_all(tf_id);
    if header.prev_sub_tf_idx != sub_tf_idx {
        // in case the previous op left behind unclaimed records,
        // we reclaim them and give them to the next
        let prev_tf = &mut sess.job_data.tf_mgr.transforms
            [header.sub_tfs[header.prev_sub_tf_idx]];
        batch_size += prev_tf.available_batch_size;
        prev_tf.available_batch_size = 0;
        header.prev_sub_tf_idx = sub_tf_idx;
    }
    if sub_tf_count == sub_tf_idx
        || (!ps.next_batch_ready && !ps.input_done && batch_size == 0)
    {
        // PERF: we could maybe figure this out from the trailer and
        // prevent unnecessary rechecks
        return;
    }
    let sub_tf_id = header.sub_tfs[sub_tf_idx];
    let successor = sess.job_data.tf_mgr.transforms[tf_id].successor;
    sess.job_data.tf_mgr.transforms[trailer_id].successor = successor;
    let sub_tf = &mut sess.job_data.tf_mgr.transforms[sub_tf_id];
    sub_tf.available_batch_size += batch_size;
    sub_tf.input_is_done |= ps.input_done;
    if !ps.input_done || sub_tf_idx + 1 != sub_tf_count {
        sess.job_data.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    sess.job_data.tf_mgr.push_tf_in_ready_stack(sub_tf_id);
}

pub fn handle_tf_aggregator_trailer(
    jd: &mut JobData,
    tf_id: nonmax::NonMaxUsize,
    agg_t: &mut TfAggregatorTrailer,
) {
    let (batch_size, mut ps) = jd.tf_mgr.claim_all(tf_id);
    if ps.input_done && agg_t.curr_sub_tf_idx != agg_t.sub_tf_count {
        agg_t.curr_sub_tf_idx += 1;
        jd.tf_mgr.transforms[tf_id].input_is_done = false;
        ps.input_done = agg_t.curr_sub_tf_idx == agg_t.sub_tf_count;
    }
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
