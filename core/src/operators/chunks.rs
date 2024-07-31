use std::iter;

use num::Integer;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::call_expr::{Argument, CallExpr, Span},
    job::{add_transform_to_job, Job, JobData},
    options::session_setup::SessionSetupData,
    record_data::{
        group_track::{GroupTrackIterId, GroupTrackIterRef},
        iter_hall::IterKind,
    },
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use super::{
    errors::OperatorCreationError,
    nop::create_op_nop,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpChunks {
    pub subchain: Vec<(OperatorData, Span)>,
    pub subchain_idx: SubchainIndex,
    pub stride: usize,
}
pub struct TfChunksHeader {
    parent_group_track_iter: GroupTrackIterId,
    stride: usize,
    curr_stride_rem: usize,
    starting_new_group: bool,
}
pub struct TfChunksTrailer {}

pub fn setup_op_chunks(
    op: &mut OpChunks,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
    op.subchain_idx = sess.chains[chain_id].subchains.next_idx();
    sess.setup_subchain(chain_id, std::mem::take(&mut op.subchain))?;
    Ok(op_id)
}

pub fn insert_tf_chunks(
    job: &mut Job,
    op: &OpChunks,
    mut tf_state: TransformState,
    chain_id: ChainId,
    op_id: OperatorId,
    prebound_outputs: &PreboundOutputsMap,
) -> OperatorInstantiation {
    let subchain_id =
        job.job_data.session_data.chains[chain_id].subchains[op.subchain_idx];
    let sc_start_op_id = job.job_data.session_data.chains[subchain_id]
        .operators
        .first();
    let ms_id = tf_state.match_set_id;
    let desired_batch_size = tf_state.desired_batch_size;
    let input_field = tf_state.input_field;

    let ms = &mut job.job_data.match_set_mgr.match_sets[ms_id];
    let next_actor_id = ms.action_buffer.borrow().next_actor_ref();
    let parent_group_track = tf_state.input_group_track_id;

    let header_tf_id_peek = job.job_data.tf_mgr.transforms.peek_claim_id();

    let parent_group_track_iter =
        job.job_data.group_track_manager.claim_group_track_iter(
            parent_group_track,
            IterKind::Transform(header_tf_id_peek),
        );
    let group_track = job.job_data.group_track_manager.add_group_track(
        Some(parent_group_track),
        ms_id,
        next_actor_id,
    );
    tf_state.output_group_track_id = group_track;

    let mut trailer_output_field = input_field;

    let header_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        TransformData::ChunksHeader(TfChunksHeader {
            parent_group_track_iter,
            stride: op.stride,
            curr_stride_rem: op.stride,
            starting_new_group: false,
        }),
    );
    debug_assert!(header_tf_id_peek == header_tf_id);

    let mut out_tf_id = header_tf_id;
    let mut out_ms_id = ms_id;
    let mut out_group_track = group_track;

    if let Some(&op_id) = sc_start_op_id {
        let instantiation = job.setup_transforms_from_op(
            ms_id,
            op_id,
            input_field,
            group_track,
            None,
            prebound_outputs,
        );
        trailer_output_field = instantiation.next_input_field;
        job.job_data.tf_mgr.transforms[header_tf_id].successor =
            Some(instantiation.tfs_begin);

        out_tf_id = instantiation.tfs_end;
        out_ms_id = instantiation.next_match_set;
        out_group_track = instantiation.next_group_track;
    }

    job.job_data
        .field_mgr
        .inc_field_refcount(trailer_output_field, 2);

    let mut trailer_tf_state = TransformState::new(
        trailer_output_field,
        trailer_output_field,
        out_ms_id,
        desired_batch_size,
        Some(op_id),
        out_group_track,
    );

    let parent_group_track_parent =
        job.job_data.group_track_manager.group_tracks[parent_group_track]
            .borrow()
            .parent_group_track_id();
    trailer_tf_state.output_group_track_id =
        job.job_data.group_track_manager.add_group_track(
            parent_group_track_parent,
            out_ms_id,
            job.job_data.match_set_mgr.match_sets[out_ms_id]
                .action_buffer
                .borrow()
                .next_actor_ref(),
        );

    #[cfg(feature = "debug_state")]
    {
        job.job_data.group_track_manager.group_tracks
            [trailer_tf_state.output_group_track_id]
            .borrow_mut()
            .corresponding_header = Some(group_track);
    }

    let trailer_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        trailer_tf_state,
        TransformData::ChunksTrailer(TfChunksTrailer {}),
    );
    job.job_data.tf_mgr.transforms[out_tf_id].successor = Some(trailer_tf_id);

    OperatorInstantiation {
        tfs_begin: header_tf_id,
        tfs_end: trailer_tf_id,
        next_input_field: trailer_output_field,
        next_group_track: parent_group_track,
        next_match_set: out_ms_id,
    }
}

pub fn handle_tf_chunks_header(
    jd: &mut JobData,
    tf_id: TransformId,
    ch: &mut TfChunksHeader,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    if batch_size == 0 {
        jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
        return;
    }
    let tf = &jd.tf_mgr.transforms[tf_id];

    let in_group_track_id = tf.input_group_track_id;
    let out_group_track_id = tf.output_group_track_id;

    let mut group_track = jd
        .group_track_manager
        .borrow_group_track_mut(out_group_track_id);

    group_track.apply_field_actions(&jd.match_set_mgr);
    let mut parent_record_group_iter =
        jd.group_track_manager.lookup_group_track_iter(
            GroupTrackIterRef {
                track_id: in_group_track_id,
                iter_id: ch.parent_group_track_iter,
            },
            &jd.match_set_mgr,
        );

    let stride = ch.stride;

    let mut size_rem = batch_size;

    let gs_rem = parent_record_group_iter.group_len_rem().min(size_rem);

    let append_prev = ch.curr_stride_rem != stride;

    let appendable = ch.curr_stride_rem.min(gs_rem);

    if append_prev {
        let idx = group_track.group_lengths.len() - 1;
        group_track.group_lengths.add_value(idx, appendable);
        parent_record_group_iter.next_n_fields(appendable);
        ch.curr_stride_rem -= appendable;
        size_rem -= appendable;
        let eog = parent_record_group_iter.is_end_of_group(ps.input_done);
        if eog {
            parent_record_group_iter.next_group();
            ch.curr_stride_rem = stride;
        }
        if (!eog && ch.curr_stride_rem > 0) || size_rem == 0 {
            parent_record_group_iter.store_iter(ch.parent_group_track_iter);
            jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
            return;
        }
    }

    group_track
        .group_lengths
        .promote_to_size_class_of_value(ch.stride);

    loop {
        let gs_rem = parent_record_group_iter.group_len_rem().min(size_rem);
        parent_record_group_iter.next_n_fields(gs_rem);

        let (full_groups, partial_group) = gs_rem.div_rem(&stride);
        let have_partial_group = partial_group != 0;
        let group_count = full_groups + usize::from(have_partial_group);
        // FIXME: this is wrong. count skipped zero groups
        group_track
            .parent_group_advancement
            .push_back_truncated(usize::from(ch.starting_new_group));
        ch.starting_new_group = false;

        group_track
            .parent_group_advancement
            .extend_truncated(iter::repeat(0).take(group_count - 1));
        group_track
            .group_lengths
            .extend_truncated(iter::repeat(stride).take(full_groups));
        if have_partial_group {
            group_track.group_lengths.push_back_truncated(partial_group);
        }
        size_rem -= gs_rem;

        if size_rem == 0 {
            ch.curr_stride_rem = stride - partial_group;
            break;
        }
        parent_record_group_iter.next_group();
        ch.starting_new_group = true;
    }
    parent_record_group_iter.store_iter(ch.parent_group_track_iter);

    jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
}

pub fn handle_tf_chunks_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    _fet: &TfChunksTrailer,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

    let tf = &jd.tf_mgr.transforms[tf_id];

    let in_group_track_id = tf.input_group_track_id;
    let out_group_track_id = tf.output_group_track_id;

    jd.group_track_manager.merge_leading_groups_into_parent(
        &jd.match_set_mgr,
        in_group_track_id,
        batch_size,
        ps.input_done,
        out_group_track_id,
    );

    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}

pub fn create_op_chunks_with_spans(
    stride: usize,
    stride_span: Span,
    subchain: impl IntoIterator<Item = (OperatorData, Span)>,
) -> Result<OperatorData, OperatorCreationError> {
    if stride == 0 {
        return Err(OperatorCreationError::new(
            "chunk stride cannot be zero",
            stride_span,
        ));
    }

    let mut subchain = subchain.into_iter().collect::<Vec<_>>();
    if subchain.is_empty() {
        subchain.push((create_op_nop(), Span::Generated));
    }
    Ok(OperatorData::Chunks(OpChunks {
        subchain,
        subchain_idx: SubchainIndex::MAX_VALUE,
        stride,
    }))
}

pub fn create_op_chunks(
    stride: usize,
    subchain: impl IntoIterator<Item = OperatorData>,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_chunks_with_spans(
        stride,
        Span::Generated,
        subchain.into_iter().map(|v| (v, Span::Generated)),
    )
}

pub fn parse_op_chunks(
    sess: &mut SessionSetupData,
    arg: &mut Argument,
) -> Result<OperatorData, ScrError> {
    let expr = CallExpr::from_argument_mut(arg)?;

    let stride_arg = expr.require_nth_arg(0, "stride")?;

    let stride = stride_arg.expect_int(expr.op_name, true)?;
    let stride_span = stride_arg.span;

    let args = std::mem::take(arg.expect_arg_array_mut()?);

    let mut subchain = Vec::new();
    for arg in args.into_iter().skip(2) {
        let span = arg.span;
        let op = sess.parse_argument(arg)?;
        subchain.push((op, span));
    }

    Ok(create_op_chunks_with_spans(stride, stride_span, subchain)?)
}
