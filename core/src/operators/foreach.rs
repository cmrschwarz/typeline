use std::iter;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::{
        call_expr::CallExpr, call_expr_iter::CallExprIter, parse_operator_data,
    },
    context::SessionSetupData,
    job::{add_transform_to_job, Job, JobData},
    options::{
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        session_options::SessionOptions,
    },
    record_data::{
        group_track::{GroupTrackIterId, GroupTrackIterRef},
        iter_hall::IterKind,
    },
    utils::indexing_type::IndexingType,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    nop::create_op_nop,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpForeach {
    pub subchain: Vec<(OperatorBaseOptions<'static>, OperatorData)>,
    pub subchain_idx: SubchainIndex,
}
pub struct TfForeachHeader {
    parent_group_track_iter: GroupTrackIterId,
}
pub struct TfForeachTrailer {}

pub fn setup_op_foreach(
    op: &mut OpForeach,
    sess: &mut SessionSetupData,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let op_id = sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        opts_interned,
        op_data_id,
    );
    op.subchain_idx = sess.chains[chain_id].subchains.next_idx();
    sess.create_subchain(chain_id, std::mem::take(&mut op.subchain))?;
    Ok(op_id)
}

pub fn insert_tf_foreach(
    job: &mut Job,
    op: &OpForeach,
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
        TransformData::ForeachHeader(TfForeachHeader {
            parent_group_track_iter,
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
        TransformData::ForeachTrailer(TfForeachTrailer {}),
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

pub fn handle_tf_foreach_header(
    jd: &mut JobData,
    tf_id: TransformId,
    feh: &mut TfForeachHeader,
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
                iter_id: feh.parent_group_track_iter,
            },
            &jd.match_set_mgr,
        );

    let mut size_rem = batch_size;
    while size_rem > 0 {
        let gs_rem = parent_record_group_iter.group_len_rem().min(size_rem);
        let parent_group_idx_stable =
            parent_record_group_iter.group_idx_stable();
        parent_record_group_iter.next_n_fields(gs_rem);
        group_track
            .parent_group_indices_stable
            .promote_to_size_class_of_value(parent_group_idx_stable);
        group_track.parent_group_indices_stable.extend_truncated(
            iter::repeat(parent_group_idx_stable).take(gs_rem),
        );
        group_track
            .group_lengths
            .extend_truncated(iter::repeat(1).take(gs_rem));
        size_rem -= gs_rem;
        parent_record_group_iter.try_next_group();
    }
    parent_record_group_iter.store_iter(feh.parent_group_track_iter);

    jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);

    #[cfg(feature = "debug_logging_output_fields")]
    {
        eprintln!(
            "foreach header (tf {tf_id}) set up group list {}: {}",
            out_group_track_id, group_track
        );
    }
}

pub fn handle_tf_foreach_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    _fet: &TfForeachTrailer,
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

pub fn parse_op_foreach(
    sess_opts: &mut SessionOptions,
    expr: CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let mut subchain = Vec::new();
    for expr in CallExprIter::from_args_iter(expr.args) {
        let expr = expr?;
        let op_base = expr.op_base_options_static();
        let op_data = parse_operator_data(sess_opts, expr)?;
        subchain.push((op_base, op_data));
    }
    Ok(create_op_foreach_with_opts(subchain))
}
pub fn create_op_foreach_with_opts(
    mut subchain: Vec<(OperatorBaseOptions<'static>, OperatorData)>,
) -> OperatorData {
    if subchain.is_empty() {
        subchain.push((
            OperatorBaseOptions::from_name("nop".into()),
            create_op_nop(),
        ));
    }
    OperatorData::Foreach(OpForeach {
        subchain,
        subchain_idx: SubchainIndex::MAX_VALUE,
    })
}
pub fn create_op_foreach(
    subchain: impl IntoIterator<Item = OperatorData>,
) -> OperatorData {
    let subchain_with_opts = subchain
        .into_iter()
        .map(|op_data| {
            (
                OperatorBaseOptions::from_name(op_data.default_op_name()),
                op_data,
            )
        })
        .collect();
    create_op_foreach_with_opts(subchain_with_opts)
}
