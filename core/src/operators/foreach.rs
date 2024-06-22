use std::iter;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::call_expr::OperatorCallExpr,
    job::{add_transform_to_job, Job, JobData},
    record_data::{
        group_track::{GroupTrackId, GroupTrackIterId, GroupTrackIterRef},
        iter_hall::IterKind,
    },
    utils::indexing_type::IndexingType,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorData, OperatorId, OperatorInstantiation, PreboundOutputsMap,
        TransformContinuationKind,
    },
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Default)]
pub struct OpForeach {
    pub subchains_start: SubchainIndex,
    pub subchains_end: SubchainIndex,
}
pub struct TfForeachHeader {
    parent_group_track_iter: GroupTrackIterId,
    group_track: GroupTrackId,
}
pub struct TfForeachTrailer {
    group_track: GroupTrackId,
}

pub fn parse_op_foreach(
    expr: &OperatorCallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_params()?;
    Ok(create_op_foreach())
}
pub fn create_op_foreach() -> OperatorData {
    OperatorData::Foreach(OpForeach::default())
}

pub fn setup_op_foreach(
    op: &mut OpForeach,
    _chain_id: ChainId,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.subchains_end > op.subchains_start + SubchainIndex::from_usize(1) {
        return Err(OperatorSetupError::new(
            "operator `foreach` does not support multiple subchains",
            op_id,
        )); // ENHANCE: error on the `next` already?
    }
    Ok(())
}

pub fn insert_tf_foreach(
    job: &mut Job,
    op: &OpForeach,
    mut tf_state: TransformState,
    chain_id: ChainId,
    op_id: OperatorId,
    prebound_outputs: &PreboundOutputsMap,
) -> OperatorInstantiation {
    let subchain_id = job.job_data.session_data.chains[chain_id].subchains
        [op.subchains_start];
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
    let mut trailer_output_group_track = parent_group_track;

    let header_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        TransformData::ForeachHeader(TfForeachHeader {
            parent_group_track_iter,
            group_track,
        }),
    );
    debug_assert!(header_tf_id_peek == header_tf_id);
    let (last_tf_id, cont) = if let Some(&op_id) = sc_start_op_id {
        let instantiation = job.setup_transforms_from_op(
            ms_id,
            op_id,
            input_field,
            group_track,
            None,
            prebound_outputs,
        );
        trailer_output_field = instantiation.next_input_field;
        trailer_output_group_track = instantiation.next_group_track;
        job.job_data.tf_mgr.transforms[header_tf_id].successor =
            Some(instantiation.tfs_begin);
        (instantiation.tfs_end, instantiation.continuation)
    } else {
        (header_tf_id, TransformContinuationKind::Regular)
    };
    match cont {
        TransformContinuationKind::SelfExpanded => {
            return OperatorInstantiation {
                tfs_begin: header_tf_id,
                tfs_end: last_tf_id,
                next_input_field: trailer_output_field,
                next_group_track: trailer_output_group_track,
                continuation: cont,
            };
        }
        TransformContinuationKind::Regular => (),
    }

    job.job_data
        .field_mgr
        .inc_field_refcount(trailer_output_field, 2);

    let mut trailer_tf_state = TransformState::new(
        trailer_output_field,
        trailer_output_field,
        ms_id,
        desired_batch_size,
        Some(op_id),
        group_track,
    );
    trailer_tf_state.output_group_track_id = parent_group_track;
    let trailer_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        trailer_tf_state,
        TransformData::ForeachTrailer(TfForeachTrailer { group_track }),
    );
    job.job_data.tf_mgr.transforms[last_tf_id].successor = Some(trailer_tf_id);

    OperatorInstantiation {
        tfs_begin: header_tf_id,
        tfs_end: trailer_tf_id,
        next_input_field: trailer_output_field,
        next_group_track: parent_group_track,
        continuation: TransformContinuationKind::Regular,
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
    let mut group_track = jd
        .group_track_manager
        .borrow_group_track_mut(feh.group_track);
    group_track.apply_field_actions(&jd.match_set_mgr);
    let mut parent_record_group_iter =
        jd.group_track_manager.lookup_group_track_iter(
            GroupTrackIterRef {
                list_id: group_track.parent_list_id().unwrap(),
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

    #[cfg(feature = "output_field_logging")]
    {
        eprintln!(
            "foreach header (tf {tf_id}) set up group list {}: {}",
            feh.group_track, group_track
        );
    }
}

pub fn handle_tf_foreach_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    fet: &TfForeachTrailer,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    let mut group_track = jd
        .group_track_manager
        .borrow_group_track_mut(fet.group_track);
    group_track.apply_field_actions(&jd.match_set_mgr);
    group_track.drop_leading_fields(true, batch_size, ps.input_done);
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
