use std::{collections::HashMap, iter};

use crate::{
    chain::{ChainId, SubchainOffset},
    cli::reject_operator_argument,
    context::SessionData,
    job::{add_transform_to_job, Job, JobData, TransformContinuationKind},
    liveness_analysis::OpOutputIdx,
    options::argument::CliArgIdx,
    record_data::{
        field::FieldId,
        group_tracker::{GroupListId, GroupListIterId},
    },
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Default)]
pub struct OpForeach {
    pub subchains_start: SubchainOffset,
    pub subchains_end: SubchainOffset,
}
pub struct TfForeachHeader {
    parent_group_list_iter: GroupListIterId,
    group_list: GroupListId,
}
pub struct TfForeachTrailer {
    group_list: GroupListId,
}

pub fn parse_op_foreach(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_argument("foreach", value, arg_idx)?;
    Ok(create_op_foreach())
}
pub fn create_op_foreach() -> OperatorData {
    OperatorData::Foreach(OpForeach::default())
}

pub fn setup_op_foreach(
    sess: &mut SessionData,
    _chain_id: ChainId,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    let OperatorData::Foreach(op) = &sess.operator_data[op_id as usize] else {
        unreachable!()
    };
    if op.subchains_end > op.subchains_start + 1 {
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
    tf_state: TransformState,
    chain_id: ChainId,
    op_id: u32,
    prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
) -> (TransformId, TransformId, FieldId, TransformContinuationKind) {
    let subchain_id = job.job_data.session_data.chains[chain_id as usize]
        .subchains[op.subchains_start as usize];
    let sc_start_op_id = job.job_data.session_data.chains
        [subchain_id as usize]
        .operators
        .first();
    let ms_id = tf_state.match_set_id;
    let desired_batch_size = tf_state.desired_batch_size;
    let input_field = tf_state.input_field;
    let mut trailer_output_field = input_field;

    let ms = &mut job.job_data.match_set_mgr.match_sets[ms_id];
    let next_actor_id = ms.action_buffer.borrow().next_actor_ref();
    let parent_group_list_iter =
        ms.group_tracker.claim_group_list_iter_for_active();
    let group_list = ms.group_tracker.add_group_list(next_actor_id);
    ms.group_tracker.push_active_group_list(group_list);
    let header_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        TransformData::ForeachHeader(TfForeachHeader {
            parent_group_list_iter,
            group_list,
        }),
    );
    let (last_tf_id, cont) = if let Some(&op_id) = sc_start_op_id {
        let (first, last, next_input_field, cont) = job
            .setup_transforms_from_op(
                ms_id,
                op_id,
                input_field,
                None,
                prebound_outputs,
            );
        trailer_output_field = next_input_field;
        job.job_data.tf_mgr.transforms[header_tf_id].successor = Some(first);
        (last, cont)
    } else {
        (header_tf_id, TransformContinuationKind::Regular)
    };
    match cont {
        TransformContinuationKind::SelfExpanded => {
            return (header_tf_id, last_tf_id, trailer_output_field, cont);
        }
        TransformContinuationKind::Regular => (),
    }

    job.job_data
        .field_mgr
        .inc_field_refcount(trailer_output_field, 2);

    let trailer_tf_state = TransformState::new(
        trailer_output_field,
        trailer_output_field,
        ms_id,
        desired_batch_size,
        Some(op_id),
    );
    let trailer_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        trailer_tf_state,
        TransformData::ForeachTrailer(TfForeachTrailer { group_list }),
    );
    job.job_data.tf_mgr.transforms[last_tf_id].successor = Some(trailer_tf_id);
    job.job_data.match_set_mgr.match_sets[ms_id]
        .group_tracker
        .pop_active_group_list();

    (
        header_tf_id,
        trailer_tf_id,
        trailer_output_field,
        TransformContinuationKind::Regular,
    )
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
    let ms = &mut jd.match_set_mgr.match_sets[tf.match_set_id];
    let mut ab = ms.action_buffer.borrow_mut();
    let mut group_list =
        ms.group_tracker.borrow_group_list_mut(feh.group_list);
    group_list.apply_field_actions(&mut ab);
    let mut parent_group_list_iter = ms.group_tracker.lookup_group_list_iter(
        group_list.parent_list_id().unwrap(),
        feh.parent_group_list_iter,
        &mut ab,
    );

    let mut size_rem = batch_size;
    while size_rem > 0 {
        let gs_rem = parent_group_list_iter.group_len_rem().min(size_rem);
        let parent_group_idx_stable =
            parent_group_list_iter.group_idx_stable();
        parent_group_list_iter.next_n_fields(gs_rem);
        group_list
            .parent_group_indices_stable
            .promote_to_size_class_of_value(parent_group_idx_stable);
        group_list.parent_group_indices_stable.extend_truncated(
            iter::repeat(parent_group_idx_stable).take(gs_rem),
        );
        group_list
            .group_lengths
            .extend_truncated(iter::repeat(1).take(gs_rem));
        size_rem -= gs_rem;
        parent_group_list_iter.try_next_group();
    }
    parent_group_list_iter.store_iter(feh.parent_group_list_iter);
    jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);

    #[cfg(feature = "output_field_logging")]
    {
        eprintln!(
            "foreach header (tf {tf_id}) set up group list {}: {}",
            feh.group_list, group_list
        );
    }
}

pub fn handle_tf_foreach_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    fet: &TfForeachTrailer,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let ms = &mut jd.match_set_mgr.match_sets[tf.match_set_id];
    let mut group_list =
        ms.group_tracker.borrow_group_list_mut(fet.group_list);
    group_list.apply_field_actions(&mut ms.action_buffer.borrow_mut());
    group_list.drop_leading_fields(batch_size, ps.input_done);
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
