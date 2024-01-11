use std::collections::HashMap;

use crate::{
    chain::{ChainId, SubchainOffset},
    cli::reject_operator_argument,
    context::SessionData,
    job::{add_transform_to_job, Job, JobData, TransformContinuationKind},
    liveness_analysis::OpOutputIdx,
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        field_action::FieldActionKind,
        field_value_repr::FieldValueRepr,
        iters::{FieldDataRef, FieldIterator},
        push_interface::PushInterface,
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
    actor_id: ActorId,
    // We don't want to insert a group separator before the first
    // record that is submitted to the successor, and we don't want
    // a separator after the last record.
    // Therefore, on each subsequent invocation of this transform, we *do*
    // want to emit a group separator to terminate the previously
    // submitted record.
    any_records_submitted: bool,
}
pub struct TfForeachTrailer {
    actor_id: ActorId,
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
    let group_separator_flag_field = tf_state.output_field;
    let ab = &mut job.job_data.match_set_mgr.match_sets[ms_id].action_buffer;
    let header_actor_id = ab.add_actor();
    let next_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
    let header_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        TransformData::ForeachHeader(TfForeachHeader {
            actor_id: header_actor_id,
            any_records_submitted: false,
        }),
    );
    job.job_data.field_mgr.fields[group_separator_flag_field]
        .borrow_mut()
        .first_actor = next_actor;

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
        .bump_field_refcount(group_separator_flag_field);
    job.job_data
        .field_mgr
        .bump_field_refcount(trailer_output_field);
    let trailer_actor_id = job.job_data.match_set_mgr.match_sets[ms_id]
        .action_buffer
        .add_actor();
    let trailer_tf_state = TransformState::new(
        group_separator_flag_field,
        trailer_output_field,
        ms_id,
        desired_batch_size,
        Some(op_id),
    );
    let trailer_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        trailer_tf_state,
        TransformData::ForeachTrailer(TfForeachTrailer {
            actor_id: trailer_actor_id,
        }),
    );
    job.job_data.tf_mgr.transforms[last_tf_id].successor = Some(trailer_tf_id);
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
    jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    if batch_size == 0 {
        jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
        return;
    }
    let tf = &jd.tf_mgr.transforms[tf_id];
    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();
    let ab = &mut jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
    ab.begin_action_group(feh.actor_id);
    let mut field_idx = if feh.any_records_submitted {
        ab.push_action(
            FieldActionKind::InsertZst(FieldValueRepr::GroupSeparator),
            0,
            1,
        );
        output_field.iter_hall.push_null(1, false);
        output_field.iter_hall.push_undefined(1, false);
        2
    } else {
        feh.any_records_submitted = true;
        output_field.iter_hall.push_undefined(1, false);
        1
    };
    for _ in 0..batch_size - 1 {
        ab.push_action(
            FieldActionKind::InsertZst(FieldValueRepr::GroupSeparator),
            field_idx,
            1,
        );
        output_field.iter_hall.push_null(1, false);
        output_field.iter_hall.push_undefined(1, false);
        field_idx += 2;
    }
    ab.end_action_group();
    jd.tf_mgr.submit_batch_ready_for_more(tf_id, field_idx, ps);
}

pub fn handle_tf_foreach_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    fet: &TfForeachTrailer,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&mut jd.match_set_mgr, tf.input_field);
    debug_assert!(
        batch_size <= input_field.destructured_field_ref().field_count()
    );
    let mut iter = input_field.iter();
    let mut field_pos = 0;
    let ab = &mut jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
    ab.begin_action_group(fet.actor_id);
    let mut bs_rem = batch_size;
    while bs_rem > 0 {
        let non_gs_records = iter.next_n_fields_with_fmt(
            batch_size,
            [FieldValueRepr::Null],
            true,
            0,
            0,
        );
        field_pos += non_gs_records;
        bs_rem -= non_gs_records;
        if bs_rem == 0 {
            break;
        }
        let gs_records = iter.next_n_fields_with_fmt(
            batch_size,
            [FieldValueRepr::Null],
            false,
            0,
            0,
        );
        ab.push_action(FieldActionKind::Drop, field_pos, gs_records);
        bs_rem -= gs_records;
        // prevent an infinite loop in case of an incorrect batch size
        assert!(non_gs_records > 0 || gs_records > 0);
    }
    ab.end_action_group();
    jd.tf_mgr.submit_batch(tf_id, field_pos, ps.input_done);
}
