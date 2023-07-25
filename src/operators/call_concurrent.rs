use std::{collections::HashMap, sync::Arc};

use bstr::ByteSlice;

use crate::{
    chain::{ChainId, INVALID_CHAIN_ID},
    context::{ContextData, SessionSettings, VentureDescription},
    field_data::{
        command_buffer::{ActionProducingFieldIndex, FieldActionKind},
        iter_hall::IterId,
        record_buffer::{
            RecordBuffer, RecordBufferData, RecordBufferField,
            RecordBufferFieldId,
        },
        FieldData,
    },
    job_session::{
        FieldId, FieldManager, JobData, JobSession, MatchSetId,
        INVALID_FIELD_ID,
    },
    options::argument::CliArgIdx,
    ref_iter::AutoDerefIter,
    utils::{
        identity_hasher::BuildIdentityHasher,
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCallConcurrent {
    pub target_name: String,
    pub target_resolved: ChainId,
}
pub struct RecordBufferFieldMapping {
    source_field_id: FieldId,
    source_field_iter: IterId,
    buf_field: RecordBufferFieldId,
}
pub struct TfCallConcurrent {
    pub expanded: bool,
    pub target_chain: ChainId,
    pub field_mappings: Vec<RecordBufferFieldMapping>,
    pub buffer: Arc<RecordBuffer>,
    pub apf_idx: ActionProducingFieldIndex,
}
pub struct TfCalleeConcurrent {
    pub target_fields: Vec<FieldId>,
    pub buffer: Arc<RecordBuffer>,
}

pub fn parse_op_call_concurrent(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new(
                "missing argument with key for select",
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "target label must be valid UTF-8",
                arg_idx,
            )
        })?;
    Ok(OperatorData::CallConcurrent(OpCallConcurrent {
        target_name: value_str.to_owned(),
        target_resolved: INVALID_CHAIN_ID,
    }))
}

pub fn setup_op_call_concurrent(
    settings: &SessionSettings,
    chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    string_store: &mut StringStore,
    op: &mut OpCallConcurrent,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if settings.max_threads == 1 {
        return Err(OperatorSetupError::new_s(
            format!(
                "callcc cannot be used with a max thread count of 1, see `h=j`"
            ),
            op_id,
        ));
    }
    if let Some(target) = string_store
        .lookup_str(&op.target_name)
        .and_then(|sse| chain_labels.get(&sse))
    {
        op.target_resolved = *target;
        Ok(())
    } else {
        Err(OperatorSetupError::new_s(
            format!("unknown chain label '{}'", op.target_name),
            op_id,
        ))
    }
}

pub fn create_op_callcc(name: String) -> OperatorData {
    OperatorData::CallConcurrent(OpCallConcurrent {
        target_name: name,
        target_resolved: INVALID_CHAIN_ID,
    })
}

pub fn setup_tf_call_concurrent(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &OpCallConcurrent,
    tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::CallConcurrent(TfCallConcurrent {
        target_chain: op.target_resolved,
        expanded: false,
        buffer: Default::default(),
        apf_idx: sess.match_set_mgr.match_sets[tf_state.match_set_id]
            .command_buffer
            .claim_apf(tf_state.ordering_id),
        field_mappings: Default::default(),
    })
}
fn insert_mapping(
    field_mgr: &FieldManager,
    mappings_present: &mut HashMap<FieldId, usize, BuildIdentityHasher>,
    source_field_id: FieldId,
    buf_data: &mut RecordBufferData,
    field_mappings: &mut Vec<RecordBufferFieldMapping>,
    name: Option<StringStoreEntry>,
) {
    match mappings_present.entry(source_field_id) {
        std::collections::hash_map::Entry::Occupied(mapping) => {
            if let Some(name) = name {
                buf_data.fields[field_mappings[*mapping.get()].buf_field]
                    .names
                    .push(name);
            }
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(field_mappings.len());
            field_mgr.bump_field_refcount(source_field_id);
            let buf_field =
                buf_data.fields.claim_with_value(RecordBufferField {
                    refcount: 1,
                    names: name
                        .map(|name| smallvec::smallvec![name])
                        .unwrap_or_default(),
                    data: Default::default(),
                });
            field_mappings.push(RecordBufferFieldMapping {
                source_field_id,
                source_field_iter: field_mgr.fields[source_field_id]
                    .borrow_mut()
                    .field_data
                    .claim_iter(),
                buf_field,
            })
        }
    }
}
fn setup_target_field_mappings(
    sess: &mut JobData,
    tf_id: TransformId,
    call: &mut TfCallConcurrent,
) {
    let buf = Arc::get_mut(&mut call.buffer).unwrap();
    let buf_data = buf.fields.get_mut().unwrap();
    let target_chain = &sess.session_data.chains[call.target_chain as usize];
    let mut mappings_present =
        HashMap::<FieldId, usize, BuildIdentityHasher>::default();
    let accessed_field_map =
        &target_chain.liveness_data.fields_accessed_before_assignment;
    let tf = &sess.tf_mgr.transforms[tf_id];
    let source_match_set = &sess.match_set_mgr.match_sets[tf.match_set_id];

    insert_mapping(
        &sess.field_mgr,
        &mut mappings_present,
        tf.input_field,
        buf_data,
        &mut call.field_mappings,
        None,
    );
    for (name, _write) in accessed_field_map {
        // PERF: if the field is never written, and we know that the source chain never writes to it aswell,
        // we could theoretically unsafely share the FieldData (and maybe add a flag for soundness)
        if let Some(source_field_id) =
            source_match_set.field_name_map.get(name).cloned()
        {
            insert_mapping(
                &sess.field_mgr,
                &mut mappings_present,
                source_field_id,
                buf_data,
                &mut call.field_mappings,
                Some(*name),
            );
        }
    }
}

pub(crate) fn handle_call_concurrent_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
    ctx: Option<&Arc<ContextData>>,
) -> Result<(), VentureDescription> {
    let call = if let TransformData::CallConcurrent(call) =
        &mut sess.transform_data[tf_id.get()]
    {
        call
    } else {
        unreachable!()
    };
    call.expanded = true;
    setup_target_field_mappings(&mut sess.job_data, tf_id, call);
    let starting_op = sess.job_data.session_data.chains
        [call.target_chain as usize]
        .operators[0];
    let mut venture_desc = VentureDescription {
        participans_needed: 2,
        starting_points: smallvec::smallvec![OperatorId::MAX, starting_op],
        buffer: call.buffer.clone(),
    };
    let ctx = ctx.unwrap();
    let mut sess = ctx.sess_mgr.lock().unwrap();
    let threads_needed = sess
        .venture_queue
        .front()
        .map(|v| v.description.participans_needed)
        .unwrap_or(0)
        + 2;

    if threads_needed > sess.session.settings.max_threads {
        return Err(venture_desc);
    }
    venture_desc.participans_needed -= 1;
    venture_desc.starting_points.swap_remove(0);
    sess.submit_venture(venture_desc, None, ctx);
    return Ok(());
}

pub fn handle_tf_call_concurrent(
    sess: &mut JobData,
    tf_id: TransformId,
    tfc: &mut TfCallConcurrent,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    assert!(tf.successor.is_none());
    for mapping in &tfc.field_mappings {
        sess.field_mgr.apply_field_actions(
            &mut sess.match_set_mgr,
            mapping.source_field_id,
        );
    }
    let mut buf_data = tfc.buffer.fields.lock().unwrap();
    while buf_data.available_batch_size != 0 {
        buf_data = tfc.buffer.updates.wait(buf_data).unwrap();
    }
    for mapping in &tfc.field_mappings {
        let src_field = sess
            .field_mgr
            .borrow_field_cow(mapping.source_field_id, false);
        let mut copy_required = src_field.has_unconsumed_input.get();
        copy_required |= src_field.field_id != mapping.source_field_id;
        // PERF: this makes us always copy for regex. maybe try to copy the
        // referenced fields into the right ids instead?
        copy_required |= !src_field.field_refs.is_empty();

        let mut tgt_data = &mut buf_data.fields[mapping.buf_field].data;
        if copy_required {
            let mut iter = AutoDerefIter::new(
                &sess.field_mgr,
                mapping.source_field_id,
                sess.field_mgr.get_iter_cow_aware(
                    mapping.source_field_id,
                    &src_field,
                    mapping.source_field_iter,
                ),
            );
            FieldData::copy_resolve_refs(
                &mut sess.match_set_mgr,
                &mut iter,
                &mut |f: &mut dyn FnMut(&mut FieldData)| f(&mut tgt_data),
            );
            sess.field_mgr.store_iter_cow_aware(
                mapping.source_field_id,
                &src_field,
                mapping.source_field_iter,
                iter.into_base_iter(),
            );
        } else {
            drop(src_field);
            let mut src_field_mut =
                sess.field_mgr.fields[mapping.source_field_id].borrow_mut();
            let src_data = unsafe { src_field_mut.field_data.raw() };
            std::mem::swap(tgt_data, src_data);
        }
    }
    buf_data.input_done = input_done;
    buf_data.available_batch_size += batch_size;
    drop(buf_data);
    tfc.buffer.updates.notify_one();
    let cb =
        &mut sess.match_set_mgr.match_sets[tf.match_set_id].command_buffer;
    cb.begin_action_list(tfc.apf_idx);
    cb.push_action_with_usize_rl(
        tfc.apf_idx,
        FieldActionKind::Drop,
        0,
        batch_size,
    );
    cb.end_action_list(tfc.apf_idx);
    for mapping in &tfc.field_mappings {
        let mut src_field =
            sess.field_mgr.fields[mapping.source_field_id].borrow_mut();
        if src_field.cow_source.is_some()
            || src_field.has_unconsumed_input.get()
        {
            continue;
        }
        sess.match_set_mgr.match_sets[tf.match_set_id]
            .command_buffer
            .drop_field_commands(mapping.source_field_id, &mut src_field);
        src_field.field_data.clear();
    }
    if input_done {
        sess.unlink_transform(tf_id, 0);
    }
}

pub fn setup_callee_concurrent(
    sess: &mut JobSession,
    ms_id: MatchSetId,
    buffer: Arc<RecordBuffer>,
    start_op_id: OperatorId,
) -> (TransformId, TransformId, bool) {
    let chain_id = sess.job_data.session_data.operator_bases
        [start_op_id as usize]
        .chain_id;
    let chain = &sess.job_data.session_data.chains[chain_id as usize];
    let tf_state = TransformState::new(
        INVALID_FIELD_ID,
        INVALID_FIELD_ID,
        ms_id,
        chain.settings.default_batch_size,
        None,
        None,
        sess.job_data.tf_mgr.claim_transform_ordering_id(),
    );
    let mut callee = TfCalleeConcurrent {
        target_fields: Default::default(),
        buffer,
    };
    let mut buf_data = callee.buffer.fields.lock().unwrap();
    for field in buf_data.fields.iter_mut() {
        let field_id = sess.job_data.field_mgr.add_field(ms_id, None);
        for n in &field.names {
            sess.job_data.match_set_mgr.add_field_name(
                &sess.job_data.field_mgr,
                field_id,
                *n,
            );
        }
        callee.target_fields.push(field_id);
    }
    drop(buf_data);
    let (tf_start, tf_end, end_reachable) = sess.setup_transforms_from_op(
        ms_id,
        start_op_id,
        callee.target_fields[0],
    );

    let tf_id =
        sess.add_transform(tf_state, TransformData::CalleeConcurrent(callee));
    sess.job_data.tf_mgr.transforms[tf_id].successor = Some(tf_start);
    sess.job_data.tf_mgr.transforms[tf_start].predecessor = Some(tf_id);
    (tf_id, tf_end, end_reachable)
}

pub fn handle_tf_callee_concurrent(
    sess: &mut JobData,
    tf_id: TransformId,
    tfc: &mut TfCalleeConcurrent,
) {
    let cb = &mut sess.match_set_mgr.match_sets
        [sess.tf_mgr.transforms[tf_id].match_set_id]
        .command_buffer;
    for field_id in tfc.target_fields.iter_mut() {
        if *field_id == INVALID_FIELD_ID {
            continue;
        }
        let mut field = sess.field_mgr.fields[*field_id].borrow_mut();
        cb.drop_field_commands(*field_id, &mut field);
        field.field_data.clear();
    }
    let mut buf_data = tfc.buffer.fields.lock().unwrap();
    while buf_data.available_batch_size == 0 && !buf_data.input_done {
        buf_data = tfc.buffer.updates.wait(buf_data).unwrap();
    }
    let input_done = buf_data.input_done;
    let available_batch_size = buf_data.available_batch_size;
    buf_data.available_batch_size = 0;
    let mut any_fields_done = false;
    for (i, field) in tfc.target_fields.iter_mut().enumerate() {
        if *field == INVALID_FIELD_ID {
            continue;
        }
        let mut field_tgt = sess.field_mgr.fields[*field].borrow_mut();
        let field_src =
            &mut buf_data.fields[RecordBufferFieldId::new(i as u32).unwrap()];
        std::mem::swap(&mut field_src.data, unsafe {
            field_tgt.field_data.raw()
        });
        if input_done || field_tgt.ref_count == 1 {
            any_fields_done = true;
            field_src.refcount -= 1;
        }
    }
    drop(buf_data);
    tfc.buffer.updates.notify_one();
    if any_fields_done {
        for field in tfc.target_fields.iter_mut() {
            if *field == INVALID_FIELD_ID {
                continue;
            }
            sess.drop_field_refcount(*field);
            *field = INVALID_FIELD_ID;
        }
    }
    if input_done {
        sess.unlink_transform(tf_id, available_batch_size);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, available_batch_size);
        sess.tf_mgr.push_tf_in_ready_queue(tf_id);
    }
}
