use std::{collections::HashMap, sync::Arc};

use bstr::ByteSlice;

use crate::{
    chain::{ChainId, INVALID_CHAIN_ID},
    context::{ContextData, SessionSettings, VentureDescription},
    field_data::{
        command_buffer::{ActionProducingFieldIndex, FieldActionKind},
        iter_hall::IterId,
        record_buffer::{RecordBuffer, RecordBufferField, RecordBufferFieldId},
        FieldData,
    },
    job_session::{FieldId, JobData, JobSession},
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
    pub target_fields: Vec<(RecordBufferFieldId, FieldId)>,
    pub buffer: Arc<RecordBuffer>,
}

pub fn parse_op_call_concurrent(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing argument with key for select", arg_idx))?
        .to_str()
        .map_err(|_| OperatorCreationError::new("target label must be valid UTF-8", arg_idx))?;
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
            format!("callcc cannot be used with a max thread count of 1, see `h=j`"),
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

fn setup_target_field_mappings(
    sess: &mut JobData,
    tf_id: TransformId,
    call: &mut TfCallConcurrent,
) {
    let buf = Arc::get_mut(&mut call.buffer).unwrap();
    let buf_data = buf.fields.get_mut().unwrap();
    let target_chain = &sess.session_data.chains[call.target_chain as usize];
    let mut mappings_present = HashMap::<StringStoreEntry, usize, BuildIdentityHasher>::default();
    let accessed_field_map = &target_chain.liveness_data.fields_accessed_before_assignment;
    let source_match_set =
        &sess.match_set_mgr.match_sets[sess.tf_mgr.transforms[tf_id].match_set_id];
    for (name, _write) in accessed_field_map {
        // PERF: if the field is never written, and we know that the source chain never writes to it aswell,
        // we could theoretically unsafely share the FieldData (and maybe add a flag for soundness)
        if let Some(source_field_id) = source_match_set.field_name_map.get(name).cloned() {
            match mappings_present.entry(*name) {
                std::collections::hash_map::Entry::Occupied(mapping) => {
                    buf_data.fields[call.field_mappings[*mapping.get()].buf_field]
                        .names
                        .push(*name);
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(call.field_mappings.len());
                    sess.field_mgr.bump_field_refcount(source_field_id);
                    let buf_field = buf_data.fields.claim_with_value(RecordBufferField {
                        refcount: 1,
                        names: smallvec::smallvec![*name],
                        data: Default::default(),
                    });
                    call.field_mappings.push(RecordBufferFieldMapping {
                        source_field_id,
                        source_field_iter: sess.field_mgr.fields[source_field_id]
                            .borrow_mut()
                            .field_data
                            .claim_iter(),
                        buf_field,
                    })
                }
            }
        }
    }
}

pub(crate) fn handle_call_concurrent_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
    ctx: Option<&ContextData>,
) -> Result<(), VentureDescription> {
    let call = if let TransformData::CallConcurrent(call) = &mut sess.transform_data[tf_id.get()] {
        call
    } else {
        unreachable!()
    };
    call.expanded = true;
    setup_target_field_mappings(&mut sess.job_data, tf_id, call);
    let venture_desc = VentureDescription {
        participans_needed: 2,
        starting_points: smallvec::smallvec![call.target_chain],
        buffer: call.buffer.clone(),
    };
    let ctx = ctx.unwrap();
    let sess = ctx.sess_mgr.lock().unwrap();
    let threads_needed = sess
        .venture_queue
        .front()
        .map(|v| v.description.participans_needed)
        .unwrap_or(0)
        + 2;
    if threads_needed > sess.session.settings.max_threads {
        return Err(venture_desc);
    }
    if threads_needed > sess.total_worker_threads {}
    return Ok(());
}

pub fn handle_tf_call_concurrent(
    sess: &mut JobData,
    tf_id: TransformId,
    tfc: &mut TfCallConcurrent,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    debug_assert!(tf.successor.is_none());
    let cb = &mut sess.match_set_mgr.match_sets[tf.match_set_id].command_buffer;
    cb.begin_action_list(tfc.apf_idx);
    cb.push_action_with_usize_rl(tfc.apf_idx, FieldActionKind::Drop, 0, batch_size);
    cb.end_action_list(tfc.apf_idx);
    let mut buf_data = tfc.buffer.fields.lock().unwrap();
    while buf_data.available_batch_size != 0 {
        buf_data = tfc.buffer.updates.wait(buf_data).unwrap();
    }
    for mapping in &tfc.field_mappings {
        let mut src_field = sess
            .field_mgr
            .borrow_field_cow_mut(mapping.source_field_id, false);
        let copy_required =
            src_field.has_unconsumed_input.get() || src_field.field_id != mapping.source_field_id;

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
            let src_data = unsafe { src_field.field_data.raw() };
            std::mem::swap(tgt_data, src_data);
        }
    }
    buf_data.input_done = input_done;
    buf_data.available_batch_size += batch_size;
    drop(buf_data);
    for mapping in &tfc.field_mappings {
        let mut src_field = sess.field_mgr.fields[mapping.source_field_id].borrow_mut();
        if src_field.cow_source.is_some() || src_field.has_unconsumed_input.get() {
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
