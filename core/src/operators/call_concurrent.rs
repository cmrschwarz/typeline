use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bstr::ByteSlice;

use crate::{
    chain::ChainId,
    context::{ContextData, SessionSettings, VentureDescription},
    job::{add_transform_to_job, Job, JobData, TransformContinuationKind},
    liveness_analysis::{
        LivenessData, Var, HEADER_WRITES_OFFSET, READS_OFFSET,
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{FieldId, FieldManager, VOID_FIELD_ID},
        field_action::FieldActionKind,
        iter_hall::IterId,
        iters::FieldIterator,
        match_set::MatchSetId,
        record_buffer::{
            RecordBuffer, RecordBufferData, RecordBufferField,
            RecordBufferFieldId,
        },
    },
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
    pub target_resolved: Option<ChainId>,
    // true means write access
    pub target_accessed_fields: Vec<(Option<StringStoreEntry>, bool)>,
}
pub struct RecordBufferFieldMapping {
    source_field_id: FieldId,
    source_field_iter: IterId,
    buf_field: RecordBufferFieldId,
}
pub struct TfCallConcurrent<'a> {
    pub expanded: bool,
    pub target_chain: ChainId,
    pub field_mappings: Vec<RecordBufferFieldMapping>,
    pub buffer: Arc<RecordBuffer>,
    pub actor_id: ActorId,
    pub target_accessed_fields: &'a Vec<(Option<StringStoreEntry>, bool)>,
}
pub struct TfCalleeConcurrent {
    pub target_fields: Vec<Option<FieldId>>,
    pub buffer: Arc<RecordBuffer>,
}

impl Drop for TfCalleeConcurrent {
    fn drop(&mut self) {
        let mut buf = self.buffer.fields.lock().unwrap();
        buf.remaining_consumers -= 1;
        drop(buf);
        self.buffer.updates.notify_all();
    }
}

pub fn parse_op_call_concurrent(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            // ENHANCE: implicitly start subchain if callcc has no argument
            OperatorCreationError::new(
                "missing argument with target chain name",
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "target chain name must be valid UTF-8",
                arg_idx,
            )
        })?;
    Ok(OperatorData::CallConcurrent(OpCallConcurrent {
        target_name: value_str.to_owned(),
        target_resolved: None,
        target_accessed_fields: Default::default(),
    }))
}

pub fn setup_op_call_concurrent(
    settings: &SessionSettings,
    chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    string_store: &StringStore,
    op: &mut OpCallConcurrent,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if settings.max_threads == 1 {
        return Err(OperatorSetupError::new(
            "callcc cannot be used with a max thread count of 1, see `h=j`",
            op_id,
        ));
    }
    if let Some(target) = string_store
        .lookup_str(&op.target_name)
        .and_then(|sse| chain_labels.get(&sse))
    {
        op.target_resolved = Some(*target);
        Ok(())
    } else {
        Err(OperatorSetupError::new_s(
            format!("unknown chain label '{}'", op.target_name),
            op_id,
        ))
    }
}

pub fn setup_op_call_concurrent_liveness_data(
    op: &mut OpCallConcurrent,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_liveness_data[op_id as usize].basic_block_id;
    debug_assert!(ld.basic_blocks[bb_id].calls.len() == 1);
    let succ_var_data = &ld.var_data[ld.get_succession_var_data_bounds(bb_id)];
    let var_count = ld.vars.len();
    for i in succ_var_data
        [var_count * READS_OFFSET..var_count * (READS_OFFSET + 1)]
        .iter_ones()
    {
        let writes = succ_var_data[var_count * HEADER_WRITES_OFFSET + i];
        match ld.vars[i] {
            Var::Named(name) => {
                op.target_accessed_fields.push((Some(name), writes))
            }
            Var::BBInput => {
                op.target_accessed_fields.push((None, writes));
            }
            Var::AnyVar | Var::DynVar => todo!(),
            Var::VoidVar => (),
        }
    }
}

pub fn create_op_callcc(name: String) -> OperatorData {
    OperatorData::CallConcurrent(OpCallConcurrent {
        target_name: name,
        target_resolved: None,
        target_accessed_fields: Default::default(),
    })
}

pub fn build_tf_call_concurrent<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpCallConcurrent,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let buffer = Arc::<RecordBuffer>::new(RecordBuffer {
        fields: Mutex::new(RecordBufferData {
            remaining_consumers: 1,
            ..Default::default()
        }),
        ..Default::default()
    });

    TransformData::CallConcurrent(TfCallConcurrent {
        expanded: false,
        target_chain: op.target_resolved.unwrap(),
        field_mappings: Default::default(),
        buffer,
        actor_id: jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .add_actor(),
        target_accessed_fields: &op.target_accessed_fields,
    })
}

fn insert_mapping(
    field_mgr: &FieldManager,
    mappings_present: &mut HashMap<FieldId, usize, BuildIdentityHasher>,
    source_field_id: FieldId,
    buf_data: &mut RecordBufferData,
    field_mappings: &mut Vec<RecordBufferFieldMapping>,
    name: Option<StringStoreEntry>,
) -> RecordBufferFieldId {
    match mappings_present.entry(source_field_id) {
        std::collections::hash_map::Entry::Occupied(mapping) => {
            let buf_field = field_mappings[*mapping.get()].buf_field;
            if let Some(name) = name {
                let bfn = &mut buf_data.fields[buf_field].name;
                assert!(bfn.is_none());
                *bfn = Some(name);
            }
            buf_field
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(field_mappings.len());
            field_mgr.bump_field_refcount(source_field_id);
            let mut rbf = RecordBufferField::default();
            rbf.refcount = 1;
            rbf.name = name;
            let buf_field = buf_data.fields.claim_with_value(rbf);
            field_mappings.push(RecordBufferFieldMapping {
                source_field_id,
                source_field_iter: field_mgr.claim_iter(source_field_id),
                buf_field,
            });
            buf_field
        }
    }
}
fn setup_target_field_mappings(
    jd: &mut JobData,
    tf_id: TransformId,
    call: &mut TfCallConcurrent,
) {
    let buf = Arc::get_mut(&mut call.buffer).unwrap();
    let buf_data = buf.fields.get_mut().unwrap();
    let mut mappings_present =
        HashMap::<FieldId, usize, BuildIdentityHasher>::default();
    let tf = &jd.tf_mgr.transforms[tf_id];
    let source_match_set = &jd.match_set_mgr.match_sets[tf.match_set_id];

    for (name, _write) in call.target_accessed_fields {
        // PERF: if the field is never written, and we know that the source
        // chain never writes to it aswell, we could theoretically
        // unsafely share the FieldData (and maybe add a flag for soundness)
        let field_id = if let Some(name) = name {
            if let Some(source_field_id) =
                source_match_set.field_name_map.get(name).copied()
            {
                source_field_id
            } else {
                continue;
            }
        } else {
            tf.input_field
        };

        insert_mapping(
            &jd.field_mgr,
            &mut mappings_present,
            field_id,
            buf_data,
            &mut call.field_mappings,
            *name,
        );
    }
    let mut i = 0;
    while i < call.field_mappings.len() {
        let src_field_id = call.field_mappings[i].source_field_id;
        let src = jd.field_mgr.fields[src_field_id].borrow();
        let tgt_buf_field_idx = call.field_mappings[i].buf_field;
        for fr in &src.field_refs {
            let mapping = insert_mapping(
                &jd.field_mgr,
                &mut mappings_present,
                *fr,
                buf_data,
                &mut call.field_mappings,
                None,
            );
            buf_data.fields[tgt_buf_field_idx].field_refs.push(mapping);
        }
        i += 1;
    }
}

pub(crate) fn handle_call_concurrent_expansion(
    job: &mut Job,
    tf_id: TransformId,
    ctx: Option<&Arc<ContextData>>,
) -> Result<(), VentureDescription> {
    let TransformData::CallConcurrent(call) =
        &mut job.transform_data[tf_id.get()]
    else {
        unreachable!()
    };
    call.expanded = true;
    setup_target_field_mappings(&mut job.job_data, tf_id, call);
    let starting_op = job.job_data.session_data.chains
        [call.target_chain as usize]
        .operators[0];
    let mut venture_desc = VentureDescription {
        participans_needed: 2,
        starting_points: smallvec::smallvec![OperatorId::MAX, starting_op],
        buffer: call.buffer.clone(),
    };
    let ctx = ctx.unwrap();
    let mut sess = ctx.session.lock().unwrap();
    let threads_needed = sess
        .venture_queue
        .front()
        .map(|v| v.description.participans_needed)
        .unwrap_or(0)
        + 2;

    if threads_needed > sess.session_data.settings.max_threads {
        return Err(venture_desc);
    }
    venture_desc.participans_needed -= 1;
    venture_desc.starting_points.swap_remove(0);
    sess.submit_venture(venture_desc, None, ctx);
    Ok(())
}

pub fn handle_tf_call_concurrent(
    jd: &mut JobData,
    tf_id: TransformId,
    tfc: &mut TfCallConcurrent,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    for mapping in &tfc.field_mappings {
        jd.field_mgr.apply_field_actions(
            &mut jd.match_set_mgr,
            mapping.source_field_id,
        );
    }
    let mut buf_data = tfc.buffer.fields.lock().unwrap();
    while buf_data.available_batch_size != 0
        && buf_data.remaining_consumers > 0
    {
        buf_data = tfc.buffer.updates.wait(buf_data).unwrap();
    }
    for mapping in tfc.field_mappings.iter() {
        let cfdr = jd
            .field_mgr
            .get_cow_field_ref(&mut jd.match_set_mgr, mapping.source_field_id);
        let mut iter = jd.field_mgr.lookup_iter(
            mapping.source_field_id,
            &cfdr,
            mapping.source_field_iter,
        );
        if iter.get_next_field_pos() != 0 {
            jd.field_mgr.append_to_buffer(
                &mut iter,
                &buf_data.fields[mapping.buf_field],
            );
            jd.field_mgr.store_iter(
                mapping.source_field_id,
                mapping.source_field_iter,
                iter,
            );
        } else {
            drop(cfdr);
            jd.field_mgr.swap_into_buffer(
                mapping.source_field_id,
                &mut buf_data.fields[mapping.buf_field],
            );
        }
    }
    buf_data.input_done = ps.input_done;
    buf_data.available_batch_size += batch_size;
    drop(buf_data);
    tfc.buffer.updates.notify_one();
    let cb = &mut jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
    cb.begin_action_group(tfc.actor_id);
    cb.push_action(FieldActionKind::Drop, 0, batch_size);
    cb.end_action_group();
    for mapping in &tfc.field_mappings {
        let src_field = jd.field_mgr.fields[mapping.source_field_id].borrow();
        drop(src_field);
        // TODO: make sure the source field does not have more rows than
        // our current batch size before stealing it
        jd.field_mgr
            .clear_if_owned(&mut jd.match_set_mgr, mapping.source_field_id);
    }
    //TODO: handle output_done
    if ps.input_done {
        for mapping in &tfc.field_mappings {
            jd.field_mgr.drop_field_refcount(
                mapping.source_field_id,
                &mut jd.match_set_mgr,
            );
        }
        jd.tf_mgr.submit_batch(tf_id, 0, true);
    }
}

pub fn setup_callee_concurrent(
    sess: &mut Job,
    ms_id: MatchSetId,
    buffer: Arc<RecordBuffer>,
    start_op_id: OperatorId,
) -> (TransformId, TransformId, FieldId, TransformContinuationKind) {
    let chain_id = sess.job_data.session_data.operator_bases
        [start_op_id as usize]
        .chain_id
        .unwrap();
    let chain = &sess.job_data.session_data.chains[chain_id as usize];
    let tf_state = TransformState::new(
        VOID_FIELD_ID,
        VOID_FIELD_ID,
        ms_id,
        chain.settings.default_batch_size,
        None,
    );
    sess.job_data.field_mgr.inc_field_refcount(VOID_FIELD_ID, 2);
    let mut callee = TfCalleeConcurrent {
        target_fields: Default::default(),
        buffer,
    };
    let mut buf_data = callee.buffer.fields.lock().unwrap();
    for field in buf_data.fields.iter_mut() {
        let field_id = sess.job_data.field_mgr.add_field(
            &mut sess.job_data.match_set_mgr,
            ms_id,
            field.name,
            ActorRef::default(),
        );
        callee.target_fields.push(Some(field_id));
    }
    for (i, field) in buf_data.fields.iter_mut().enumerate() {
        let tgt_field_id = callee.target_fields[i].unwrap();
        let mut tgt =
            sess.job_data.field_mgr.fields[tgt_field_id].borrow_mut();
        for fr in &field.field_refs {
            let ref_field_id =
                callee.target_fields[fr.get() as usize].unwrap();
            tgt.field_refs.push(ref_field_id);
            sess.job_data.field_mgr.fields[ref_field_id]
                .borrow_mut()
                .ref_count += 1;
        }
    }
    drop(buf_data);
    let input_field = callee.target_fields[0];
    let tf_id = add_transform_to_job(
        &mut sess.job_data,
        &mut sess.transform_data,
        tf_state,
        TransformData::CalleeConcurrent(callee),
    );
    let (_tf_start, tf_end, next_input_field, cont) = sess
        .setup_transforms_from_op(
            ms_id,
            start_op_id,
            input_field.unwrap(),
            Some(tf_id),
            &Default::default(),
        );
    (tf_id, tf_end, next_input_field, cont)
}

pub fn handle_tf_callee_concurrent(
    jd: &mut JobData,
    tf_id: TransformId,
    tfc: &mut TfCalleeConcurrent,
) {
    jd.tf_mgr.prepare_for_output(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
        tfc.target_fields.iter().filter_map(|f| *f),
    );
    let mut buf_data = tfc.buffer.fields.lock().unwrap();
    while buf_data.available_batch_size == 0 && !buf_data.input_done {
        buf_data = tfc.buffer.updates.wait(buf_data).unwrap();
    }
    let input_done = buf_data.input_done;
    let available_batch_size = buf_data.available_batch_size;
    buf_data.available_batch_size = 0;
    for i in 0..tfc.target_fields.len() {
        if let Some(field_id) = tfc.target_fields[i] {
            let mut field_tgt = jd.field_mgr.fields[field_id].borrow_mut();
            let field_src = &mut buf_data.fields
                [RecordBufferFieldId::new(i as u32).unwrap()];
            std::mem::swap(field_src.get_data_mut(), unsafe {
                field_tgt.iter_hall.raw()
            });
            if input_done || field_tgt.ref_count == 1 {
                field_src.refcount -= 1;
                drop(field_tgt);
                jd.field_mgr
                    .drop_field_refcount(field_id, &mut jd.match_set_mgr);
                tfc.target_fields[i] = None;
            }
        }
    }
    drop(buf_data);
    tfc.buffer.updates.notify_one();
    if !input_done {
        // trigger a condvar wait if no further input is present
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr
        .submit_batch(tf_id, available_batch_size, input_done);
}
