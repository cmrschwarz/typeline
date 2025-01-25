use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::{ContextData, VentureDescription},
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{
        LivenessData, OperatorCallEffect, Var, VarId, VarLivenessSlotGroup,
        VarLivenessSlotKind,
    },
    options::{
        chain_settings::{SettingBatchSize, SettingMaxThreads},
        session_setup::SessionSetupData,
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{FieldId, FieldManager},
        field_action::FieldActionKind,
        group_track::VOID_GROUP_TRACK_ID,
        iter::field_iterator::FieldIterator,
        iter_hall::{FieldIterId, IterKind},
        match_set::MatchSetId,
        record_buffer::{
            RecordBuffer, RecordBufferData, RecordBufferField,
            RecordBufferFieldId,
        },
    },
    typeline_error::TypelineError,
    utils::{
        identity_hasher::BuildIdentityHasher, indexing_type::IndexingType,
        string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OffsetInChain, Operator, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain, OutputFieldKind,
        TransformInstatiation,
    },
    transform::{Transform, TransformId, TransformState},
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
    source_field_iter: FieldIterId,
    buf_field: RecordBufferFieldId,
}

pub struct TfCallConcurrent {
    pub expanded: bool,
    pub target_chain: ChainId,
    pub field_mappings: Vec<RecordBufferFieldMapping>,
    pub buffer: Arc<RecordBuffer>,
    pub actor_id: ActorId,
}
pub struct TfCalleeConcurrent {
    pub target_fields: Vec<Option<FieldId>>,
    pub buffer: Arc<RecordBuffer>,
}
// These two drop impls are just to prevent deadlocks
// in case the other side's thread panics. This is mainly useful for debugging.
impl Drop for TfCallConcurrent {
    fn drop(&mut self) {
        let mut buf = self.buffer.fields.lock().unwrap();
        buf.input_done = true;
        drop(buf);
        self.buffer.updates.notify_all();
    }
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
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let target = expr.require_single_string_arg()?;
    Ok(Box::new(OpCallConcurrent {
        target_name: target.to_owned(),
        target_resolved: None,
        target_accessed_fields: Vec::new(),
    }))
}

pub fn create_op_callcc(name: String) -> Box<dyn Operator> {
    Box::new(OpCallConcurrent {
        target_name: name,
        target_resolved: None,
        target_accessed_fields: Vec::new(),
    })
}

fn insert_mapping(
    field_mgr: &FieldManager,
    tf_id: TransformId,
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
                source_field_iter: field_mgr.claim_iter(
                    source_field_id,
                    ActorId::ZERO,
                    IterKind::Transform(tf_id),
                ),
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
    let op_id = tf.op_id.unwrap();

    let scope = jd.match_set_mgr.match_sets[tf.match_set_id].active_scope;

    let call_op = jd.session_data.operator_data
        [jd.session_data.op_data_id(op_id)]
    .downcast_ref::<OpCallConcurrent>()
    .unwrap();

    for &(name, _write) in &call_op.target_accessed_fields {
        // PERF: if the field is never written, and we know that the source
        // chain never writes to it aswell, we could theoretically
        // unsafely share the FieldData (and maybe add a flag for soundness)
        let field_id = if let Some(name) = name {
            if let Some(source_field_id) =
                jd.scope_mgr.lookup_field(scope, name)
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
            tf_id,
            &mut mappings_present,
            field_id,
            buf_data,
            &mut call.field_mappings,
            name,
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
                tf_id,
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

pub fn setup_callee_concurrent(
    job: &mut Job,
    ms_id: MatchSetId,
    buffer: Arc<RecordBuffer>,
    start_op_id: OperatorId,
) -> OperatorInstantiation {
    // TODO //HACK: this is busted
    let group_track = VOID_GROUP_TRACK_ID;
    let dummy_field = job.job_data.match_set_mgr.get_dummy_field(ms_id);
    let ms_scope_id =
        job.job_data.match_set_mgr.match_sets[ms_id].active_scope;

    let default_batch_size = job
        .job_data
        .get_scope_setting_or_default::<SettingBatchSize>(ms_scope_id);

    let tf_state = TransformState::new(
        dummy_field,
        dummy_field,
        ms_id,
        default_batch_size,
        None,
        group_track,
    );
    job.job_data.field_mgr.inc_field_refcount(dummy_field, 2);
    let mut callee = TfCalleeConcurrent {
        target_fields: Vec::new(),
        buffer,
    };
    let tgt_scope_id =
        job.job_data.match_set_mgr.match_sets[ms_id].active_scope;
    let mut buf_data = callee.buffer.fields.lock().unwrap();
    for field in &buf_data.fields {
        let field_id = job.job_data.field_mgr.add_field(
            &job.job_data.match_set_mgr,
            ms_id,
            ActorRef::default(),
        );
        job.job_data.scope_mgr.insert_field_name_opt(
            tgt_scope_id,
            field.name,
            field_id,
        );
        callee.target_fields.push(Some(field_id));
    }
    for (i, field) in buf_data.fields.iter_mut().enumerate() {
        let tgt_field_id = callee.target_fields[i].unwrap();
        let mut tgt = job.job_data.field_mgr.fields[tgt_field_id].borrow_mut();
        for fr in &field.field_refs {
            let ref_field_id =
                callee.target_fields[fr.get() as usize].unwrap();
            tgt.field_refs.push(ref_field_id);
            job.job_data.field_mgr.fields[ref_field_id]
                .borrow_mut()
                .ref_count += 1;
        }
    }
    drop(buf_data);
    let input_field = callee.target_fields[0];
    let tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        Box::new(callee),
    );
    let mut instantiation = job.setup_transforms_from_op(
        ms_id,
        start_op_id,
        input_field.unwrap(),
        group_track,
        Some(tf_id),
        &HashMap::default(),
    );
    instantiation.tfs_begin = tf_id;
    instantiation
}

impl Operator for OpCallConcurrent {
    fn default_name(&self) -> super::operator::OperatorName {
        "callcc".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let buffer = Arc::<RecordBuffer>::new(RecordBuffer {
            fields: Mutex::new(RecordBufferData {
                remaining_consumers: 1,
                ..Default::default()
            }),
            ..Default::default()
        });

        TransformInstatiation::Single(Box::new(TfCallConcurrent {
            expanded: false,
            target_chain: self.target_resolved.unwrap(),
            field_mappings: Vec::new(),
            buffer,
            actor_id: job.job_data.match_set_mgr.match_sets
                [tf_state.match_set_id]
                .action_buffer
                .borrow_mut()
                .add_actor(),
        }))
    }

    fn update_bb_for_op(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut LivenessData,
        _op_id: OperatorId,
        op_n: OffsetInChain,
        _cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        ld.basic_blocks[bb_id]
            .calls
            .push(self.target_resolved.unwrap().into_bb_id());
        ld.split_bb_at_call(sess, bb_id, op_n);
        true
    }
    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.call_effect = OperatorCallEffect::Diverge
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
        OutputFieldKind::Unconfigured
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

        if let Some(target) = sess
            .string_store
            .lookup_str(&self.target_name)
            .and_then(|sse| sess.chain_labels.get(&sse))
        {
            self.target_resolved = Some(*target);
        } else {
            return Err(OperatorSetupError::new_s(
                format!("unknown chain label '{}'", self.target_name),
                op_id,
            )
            .into());
        }

        if sess.get_chain_setting::<SettingMaxThreads>(chain_id) == 1 {
            return Err(OperatorSetupError::new(
                    "callcc cannot be used with a max thread count of 1, see `h=j`",
                    op_id,
                )
                .into());
        }
        Ok(op_id)
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut crate::context::SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        let bb_id = ld.operator_liveness_data[op_id].basic_block_id;
        debug_assert!(ld.basic_blocks[bb_id].calls.len() == 1);
        let succ_var_data =
            ld.get_var_liveness(bb_id, VarLivenessSlotGroup::Succession);
        for var_id in succ_var_data
            .get_slot(VarLivenessSlotKind::Reads)
            .iter_ones()
            .map(VarId::from_usize)
        {
            let writes = succ_var_data
                .get_slot(VarLivenessSlotKind::HeaderWrites)
                [var_id.into_usize()];
            match ld.vars[var_id] {
                Var::Named(name) => {
                    self.target_accessed_fields.push((Some(name), writes))
                }
                Var::BBInput => {
                    self.target_accessed_fields.push((None, writes));
                }
                Var::DynVar => todo!(),
                Var::VoidVar => (),
            }
        }
    }
}

impl<'a> Transform<'a> for TfCallConcurrent {
    fn pre_update_required(&self) -> bool {
        !self.expanded
    }
    fn pre_update(
        &mut self,
        ctx: Option<&Arc<ContextData>>,
        job: &mut Job<'a>,
        tf_id: TransformId,
    ) -> Result<(), VentureDescription> {
        self.expanded = true;
        let call = job.transform_data[tf_id]
            .downcast_mut::<TfCallConcurrent>()
            .unwrap();
        call.expanded = true;
        setup_target_field_mappings(&mut job.job_data, tf_id, call);
        let starting_op = job.job_data.session_data.chains[call.target_chain]
            .operators[OffsetInChain::zero()];
        let mut venture_desc = VentureDescription {
            participans_needed: 2,
            starting_points: smallvec::smallvec![
                OperatorId::MAX_VALUE,
                starting_op
            ],
            buffer: call.buffer.clone(),
        };

        let ctx_data = ctx.unwrap();
        let mut sess = ctx_data.session.lock().unwrap();
        let threads_needed = sess
            .venture_queue
            .front()
            .map_or(0, |v| v.description.participans_needed)
            + 2;

        if threads_needed > sess.session_data.settings.max_threads {
            return Err(venture_desc);
        }
        venture_desc.participans_needed -= 1;
        venture_desc.starting_points.swap_remove(0);
        sess.submit_venture(venture_desc, None, ctx_data);
        Ok(())
    }
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        // The `get_cow_field_ref` below would also do this,
        // but we want to do it outside the lock
        for mapping in &self.field_mappings {
            jd.field_mgr.apply_field_actions(
                &jd.match_set_mgr,
                mapping.source_field_id,
                true,
            );
        }
        let mut buf_data = self.buffer.fields.lock().unwrap();
        while buf_data.available_batch_size != 0
            && buf_data.remaining_consumers > 0
        {
            buf_data = self.buffer.updates.wait(buf_data).unwrap();
        }
        for mapping in &self.field_mappings {
            let cfdr = jd
                .field_mgr
                .get_cow_field_ref(&jd.match_set_mgr, mapping.source_field_id);
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
        self.buffer.updates.notify_one();
        let mut ab = jd.match_set_mgr.match_sets[tf.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        ab.push_action(FieldActionKind::Drop, 0, batch_size);
        ab.end_action_group();
        drop(ab);
        // TODO: handle output_done
        if ps.input_done {
            for mapping in &self.field_mappings {
                jd.field_mgr.drop_field_refcount(
                    mapping.source_field_id,
                    &mut jd.match_set_mgr,
                );
            }
            jd.tf_mgr.submit_batch(tf_id, 0, ps.group_to_truncate, true);
        }
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

impl<'a> Transform<'a> for TfCalleeConcurrent {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        "callcc_callee".into()
    }
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            self.target_fields.iter().filter_map(|f| *f),
        );
        let mut buf_data = self.buffer.fields.lock().unwrap();
        while buf_data.available_batch_size == 0 && !buf_data.input_done {
            buf_data = self.buffer.updates.wait(buf_data).unwrap();
        }
        let input_done = buf_data.input_done;
        let available_batch_size = buf_data.available_batch_size;
        buf_data.available_batch_size = 0;
        for i in 0..self.target_fields.len() {
            if let Some(field_id) = self.target_fields[i] {
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
                    self.target_fields[i] = None;
                }
            }
        }
        drop(buf_data);
        self.buffer.updates.notify_one();
        if !input_done {
            // trigger a condvar wait if no further input is present
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr
            .submit_batch(tf_id, available_batch_size, None, input_done);
    }
}
