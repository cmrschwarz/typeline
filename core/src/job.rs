use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    sync::Arc,
};

use crate::{
    chain::{Chain, ChainId},
    context::{ContextData, JobDescription, SessionData, VentureDescription},
    debug_log::{
        write_debug_log_html_head, write_debug_log_html_tail,
        write_transform_update_to_html,
    },
    operators::{
        call::handle_eager_call_expansion,
        call_concurrent::setup_callee_concurrent,
        operator::{
            OperatorBase, OperatorData, OperatorId, OperatorInstantiation,
            OutputFieldKind, PreboundOutputsMap, TransformContinuationKind,
        },
        terminator::add_terminator_tf_cont_dependant,
        transform::{
            stream_producer_update, transform_pre_update,
            transform_stream_value_update, transform_update, TransformData,
            TransformId, TransformState,
        },
    },
    record_data::{
        action_buffer::{ActorId, ActorRef, SnapshotRef},
        field::{FieldId, FieldManager, VOID_FIELD_ID},
        field_action::FieldActionKind,
        group_track::{GroupTrackId, GroupTrackManager},
        iter_hall::{IterId, IterKind},
        match_set::{MatchSetId, MatchSetManager},
        push_interface::PushInterface,
        record_buffer::RecordBuffer,
        stream_value::{StreamValueManager, StreamValueUpdate},
    },
    utils::{
        index_vec::{IndexSlice, IndexVec},
        text_write::TextWriteIoAdapter,
        universe::Universe,
    },
};

// a helper type so we can pass a transform handler typed
// TransformData + all the other Data of the WorkerThreadSession
pub struct JobData<'a> {
    pub session_data: &'a SessionData,
    pub tf_mgr: TransformManager,
    pub match_set_mgr: MatchSetManager,
    pub group_track_manager: GroupTrackManager,
    pub field_mgr: FieldManager,
    pub sv_mgr: StreamValueManager<'a>,
    pub start_tf: Option<TransformId>,
    pub temp_vec: Vec<u8>,
}

pub struct Job<'a> {
    pub job_data: JobData<'a>,
    pub transform_data: IndexVec<TransformId, TransformData<'a>>,
    pub temp_vec: Vec<FieldId>,
    debug_log: Option<File>,
}

#[derive(Default)]
pub struct TransformManager {
    pub transforms: Universe<TransformId, TransformState>,
    pub ready_stack: Vec<TransformId>,
    pub stream_producers: VecDeque<TransformId>,
    // index of the transform on the stack before the *earliest* stream
    // we won't update this transform as long as we have stream producers
    // because we want to drive those streams to completion first.
    // e.g `bs=42 str=example.com dup=10K GET p`: we want 42 concurrent
    // connections, not 10K
    pub pre_stream_transform_stack_cutoff: usize,
}

#[derive(Clone, Copy, Default)]
pub struct PipelineState {
    pub input_done: bool,
    pub successor_done: bool,
    pub next_batch_ready: bool,
}

impl TransformManager {
    pub fn format_transform_state(
        &self,
        tf_id: TransformId,
        tf_data: &IndexSlice<TransformId, TransformData<'_>>,
        override_batch_size_available: Option<usize>,
    ) -> String {
        let tf = &self.transforms[tf_id];
        format!(
                "tf {tf_id:02} {:>20}, in_fid: {}, bsa: {}, pred_done: {:>5}, done: {:>5}, stack:{:?}",
            format!("`{}`", tf_data[tf_id].display_name()),
            tf.input_field,
            override_batch_size_available.unwrap_or(tf.available_batch_size),
            tf.predecessor_done,
            tf.done,
            self.ready_stack
        )
    }
    pub fn get_input_field_id(
        &mut self,
        fm: &FieldManager,
        tf_id: TransformId,
    ) -> FieldId {
        let tf = &mut self.transforms[tf_id];
        tf.input_field = fm.dealias_field_id(tf.input_field);
        tf.input_field
    }
    pub fn claim_batch_with_limit(
        &mut self,
        tf_id: TransformId,
        limit: usize,
    ) -> (usize, PipelineState) {
        let tf = &mut self.transforms[tf_id];
        let batch_size = tf.available_batch_size.min(limit);
        tf.available_batch_size -= batch_size;
        let next_batch_ready = tf.available_batch_size > 0;
        let input_done = tf.predecessor_done && !next_batch_ready;
        let successor_done = if let Some(succ) = tf.successor {
            self.transforms[succ].done
        } else {
            true
        };
        let ps = PipelineState {
            input_done,
            successor_done,
            next_batch_ready,
        };
        (batch_size, ps)
    }
    pub fn claim_batch(
        &mut self,
        tf_id: TransformId,
    ) -> (usize, PipelineState) {
        self.claim_batch_with_limit(
            tf_id,
            self.transforms[tf_id].desired_batch_size,
        )
    }
    pub fn claim_all(&mut self, tf_id: TransformId) -> (usize, PipelineState) {
        self.claim_batch_with_limit(tf_id, usize::MAX)
    }
    pub fn unclaim_batch_size(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
    ) {
        self.transforms[tf_id].available_batch_size += batch_size;
    }
    pub fn inform_transform_batch_available(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
        predecessor_done: bool,
    ) {
        let tf = &mut self.transforms[tf_id];
        tf.available_batch_size += batch_size;
        if tf.available_batch_size == 0
            && (!predecessor_done || tf.predecessor_done)
        {
            return;
        }
        tf.predecessor_done |= predecessor_done;
        if tf.is_ready {
            return;
        }
        self.push_tf_in_ready_stack(tf_id);
    }
    pub fn inform_cross_ms_transform_batch_available(
        &mut self,
        fm: &FieldManager,
        msm: &MatchSetManager,
        tf_id: TransformId,
        batch_size: usize,
        predecessor_done: bool,
    ) {
        msm.update_cross_ms_cow_targets(
            fm,
            self.transforms[tf_id].match_set_id,
        );
        self.inform_transform_batch_available(
            tf_id,
            batch_size,
            predecessor_done,
        );
    }
    pub fn submit_batch(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
        done: bool,
    ) {
        if done {
            debug_assert!(!self.transforms[tf_id].done);
            self.transforms[tf_id].done = true;
        }
        if let Some(succ_tf_id) = self.transforms[tf_id].successor {
            self.inform_transform_batch_available(
                succ_tf_id, batch_size, done,
            );
        }
    }
    // Help out with dropping records if the successor is done.
    // If we have an action_buffer we might aswell use it.
    pub fn help_out_with_output_done(
        &mut self,
        msm: &mut MatchSetManager,
        tf_id: TransformId,
        actor_id: ActorId,
        batch_size: usize,
    ) {
        let tf = &self.transforms[tf_id];
        let ms_id = tf.match_set_id;
        let mut ab = msm.match_sets[ms_id].action_buffer.borrow_mut();
        ab.begin_action_group(actor_id);
        ab.push_action(FieldActionKind::Drop, 0, batch_size);
        ab.end_action_group();
        if !tf.done {
            self.submit_batch(tf_id, 0, true);
        }
    }
    pub fn submit_batch_ready_for_more(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
        ps: PipelineState,
    ) {
        let mut done = ps.input_done || ps.successor_done;
        if !ps.successor_done && ps.next_batch_ready {
            self.push_tf_in_ready_stack(tf_id);
            done = false;
        }
        self.submit_batch(tf_id, batch_size, done);
    }
    pub fn declare_transform_done(&mut self, tf_id: TransformId) {
        self.submit_batch(tf_id, 0, true);
    }
    pub fn push_tf_in_ready_stack(&mut self, tf_id: TransformId) {
        let tf = &mut self.transforms[tf_id];
        if !tf.is_ready {
            tf.is_ready = true;
            self.ready_stack.push(tf_id);
        }
    }
    pub fn push_successor_in_ready_queue(&mut self, tf_id: TransformId) {
        if let Some(succ_tf_id) = self.transforms[tf_id].successor {
            self.push_tf_in_ready_stack(succ_tf_id);
        }
    }
    pub fn make_stream_producer(&mut self, tf_id: TransformId) {
        let tf = &mut self.transforms[tf_id];
        if !tf.is_stream_producer {
            #[cfg(feature = "stream_logging")]
            {
                eprintln!(
                    ":: tf {tf_id:02} became a stream producer: {:?}, stack: {:?}, cutoff: {}",
                    self.stream_producers,
                    self.ready_stack,
                    self.pre_stream_transform_stack_cutoff
                )
            }

            if self.pre_stream_transform_stack_cutoff != 0 {
                self.pre_stream_transform_stack_cutoff =
                    self.ready_stack.len();
            }

            tf.is_stream_producer = true;
            self.stream_producers.push_back(tf_id);
        }
    }

    pub fn prepare_for_output(
        &mut self,
        fm: &mut FieldManager,
        msm: &mut MatchSetManager,
        _tf_id: TransformId,
        output_fields: impl IntoIterator<Item = FieldId>,
    ) {
        for ofid in output_fields {
            fm.uncow(msm, ofid);
            let f = fm.fields[ofid].borrow();
            drop(f);
            fm.apply_field_actions(msm, ofid);
        }
    }
    pub fn prepare_output_field(
        &mut self,
        field_mgr: &mut FieldManager,
        match_set_mgr: &mut MatchSetManager,
        tf_id: TransformId,
    ) -> FieldId {
        let output_field_id = self.transforms[tf_id].output_field;
        self.prepare_for_output(
            field_mgr,
            match_set_mgr,
            tf_id,
            [output_field_id],
        );
        output_field_id
    }
}

// TODO: bump field refcounts in here and initialize fields with rc 0 isntead
// (plus add a check refcount func)
pub fn add_transform_to_job<'a>(
    jd: &mut JobData<'a>,
    tf_data: &mut IndexVec<TransformId, TransformData<'a>>,
    state: TransformState,
    data: TransformData<'a>,
) -> TransformId {
    let id = jd.tf_mgr.transforms.claim_with_value(state);
    if tf_data.len() < jd.tf_mgr.transforms.used_capacity() {
        tf_data.resize_with(jd.tf_mgr.transforms.used_capacity(), || {
            TransformData::Disabled
        });
    }
    tf_data[id] = data;
    id
}

impl<'a> JobData<'a> {
    pub fn new(sess: &'a SessionData) -> Self {
        Self {
            session_data: sess,
            tf_mgr: TransformManager::default(),
            field_mgr: FieldManager::default(),
            match_set_mgr: MatchSetManager {
                match_sets: Universe::default(),
            },
            group_track_manager: GroupTrackManager::default(),
            sv_mgr: StreamValueManager::default(),
            temp_vec: Vec::default(),
            start_tf: None,
        }
    }
    pub fn unlink_transform(
        &mut self,
        tf_id: TransformId,
        available_batch_for_successor: usize,
    ) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        tf.mark_for_removal = true;
        let successor = tf.successor;
        let input_is_done = tf.predecessor_done;
        let available_batch_size = tf.available_batch_size;
        let is_transparent = tf.is_transparent;
        if let Some(succ_id) = successor {
            let succ = &mut self.tf_mgr.transforms[succ_id];
            succ.predecessor_done = input_is_done;
            let mut bs = available_batch_for_successor;
            if is_transparent {
                bs += available_batch_size;
            }
            succ.available_batch_size += bs;
            if input_is_done || succ.available_batch_size > 0 {
                self.tf_mgr.push_tf_in_ready_stack(succ_id);
            }
        }
    }
}

impl<'a> Job<'a> {
    pub fn log_state(&self, message: &str) {
        if cfg!(feature = "debug_logging") {
            eprintln!("{message}");
            for (i, tf) in self.job_data.tf_mgr.transforms.iter_enumerated() {
                let name = self.transform_data[i].display_name();
                eprintln!(
                    "tf {:02} -> {} [fields {} {} {}] (ms {}): {}",
                    i,
                    if let Some(s) = tf.successor {
                        format!("{s}")
                    } else {
                        "_".to_string()
                    },
                    tf.input_field,
                    if tf.is_transparent { "_>" } else { "->" },
                    tf.output_field,
                    tf.match_set_id,
                    name
                );
            }
            #[cfg(feature = "debug_logging")]
            for (i, _) in self.job_data.field_mgr.fields.iter_enumerated() {
                self.job_data.field_mgr.print_field_stats(i);
                eprintln!();
            }
        }
    }
    pub fn setup_job(&mut self, mut job_desc: JobDescription) {
        let ms_id = self.job_data.match_set_mgr.add_match_set();
        // TODO: unpack record set properly here
        let input_record_count = job_desc.data.adjust_field_lengths();
        let mut input_field = None;
        let mut input_data_fields = std::mem::take(&mut self.temp_vec);
        for mut fd in job_desc.data.fields {
            if input_record_count == 0 {
                fd.data.push_null(1, true);
            }
            let field_id = self.job_data.field_mgr.add_field_with_data(
                &mut self.job_data.match_set_mgr,
                ms_id,
                fd.name,
                ActorRef::default(),
                fd.data,
            );
            input_data_fields.push(field_id);
            if input_field.is_none() {
                input_field = Some(field_id);
            }
        }
        let input_record_count = input_record_count.max(1);
        let input_field = input_field.unwrap();
        let rgt = &mut self.job_data.group_track_manager;
        let input_group_track =
            rgt.add_group_track(None, ms_id, ActorRef::default());
        rgt.append_group_to_track(input_group_track, input_record_count);

        #[cfg(feature = "debug_logging")]
        for (i, f) in input_data_fields.iter().enumerate() {
            self.job_data.field_mgr.fields[*f]
                .borrow_mut()
                .producing_transform_arg = format!("<Input Field #{i}>");
        }
        let instantiation = self.setup_transforms_from_op(
            ms_id,
            job_desc.operator,
            input_field,
            input_group_track,
            None,
            &HashMap::default(),
        );

        self.job_data.start_tf = Some(instantiation.tfs_begin);

        add_terminator_tf_cont_dependant(
            self,
            instantiation.tfs_end,
            instantiation.continuation,
        );
        self.job_data
            .tf_mgr
            .push_tf_in_ready_stack(instantiation.tfs_begin);
        let tf = &mut self.job_data.tf_mgr.transforms[instantiation.tfs_begin];
        tf.predecessor_done = true;
        tf.available_batch_size = input_record_count;
        for input_field_id in &input_data_fields {
            self.job_data.field_mgr.drop_field_refcount(
                *input_field_id,
                &mut self.job_data.match_set_mgr,
            );
        }
        let _ = std::mem::replace(&mut self.temp_vec, input_data_fields);
        self.log_state("setting up job");
    }
    pub(crate) fn setup_venture(
        &mut self,
        _ctx: Option<&Arc<ContextData>>,
        buffer: Arc<RecordBuffer>,
        start_op_id: OperatorId,
    ) {
        let ms_id = self.job_data.match_set_mgr.add_match_set();
        let instantiation =
            setup_callee_concurrent(self, ms_id, buffer, start_op_id);
        self.job_data
            .tf_mgr
            .push_tf_in_ready_stack(instantiation.tfs_begin);
        self.log_state("setting up venture");
    }

    pub fn remove_transform(&mut self, tf_id: TransformId) {
        let tf = &self.job_data.tf_mgr.transforms[tf_id];
        debug_assert!(!tf.is_ready);
        let tf_in_fid = tf.input_field;
        let tf_out_fid = tf.output_field;
        #[cfg(feature = "debug_logging")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
            let name: String = if let Some(op_id) = tf.op_id {
                self.job_data
                    .session_data
                    .string_store
                    .read()
                    .unwrap()
                    .lookup(
                        self.job_data.session_data.operator_bases[op_id]
                            .argname,
                    )
                    .into()
            } else {
                self.transform_data[tf_id].display_name().to_string()
            };
            eprintln!("removing tf id {tf_id}: `{name}`");
        }
        self.job_data
            .field_mgr
            .drop_field_refcount(tf_in_fid, &mut self.job_data.match_set_mgr);
        self.job_data
            .field_mgr
            .drop_field_refcount(tf_out_fid, &mut self.job_data.match_set_mgr);
        self.job_data.tf_mgr.transforms.release(tf_id);
        self.transform_data[tf_id] = TransformData::Disabled;
    }
    pub fn setup_transforms_for_chain(
        &mut self,
        ms_id: MatchSetId,
        chain_id: ChainId,
        input_field_id: FieldId,
        input_group_track: GroupTrackId,
        predecessor_tf: Option<TransformId>,
        prebound_outputs: &PreboundOutputsMap,
    ) -> OperatorInstantiation {
        self.setup_transforms_from_op(
            ms_id,
            *self.job_data.session_data.chains[chain_id]
                .operators
                .first()
                .unwrap(),
            input_field_id,
            input_group_track,
            predecessor_tf,
            prebound_outputs,
        )
    }
    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        start_op_id: OperatorId,
        input_field_id: FieldId,
        input_group_track: GroupTrackId,
        predecessor_tf: Option<TransformId>,
        prebound_outputs: &PreboundOutputsMap,
    ) -> OperatorInstantiation {
        let start_op = &self.job_data.session_data.operator_bases[start_op_id];
        let ops = &self.job_data.session_data.chains
            [start_op.chain_id.unwrap()]
        .operators[start_op.offset_in_chain..];
        self.setup_transforms_for_op_iter(
            ops.iter().map(|op_id| {
                (
                    *op_id,
                    &self.job_data.session_data.operator_bases[*op_id],
                    &self.job_data.session_data.operator_data[*op_id],
                )
            }),
            ms_id,
            input_field_id,
            input_group_track,
            predecessor_tf,
            prebound_outputs,
        )
    }

    pub fn setup_transforms_for_op_iter(
        &mut self,
        ops: impl IntoIterator<
            Item = (OperatorId, &'a OperatorBase, &'a OperatorData),
        >,
        ms_id: MatchSetId,
        mut input_field: FieldId,
        mut input_group_track: GroupTrackId,
        mut predecessor_tf: Option<TransformId>,
        prebound_outputs: &PreboundOutputsMap,
    ) -> OperatorInstantiation {
        let mut start_tf_id = None;
        for (op_id, op_base, op_data) in ops {
            match op_data {
                OperatorData::Call(op) => {
                    if !op.lazy {
                        let mut instantiation = handle_eager_call_expansion(
                            op,
                            self,
                            ms_id,
                            input_field,
                            input_group_track,
                            predecessor_tf,
                        );
                        if let Some(start) = start_tf_id {
                            instantiation.tfs_begin = start;
                        }
                        return instantiation;
                    }
                }
                OperatorData::Select(op) => {
                    if let Some(field_id) =
                        self.job_data.match_set_mgr.match_sets[ms_id]
                            .field_name_map
                            .get(&op.key_interned.unwrap())
                            .copied()
                    {
                        input_field = field_id;
                    } else {
                        let actor = ActorRef::Unconfirmed(
                            self.job_data.match_set_mgr.match_sets[ms_id]
                                .action_buffer
                                .borrow()
                                .peek_next_actor_id(),
                        );
                        input_field = self.job_data.field_mgr.add_field(
                            &mut self.job_data.match_set_mgr,
                            ms_id,
                            Some(op.key_interned.unwrap()),
                            actor,
                        );
                    }
                    if !op.field_is_read {
                        continue;
                    }
                }
                OperatorData::Key(k) => {
                    if let Some(name) = op_base.label {
                        self.job_data.match_set_mgr.add_field_alias(
                            &mut self.job_data.field_mgr,
                            input_field,
                            name,
                        );
                    }
                    let output_field =
                        self.job_data.match_set_mgr.add_field_alias(
                            &mut self.job_data.field_mgr,
                            input_field,
                            k.key_interned.unwrap(),
                        );
                    if !op_base.transparent_mode {
                        input_field = output_field;
                    }
                    continue;
                }
                _ => (),
            }
            let mut label_added = false;
            let output_field_kind = op_data.output_field_kind(op_base);
            let output_field = match output_field_kind {
                OutputFieldKind::Dummy => {
                    self.job_data.field_mgr.bump_field_refcount(VOID_FIELD_ID);
                    VOID_FIELD_ID
                }
                OutputFieldKind::SameAsInput => {
                    self.job_data.field_mgr.bump_field_refcount(input_field);
                    input_field
                }
                OutputFieldKind::Unique => {
                    let first_actor = ActorRef::Unconfirmed(
                        self.job_data.match_set_mgr.match_sets[ms_id]
                            .action_buffer
                            .borrow()
                            .peek_next_actor_id(),
                    );
                    if let Some(field_idx) =
                        prebound_outputs.get(&op_base.outputs_start)
                    {
                        self.job_data
                            .field_mgr
                            .bump_field_refcount(*field_idx);
                        let mut f = self.job_data.field_mgr.fields[*field_idx]
                            .borrow_mut();
                        debug_assert!(f.name == op_base.label);
                        label_added = true;
                        f.first_actor = first_actor;
                        f.snapshot = SnapshotRef::default();
                        f.match_set = ms_id;
                        *field_idx
                    } else {
                        label_added = true;
                        self.job_data.field_mgr.add_field(
                            &mut self.job_data.match_set_mgr,
                            ms_id,
                            op_base.label,
                            first_actor,
                        )
                    }
                }
                OutputFieldKind::Unconfigured => VOID_FIELD_ID,
            };
            if !label_added {
                if let Some(name) = op_base.label {
                    self.job_data.match_set_mgr.add_field_alias(
                        &mut self.job_data.field_mgr,
                        output_field,
                        name,
                    );
                }
            }
            self.job_data.field_mgr.setup_field_refs(
                &mut self.job_data.match_set_mgr,
                input_field,
            );
            self.job_data.field_mgr.bump_field_refcount(input_field);

            let mut tf_state = TransformState::new(
                input_field,
                output_field,
                ms_id,
                op_base.desired_batch_size,
                Some(op_id),
                input_group_track,
            );
            tf_state.is_transparent = op_base.transparent_mode;

            #[cfg(feature = "debug_logging")]
            if output_field_kind == OutputFieldKind::Unique {
                let mut of =
                    self.job_data.field_mgr.fields[output_field].borrow_mut();
                of.producing_transform_id =
                    Some(self.job_data.tf_mgr.transforms.peek_claim_id());
                of.producing_transform_arg =
                    self.job_data.session_data.operator_data[op_id]
                        .default_op_name()
                        .to_string();
            }
            let mut instantiation = op_data.operator_build_transforms(
                self,
                tf_state,
                op_id,
                prebound_outputs,
            );

            input_field = instantiation.next_input_field;
            input_group_track = instantiation.next_group_track;

            if let Some(pred) = predecessor_tf {
                self.job_data.tf_mgr.transforms[pred].successor =
                    Some(instantiation.tfs_begin);
            }

            if let Some(start) = start_tf_id {
                instantiation.tfs_begin = start;
            } else {
                start_tf_id = Some(instantiation.tfs_begin);
            }

            if instantiation.continuation
                == TransformContinuationKind::SelfExpanded
            {
                return instantiation;
            }

            predecessor_tf = Some(instantiation.tfs_end);
        }
        let start = start_tf_id.unwrap();
        let end = predecessor_tf.unwrap_or(start);
        OperatorInstantiation {
            tfs_begin: start,
            tfs_end: end,
            next_input_field: input_field,
            next_group_track: input_group_track,
            continuation: TransformContinuationKind::Regular,
        }
    }
    fn handle_stream_value_update(&mut self, svu: StreamValueUpdate) {
        #[cfg(feature = "debug_logging")]
        {
            let jd = &mut self.job_data;
            eprintln!(
                ">    stream value update tf {:02} {:>20}, sv: {:02}, producers: {:?}, stack: {:?}, cutoff: {:?}",
                svu.tf_id,
                format!("`{}`", self.transform_data[svu.tf_id].display_name()),
                svu.sv_id,
                jd.tf_mgr.stream_producers,
                jd.tf_mgr.ready_stack,
                jd.tf_mgr.pre_stream_transform_stack_cutoff
            );
            #[cfg(feature = "stream_logging")]
            if !jd.sv_mgr.updates.is_empty() {
                eprint!("     :: pending sv updates: ");
                jd.sv_mgr.log_pending_updates(4);
                eprintln!();
            }
        }
        transform_stream_value_update(self, svu);
    }
    pub fn handle_transform(
        &mut self,
        tf_id: TransformId,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        let batch_size_available =
            self.job_data.tf_mgr.transforms[tf_id].available_batch_size;
        #[cfg(feature = "debug_logging")]
        eprintln!(
            "> transform update {}",
            self.job_data.tf_mgr.format_transform_state(
                tf_id,
                &self.transform_data,
                None
            )
        );

        transform_pre_update(self, tf_id, ctx)?;
        transform_update(self, tf_id);
        if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
            if tf.mark_for_removal && !tf.is_stream_producer {
                self.remove_transform(tf_id);
            }
        }

        #[cfg(feature = "output_field_logging")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
            let output_field_id = tf.output_field;
            eprintln!(
                "/> transform update {}",
                self.job_data.tf_mgr.format_transform_state(
                    tf_id,
                    &self.transform_data,
                    None
                )
            );
            let group_track_id = tf.output_group_track_id;
            let group_track = self.job_data.group_track_manager.group_tracks
                [group_track_id]
                .borrow();
            eprint!("   - group {group_track_id} data: {group_track} (may have pending actions)");
            #[cfg(feature = "iter_state_logging")]
            group_track.eprint_iter_states(4);
            eprint!("\n   - out ");
            self.job_data.field_mgr.print_field_stats(output_field_id);
            self.job_data
                .field_mgr
                .print_field_header_data(output_field_id, 4);
            #[cfg(feature = "iter_state_logging")]
            self.job_data
                .field_mgr
                .print_field_iter_data(output_field_id, 4);
            eprintln!();
        }
        if let (Some(f), Some(start_tf)) =
            (&mut self.debug_log, self.job_data.start_tf)
        {
            write_transform_update_to_html(
                &self.job_data,
                &self.transform_data,
                tf_id,
                batch_size_available,
                start_tf,
                f,
            )
            .expect("debug log write must succeed"); // TODO: handle this
                                                     // better
        }
        Ok(())
    }

    pub(crate) fn run_stream_producer_update(&mut self, tf_id: TransformId) {
        #[cfg(feature = "debug_logging")]
        eprintln!(
            "> stream producer update tf {:02} {:>20}, producers: {:?}, stack: {:?}",
            tf_id,
            format!("`{}`", self.transform_data[tf_id].display_name()),
            self.job_data.tf_mgr.stream_producers,
            self.job_data.tf_mgr.ready_stack,
        );
        let tf_state = &mut self.job_data.tf_mgr.transforms[tf_id];
        tf_state.is_stream_producer = false;
        stream_producer_update(self, tf_id);
    }
    pub fn is_in_streaming_mode(&self) -> bool {
        !self.job_data.tf_mgr.stream_producers.is_empty()
    }
    pub(crate) fn run(
        &mut self,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        if let Some(dl) = &mut self.debug_log {
            write_debug_log_html_head(&mut TextWriteIoAdapter(dl))
                .expect("debug log write succeeds");
        }
        // NOTE: we should consider adding a pipeline position attribute on
        // each transform (in a fork both subchains would start at the
        // same pipeline position,  continuation would have position
        // max(subchains) + 1). use this for a priority queue instead of stack
        // for readyness so we always drive out records as fast as possible.
        // This might also be neccessary once flatten (stream unfolder) may
        // produce stream value records (-> array streams) that we have to
        // propagate eagerly.

        // NOTE: terminology regarding how a transform handles streams:
        // - stream producer: causes sv updates 'unprovoked' (GET, join)
        // - stream folder: given records, may cause sv updates (join, collect)
        // - stream unfolder: given sv updates, may produce records (flatten)
        // These states are not exclusive.
        // Before a stream folder can update or stream producer can produce:
        //      - it's own updates must have been processed
        //      - it's produced stream values must have been fully propagated
        loop {
            if let Some(svu) = self.job_data.sv_mgr.updates.pop_back() {
                self.handle_stream_value_update(svu);
                continue;
            }
            // Transforms before the *earliest* stream are not updated
            // until pending streams are done.
            if self.job_data.tf_mgr.ready_stack.len()
                > self.job_data.tf_mgr.pre_stream_transform_stack_cutoff
            {
                let tf_id = self.job_data.tf_mgr.ready_stack.pop().unwrap();
                let tf = &mut self.job_data.tf_mgr.transforms[tf_id];
                tf.is_ready = false;
                self.handle_transform(tf_id, ctx)?;
                continue;
            }
            // we need to process updates in a stack, because updates
            if let Some(svu) = self.job_data.sv_mgr.updates.pop_back() {
                self.handle_stream_value_update(svu);
                continue;
            }
            if let Some(tf_id) =
                self.job_data.tf_mgr.stream_producers.pop_front()
            {
                self.run_stream_producer_update(tf_id);
                continue;
            }
            if self.job_data.tf_mgr.pre_stream_transform_stack_cutoff == 0 {
                break;
            }
            self.job_data.tf_mgr.pre_stream_transform_stack_cutoff = 0;
        }
        if let Some(dl) = &mut self.debug_log {
            write_debug_log_html_tail(&mut TextWriteIoAdapter(dl))
                .expect("debug log write succeeds");
        }
        Ok(())
    }
}
impl<'a> Job<'a> {
    pub fn new(sess_data: &'a SessionData) -> Self {
        Self::from_job_data(JobData::new(sess_data))
    }
    pub fn from_job_data(job_data: JobData<'a>) -> Self {
        Job {
            transform_data: IndexVec::new(),
            temp_vec: Vec::new(),
            // TODO: nicer error handling for this
            debug_log: job_data
                .session_data
                .settings
                .debug_log_path
                .as_ref()
                .map(|p| {
                    File::create(p).expect("debug log path must be valid")
                }),
            job_data,
        }
    }
}
impl JobData<'_> {
    pub fn get_transform_chain_from_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> &Chain {
        let op_id = tf_state.op_id.unwrap();
        let chain_id =
            self.session_data.operator_bases[op_id].chain_id.unwrap();
        &self.session_data.chains[chain_id]
    }
    pub fn get_transform_chain(&self, tf_id: TransformId) -> &Chain {
        self.get_transform_chain_from_tf_state(&self.tf_mgr.transforms[tf_id])
    }
    pub fn add_actor_for_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> ActorId {
        let mut ab = self.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow_mut();
        let actor_id = ab.add_actor();
        self.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        actor_id
    }
    pub fn add_iter_for_tf_state(&self, tf_state: &TransformState) -> IterId {
        self.field_mgr.claim_iter(
            tf_state.input_field,
            IterKind::Transform(self.tf_mgr.transforms.peek_claim_id()),
        )
    }
}
