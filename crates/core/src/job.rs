use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    chain::{Chain, ChainId},
    context::{ContextData, JobDescription, SessionData, VentureDescription},
    operators::{
        atom::{assign_atom, OpAtom},
        call::{handle_eager_call_expansion, OpCall},
        call_concurrent::setup_callee_concurrent,
        key::OpKey,
        nop::TfNop,
        operator::{
            InputFieldKind, Operator, OperatorBase, OperatorId,
            OperatorInstantiation, OperatorOffsetInChain, OutputFieldKind,
            PreboundOutputsMap,
        },
        select::OpSelect,
        terminator::add_terminator,
        transform::{TransformData, TransformId, TransformState},
        utils::nested_op::NestedOp,
    },
    options::chain_settings::ChainSetting,
    record_data::{
        action_buffer::{ActorId, ActorRef, SnapshotRef},
        field::{FieldId, FieldIterRef, FieldManager},
        field_action::FieldActionKind,
        group_track::{
            GroupIdxStable, GroupTrackId, GroupTrackIterRef, GroupTrackManager,
        },
        iter_hall::{FieldIterId, IterKind},
        match_set::{MatchSetId, MatchSetManager},
        push_interface::PushInterface,
        record_buffer::RecordBuffer,
        scope_manager::{ScopeId, ScopeManager},
        stream_value::{StreamValueManager, StreamValueUpdate},
    },
};

use indexland::{
    index_slice::IndexSlice, index_vec::IndexVec, indexing_type::IndexingType,
    universe::Universe,
};

// a helper type so we can pass a transform handler typed
// TransformData + all the other Data of the WorkerThreadSession
pub struct JobData<'a> {
    pub session_data: &'a SessionData,
    pub tf_mgr: TransformManager,
    pub match_set_mgr: MatchSetManager,
    pub group_track_manager: GroupTrackManager,
    pub field_mgr: FieldManager,
    pub scope_mgr: ScopeManager,
    pub sv_mgr: StreamValueManager<'a>,
    pub start_tf: Option<TransformId>,
    pub temp_vec: Vec<u8>,
    pub transform_step_count: usize,
}

pub struct Job<'a> {
    pub job_data: JobData<'a>,
    pub transform_data: IndexVec<TransformId, TransformData<'a>>,
    pub temp_vec: Vec<FieldId>,
    #[cfg(feature = "debug_log")]
    debug_log: Option<std::fs::File>,
    #[cfg(feature = "debug_logging_transform_update_timestamps")]
    job_start: std::time::SystemTime,
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
    pub group_to_truncate: Option<GroupIdxStable>,
}

impl TransformManager {
    pub fn format_transform_state(
        &self,
        jd: &JobData,
        tf_id: TransformId,
        tf_data: &IndexSlice<TransformId, TransformData<'_>>,
        override_batch_size_available: Option<usize>,
    ) -> String {
        let tf = &self.transforms[tf_id];
        let bsa =
            override_batch_size_available.unwrap_or(tf.available_batch_size);
        format!(
                "tf {tf_id:02} {:>20}, in_fid: {}, bsa: {bsa}{}, pred_done: {:>5}, done: {:>5}, stack:{:?}",
            format!("`{}`", tf_data[tf_id].display_name(jd, tf_id)),
            tf.input_field,
            if bsa > tf.desired_batch_size {
                format!(" (tgt bs: {})", tf.desired_batch_size)
            } else {
                String::new()
            },
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
        let mut successor_done = false;
        let mut group_to_truncate = None;
        if let Some(succ) = tf.successor {
            let tf = &self.transforms[succ];
            successor_done = tf.done;
            group_to_truncate = tf.group_to_truncate;
        }
        let ps = PipelineState {
            input_done,
            successor_done,
            next_batch_ready,
            group_to_truncate,
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
    pub fn claim_batch_with_limit_bump(
        &mut self,
        tf_id: TransformId,
        limit_bump: usize,
    ) -> (usize, PipelineState) {
        self.claim_batch_with_limit(
            tf_id,
            self.transforms[tf_id].desired_batch_size + limit_bump,
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
        if tf.is_ready || tf.done {
            return;
        }
        self.push_tf_in_ready_stack(tf_id);
    }
    pub fn submit_batch(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
        group_to_truncate: Option<GroupIdxStable>,
        done: bool,
    ) {
        self.transforms[tf_id].group_to_truncate = group_to_truncate;
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
            self.submit_batch(tf_id, 0, None, true);
        }
    }
    pub fn make_tf_ready_for_more(
        &mut self,
        tf_id: TransformId,
        ps: PipelineState,
    ) {
        if !ps.successor_done && ps.next_batch_ready {
            self.push_tf_in_ready_stack(tf_id);
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
        self.submit_batch(tf_id, batch_size, ps.group_to_truncate, done);
    }
    pub fn declare_transform_done(&mut self, tf_id: TransformId) {
        self.submit_batch(tf_id, 0, None, true);
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
            #[cfg(feature = "debug_logging_streams")]
            {
                eprintln!(
                    ":: tf {tf_id:02} became a stream producer: {:?}, stack: {:?}, cutoff: {}",
                    self.stream_producers,
                    self.ready_stack,
                    self.pre_stream_transform_stack_cutoff
                )
            }

            if self.pre_stream_transform_stack_cutoff == 0 {
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
            fm.apply_field_actions(msm, ofid, true);
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
            Box::new(TfNop::default())
        });
    }
    tf_data[id] = data;
    id
}

impl<'a> Job<'a> {
    pub fn new(sess_data: &'a SessionData) -> Self {
        Self::from_job_data(JobData::new(sess_data))
    }
    pub fn from_job_data(job_data: JobData<'a>) -> Self {
        Job {
            transform_data: IndexVec::new(),
            temp_vec: Vec::new(),
            #[cfg(feature = "debug_log")]
            // TODO: nicer error handling for this
            debug_log: job_data
                .session_data
                .settings
                .debug_log_path
                .as_ref()
                .map(|p| {
                    std::fs::File::create(p).expect("debug log path must be valid")
                }),
            job_data,

            #[cfg(feature = "debug_logging_transform_update_timestamps")]
            job_start: std::time::SystemTime::now()
        }
    }
    pub fn log_state(&self, message: &str) {
        eprintln!("{message}");
        for (i, tf) in self.job_data.tf_mgr.transforms.iter_enumerated() {
            let name = self.transform_data[i].display_name(&self.job_data, i);
            eprintln!(
                "tf {:02} -> {} [fields {} -> {}] (ms {}): {}",
                i,
                if let Some(s) = tf.successor {
                    format!("{s}")
                } else {
                    "_".to_string()
                },
                tf.input_field,
                tf.output_field,
                tf.match_set_id,
                name
            );
        }
        for (i, _) in self.job_data.field_mgr.fields.iter_enumerated() {
            self.job_data.field_mgr.print_field_stats(i);
            eprintln!();
        }
    }
    pub fn setup_job(&mut self, mut job_desc: JobDescription) {
        let root_chain_scope = self.job_data.session_data.chains[self
            .job_data
            .session_data
            .operator_bases[job_desc.operator]
            .chain_id]
            .scope_id;
        let scope_id =
            self.job_data.scope_mgr.add_scope(Some(root_chain_scope));
        let ms_id = self.job_data.match_set_mgr.add_match_set(
            &mut self.job_data.field_mgr,
            &mut self.job_data.scope_mgr,
            scope_id,
        );
        // TODO: unpack record set properly here
        let input_record_count = job_desc.data.adjust_field_lengths();
        let mut input_field = None;
        let mut input_data_fields = std::mem::take(&mut self.temp_vec);
        for mut fd in job_desc.data.fields {
            if input_record_count == 0 {
                fd.data.push_null(1, true);
            }
            let field_id = self.job_data.field_mgr.add_field_with_data(
                &self.job_data.match_set_mgr,
                ms_id,
                ActorRef::default(),
                fd.data,
            );
            self.job_data
                .scope_mgr
                .insert_field_name_opt(scope_id, fd.name, field_id);
            input_data_fields.push(field_id);
            if input_field.is_none() {
                input_field = Some(field_id);
            }
        }
        let input_record_count = input_record_count.max(1);
        let dummy_field = self.job_data.match_set_mgr.get_dummy_field(ms_id);
        self.job_data.field_mgr.fields[dummy_field]
            .borrow_mut()
            .iter_hall
            .push_undefined(input_record_count, false);
        let input_field = input_field.unwrap_or(dummy_field);
        let rgt = &mut self.job_data.group_track_manager;
        let input_group_track = rgt.add_group_track(
            &self.job_data.match_set_mgr,
            None,
            ms_id,
            ActorRef::default(),
        );
        rgt.append_group_to_track(input_group_track, input_record_count, 0);

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

        if !cfg!(feature = "debug_disable_terminator") {
            add_terminator(self, instantiation.tfs_end);
        }

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
        #[cfg(feature = "debug_logging_setup")]
        self.log_state("setting up job");
    }
    pub(crate) fn setup_venture(
        &mut self,
        _ctx: Option<&Arc<ContextData>>,
        buffer: Arc<RecordBuffer>,
        start_op_id: OperatorId,
    ) {
        let scope_id = self.job_data.scope_mgr.add_scope(None);
        let ms_id = self.job_data.match_set_mgr.add_match_set(
            &mut self.job_data.field_mgr,
            &mut self.job_data.scope_mgr,
            scope_id,
        );
        let instantiation =
            setup_callee_concurrent(self, ms_id, buffer, start_op_id);
        self.job_data
            .tf_mgr
            .push_tf_in_ready_stack(instantiation.tfs_begin);
        #[cfg(feature = "debug_logging_setup")]
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
                self.job_data.session_data.operator_data
                    [self.job_data.session_data.op_data_id(op_id)]
                .debug_op_name()
                .to_string()
            } else {
                self.transform_data[tf_id]
                    .display_name(&self.job_data, tf_id)
                    .to_string()
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
        self.transform_data[tf_id] = Box::new(TfNop::default());
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
        let OperatorOffsetInChain::Direct(offset_in_chain) =
            start_op.offset_in_chain
        else {
            panic!("starting op cannot be part of an aggregation");
        };

        let ops = &self.job_data.session_data.chains[start_op.chain_id]
            .operators[offset_in_chain..];
        self.setup_transforms_for_op_iter(
            ops.iter().map(|op_id| {
                let op_base =
                    &self.job_data.session_data.operator_bases[*op_id];
                let op_data = &*self.job_data.session_data.operator_data
                    [op_base.op_data_id];
                (*op_id, op_base, op_data)
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
            Item = (
                OperatorId,
                &'a OperatorBase,
                &'a (dyn Operator + 'static),
            ),
        >,
        mut ms_id: MatchSetId,
        mut input_field: FieldId,
        mut input_group_track: GroupTrackId,
        mut predecessor_tf: Option<TransformId>,
        prebound_outputs: &PreboundOutputsMap,
    ) -> OperatorInstantiation {
        let mut start_tf_id = None;
        for (mut op_id, mut op_base, mut op_data) in ops {
            let mut label = None;
            if let Some(op) = op_data.downcast_ref::<OpSelect>() {
                if let Some(field_id) = self.job_data.scope_mgr.lookup_field(
                    self.job_data.match_set_mgr.match_sets[ms_id].active_scope,
                    op.key_interned.unwrap(),
                ) {
                    input_field = field_id;
                } else {
                    let ms = &self.job_data.match_set_mgr.match_sets[ms_id];
                    input_field =
                        self.job_data.match_set_mgr.get_dummy_field(ms_id);
                    self.job_data.scope_mgr.insert_field_name(
                        ms.active_scope,
                        op.key_interned.unwrap(),
                        input_field,
                    );
                }
                continue;
            }
            if let Some(k) = op_data.downcast_ref::<OpKey>() {
                let Some(NestedOp::SetUp(nested_op_id)) = k.nested_op else {
                    debug_assert!(k.nested_op.is_none());
                    let output_field =
                        self.job_data.match_set_mgr.add_field_alias(
                            &mut self.job_data.field_mgr,
                            &mut self.job_data.scope_mgr,
                            input_field,
                            k.key_interned.unwrap(),
                        );
                    input_field = output_field;
                    continue;
                };
                op_id = nested_op_id;
                op_base =
                    &self.job_data.session_data.operator_bases[nested_op_id];
                op_data = &*self.job_data.session_data.operator_data
                    [op_base.op_data_id];
                label = k.key_interned;
            }
            if let Some(op) = op_data.downcast_ref::<OpCall>() {
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
            if let Some(op) = op_data.downcast_ref::<OpAtom>() {
                let active_scope =
                    self.job_data.match_set_mgr.match_sets[ms_id].active_scope;
                assign_atom(op, &mut self.job_data, active_scope);
                continue;
            }

            let output_field_kind =
                op_data.output_field_kind(self.job_data.session_data, op_id);
            let output_field = match output_field_kind {
                OutputFieldKind::Dummy => {
                    self.job_data.match_set_mgr.get_dummy_field_with_ref_count(
                        &self.job_data.field_mgr,
                        ms_id,
                    )
                }
                OutputFieldKind::SameAsInput => {
                    self.job_data.field_mgr.bump_field_refcount(input_field);
                    input_field
                }
                OutputFieldKind::Unique => {
                    debug_assert!(
                        op_data
                            .output_count(self.job_data.session_data, op_id)
                            != 0,
                        "field requesting a unique output field must have at least one output"
                    );
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
                        f.first_actor.set(first_actor);
                        f.snapshot.set(SnapshotRef::default());
                        f.match_set = ms_id;
                        *field_idx
                    } else {
                        self.job_data.field_mgr.add_field(
                            &self.job_data.match_set_mgr,
                            ms_id,
                            first_actor,
                        )
                    }
                }
                OutputFieldKind::Unconfigured => {
                    self.job_data.match_set_mgr.get_dummy_field(ms_id)
                }
            };
            if let Some(label) = label {
                match output_field_kind {
                    OutputFieldKind::Unique => {
                        self.job_data.scope_mgr.insert_field_name(
                            self.job_data.match_set_mgr.match_sets[ms_id]
                                .active_scope,
                            label,
                            output_field,
                        );
                    }
                    OutputFieldKind::Dummy
                    | OutputFieldKind::SameAsInput
                    | OutputFieldKind::Unconfigured => {
                        self.job_data.match_set_mgr.add_field_alias(
                            &mut self.job_data.field_mgr,
                            &mut self.job_data.scope_mgr,
                            output_field,
                            label,
                        );
                    }
                }
            }

            match op_data.input_field_kind() {
                InputFieldKind::LastOutput => {
                    self.job_data.field_mgr.setup_field_refs(
                        &mut self.job_data.match_set_mgr,
                        input_field,
                    );
                    self.job_data.field_mgr.bump_field_refcount(input_field);
                }
                InputFieldKind::Dummy => {
                    input_field = self
                        .job_data
                        .match_set_mgr
                        .get_dummy_field_with_ref_count(
                            &self.job_data.field_mgr,
                            ms_id,
                        );
                }
                InputFieldKind::Unconfigured => (),
            };

            let tf_state = TransformState::new(
                input_field,
                output_field,
                ms_id,
                op_base.desired_batch_size,
                Some(op_id),
                input_group_track,
            );

            #[cfg(feature = "debug_logging")]
            if output_field_kind == OutputFieldKind::Unique {
                let mut of =
                    self.job_data.field_mgr.fields[output_field].borrow_mut();
                of.producing_transform_id =
                    Some(self.job_data.tf_mgr.transforms.peek_claim_id());

                let op_data_id = self.job_data.session_data.operator_bases
                    [op_id]
                    .op_data_id;

                of.producing_transform_arg =
                    self.job_data.session_data.operator_data[op_data_id]
                        .default_name()
                        .to_string();
            }
            let Some(mut instantiation) = op_data
                .build_transforms_expand_single(
                    self,
                    tf_state,
                    op_id,
                    prebound_outputs,
                )
            else {
                continue;
            };

            input_field = instantiation.next_input_field;
            input_group_track = instantiation.next_group_track;
            ms_id = instantiation.next_match_set;

            if let Some(pred) = predecessor_tf {
                self.job_data.tf_mgr.transforms[pred].successor =
                    Some(instantiation.tfs_begin);
            }

            if let Some(start) = start_tf_id {
                instantiation.tfs_begin = start;
            } else {
                start_tf_id = Some(instantiation.tfs_begin);
            }

            predecessor_tf = Some(instantiation.tfs_end);
        }
        let start = start_tf_id.unwrap();
        let end = predecessor_tf.unwrap_or(start);
        OperatorInstantiation {
            tfs_begin: start,
            tfs_end: end,
            next_match_set: ms_id,
            next_input_field: input_field,
            next_group_track: input_group_track,
        }
    }
    fn handle_stream_value_update(&mut self, svu: StreamValueUpdate) {
        #[cfg(feature = "debug_logging_streams")]
        {
            let jd = &mut self.job_data;
            eprintln!(
                ">    stream value update tf {:02} {:>20}, sv: {:02}, producers: {:?}, stack: {:?}, cutoff: {:?}",
                svu.tf_id,
                format!("`{}`", self.transform_data[svu.tf_id].display_name(jd, svu.tf_id)),
                svu.sv_id,
                jd.tf_mgr.stream_producers,
                jd.tf_mgr.ready_stack,
                jd.tf_mgr.pre_stream_transform_stack_cutoff
            );
            if !jd.sv_mgr.updates.is_empty() {
                eprint!("     :: pending sv updates: ");
                jd.sv_mgr.log_pending_updates(4);
                eprintln!();
            }
        }
        self.transform_data[svu.tf_id]
            .handle_stream_value_update(&mut self.job_data, svu);

        #[cfg(feature = "debug_log")]
        if let (Some(f), Some(start_tf)) =
            (&mut self.debug_log, self.job_data.start_tf)
        {
            if self.job_data.transform_step_count
                >= self.job_data.session_data.settings.debug_log_step_min
            {
                crate::debug_log::write_stream_value_update_to_html(
                    &self.job_data,
                    &self.transform_data,
                    svu,
                    start_tf,
                    f,
                )
                .expect("debug log write must succeed"); // TODO: handle this
                                                         // better
            }
        }
    }
    pub fn handle_transform(
        &mut self,
        tf_id: TransformId,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        #[cfg(feature = "debug_log")]
        let batch_size_available =
            self.job_data.tf_mgr.transforms[tf_id].available_batch_size;

        #[cfg(feature = "debug_logging_transform_update")]
        {
            #[cfg(feature = "debug_logging_transform_update_timestamps")]
            let timestamp =
                format!("[{}]", self.job_start.elapsed().unwrap().as_micros());

            #[cfg(not(feature = "debug_logging_transform_update_timestamps"))]
            let timestamp = "";

            eprintln!(
                "{timestamp}> {}: transform update {}",
                self.job_data.transform_step_count,
                self.job_data.tf_mgr.format_transform_state(
                    &self.job_data,
                    tf_id,
                    &self.transform_data,
                    None
                )
            );
        }
        if self.transform_data[tf_id].pre_update_required() {
            let mut tf = std::mem::replace(
                &mut self.transform_data[tf_id],
                Box::new(TfNop::default()),
            );
            let res = tf.pre_update(ctx, self, tf_id);
            let _ = std::mem::replace(&mut self.transform_data[tf_id], tf);
            res?;
        }
        self.transform_data[tf_id].update(&mut self.job_data, tf_id);
        if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
            if tf.mark_for_removal && !tf.is_stream_producer {
                self.remove_transform(tf_id);
            }
        }

        #[cfg(feature = "debug_logging_output_fields")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
            let output_field_id = tf.output_field;
            eprintln!(
                "/> {}: transform update {}",
                self.job_data.transform_step_count,
                self.job_data.tf_mgr.format_transform_state(
                    &self.job_data,
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
            #[cfg(feature = "debug_logging_iter_states")]
            group_track.eprint_iter_states(4);
            eprint!("\n   - out ");
            self.job_data.field_mgr.print_field_stats(output_field_id);
            self.job_data
                .field_mgr
                .print_field_header_data(output_field_id, 4);
            #[cfg(feature = "debug_logging_iter_states")]
            self.job_data
                .field_mgr
                .print_field_iter_data(output_field_id, 4);
            eprintln!();
        }
        #[cfg(feature = "debug_log")]
        if let (Some(f), Some(start_tf)) =
            (&mut self.debug_log, self.job_data.start_tf)
        {
            if self.job_data.transform_step_count
                >= self.job_data.session_data.settings.debug_log_step_min
            {
                crate::debug_log::write_transform_update_to_html(
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
        }
        Ok(())
    }

    pub(crate) fn run_stream_producer_update(&mut self, tf_id: TransformId) {
        #[cfg(feature = "debug_logging_streams")]
        eprintln!(
            "> stream producer update tf {:02} {:>20}, producers: {:?}, stack: {:?}",
            tf_id,
            format!("`{}`", self.transform_data[tf_id].display_name(&self.job_data, tf_id)),
            self.job_data.tf_mgr.stream_producers,
            self.job_data.tf_mgr.ready_stack,
        );
        let tf_state = &mut self.job_data.tf_mgr.transforms[tf_id];
        tf_state.is_stream_producer = false;
        self.transform_data[tf_id]
            .stream_producer_update(&mut self.job_data, tf_id);

        #[cfg(feature = "debug_log")]
        if let (Some(f), Some(start_tf)) =
            (&mut self.debug_log, self.job_data.start_tf)
        {
            if self.job_data.transform_step_count
                >= self.job_data.session_data.settings.debug_log_step_min
            {
                crate::debug_log::write_stream_producer_update_to_html(
                    &self.job_data,
                    &self.transform_data,
                    tf_id,
                    start_tf,
                    f,
                )
                .expect("debug log write must succeed"); // TODO: handle this
                                                         // better
            }
        }

        #[cfg(feature = "debug_logging_streams")]
        eprintln!(
            "/> stream producer update tf {:02} {:>20}, producers: {:?}, stack: {:?}",
            tf_id,
            format!("`{}`", self.transform_data[tf_id].display_name(&self.job_data, tf_id)),
            self.job_data.tf_mgr.stream_producers,
            self.job_data.tf_mgr.ready_stack,
         );
    }
    pub fn is_in_streaming_mode(&self) -> bool {
        !self.job_data.tf_mgr.stream_producers.is_empty()
    }
    fn cleanup_action_lists(&mut self) {
        #[cfg(feature = "debug_log")]
        let mut count_diff = 0;

        #[cfg(feature = "debug_log")]
        {
            for ms in &self.job_data.match_set_mgr.match_sets {
                count_diff += ms.action_buffer.borrow().action_group_count();
            }
        }

        for field_id in self.job_data.field_mgr.fields.indices() {
            self.job_data.field_mgr.apply_field_actions(
                &self.job_data.match_set_mgr,
                field_id,
                false,
            );
        }
        for track_id in
            self.job_data.group_track_manager.group_tracks.indices()
        {
            self.job_data.group_track_manager.apply_actions_to_track(
                &self.job_data.match_set_mgr,
                track_id,
            );
        }

        #[cfg(debug_assertions)]
        {
            for ms in &self.job_data.match_set_mgr.match_sets {
                debug_assert_eq!(
                    ms.action_buffer.borrow().action_group_count(),
                    0
                );
            }
        }

        #[cfg(feature = "debug_log")]
        {
            #[cfg(feature = "debug_logging_field_action_group_accel")]
            eprintln!("action list cleanup: {count_diff} cleaned");

            if let Some(dl) = &mut self.debug_log {
                if self.job_data.transform_step_count
                    >= self.job_data.session_data.settings.debug_log_step_min
                {
                    crate::debug_log::write_action_list_cleanup_to_html(
                        &self.job_data,
                        &self.transform_data,
                        self.job_data.start_tf.unwrap(),
                        count_diff,
                        dl,
                    )
                    .expect("debug log write succeeds");
                }
            }
        }
    }
    pub(crate) fn run(
        &mut self,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        #[cfg(feature = "debug_log")]
        if let Some(dl) = &mut self.debug_log {
            crate::debug_log::write_debug_log_html_head(dl)
                .expect("debug log write succeeds");
            if let Some(start_tf) = self.job_data.start_tf {
                crate::debug_log::write_initial_state_to_html(
                    &self.job_data,
                    &self.transform_data,
                    start_tf,
                    dl,
                )
                .expect("debug log write must succeed");
            }
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
            self.job_data.transform_step_count += 1;

            if self.job_data.transform_step_count
                % self
                    .job_data
                    .session_data
                    .settings
                    .action_list_cleanup_frequency
                == 0
            {
                self.cleanup_action_lists();
            }

            #[cfg(debug_assertions)]
            if self.job_data.session_data.settings.debug_break_on_step
                == Some(self.job_data.transform_step_count)
            {
                debugbreak!();
            }
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
        #[cfg(feature = "debug_log")]
        if let Some(dl) = &mut self.debug_log {
            crate::debug_log::write_debug_log_html_tail(dl)
                .expect("debug log write succeeds");
        }
        Ok(())
    }
}

impl<'a> JobData<'a> {
    pub fn new(sess: &'a SessionData) -> Self {
        Self {
            session_data: sess,
            tf_mgr: TransformManager::default(),
            field_mgr: FieldManager::default(),
            match_set_mgr: MatchSetManager::default(),
            // PERF: we should probably try to reuse these scopes somehow,
            // or use an offset universe or something instead of this clone
            scope_mgr: sess.scope_mgr.clone(),
            group_track_manager: GroupTrackManager::default(),
            sv_mgr: StreamValueManager::default(),
            temp_vec: Vec::default(),
            start_tf: None,
            transform_step_count: 0,
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
        if let Some(succ_id) = successor {
            let succ = &mut self.tf_mgr.transforms[succ_id];
            succ.predecessor_done = input_is_done;
            let bs = available_batch_for_successor;
            succ.available_batch_size += bs;
            if input_is_done || succ.available_batch_size > 0 {
                self.tf_mgr.push_tf_in_ready_stack(succ_id);
            }
        }
    }
    pub fn get_scope_setting_or_default<S: ChainSetting>(
        &self,
        scope_id: ScopeId,
    ) -> S::Type {
        S::lookup(
            &self.scope_mgr,
            &self.session_data.settings.chain_setting_names,
            scope_id,
        )
        .and_then(|(v, _span)| v.ok())
        .unwrap_or(S::DEFAULT)
    }
    pub fn get_setting_from_tf_state<S: ChainSetting>(
        &self,
        tf_state: &TransformState,
    ) -> S::Type {
        self.get_scope_setting_or_default::<S>(
            self.match_set_mgr.match_sets[tf_state.match_set_id].active_scope,
        )
    }
    pub fn get_transform_chain_from_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> &Chain {
        let op_id = tf_state.op_id.unwrap();
        let chain_id = self.session_data.operator_bases[op_id].chain_id;
        &self.session_data.chains[chain_id]
    }
    pub fn get_transform_chain(&self, tf_id: TransformId) -> &Chain {
        self.get_transform_chain_from_tf_state(&self.tf_mgr.transforms[tf_id])
    }

    /// Like `add_actor_for_tf_state`, but does not update the output field.
    /// Useful for transforms without a unique output field
    pub fn add_actor_for_tf_state_ignore_output_field(
        &self,
        tf_state: &TransformState,
    ) -> ActorId {
        let ms = &self.match_set_mgr.match_sets[tf_state.match_set_id];
        // can't just use debug assert because it wouldn't compile without the
        // `iters_added` member
        #[cfg(debug_assertions)]
        assert!(
            !tf_state.iters_added.get(),
            "transform actor should be added to before iters"
        );
        let mut ab = ms.action_buffer.borrow_mut();
        ab.add_actor()
    }

    /// Adds an actor to the match set specified by the transform state
    /// and applies it to the first actor entry of the `output_field`.
    /// !!! This should be done *before* and iters
    /// are added using `claim_iter_for_tf_state`, otherwise those iters
    /// will have an incorrect `ActorId` for `first_right_leaning_actor`.
    pub fn add_actor_for_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> ActorId {
        let actor_id =
            self.add_actor_for_tf_state_ignore_output_field(tf_state);

        let ms = &self.match_set_mgr.match_sets[tf_state.match_set_id];

        debug_assert_ne!(ms.dummy_field, tf_state.output_field);
        debug_assert_ne!(tf_state.input_field, tf_state.output_field);
        self.field_mgr.fields[tf_state.output_field]
            .borrow()
            .first_actor
            .set(ActorRef::Unconfirmed(actor_id.wrapping_add(ActorId::ONE)));
        actor_id
    }
    pub fn claim_iter_for_tf_state_and_field(
        &self,
        tf_state: &TransformState,
        field_id: FieldId,
    ) -> FieldIterId {
        #[cfg(debug_assertions)]
        tf_state.iters_added.set(true);
        self.field_mgr.claim_iter(
            field_id,
            self.match_set_mgr.match_sets[tf_state.match_set_id]
                .action_buffer
                .borrow()
                .peek_next_actor_id(),
            IterKind::Transform(self.tf_mgr.transforms.peek_claim_id()),
        )
    }
    pub fn claim_iter_ref_for_tf_state_and_field(
        &self,
        tf_state: &TransformState,
        field_id: FieldId,
    ) -> FieldIterRef {
        FieldIterRef {
            iter_id: self
                .claim_iter_for_tf_state_and_field(tf_state, field_id),
            field_id,
        }
    }
    pub fn claim_iter_for_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> FieldIterId {
        self.claim_iter_for_tf_state_and_field(tf_state, tf_state.input_field)
    }
    pub fn claim_iter_ref_for_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> FieldIterRef {
        self.claim_iter_ref_for_tf_state_and_field(
            tf_state,
            tf_state.input_field,
        )
    }
    pub fn claim_group_track_iter_for_tf_state(
        &mut self,
        tf_state: &TransformState,
    ) -> GroupTrackIterRef {
        self.group_track_manager.claim_group_track_iter_ref(
            tf_state.input_group_track_id,
            self.match_set_mgr.match_sets[tf_state.match_set_id]
                .action_buffer
                .borrow()
                .peek_next_actor_id(),
            IterKind::Transform(self.tf_mgr.transforms.peek_claim_id()),
        )
    }
}
