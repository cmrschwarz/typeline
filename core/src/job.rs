use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    chain::Chain,
    context::{ContextData, JobDescription, SessionData, VentureDescription},
    liveness_analysis::OpOutputIdx,
    operators::{
        call::handle_eager_call_expansion,
        call_concurrent::setup_callee_concurrent,
        operator::{
            operator_build_transforms, OperatorData, OperatorId,
            OperatorInstantiation, OutputFieldKind, TransformContinuationKind,
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
        iter_hall::{IterId, IterKind},
        match_set::{MatchSetId, MatchSetManager},
        push_interface::PushInterface,
        record_buffer::RecordBuffer,
        stream_value::{StreamValueManager, StreamValueUpdate},
    },
    utils::{identity_hasher::BuildIdentityHasher, universe::Universe},
};

// a helper type so we can pass a transform handler typed
// TransformData + all the other Data of the WorkerThreadSession
pub struct JobData<'a> {
    pub session_data: &'a SessionData,
    pub tf_mgr: TransformManager,
    pub match_set_mgr: MatchSetManager,
    pub field_mgr: FieldManager,
    pub sv_mgr: StreamValueManager<'a>,
    pub temp_vec: Vec<u8>,
}

pub struct Job<'a> {
    pub job_data: JobData<'a>,
    pub transform_data: Vec<TransformData<'a>>,
    pub temp_vec: Vec<FieldId>,
}

#[derive(Default)]
pub struct TransformManager {
    pub transforms: Universe<TransformId, TransformState>,
    pub ready_stack: Vec<TransformId>,
    pub stream_producers: VecDeque<TransformId>,
    pub pre_stream_transform_stack_cutoff: usize,
}

#[derive(Clone, Copy, Default)]
pub struct PipelineState {
    pub input_done: bool,
    pub successor_done: bool,
    pub next_batch_ready: bool,
}

impl TransformManager {
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
        msm: &mut MatchSetManager,
        tf_id: TransformId,
        batch_size: usize,
        predecessor_done: bool,
    ) {
        msm.update_cow_targets(fm, self.transforms[tf_id].match_set_id);
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
            #[cfg(feature = "debug_logging")]
            {
                eprintln!(
                    ">> tf {tf_id:02} became a stream producer: {:?}, stack: {:?}, cutoff: {}",
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
    tf_data: &mut Vec<TransformData<'a>>,
    state: TransformState,
    data: TransformData<'a>,
) -> TransformId {
    let id = jd.tf_mgr.transforms.claim_with_value(state);
    if tf_data.len() < jd.tf_mgr.transforms.used_capacity() {
        tf_data.resize_with(jd.tf_mgr.transforms.used_capacity(), || {
            TransformData::Disabled
        });
    }
    tf_data[usize::from(id)] = data;
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
            sv_mgr: StreamValueManager::default(),
            temp_vec: Vec::default(),
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
                let name = self.transform_data[i.get()].display_name();
                eprintln!(
                    "tf {} -> {} [fields {} {} {}] (ms {}): {}",
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
    pub fn setup_job(&mut self, mut job: JobDescription) {
        let ms_id = self.job_data.match_set_mgr.add_match_set();
        // TODO: unpack record set properly here
        let input_record_count = job.data.adjust_field_lengths();
        let mut input_field = None;
        let mut input_data_fields = std::mem::take(&mut self.temp_vec);
        for mut fd in job.data.fields {
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
        self.job_data.match_set_mgr.match_sets[ms_id]
            .group_tracker
            .append_group_to_all_active_lists(input_record_count);

        #[cfg(feature = "debug_logging")]
        for (i, f) in input_data_fields.iter().enumerate() {
            self.job_data.field_mgr.fields[*f]
                .borrow_mut()
                .producing_transform_arg = format!("<Input Field #{i}>");
        }
        let instantiation = self.setup_transforms_from_op(
            ms_id,
            job.operator,
            input_field,
            None,
            &HashMap::default(),
        );
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
                        self.job_data.session_data.operator_bases
                            [op_id as usize]
                            .argname,
                    )
                    .into()
            } else {
                self.transform_data[tf_id.get()].display_name().to_string()
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
        self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
    }
    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
        mut predecessor_tf: Option<TransformId>,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> OperatorInstantiation {
        let mut start_tf_id = None;
        let start_op =
            &self.job_data.session_data.operator_bases[start_op_id as usize];
        let mut input_field = chain_input_field_id;
        let ops = &self.job_data.session_data.chains
            [start_op.chain_id.unwrap() as usize]
            .operators[start_op.offset_in_chain as usize..];
        for &op_id in ops {
            let op_base =
                &self.job_data.session_data.operator_bases[op_id as usize];
            let op_data =
                &self.job_data.session_data.operator_data[op_id as usize];
            match op_data {
                OperatorData::Call(op) => {
                    if !op.lazy {
                        let mut instantiation = handle_eager_call_expansion(
                            self,
                            op_id,
                            ms_id,
                            input_field,
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
            );
            tf_state.is_transparent = op_base.transparent_mode;

            #[cfg(feature = "debug_logging")]
            if output_field_kind == OutputFieldKind::Unique {
                let mut of =
                    self.job_data.field_mgr.fields[output_field].borrow_mut();
                of.producing_transform_id =
                    Some(self.job_data.tf_mgr.transforms.peek_claim_id());
                of.producing_transform_arg =
                    self.job_data.session_data.operator_data[op_id as usize]
                        .default_op_name()
                        .to_string();
            }
            let mut instantiation = operator_build_transforms(
                self,
                tf_state,
                op_id,
                prebound_outputs,
            );

            input_field = instantiation.next_input_field;

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
            continuation: TransformContinuationKind::Regular,
        }
    }
    fn handle_stream_value_update(&mut self, svu: StreamValueUpdate) {
        #[cfg(feature = "debug_logging")]
        {
            let jd = &mut self.job_data;
            eprintln!(
            "> handling stream value {} update for tf {} (`{}`): producers: {:?}, stack: {:?}, cutoff: {:?}",
            svu.sv_id,
            svu.tf_id,
            self.transform_data[svu.tf_id.get()].display_name(),
            jd.tf_mgr.stream_producers,
            jd.tf_mgr.ready_stack,
            jd.tf_mgr.pre_stream_transform_stack_cutoff

        );
            eprintln!("     pending updates: {:?}", jd.sv_mgr.updates);
        }
        transform_stream_value_update(self, svu);
    }
    pub fn handle_transform(
        &mut self,
        tf_id: TransformId,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        #[cfg(feature = "debug_logging")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
            eprintln!(
            "> handling tf {tf_id} `{}`, in_fid: {}, bsa: {}, pred_done: {}, done: {}, stack: {:?}",
            self.transform_data[tf_id.get()].display_name(),
            tf.input_field,
            tf.available_batch_size,
            tf.predecessor_done,
            tf.done,
            self.job_data.tf_mgr.ready_stack
        );
        }
        transform_pre_update(self, tf_id, ctx)?;
        transform_update(self, tf_id);
        if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
            if tf.mark_for_removal && !tf.is_stream_producer {
                self.remove_transform(tf_id);
            }
        }

        #[cfg(feature = "output_field_logging")]
        {
            let output_field_id =
                self.job_data.tf_mgr.transforms[tf_id].output_field;
            eprint!(
                "/> tf {} `{}` output field: ",
                tf_id,
                self.transform_data[tf_id.get()].display_name()
            );
            self.job_data.field_mgr.print_field_stats(output_field_id);
            self.job_data
                .field_mgr
                .print_field_header_data(output_field_id, 0);
            eprintln!();
        }
        Ok(())
    }

    pub(crate) fn run_stream_producer_update(&mut self, tf_id: TransformId) {
        #[cfg(feature = "debug_logging")]
        eprintln!(
            "> handling stream producer update for tf {} (`{}`), producers: {:?}, stack: {:?}",
            tf_id,
            self.transform_data[tf_id.get()].display_name(),
            self.job_data.tf_mgr.stream_producers,
            self.job_data.tf_mgr.ready_stack,
        );
        let tf_state = &mut self.job_data.tf_mgr.transforms[tf_id];
        tf_state.is_stream_producer = false;
        stream_producer_update(self, tf_id);
        #[cfg(feature = "debug_logging")]
        eprintln!(
            "/> finished stream producer update for tf {} (`{}`), producers: {:?}, stack: {:?}",
            tf_id,
            self.transform_data[tf_id.get()].display_name(),
            self.job_data.tf_mgr.stream_producers,
            self.job_data.tf_mgr.ready_stack,
        );
    }
    pub fn is_in_streaming_mode(&self) -> bool {
        !self.job_data.tf_mgr.stream_producers.is_empty()
    }
    pub(crate) fn run(
        &mut self,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        loop {
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
        Ok(())
    }
}

impl JobData<'_> {
    pub fn get_transform_chain_from_tf_state(
        &self,
        tf_state: &TransformState,
    ) -> &Chain {
        let op_id = tf_state.op_id.unwrap();
        let chain_id = self.session_data.operator_bases[op_id as usize]
            .chain_id
            .unwrap();
        &self.session_data.chains[chain_id as usize]
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
