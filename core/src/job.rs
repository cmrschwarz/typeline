use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    chain::Chain,
    context::{ContextData, JobDescription, SessionData, VentureDescription},
    liveness_analysis::OpOutputIdx,
    operators::{
        aggregator::{
            handle_tf_aggregator_header, handle_tf_aggregator_trailer,
            insert_tf_aggregator,
        },
        call::{
            build_tf_call, handle_eager_call_expansion,
            handle_lazy_call_expansion,
        },
        call_concurrent::{
            build_tf_call_concurrent, handle_call_concurrent_expansion,
            handle_tf_call_concurrent, handle_tf_callee_concurrent,
            setup_callee_concurrent,
        },
        cast::{build_tf_cast, handle_tf_cast},
        count::{build_tf_count, handle_tf_count},
        field_value_sink::{
            build_tf_field_value_sink, handle_tf_field_value_sink,
            handle_tf_field_value_sink_stream_value_update,
        },
        file_reader::{
            build_tf_file_reader, handle_tf_file_reader,
            handle_tf_file_reader_stream,
        },
        foreach::{
            handle_tf_foreach_header, handle_tf_foreach_trailer,
            insert_tf_foreach,
        },
        fork::{build_tf_fork, handle_fork_expansion, handle_tf_fork},
        forkcat::{
            handle_forkcat_subchain_expansion, handle_tf_forkcat,
            insert_tf_forkcat,
        },
        format::{
            build_tf_format, handle_tf_format,
            handle_tf_format_stream_value_update,
        },
        input_done_eater::handle_tf_input_done_eater,
        join::{
            build_tf_join, handle_tf_join, handle_tf_join_stream_value_update,
        },
        literal::{build_tf_literal, handle_tf_literal},
        nop::{build_tf_nop, handle_tf_nop},
        nop_copy::{build_tf_nop_copy, handle_tf_nop_copy},
        operator::{OperatorData, OperatorId},
        print::{
            build_tf_print, handle_tf_print,
            handle_tf_print_stream_value_update,
        },
        regex::{
            build_tf_regex, handle_tf_regex,
            handle_tf_regex_stream_value_update,
        },
        select::{build_tf_select, handle_tf_select},
        sequence::{build_tf_sequence, handle_tf_sequence},
        string_sink::{
            build_tf_string_sink, handle_tf_string_sink,
            handle_tf_string_sink_stream_value_update,
        },
        terminator::{add_terminator, handle_tf_terminator},
        transform::{TransformData, TransformId, TransformState},
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{FieldId, FieldManager, VOID_FIELD_ID},
        field_action::FieldActionKind,
        match_set::{MatchSetId, MatchSetManager},
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
    pub sv_mgr: StreamValueManager,
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
    pub pre_stream_transform_stack_cutoff: Option<usize>,
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
            next_batch_ready,
            successor_done,
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
        let ms_id = self.transforms[tf_id].match_set_id;
        let ab = &mut msm.match_sets[ms_id].action_buffer;
        ab.begin_action_group(actor_id);
        ab.push_action(FieldActionKind::Drop, 0, batch_size);
        ab.end_action_group();
        self.submit_batch(tf_id, 0, true);
    }
    pub fn submit_batch_ready_for_more(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
        ps: PipelineState,
    ) {
        let done = ps.input_done || ps.successor_done;
        // In case we are done, there's no need to re-ready. There's 3 cases:
        // a) our predecessor has more records and will push us himself (fine)
        // b) a reset happens
        // c) we terminate happily
        // In all 3 cases we don't have to do anything.
        if !done && ps.next_batch_ready {
            self.push_tf_in_ready_stack(tf_id);
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
            tf.is_stream_producer = true;
            self.stream_producers.push_back(tf_id);
        }
    }
    pub fn maintain_single_value(
        &mut self,
        tf_id: TransformId,
        length: &mut Option<usize>,
        field_mgr: &FieldManager,
        match_set_mgr: &mut MatchSetManager,
        initial_call: bool,
        final_call_if_input_done: bool,
    ) -> (usize, PipelineState) {
        let tf = &mut self.transforms[tf_id];
        let output_field_id = tf.output_field;
        let match_set_id = tf.match_set_id;
        let desired_batch_size = tf.desired_batch_size;
        let has_appender = tf.has_appender;
        let max_batch_size = if let Some(len) = length {
            *len
        } else if has_appender {
            if !initial_call {
                if final_call_if_input_done {
                    field_mgr.fields[output_field_id]
                        .borrow_mut()
                        .iter_hall
                        .drop_last_value(1);
                }
                let ps = PipelineState {
                    input_done: true,
                    successor_done: false,
                    next_batch_ready: false,
                };
                return (0, ps);
            }
            1
        } else {
            usize::MAX
        };
        let (mut batch_size, mut ps) = self.claim_batch_with_limit(
            tf_id,
            max_batch_size.min(desired_batch_size),
        );
        if batch_size == 0 {
            if !initial_call {
                if final_call_if_input_done {
                    field_mgr.fields[output_field_id]
                        .borrow_mut()
                        .iter_hall
                        .drop_last_value(1);
                }
                ps.input_done = true;
                return (0, ps);
            }
            batch_size = length.unwrap_or(1);
        }
        if let Some(len) = length {
            *len -= batch_size;
            if *len == 0 {
                ps.input_done = true;
            }
        } else if has_appender {
            ps.input_done = true;
        }
        match_set_mgr.match_sets[match_set_id]
            .action_buffer
            .execute(field_mgr, output_field_id);
        // this results in always one more element being present than we
        // advertise as batch size. this prevents apply_field_actions
        // from deleting our value. unless we are done, in which case
        // no additional value is inserted
        let mut output_field = field_mgr.fields[output_field_id].borrow_mut();
        let done = ps.input_done && final_call_if_input_done;
        if batch_size == 0 && done {
            output_field.iter_hall.drop_last_value(1);
        } else {
            output_field
                .iter_hall
                .dup_last_value(batch_size - done as usize);
        }
        if done {
            ps.next_batch_ready = false;
        }
        (batch_size, ps)
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
            let clear_delay = f.get_clear_delay_request_count() > 0;
            if clear_delay {
                drop(f);
                fm.apply_field_actions(msm, ofid);
            } else {
                drop(f);
                fm.clear_if_owned(msm, ofid);
            }
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
                match_sets: Default::default(),
            },
            sv_mgr: Default::default(),
            temp_vec: Default::default(),
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
    pub fn print_field_stats(&self, _id: FieldId) {
        #[cfg(feature = "debug_logging")]
        {
            let id = _id;
            let field = self.field_mgr.fields[id].borrow();
            print!("field id {id}");
            // if let Some(name) = field.name {
            //    print!(" '@{}'", self.session_data.string_store.lookup(name));
            //}
            print!(", ms {}", field.match_set);
            if let Some(prod_id) = field.producing_transform_id {
                print!(
                    " (output of tf {prod_id} `{}`)",
                    field.producing_transform_arg
                )
            } else if !field.producing_transform_arg.is_empty() {
                print!(" (`{}`)", field.producing_transform_arg)
            }
            if field.shadowed_by != VOID_FIELD_ID {
                print!(
                    " (aliased by field id {} since actor id `{}`)",
                    field.shadowed_by, field.shadowed_since
                )
            }
            if let (cow_src_field, Some(data_cow)) =
                field.iter_hall.cow_source_field(&self.field_mgr)
            {
                print!(
                    " [{}cow{}]",
                    if data_cow { "data " } else { "" },
                    if let Some(src) = cow_src_field {
                        format!(" src: {src}")
                    } else {
                        "".to_owned()
                    }
                );
            }
            if !field.field_refs.is_empty() {
                print!(" ( field refs:");
                for fr in &field.field_refs {
                    print!(" {fr}");
                }
                print!(" )");
            }
            print!(" (rc {})", field.ref_count);
        }
    }
}

pub enum TransformContinuationKind {
    Regular,
    SelfExpanded,
}

impl<'a> Job<'a> {
    pub fn log_state(&self, message: &str) {
        if cfg!(feature = "debug_logging") {
            println!("{message}");
            for (i, tf) in self.job_data.tf_mgr.transforms.iter_enumerated() {
                let name = self.transform_data[i.get()].display_name();
                println!(
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
                self.job_data.print_field_stats(i);
                println!();
            }
        }
    }
    pub fn setup_job(&mut self, mut job: JobDescription) {
        let ms_id = self.job_data.match_set_mgr.add_match_set();
        // TODO: unpack record set properly here
        let input_record_count = job.data.adjust_field_lengths();
        let mut input_data = None;
        let mut input_data_fields = std::mem::take(&mut self.temp_vec);
        for fd in job.data.fields.into_iter() {
            let field_id = self.job_data.field_mgr.add_field_with_data(
                &mut self.job_data.match_set_mgr,
                ms_id,
                fd.name,
                ActorRef::default(),
                fd.data,
            );
            input_data_fields.push(field_id);
            if input_data.is_none() {
                input_data = Some(field_id);
            }
        }
        let input_data = input_data.unwrap_or(VOID_FIELD_ID);

        #[cfg(feature = "debug_logging")]
        for (i, f) in input_data_fields.iter().enumerate() {
            self.job_data.field_mgr.fields[*f]
                .borrow_mut()
                .producing_transform_arg = format!("<Input Field #{i}>");
        }
        let (start_tf_id, end_tf_id, _next_input_field) = self
            .setup_transforms_from_op(
                ms_id,
                job.operator,
                input_data,
                None,
                &Default::default(),
            );
        add_terminator(self, ms_id, end_tf_id);
        self.job_data.tf_mgr.push_tf_in_ready_stack(start_tf_id);
        let tf = &mut self.job_data.tf_mgr.transforms[start_tf_id];
        tf.predecessor_done = true;
        tf.available_batch_size = input_record_count;
        for input_field_id in input_data_fields.iter() {
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
        let (start_tf_id, _end_tf_id, _next_input_field) =
            setup_callee_concurrent(self, ms_id, buffer, start_op_id);
        self.job_data.tf_mgr.push_tf_in_ready_stack(start_tf_id);
        self.log_state("setting up venture");
    }

    pub fn remove_transform(&mut self, tf_id: TransformId) {
        let tf = &self.job_data.tf_mgr.transforms[tf_id];
        debug_assert!(!tf.is_ready);
        let tfif = tf.input_field;
        let tfof = tf.output_field;
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
            println!("removing tf id {tf_id}: `{name}`");
        }
        self.job_data
            .field_mgr
            .drop_field_refcount(tfif, &mut self.job_data.match_set_mgr);
        self.job_data
            .field_mgr
            .drop_field_refcount(tfof, &mut self.job_data.match_set_mgr);
        self.job_data.tf_mgr.transforms.release(tf_id);
        self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
    }
    pub fn insert_transform_from_op(
        &mut self,
        mut tf_state: TransformState,
        op_id: OperatorId,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> (TransformId, TransformId, FieldId, TransformContinuationKind) {
        let jd = &mut self.job_data;
        let op_base = &jd.session_data.operator_bases[op_id as usize];
        let op_data = &jd.session_data.operator_data[op_id as usize];
        let tfs = &mut tf_state;
        let mut next_input_field = tfs.input_field;
        let tf_data = match op_data {
            OperatorData::Nop(op) => build_tf_nop(op, tfs),
            OperatorData::NopCopy(op) => build_tf_nop_copy(jd, op, tfs),
            OperatorData::Cast(op) => build_tf_cast(jd, op_base, op, tfs),
            OperatorData::Count(op) => build_tf_count(jd, op_base, op, tfs),
            OperatorData::Foreach(op) => {
                return insert_tf_foreach(
                    self,
                    op,
                    tf_state,
                    op_base.chain_id.unwrap(),
                    op_id,
                    prebound_outputs,
                );
            }
            OperatorData::Fork(op) => build_tf_fork(jd, op_base, op, tfs),
            OperatorData::ForkCat(op) => {
                return insert_tf_forkcat(self, op_base, op, tf_state);
            }
            OperatorData::Print(op) => build_tf_print(jd, op_base, op, tfs),
            OperatorData::Join(op) => build_tf_join(jd, op_base, op, tfs),
            OperatorData::Regex(op) => {
                build_tf_regex(jd, op_base, op, tfs, prebound_outputs)
            }
            OperatorData::Format(op) => build_tf_format(jd, op_base, op, tfs),
            OperatorData::StringSink(op) => {
                build_tf_string_sink(jd, op_base, op, tfs)
            }
            OperatorData::FieldValueSink(op) => {
                build_tf_field_value_sink(jd, op_base, op, tfs)
            }
            OperatorData::FileReader(op) => {
                build_tf_file_reader(jd, op_base, op, tfs)
            }
            OperatorData::Literal(op) => {
                build_tf_literal(jd, op_base, op, tfs)
            }
            OperatorData::Sequence(op) => {
                build_tf_sequence(jd, op_base, op, tfs)
            }
            OperatorData::Select(op) => build_tf_select(jd, op_base, op, tfs),
            OperatorData::Call(op) => build_tf_call(jd, op_base, op, tfs),
            OperatorData::CallConcurrent(op) => {
                build_tf_call_concurrent(jd, op_base, op, tfs)
            }
            OperatorData::Key(_) => unreachable!(),
            OperatorData::Next(_) => unreachable!(),
            OperatorData::End(_) => unreachable!(),
            OperatorData::Custom(op) => {
                op.build_transform(jd, op_base, tfs, prebound_outputs)
            }
            OperatorData::Aggregator(op) => {
                return insert_tf_aggregator(
                    self,
                    op,
                    tf_state,
                    op_id,
                    prebound_outputs,
                );
            }
        };
        if !tf_state.is_transparent {
            next_input_field = tf_state.output_field;
        };
        let tf_id = add_transform_to_job(
            &mut self.job_data,
            &mut self.transform_data,
            tf_state,
            tf_data,
        );
        (
            tf_id,
            tf_id,
            next_input_field,
            TransformContinuationKind::Regular,
        )
    }

    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
        mut predecessor_tf: Option<TransformId>,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> (TransformId, TransformId, FieldId) {
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
            let mut dummy_output = false;
            let mut make_input_output = false;
            match op_data {
                OperatorData::Call(op) => {
                    if !op.lazy {
                        let (start_exp, end_exp, next_input_field) =
                            handle_eager_call_expansion(
                                self,
                                op_id,
                                ms_id,
                                input_field,
                                predecessor_tf,
                            );
                        return (
                            start_tf_id.unwrap_or(start_exp),
                            end_exp,
                            next_input_field,
                        );
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
                        input_field = self.job_data.field_mgr.add_field(
                            &mut self.job_data.match_set_mgr,
                            ms_id,
                            Some(op.key_interned.unwrap()),
                            self.job_data
                                .field_mgr
                                .get_first_actor(input_field),
                        );
                    }
                    if !op.field_is_read {
                        continue;
                    }
                    make_input_output = true;
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
                OperatorData::Fork(_) | OperatorData::Nop(_) => {
                    dummy_output = true
                }
                _ => (),
            }
            let mut label_added = false;
            let output_field = if dummy_output {
                self.job_data.field_mgr.bump_field_refcount(VOID_FIELD_ID);
                VOID_FIELD_ID
            } else if make_input_output {
                self.job_data.field_mgr.bump_field_refcount(input_field);
                input_field
            } else {
                let first_actor =
                    self.job_data.field_mgr.get_first_actor(input_field);
                if let Some(field_idx) =
                    prebound_outputs.get(&op_base.outputs_start)
                {
                    self.job_data.field_mgr.bump_field_refcount(*field_idx);
                    let mut f = self.job_data.field_mgr.fields[*field_idx]
                        .borrow_mut();
                    debug_assert!(f.name == op_base.label);
                    label_added = true;
                    f.first_actor = first_actor;
                    f.snapshot = Default::default();
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
            if !dummy_output && !make_input_output {
                let mut of =
                    self.job_data.field_mgr.fields[output_field].borrow_mut();
                of.producing_transform_id =
                    Some(self.job_data.tf_mgr.transforms.peek_claim_id());
                of.producing_transform_arg =
                    self.job_data.session_data.operator_data[op_id as usize]
                        .default_op_name()
                        .to_string();
            }
            let (first_tf_id, last_tf_id, next_input_field, cont) = self
                .insert_transform_from_op(tf_state, op_id, prebound_outputs);
            input_field = next_input_field;

            if let Some(pred) = predecessor_tf {
                self.job_data.tf_mgr.transforms[pred].successor =
                    Some(first_tf_id);
            }

            if start_tf_id.is_none() {
                start_tf_id = Some(first_tf_id);
            }

            match cont {
                TransformContinuationKind::Regular => (),
                TransformContinuationKind::SelfExpanded => {
                    return (
                        start_tf_id.unwrap_or(first_tf_id),
                        last_tf_id,
                        next_input_field,
                    )
                }
            }

            predecessor_tf = Some(last_tf_id);
        }
        let start = start_tf_id.unwrap();
        let end = predecessor_tf.unwrap_or(start);
        (start, end, input_field)
    }
    fn handle_stream_value_update(&mut self, svu: StreamValueUpdate) {
        #[cfg(feature = "debug_logging")]
        println!(
            "> handling stream value {} update for tf {} (`{}`)",
            svu.sv_id,
            svu.tf_id,
            self.transform_data[svu.tf_id.get()].display_name()
        );
        match &mut self.transform_data[usize::from(svu.tf_id)] {
            TransformData::Print(tf) => handle_tf_print_stream_value_update(
                &mut self.job_data,
                svu.tf_id,
                tf,
                svu.sv_id,
                svu.custom,
            ),
            TransformData::Join(tf) => handle_tf_join_stream_value_update(
                &mut self.job_data,
                svu.tf_id,
                tf,
                svu.sv_id,
                svu.custom,
            ),
            TransformData::StringSink(tf) => {
                handle_tf_string_sink_stream_value_update(
                    &mut self.job_data,
                    svu.tf_id,
                    tf,
                    svu.sv_id,
                    svu.custom,
                )
            }
            TransformData::FieldValueSink(tf) => {
                handle_tf_field_value_sink_stream_value_update(
                    &mut self.job_data,
                    svu.tf_id,
                    tf,
                    svu.sv_id,
                    svu.custom,
                )
            }
            TransformData::Format(tf) => handle_tf_format_stream_value_update(
                &mut self.job_data,
                svu.tf_id,
                tf,
                svu.sv_id,
                svu.custom,
            ),
            TransformData::Regex(tf) => handle_tf_regex_stream_value_update(
                &mut self.job_data,
                svu.tf_id,
                tf,
                svu.sv_id,
                svu.custom,
            ),
            TransformData::Fork(_) => (),
            TransformData::ForeachHeader(_) => unreachable!(),
            TransformData::ForeachTrailer(_) => unreachable!(),
            TransformData::ForkCat(_) => (),
            TransformData::CallConcurrent(_) => (),
            TransformData::Terminator(_) => unreachable!(),
            TransformData::Call(_) => unreachable!(),
            TransformData::Nop(_) => unreachable!(),
            TransformData::NopCopy(_) => unreachable!(),
            TransformData::InputDoneEater(_) => unreachable!(),
            TransformData::Cast(_) => unreachable!(),
            TransformData::Count(_) => unreachable!(),
            TransformData::Select(_) => unreachable!(),
            TransformData::FileReader(_) => unreachable!(),
            TransformData::Sequence(_) => unreachable!(),
            TransformData::Disabled => unreachable!(),
            TransformData::Literal(_) => unreachable!(),
            TransformData::CalleeConcurrent(_) => unreachable!(),
            //these go to the individual transforms
            TransformData::AggregatorHeader(_) => unreachable!(),
            TransformData::AggregatorTrailer(_) => unreachable!(),
            TransformData::Custom(tf) => tf.handle_stream_value_update(
                &mut self.job_data,
                svu.tf_id,
                svu.sv_id,
                svu.custom,
            ),
        }
    }
    pub fn handle_transform(
        &mut self,
        tf_id: TransformId,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        #[cfg(feature = "debug_logging")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
            println!(
            "> handling tf {tf_id} `{}`, bsa: {}, pred_done: {}, done: {}, stack: {:?}",
            self.transform_data[tf_id.get()].display_name(),
            tf.available_batch_size,
            tf.predecessor_done,
            tf.done,
            self.job_data.tf_mgr.ready_stack
        );
        }
        match &mut self.transform_data[usize::from(tf_id)] {
            TransformData::Fork(fork) => {
                if !fork.expanded {
                    handle_fork_expansion(self, tf_id, ctx);
                }
            }
            TransformData::ForkCat(_) => {
                if self.job_data.tf_mgr.transforms[tf_id].successor.is_none() {
                    handle_forkcat_subchain_expansion(self, tf_id);
                }
            }
            TransformData::CallConcurrent(callcc) => {
                if !callcc.expanded {
                    handle_call_concurrent_expansion(self, tf_id, ctx)?
                }
            }
            TransformData::Call(_) => {
                // this removes itself on the first invocation,
                // so no need for any check
                handle_lazy_call_expansion(self, tf_id);
            }
            TransformData::Disabled => (),
            TransformData::ForeachHeader(_) => (),
            TransformData::ForeachTrailer(_) => (),
            TransformData::CalleeConcurrent(_) => (),
            TransformData::Cast(_) => (),
            TransformData::Nop(_) => (),
            TransformData::NopCopy(_) => (),
            TransformData::InputDoneEater(_) => (),
            TransformData::Count(_) => (),
            TransformData::Print(_) => (),
            TransformData::Join(_) => (),
            TransformData::Select(_) => (),
            TransformData::StringSink(_) => (),
            TransformData::FieldValueSink(_) => (),
            TransformData::Regex(_) => (),
            TransformData::Format(_) => (),
            TransformData::FileReader(_) => (),
            TransformData::Literal(_) => (),
            TransformData::Sequence(_) => (),
            TransformData::Terminator(_) => (),
            TransformData::AggregatorHeader(_) => (),
            TransformData::AggregatorTrailer(_) => (),
            TransformData::Custom(tf) => {
                if tf.pre_update_required() {
                    let mut tf = std::mem::replace(
                        &mut self.transform_data[usize::from(tf_id)],
                        TransformData::Disabled,
                    );
                    let TransformData::Custom(tf_custom) = &mut tf else {
                        unreachable!()
                    };
                    tf_custom.pre_update(self, tf_id);
                    let _ = std::mem::replace(
                        &mut self.transform_data[usize::from(tf_id)],
                        tf,
                    );
                }
            }
        }
        let jd = &mut self.job_data;
        match &mut self.transform_data[usize::from(tf_id)] {
            TransformData::Fork(tf) => {
                handle_tf_fork(&mut self.job_data, tf_id, tf)
            }
            TransformData::ForkCat(fork) => {
                handle_tf_forkcat(&mut self.job_data, tf_id, fork)
            }
            TransformData::Nop(tf) => handle_tf_nop(jd, tf_id, tf),
            TransformData::NopCopy(tf) => handle_tf_nop_copy(jd, tf_id, tf),
            TransformData::InputDoneEater(tf) => {
                handle_tf_input_done_eater(jd, tf_id, tf)
            }
            TransformData::Print(tf) => handle_tf_print(jd, tf_id, tf),
            TransformData::Regex(tf) => handle_tf_regex(jd, tf_id, tf),
            TransformData::StringSink(tf) => {
                handle_tf_string_sink(jd, tf_id, tf)
            }
            TransformData::FieldValueSink(tf) => {
                handle_tf_field_value_sink(jd, tf_id, tf)
            }
            TransformData::FileReader(tf) => {
                handle_tf_file_reader(jd, tf_id, tf)
            }
            TransformData::Literal(tf) => handle_tf_literal(jd, tf_id, tf),
            TransformData::Sequence(tf) => handle_tf_sequence(jd, tf_id, tf),
            TransformData::Format(tf) => handle_tf_format(jd, tf_id, tf),
            TransformData::Join(tf) => handle_tf_join(jd, tf_id, tf),
            TransformData::Select(tf) => handle_tf_select(jd, tf_id, tf),
            TransformData::Count(tf) => handle_tf_count(jd, tf_id, tf),
            TransformData::Cast(tf) => handle_tf_cast(jd, tf_id, tf),
            TransformData::CallConcurrent(tf) => {
                handle_tf_call_concurrent(jd, tf_id, tf)
            }
            TransformData::CalleeConcurrent(tf) => {
                handle_tf_callee_concurrent(jd, tf_id, tf)
            }
            TransformData::Call(_) => (),
            TransformData::Terminator(tf) => {
                handle_tf_terminator(jd, tf_id, tf)
            }
            TransformData::Custom(tf) => tf.update(&mut self.job_data, tf_id),
            TransformData::AggregatorHeader(agg_header) => {
                handle_tf_aggregator_header(
                    &mut self.job_data,
                    tf_id,
                    agg_header,
                )
            }
            TransformData::AggregatorTrailer(_) => {
                handle_tf_aggregator_trailer(self, tf_id)
            }
            TransformData::ForeachHeader(eh) => {
                handle_tf_foreach_header(jd, tf_id, eh)
            }
            TransformData::ForeachTrailer(et) => {
                handle_tf_foreach_trailer(jd, tf_id, et)
            }
            TransformData::Disabled => unreachable!(),
        }
        if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
            if tf.mark_for_removal && !tf.is_stream_producer {
                self.remove_transform(tf_id);
            }
        }
        Ok(())
    }

    pub(crate) fn run_stream_producer_update(&mut self, tf_id: TransformId) {
        //#[cfg(feature = "debug_logging")]
        //println!(
        //    "> handling stream producer update for tf {} (`{}`)",
        //    tf_id,
        //    self.transform_data[tf_id.get() as usize].display_name()
        //);
        let tf_state = &mut self.job_data.tf_mgr.transforms[tf_id];
        tf_state.is_stream_producer = false;
        match &mut self.transform_data[tf_id.get()] {
            TransformData::Disabled
            | TransformData::Nop(_)
            | TransformData::NopCopy(_)
            | TransformData::InputDoneEater(_)
            | TransformData::Terminator(_)
            | TransformData::Call(_)
            | TransformData::CallConcurrent(_)
            | TransformData::CalleeConcurrent(_)
            | TransformData::Cast(_)
            | TransformData::Count(_)
            | TransformData::Print(_)
            | TransformData::Join(_)
            | TransformData::Select(_)
            | TransformData::StringSink(_)
            | TransformData::FieldValueSink(_)
            | TransformData::Fork(_)
            | TransformData::ForkCat(_)
            | TransformData::Regex(_)
            | TransformData::Literal(_)
            | TransformData::Sequence(_)
            | TransformData::Format(_)
            //these go straight to the sub transforms
            | TransformData::AggregatorHeader(_)
            | TransformData::AggregatorTrailer(_)
            | TransformData::ForeachHeader(_)
            | TransformData::ForeachTrailer(_) => unreachable!(),
            TransformData::FileReader(f) => {
                handle_tf_file_reader_stream(&mut self.job_data, tf_id, f)
            }
            TransformData::Custom(c) => {
                c.stream_producer_update(&mut self.job_data, tf_id)
            }
        }
    }
    pub fn is_in_streaming_mode(&self) -> bool {
        !self.job_data.tf_mgr.stream_producers.is_empty()
    }
    pub(crate) fn run_streams(
        &mut self,
        transform_stack_cutoff: usize,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        self.job_data.tf_mgr.pre_stream_transform_stack_cutoff =
            Some(transform_stack_cutoff);
        loop {
            if self.job_data.tf_mgr.ready_stack.len() > transform_stack_cutoff
            {
                let tf_id = self.job_data.tf_mgr.ready_stack.pop().unwrap();
                let tf = &mut self.job_data.tf_mgr.transforms[tf_id];
                tf.is_ready = false;
                self.handle_transform(tf_id, ctx)?;
                continue;
            }
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
            self.job_data.tf_mgr.pre_stream_transform_stack_cutoff = None;
            return Ok(());
        }
    }
    pub(crate) fn run(
        &mut self,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        if let Some(tsc) =
            self.job_data.tf_mgr.pre_stream_transform_stack_cutoff
        {
            // happens if we continue after we suspended during a stream
            // TODO: should we allow that at all?
            self.run_streams(tsc, ctx)?;
        }
        while let Some(tf_id) = self.job_data.tf_mgr.ready_stack.pop() {
            let stack_height = self.job_data.tf_mgr.ready_stack.len();
            let tf = &mut self.job_data.tf_mgr.transforms[tf_id];
            tf.is_ready = false;
            self.handle_transform(tf_id, ctx)?;
            if self.is_in_streaming_mode() {
                self.run_streams(stack_height, ctx)?;
            }
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
}
