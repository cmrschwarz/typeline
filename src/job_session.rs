use std::{
    collections::{HashMap, VecDeque},
    ops::DerefMut,
    sync::Arc,
};

use nonmax::NonMaxU32;

use crate::{
    chain::ChainId,
    context::{ContextData, Job, Session, VentureDescription},
    liveness_analysis::OpOutputIdx,
    operators::{
        call::{
            handle_eager_call_expansion, handle_lazy_call_expansion,
            setup_tf_call,
        },
        call_concurrent::{
            handle_call_concurrent_expansion, handle_tf_call_concurrent,
            handle_tf_callee_concurrent, setup_callee_concurrent,
            setup_tf_call_concurrent,
        },
        cast::{handle_tf_cast, setup_tf_cast},
        count::{handle_tf_count, setup_tf_count},
        file_reader::{handle_tf_file_reader, setup_tf_file_reader},
        fork::{handle_fork_expansion, handle_tf_fork, setup_tf_fork},
        forkcat::{
            handle_forkcat_expansion, handle_tf_forkcat, setup_tf_forkcat,
        },
        format::{
            handle_tf_format, handle_tf_format_stream_value_update,
            setup_tf_format,
        },
        join::{
            handle_tf_join, handle_tf_join_stream_value_update, setup_tf_join,
        },
        literal::{handle_tf_literal, setup_tf_literal},
        nop::{create_tf_nop, handle_tf_nop, setup_tf_nop},
        operator::{OperatorData, OperatorId},
        print::{
            handle_tf_print, handle_tf_print_stream_value_update,
            setup_tf_print,
        },
        regex::{
            handle_tf_regex, handle_tf_regex_stream_value_update,
            setup_tf_regex,
        },
        select::{handle_tf_select, setup_tf_select},
        sequence::{handle_tf_sequence, setup_tf_sequence},
        string_sink::{
            handle_tf_string_sink, handle_tf_string_sink_stream_value_update,
            setup_tf_string_sink,
        },
        terminator::{
            handle_tf_terminator, setup_tf_terminator, OpTerminator,
        },
        transform::{TransformData, TransformId, TransformState},
    },
    record_data::{
        command_buffer::FieldActionIndices,
        field::{FieldId, FieldManager, DUMMY_INPUT_FIELD_ID},
        match_set::{MatchSetId, MatchSetManager},
        record_buffer::RecordBuffer,
        stream_value::{StreamValueManager, StreamValueUpdate},
    },
    utils::{identity_hasher::BuildIdentityHasher, universe::Universe},
};

pub struct JobSession<'a> {
    pub transform_data: Vec<TransformData<'a>>,
    pub job_data: JobData<'a>,
    pub temp_vec: Vec<NonMaxU32>,
}
// a helper type so we can pass a transform handler typed
// TransformData + all the other Data of the WorkerThreadSession
pub struct JobData<'a> {
    pub session_data: &'a Session,
    pub tf_mgr: TransformManager,
    pub match_set_mgr: MatchSetManager,
    pub field_mgr: FieldManager,
    pub sv_mgr: StreamValueManager,
}

pub struct TransformManager {
    pub transforms: Universe<TransformId, TransformState>,
    pub ready_stack: Vec<TransformId>,
    pub stream_producers: VecDeque<TransformId>,
}

impl TransformManager {
    pub fn claim_batch_with_limit(
        &mut self,
        tf_id: TransformId,
        limit: usize,
    ) -> (usize, bool) {
        let tf = &mut self.transforms[tf_id];
        let batch_size = tf.available_batch_size.min(limit);
        tf.available_batch_size -= batch_size;
        let input_done = tf.input_is_done && tf.available_batch_size == 0;
        (batch_size, input_done)
    }
    pub fn claim_batch(&mut self, tf_id: TransformId) -> (usize, bool) {
        self.claim_batch_with_limit(
            tf_id,
            self.transforms[tf_id].desired_batch_size,
        )
    }
    pub fn claim_all(&mut self, tf_id: TransformId) -> (usize, bool) {
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
        any_prev_has_unconsumed_input: bool,
    ) {
        let tf = &mut self.transforms[tf_id];
        tf.available_batch_size += batch_size;
        tf.any_prev_has_unconsumed_input = any_prev_has_unconsumed_input;
        if tf.available_batch_size > 0 && !tf.is_ready {
            self.push_tf_in_ready_stack(tf_id);
        }
    }
    pub fn inform_successor_batch_available(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
    ) {
        let tf = &self.transforms[tf_id];
        if let Some(succ_tf_id) = tf.successor {
            self.inform_transform_batch_available(
                succ_tf_id,
                batch_size,
                tf.has_unconsumed_input(),
            );
        }
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
        tf.is_stream_producer = true;
        self.stream_producers.push_back(tf_id);
    }
    pub fn update_ready_state(&mut self, tf_id: TransformId) {
        let tf = &self.transforms[tf_id];
        if tf.available_batch_size > 0 {
            self.push_tf_in_ready_stack(tf_id);
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
    ) -> (usize, bool) {
        let tf = &mut self.transforms[tf_id];
        let output_field_id = tf.output_field;
        let match_set_id = tf.match_set_id;
        let desired_batch_size = tf.desired_batch_size;
        let has_cont = tf.continuation.is_some();
        let max_batch_size = if let Some(len) = length {
            *len
        } else if has_cont {
            if !initial_call {
                if final_call_if_input_done {
                    field_mgr.fields[output_field_id]
                        .borrow_mut()
                        .field_data
                        .drop_last_value(1);
                }
                return (0, true);
            }
            1
        } else {
            usize::MAX
        };
        let (mut batch_size, mut input_done) = self.claim_batch_with_limit(
            tf_id,
            max_batch_size.min(desired_batch_size),
        );
        if batch_size == 0 {
            if !initial_call {
                if final_call_if_input_done {
                    field_mgr.fields[output_field_id]
                        .borrow_mut()
                        .field_data
                        .drop_last_value(1);
                }
                return (0, true);
            }
            batch_size = length.unwrap_or(1);
        }
        if let Some(len) = length {
            *len -= batch_size;
            if *len == 0 {
                input_done = true;
            }
        } else if has_cont {
            input_done = true;
        }
        let mut output_field = field_mgr.fields[output_field_id].borrow_mut();
        let of = output_field.deref_mut();
        match_set_mgr.match_sets[match_set_id]
            .command_buffer
            .execute_for_iter_hall(
                output_field_id.get() as usize,
                &mut of.field_data,
                &mut of.action_indices,
            );
        // this results in always one more element being present than we
        // advertise as batch size. this prevents apply_field_actions
        // from deleting our value. unless we are done, in which case
        // no additional value is inserted
        let drop_oversize = input_done && final_call_if_input_done;
        if batch_size == 0 && drop_oversize {
            output_field.field_data.drop_last_value(1);
        } else {
            output_field
                .field_data
                .dup_last_value(batch_size - drop_oversize as usize);
        }
        (batch_size, input_done)
    }
    pub fn prepare_for_output_cow(
        &mut self,
        field_mgr: &mut FieldManager,
        match_set_mgr: &mut MatchSetManager,
        tf_id: TransformId,
        output_fields: impl IntoIterator<Item = FieldId>,
    ) {
        let tf = &mut self.transforms[tf_id];
        let appending = tf.is_appending;
        let request_uncow = tf.request_uncow;
        tf.request_uncow = false;
        tf.is_appending = false;

        for ofid in output_fields {
            let mut f = field_mgr.fields[ofid].borrow_mut();
            let clear_delay = f.get_clear_delay_request_count() > 0;
            if clear_delay || request_uncow {
                drop(f);
                field_mgr.uncow(match_set_mgr, ofid);
                f = field_mgr.fields[ofid].borrow_mut();
            }
            if clear_delay {
                field_mgr.apply_field_actions(match_set_mgr, ofid);
            } else {
                match_set_mgr.match_sets[tf.match_set_id]
                    .command_buffer
                    .drop_field_commands(
                        ofid.get() as usize,
                        &mut f.action_indices,
                    );
                if !appending {
                    f.field_data.clear_if_owned(field_mgr);
                    f.has_unconsumed_input.set(false);
                }
            }
        }
    }
    pub fn prepare_for_output(
        &mut self,
        field_mgr: &mut FieldManager,
        match_set_mgr: &mut MatchSetManager,
        tf_id: TransformId,
        output_fields: impl IntoIterator<Item = FieldId>,
    ) {
        let tf = &mut self.transforms[tf_id];
        let appending = tf.is_appending;
        tf.request_uncow = false;
        tf.is_appending = false;

        for ofid in output_fields {
            field_mgr.uncow(match_set_mgr, ofid);
            let mut f = field_mgr.fields[ofid].borrow_mut();
            let clear_delay = f.get_clear_delay_request_count() > 0;
            if clear_delay {
                field_mgr.apply_field_actions(match_set_mgr, ofid);
            } else {
                match_set_mgr.match_sets[tf.match_set_id]
                    .command_buffer
                    .drop_field_commands(
                        ofid.get() as usize,
                        &mut f.action_indices,
                    );
                if !appending {
                    f.field_data.clear_if_owned(field_mgr);
                    f.has_unconsumed_input.set(false);
                }
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
    pub fn connect_tfs(&mut self, left: TransformId, right: TransformId) {
        self.transforms[left].successor = Some(right);
        self.transforms[right].predecessor = Some(left);
    }
    pub fn disconnect_tf_from_predecessor(&mut self, tf_id: TransformId) {
        if let Some(pred) = self.transforms[tf_id].predecessor {
            self.transforms[pred].successor = None;
            self.transforms[tf_id].predecessor = None;
        }
    }
}

impl<'a> JobData<'a> {
    pub fn new(sess: &'a Session) -> Self {
        Self {
            session_data: sess,
            tf_mgr: TransformManager {
                transforms: Default::default(),
                ready_stack: Default::default(),
                stream_producers: Default::default(),
            },
            field_mgr: FieldManager::default(),
            match_set_mgr: MatchSetManager {
                match_sets: Default::default(),
            },
            sv_mgr: Default::default(),
        }
    }
    pub fn unlink_transform(
        &mut self,
        tf_id: TransformId,
        available_batch_for_successor: usize,
    ) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        tf.mark_for_removal = true;
        let predecessor = tf.predecessor;
        let successor = tf.successor;
        let continuation = tf.continuation;
        let input_is_done = tf.input_is_done;
        let available_batch_size = tf.available_batch_size;
        let is_transparent = tf.is_transparent;
        if let Some(cont_id) = continuation {
            let cont = &mut self.tf_mgr.transforms[cont_id];
            cont.input_is_done = input_is_done;
            cont.successor = successor;
            cont.predecessor = predecessor;
            cont.available_batch_size = available_batch_size;
            cont.request_uncow = true;
            if let Some(pred_id) = predecessor {
                self.tf_mgr.transforms[pred_id].successor = continuation;
            }
            let mut cont_pushed = false;
            if let Some(succ_id) = successor {
                let succ = &mut self.tf_mgr.transforms[succ_id];
                succ.predecessor = continuation;
                succ.available_batch_size += available_batch_for_successor;
                if succ.available_batch_size > 0 {
                    if succ.is_ready {
                        let succ_tf_id =
                            self.tf_mgr.ready_stack.pop().unwrap();
                        assert!(succ_tf_id == succ_id);
                        self.tf_mgr.push_tf_in_ready_stack(cont_id);
                        self.tf_mgr.ready_stack.push(succ_id);
                    } else {
                        self.tf_mgr.push_tf_in_ready_stack(cont_id);
                        self.tf_mgr.push_tf_in_ready_stack(succ_id);
                    }
                    cont_pushed = true;
                    self.tf_mgr.transforms[cont_id].is_appending = false;
                }
            }
            if !cont_pushed {
                self.tf_mgr.push_tf_in_ready_stack(cont_id);
            }
            return;
        }
        if let Some(pred_id) = predecessor {
            self.tf_mgr.transforms[pred_id].successor = successor;
        }
        if let Some(succ_id) = successor {
            let succ = &mut self.tf_mgr.transforms[succ_id];
            succ.predecessor = predecessor;
            succ.input_is_done = input_is_done;
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
            if let Some(prod_id) = field.producing_transform_id {
                print!(
                    " (output of tf {prod_id} `{}`)",
                    field.producing_transform_arg
                )
            } else if !field.producing_transform_arg.is_empty() {
                print!(" (`{}`)", field.producing_transform_arg)
            }
            if let (cow_src_field, Some(data_cow)) =
                field.field_data.cow_source_field()
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
            if !field.names.is_empty() {
                print!(" ( names:");
                for n in &field.names {
                    print!(" {}", self.session_data.string_store.lookup(*n));
                }
                print!(" )");
            }
        }
    }
}

impl<'a> JobSession<'a> {
    pub fn log_state(&self, message: &str) {
        if cfg!(feature = "debug_logging") {
            println!("{message}");
            for (i, tf) in self.job_data.tf_mgr.transforms.iter_enumerated() {
                let name = if let Some(op_id) = tf.op_id {
                    self.job_data.session_data.operator_data[op_id as usize]
                        .default_op_name()
                } else {
                    self.transform_data[i.get()].alternative_display_name()
                };
                println!(
                    "tf {} [{} {}{}{}]: {}",
                    i,
                    tf.input_field,
                    if tf.is_transparent { "_>" } else { "->" },
                    if tf.is_appending { "+" } else { " " },
                    tf.output_field,
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
    pub fn setup_job(&mut self, mut job: Job) {
        let ms_id = self.job_data.match_set_mgr.add_match_set();
        // TODO: unpack record set properly here
        let input_record_count = job.data.adjust_field_lengths();
        let mut input_data = None;
        let mut input_data_fields = std::mem::take(&mut self.temp_vec);
        for fd in job.data.fields.into_iter() {
            let field_id = self
                .job_data
                .field_mgr
                .add_field_with_data(ms_id, None, fd.data);
            if let Some(name) = fd.name {
                self.job_data.match_set_mgr.add_field_name(
                    &self.job_data.field_mgr,
                    field_id,
                    name,
                );
            }
            input_data_fields.push(field_id);
            if input_data.is_none() {
                input_data = Some(field_id);
            }
        }
        let input_data = input_data.unwrap_or_else(|| {
            let field_id = self.job_data.field_mgr.add_field(ms_id, None);
            input_data_fields.push(field_id);
            field_id
        });

        #[cfg(feature = "debug_logging")]
        for (i, f) in input_data_fields.iter().enumerate() {
            self.job_data.field_mgr.fields[*f]
                .borrow_mut()
                .producing_transform_arg = format!("<Input Field #{i}>");
        }

        let (start_tf_id, end_tf_id, end_reachable) = self
            .setup_transforms_from_op(
                ms_id,
                job.operator,
                input_data,
                None,
                &Default::default(),
            );
        if end_reachable {
            self.add_terminator(end_tf_id, false);
        }
        let tf = &mut self.job_data.tf_mgr.transforms[start_tf_id];
        tf.input_is_done = true;
        if tf.is_appending {
            let successor = tf.successor;
            self.job_data.tf_mgr.push_tf_in_ready_stack(start_tf_id);
            if let Some(succ) = successor {
                let tf_succ = &mut self.job_data.tf_mgr.transforms[succ];
                tf_succ.available_batch_size = input_record_count;
                if tf_succ.desired_batch_size <= input_record_count {
                    self.job_data.tf_mgr.transforms[start_tf_id]
                        .is_appending = false;
                    self.job_data.tf_mgr.push_tf_in_ready_stack(succ);
                }
            }
        } else {
            tf.available_batch_size = input_record_count;
            self.job_data.tf_mgr.push_tf_in_ready_stack(start_tf_id);
        }

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

        let (start_tf_id, end_tf_id, end_reachable) =
            setup_callee_concurrent(self, ms_id, buffer, start_op_id);
        if end_reachable {
            self.add_terminator(end_tf_id, false);
        }
        self.job_data.tf_mgr.push_tf_in_ready_stack(start_tf_id);
        self.log_state("setting up venture");
    }

    pub fn remove_transform(&mut self, tf_id: TransformId) {
        let tf = &self.job_data.tf_mgr.transforms[tf_id];
        let tfif = tf.input_field;
        let tfof = tf.output_field;
        #[cfg(feature = "debug_logging")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
            let name: String = if let Some(op_id) = tf.op_id {
                self.job_data
                    .session_data
                    .string_store
                    .lookup(
                        self.job_data.session_data.operator_bases
                            [op_id as usize]
                            .argname,
                    )
                    .into()
            } else {
                self.transform_data[tf_id.get()]
                    .alternative_display_name()
                    .to_string()
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

    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
        mut predecessor_tf: Option<TransformId>,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> (TransformId, TransformId, bool) {
        let mut start_tf_id = None;
        let start_op =
            &self.job_data.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.job_data.session_data.chains
            [start_op.chain_id.unwrap() as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = predecessor_tf;
        let mut end_reachable = true;
        let mut input_field = chain_input_field_id;
        let mut last_output_field = chain_input_field_id;
        let ops = &self.job_data.session_data.chains
            [start_op.chain_id.unwrap() as usize]
            .operators[start_op.offset_in_chain as usize..];
        let mut mark_prev_field_as_placeholder = false;
        for &op_id in ops {
            let op_base =
                &self.job_data.session_data.operator_bases[op_id as usize];
            let op_data =
                &self.job_data.session_data.operator_data[op_id as usize];
            let mut make_input_output = false;
            match op_data {
                OperatorData::Call(op) => {
                    if !op.lazy {
                        let (start_exp, end_exp, end_reachable) =
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
                            end_reachable,
                        );
                    }
                }
                OperatorData::Key(op) => {
                    assert!(op_base.label.is_none()); // TODO
                    self.job_data.match_set_mgr.add_field_name(
                        &self.job_data.field_mgr,
                        input_field,
                        op.key_interned.unwrap(),
                    );

                    continue;
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
                        let field_id = self.job_data.field_mgr.add_field(
                            ms_id,
                            self.job_data
                                .field_mgr
                                .get_min_apf_idx(input_field),
                        );
                        self.job_data.match_set_mgr.add_field_name(
                            &self.job_data.field_mgr,
                            field_id,
                            op.key_interned.unwrap(),
                        );
                        input_field = field_id;
                    }
                    if !op.field_is_read {
                        last_output_field = input_field;
                        continue;
                    }
                    make_input_output = true;
                }
                _ => (),
            }
            let mut output_field = if make_input_output {
                self.job_data.field_mgr.bump_field_refcount(input_field);
                input_field
            } else if op_base.append_mode {
                self.job_data
                    .field_mgr
                    .bump_field_refcount(last_output_field);
                last_output_field
            } else {
                let min_apf =
                    self.job_data.field_mgr.get_min_apf_idx(input_field);
                if let Some(field_idx) =
                    prebound_outputs.get(&op_base.outputs_start)
                {
                    self.job_data.field_mgr.bump_field_refcount(*field_idx);
                    let mut f = self.job_data.field_mgr.fields[*field_idx]
                        .borrow_mut();
                    f.action_indices = FieldActionIndices::new(min_apf);
                    f.match_set = ms_id;
                    *field_idx
                } else {
                    self.job_data.field_mgr.add_field(ms_id, min_apf)
                }
            };

            if let Some(name) = op_base.label {
                self.job_data.match_set_mgr.add_field_name(
                    &self.job_data.field_mgr,
                    output_field,
                    name,
                );
            }
            let input = if op_base.append_mode
                && last_output_field == chain_input_field_id
            {
                DUMMY_INPUT_FIELD_ID
            } else {
                input_field
            };
            self.job_data.field_mgr.bump_field_refcount(input);
            let mut tf_state = TransformState::new(
                input,
                output_field,
                ms_id,
                default_batch_size,
                predecessor_tf,
                Some(op_id),
            );
            tf_state.is_transparent = op_base.transparent_mode;
            tf_state.is_appending = op_base.append_mode;

            let tf_id_peek = self.job_data.tf_mgr.transforms.peek_claim_id();
            #[cfg(feature = "debug_logging")]
            {
                let mut of =
                    self.job_data.field_mgr.fields[output_field].borrow_mut();
                if of.producing_transform_id.is_none() {
                    of.producing_transform_id = Some(tf_id_peek);
                    of.producing_transform_arg =
                        self.job_data.session_data.operator_data
                            [op_id as usize]
                            .default_op_name()
                            .to_string();
                }
            }
            if mark_prev_field_as_placeholder {
                // let mut f =
                //    self.job_data.field_mgr.fields[input_field].borrow_mut();
                // f.added_as_placeholder_by_tf = Some(tf_id_peek);
                mark_prev_field_as_placeholder = false;
            }
            let b = op_base;

            let jd = &mut self.job_data;
            let tf_data = match op_data {
                OperatorData::Nop(op) => setup_tf_nop(op, &tf_state),
                OperatorData::Cast(op) => {
                    setup_tf_cast(jd, b, op, &mut tf_state)
                }
                OperatorData::Count(op) => {
                    setup_tf_count(jd, b, op, &mut tf_state)
                }
                OperatorData::Fork(op) => {
                    // in case of fork, we keep the end as 'reachable'
                    // because the current match chain ends there,
                    // and the fork itself does not drop the fields
                    setup_tf_fork(jd, b, op, &mut tf_state)
                }
                OperatorData::ForkCat(op) => {
                    // fork cat on the other hand has to keep the records
                    // temporarily alive, so it handles
                    // termination itself
                    end_reachable = false;
                    setup_tf_forkcat(jd, b, op, &mut tf_state)
                }
                OperatorData::Print(op) => {
                    setup_tf_print(jd, b, op, &mut tf_state)
                }
                OperatorData::Join(op) => {
                    setup_tf_join(jd, b, op, &mut tf_state)
                }
                OperatorData::Regex(op) => {
                    setup_tf_regex(jd, b, op, &mut tf_state, prebound_outputs)
                }
                OperatorData::Format(op) => {
                    setup_tf_format(jd, b, op, tf_id_peek, &tf_state)
                }
                OperatorData::StringSink(op) => {
                    setup_tf_string_sink(jd, b, op, &mut tf_state)
                }
                OperatorData::FileReader(op) => {
                    setup_tf_file_reader(jd, b, op, &tf_state)
                }
                OperatorData::Literal(op) => {
                    setup_tf_literal(jd, op_base, op, &mut tf_state)
                }
                OperatorData::Sequence(op) => {
                    setup_tf_sequence(jd, op_base, op, &mut tf_state)
                }
                OperatorData::Select(op) => {
                    setup_tf_select(jd, b, op, &mut tf_state)
                }
                OperatorData::Call(op) => {
                    setup_tf_call(jd, b, op, &mut tf_state)
                }
                OperatorData::CallConcurrent(op) => {
                    end_reachable = false;
                    setup_tf_call_concurrent(jd, b, op, &tf_state)
                }
                OperatorData::Key(_) => unreachable!(),
                OperatorData::Next(_) => unreachable!(),
                OperatorData::Up(_) => unreachable!(),
            };
            output_field = tf_state.output_field;
            let appending = tf_state.is_appending;
            let transparent = tf_state.is_transparent;
            let tf_id = self.add_transform(tf_state, tf_data);
            debug_assert!(tf_id_peek == tf_id);

            if appending {
                if let Some(prev) = prev_tf {
                    self.job_data.tf_mgr.transforms[prev].continuation =
                        Some(tf_id);
                }
            } else if let Some(pred) = predecessor_tf {
                self.job_data.tf_mgr.transforms[pred].successor = Some(tf_id);
            }

            if start_tf_id.is_none() {
                start_tf_id = Some(tf_id);
                if predecessor_tf.is_none() {
                    predecessor_tf = start_tf_id;
                }
            }
            prev_tf = Some(tf_id);
            last_output_field = output_field;
            if !appending {
                predecessor_tf = Some(tf_id);
                if !transparent {
                    input_field = output_field;
                }
            }
            if !end_reachable {
                break;
            }
        }
        let start = start_tf_id.unwrap();
        let end = predecessor_tf.unwrap_or(start);
        (start, end, end_reachable)
    }
    // Because a fork / forkcat / etc. has multiple targets / successors, these
    // can't be stored in the usual successor / predecessor fields in
    // TransformState. Therefore the fork has a list of successor transform
    // ids. This list is unknown to unlink_transform, which would therefore
    // break the propagation if the first Transform after the fork has an
    // appender. To solve this, we simply insert a nop transform before the
    // first transform (if necessary) to get a stable transform index.
    pub fn setup_transforms_with_stable_start(
        &mut self,
        ms_id: MatchSetId,
        chain_id: ChainId,
        start_op_id: OperatorId,
        input_field_id: FieldId,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
        manual_unlink: bool,
    ) -> (TransformId, TransformId, bool) {
        let mut tf_state = TransformState::new(
            input_field_id,
            DUMMY_INPUT_FIELD_ID,
            ms_id,
            self.job_data.session_data.chains[chain_id as usize]
                .settings
                .default_batch_size,
            None,
            None,
        );
        self.job_data.field_mgr.bump_field_refcount(input_field_id);
        self.job_data
            .field_mgr
            .bump_field_refcount(DUMMY_INPUT_FIELD_ID);
        tf_state.is_transparent = true;
        let tf_data = create_tf_nop(manual_unlink);
        let mut pred_tf = self.add_transform(tf_state, tf_data);
        let (start_tf, end_tf, end_reachable) = self.setup_transforms_from_op(
            ms_id,
            start_op_id,
            input_field_id,
            Some(pred_tf),
            prebound_outputs,
        );
        if !manual_unlink
            && self.job_data.tf_mgr.transforms[start_tf]
                .continuation
                .is_none()
        {
            self.job_data.unlink_transform(pred_tf, 0);
            self.remove_transform(pred_tf);
            pred_tf = start_tf;
        }
        (pred_tf, end_tf, end_reachable)
    }
    pub fn add_terminator(
        &mut self,
        predecessor_tf_id: TransformId,
        manual_unlink: bool,
    ) -> TransformId {
        let tf_id = self.job_data.tf_mgr.transforms.peek_claim_id();
        let pred = &mut self.job_data.tf_mgr.transforms[predecessor_tf_id];
        let input_field = pred.output_field;
        let tf_state = TransformState::new(
            input_field,
            DUMMY_INPUT_FIELD_ID,
            pred.match_set_id,
            pred.desired_batch_size,
            Some(predecessor_tf_id),
            None,
        );
        pred.successor = Some(tf_id);
        let tf_data = setup_tf_terminator(
            &mut self.job_data,
            &tf_state,
            OpTerminator { manual_unlink },
        );
        self.job_data.field_mgr.bump_field_refcount(input_field);
        self.job_data
            .field_mgr
            .bump_field_refcount(DUMMY_INPUT_FIELD_ID);
        self.add_transform(tf_state, tf_data)
    }
    pub fn add_transform(
        &mut self,
        state: TransformState,
        data: TransformData<'a>,
    ) -> TransformId {
        let id = self.job_data.tf_mgr.transforms.claim_with_value(state);
        if self.transform_data.len()
            < self.job_data.tf_mgr.transforms.used_capacity()
        {
            self.transform_data.resize_with(
                self.job_data.tf_mgr.transforms.used_capacity(),
                || TransformData::Disabled,
            );
        }
        self.transform_data[usize::from(id)] = data;
        id
    }
    fn handle_stream_value_update(&mut self, svu: StreamValueUpdate) {
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
            TransformData::Fork(_) => todo!(),
            TransformData::ForkCat(_) => todo!(),
            TransformData::CallConcurrent(_) => todo!(),
            TransformData::Call(_) => unreachable!(),
            TransformData::Nop(_) => unreachable!(),
            TransformData::Cast(_) => unreachable!(),
            TransformData::Count(_) => unreachable!(),
            TransformData::Select(_) => unreachable!(),
            TransformData::Terminator(_) => unreachable!(),
            TransformData::FileReader(_) => unreachable!(),
            TransformData::Sequence(_) => unreachable!(),
            TransformData::Disabled => unreachable!(),
            TransformData::Literal(_) => unreachable!(),
            TransformData::CalleeConcurrent(_) => unreachable!(),
        }
    }
    fn handle_transform(
        &mut self,
        tf_id: TransformId,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        match &mut self.transform_data[usize::from(tf_id)] {
            TransformData::Fork(fork) => {
                if !fork.expanded {
                    handle_fork_expansion(self, tf_id, ctx);
                }
            }
            TransformData::ForkCat(forkcat) => {
                if forkcat.curr_subchain_start.is_none() {
                    handle_forkcat_expansion(self, tf_id);
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
            TransformData::CalleeConcurrent(_) => (),
            TransformData::Cast(_) => (),
            TransformData::Nop(_) => (),
            TransformData::Count(_) => (),
            TransformData::Print(_) => (),
            TransformData::Join(_) => (),
            TransformData::Select(_) => (),
            TransformData::StringSink(_) => (),
            TransformData::Regex(_) => (),
            TransformData::Format(_) => (),
            TransformData::FileReader(_) => (),
            TransformData::Literal(_) => (),
            TransformData::Sequence(_) => (),
            TransformData::Terminator(_) => (),
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
            TransformData::Print(tf) => handle_tf_print(jd, tf_id, tf),
            TransformData::Regex(tf) => handle_tf_regex(jd, tf_id, tf),
            TransformData::StringSink(tf) => {
                handle_tf_string_sink(jd, tf_id, tf)
            }
            TransformData::FileReader(tf) => {
                handle_tf_file_reader(jd, tf_id, tf)
            }
            TransformData::Literal(tf) => handle_tf_literal(jd, tf_id, tf),
            TransformData::Sequence(tf) => handle_tf_sequence(jd, tf_id, tf),
            TransformData::Format(tf) => handle_tf_format(jd, tf_id, tf),
            TransformData::Terminator(tf) => {
                handle_tf_terminator(jd, tf_id, tf)
            }
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
            TransformData::Disabled => unreachable!(),
        }
        if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
            if tf.mark_for_removal {
                self.remove_transform(tf_id);
            }
        }
        Ok(())
    }
    pub(crate) fn run(
        &mut self,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        loop {
            if let Some(svu) = self.job_data.sv_mgr.updates.pop_back() {
                self.handle_stream_value_update(svu);
                continue;
            }
            if let Some(mut tf_id) = self.job_data.tf_mgr.ready_stack.pop() {
                let mut tf = &mut self.job_data.tf_mgr.transforms[tf_id];
                tf.is_ready = false;
                if tf.is_stream_producer {
                    tf_id = self
                        .job_data
                        .tf_mgr
                        .stream_producers
                        .pop_front()
                        .unwrap();
                    tf = &mut self.job_data.tf_mgr.transforms[tf_id];
                    tf.is_stream_producer = false;
                }
                self.handle_transform(tf_id, ctx)?;
                continue;
            }
            return Ok(());
        }
    }
}
