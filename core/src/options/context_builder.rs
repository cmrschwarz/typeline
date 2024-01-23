use std::sync::Arc;

use crate::{
    context::{Context, SessionData},
    operators::{
        aggregator::{
            add_aggregate_to_sess_opts_uninit, create_op_aggregate,
            create_op_aggregator_append_leader, AGGREGATOR_DEFAULT_NAME,
        },
        field_value_sink::{create_op_field_value_sink, FieldValueSinkHandle},
        operator::{DefaultOperatorName, OperatorData, OperatorId},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    record_data::{
        custom_data::CustomDataBox, field_data::FixedSizeFieldValueType,
        field_value::FieldValue, push_interface::PushInterface,
        record_set::RecordSet,
    },
    scr_error::{CollectTypeMissmatch, ContextualizedScrError},
};

use super::{
    operator_base_options::OperatorBaseOptions,
    session_options::SessionOptions,
};

pub struct ContextBuilder {
    opts: SessionOptions,
    input_data: RecordSet,
    pending_aggregate: Vec<OperatorId>,
    last_non_append_op_id: Option<OperatorId>,
    curr_op_appendable: bool,
}

impl Default for ContextBuilder {
    fn default() -> Self {
        Self {
            opts: SessionOptions::default(),
            input_data: RecordSet::default(),
            pending_aggregate: Vec::new(),
            last_non_append_op_id: None,
            curr_op_appendable: true,
        }
    }
}

impl ContextBuilder {
    pub fn from_session_opts(opts: SessionOptions) -> Self {
        Self {
            opts,
            ..Default::default()
        }
    }
    fn create_op_base_opts<F: FnOnce() -> DefaultOperatorName>(
        &mut self,
        default_name: F,
        argname: Option<&str>,
        label: Option<&str>,
        transparent_mode: bool,
    ) -> OperatorBaseOptions {
        OperatorBaseOptions::new(
            self.opts
                .string_store
                .intern_cloned(argname.unwrap_or(default_name().as_str())),
            label.map(|lbl| self.opts.string_store.intern_cloned(lbl)),
            transparent_mode,
            None,
        )
    }
    fn add_op_uninit(
        &mut self,
        op_data: OperatorData,
        argname: Option<&str>,
        label: Option<&str>,
        transparent_mode: bool,
    ) -> OperatorId {
        let op_base = self.create_op_base_opts(
            || op_data.default_op_name(),
            argname,
            label,
            transparent_mode,
        );
        self.opts.add_op_uninit(op_base, op_data)
    }
    pub fn ref_add_op_with_opts(
        &mut self,
        op_data: OperatorData,
        argname: Option<&str>,
        label: Option<&str>,
        append_mode: bool,
        transparent_mode: bool,
    ) {
        let prev_op_appendable = self.curr_op_appendable;
        self.curr_op_appendable = op_data.can_be_appended();
        let op_id =
            self.add_op_uninit(op_data, argname, label, transparent_mode);
        if !append_mode || !prev_op_appendable {
            self.ref_terminate_current_aggregate();
            if append_mode {
                let (op_base_opts, op_data) =
                    create_op_aggregator_append_leader(&mut self.opts);
                self.pending_aggregate
                    .push(self.opts.add_op_uninit(op_base_opts, op_data));
                self.pending_aggregate.push(op_id);
                self.last_non_append_op_id = None;
                return;
            }
            self.last_non_append_op_id = Some(op_id);
            return;
        }
        if self.pending_aggregate.is_empty() {
            if let Some(pred) = self.last_non_append_op_id {
                self.pending_aggregate.push(pred);
                self.last_non_append_op_id = None;
            } else {
                let (op_base_opts, op_data) =
                    create_op_aggregator_append_leader(&mut self.opts);
                self.pending_aggregate
                    .push(self.opts.add_op_uninit(op_base_opts, op_data));
            }
        }
        self.pending_aggregate.push(op_id);
    }
    fn ref_terminate_current_aggregate(&mut self) {
        if !self.pending_aggregate.is_empty() {
            let op_data = create_op_aggregate(std::mem::take(
                &mut self.pending_aggregate,
            ));
            let op_base = self.create_op_base_opts(
                || op_data.default_op_name(),
                None,
                None,
                false,
            );
            self.opts.add_op(op_base, op_data);
        }
        if let Some(pred) = self.last_non_append_op_id {
            self.opts.init_op(pred, true);
            self.last_non_append_op_id = None;
        }
    }
    pub fn add_op_with_opts(
        mut self,
        op_data: OperatorData,
        argname: Option<&str>,
        label: Option<&str>,
        append_mode: bool,
        transparent_mode: bool,
    ) -> Self {
        self.ref_add_op_with_opts(
            op_data,
            argname,
            label,
            append_mode,
            transparent_mode,
        );
        self
    }
    pub fn add_label(mut self, label: String) -> Self {
        self.ref_terminate_current_aggregate();
        self.opts.add_label(label);
        self
    }
    pub fn add_op(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, false, false)
    }
    pub fn ref_add_op(&mut self, op_data: OperatorData) {
        self.ref_add_op_with_opts(op_data, None, None, false, false)
    }
    pub fn add_op_aggregate_with_opts(
        mut self,
        argname: Option<&str>,
        label: Option<&str>,
        append_mode: bool,
        transparent_mode: bool,
        sub_ops: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        self.ref_terminate_current_aggregate();
        let op_base = self.create_op_base_opts(
            || AGGREGATOR_DEFAULT_NAME.into(),
            argname,
            label,
            transparent_mode,
        );
        self.last_non_append_op_id = Some(add_aggregate_to_sess_opts_uninit(
            &mut self.opts,
            op_base,
            append_mode,
            sub_ops,
        ));
        self
    }
    pub fn add_op_aggregate(
        self,
        sub_ops: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        self.add_op_aggregate_with_opts(None, None, false, false, sub_ops)
    }

    pub fn add_op_with_label(
        self,
        op_data: OperatorData,
        label: &str,
    ) -> Self {
        self.add_op_with_opts(op_data, None, Some(label), false, false)
    }
    pub fn add_ops(
        self,
        op_data: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        let mut this = self;
        for op in op_data {
            this = this.add_op(op);
        }
        this
    }
    pub fn add_op_appending(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, true, false)
    }
    pub fn add_op_transparent(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, false, true)
    }
    pub fn add_op_transparent_appending(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, true, true)
    }
    pub fn set_input(mut self, rs: RecordSet) -> Self {
        self.input_data = rs;
        self
    }
    pub fn build_session(
        mut self,
    ) -> Result<SessionData, ContextualizedScrError> {
        self.ref_terminate_current_aggregate();
        self.opts.build_session()
    }
    pub fn build(self) -> Result<Context, ContextualizedScrError> {
        Ok(Context::new(Arc::new(self.build_session()?)))
    }
    pub fn run_collect_stringified(
        mut self,
    ) -> Result<Vec<String>, ContextualizedScrError> {
        let sink = StringSinkHandle::default();
        self.ref_add_op(create_op_string_sink(&sink));
        let input_data = std::mem::take(&mut self.input_data);
        let sess = self.build_session()?;
        let job = sess.construct_main_chain_job(input_data);
        let mut val = if sess.settings.max_threads == 1 {
            sess.run_job_unthreaded(job);
            sink.get_data().map_err(|e| {
                ContextualizedScrError::from_scr_error(
                    (*e).clone().into(),
                    None,
                    None,
                    Some(&sess),
                )
            })?
        } else {
            let sess_arc = Arc::new(sess);
            Context::new(sess_arc.clone()).run_job(job);
            sink.get_data().map_err(|e| {
                ContextualizedScrError::from_scr_error(
                    (*e).clone().into(),
                    None,
                    None,
                    Some(&sess_arc),
                )
            })?
        };
        Ok(std::mem::take(&mut *val))
    }
    pub fn run_collect(
        mut self,
    ) -> Result<Vec<FieldValue>, ContextualizedScrError> {
        let sink = FieldValueSinkHandle::default();
        self.ref_add_op(create_op_field_value_sink(&sink));
        self.run()?;
        let mut v = sink.get();
        Ok(std::mem::take(&mut *v))
    }
    pub fn run_collect_as<T: FixedSizeFieldValueType>(
        self,
    ) -> Result<Vec<T>, ContextualizedScrError> {
        let mut res = Vec::new();
        for (i, fv) in self.run_collect()?.into_iter().enumerate() {
            let kind = fv.kind();
            if let Some(v) = fv.downcast_allowing_text_as_bytes() {
                res.push(v)
            } else {
                return Err(ContextualizedScrError::from_scr_error(
                    CollectTypeMissmatch {
                        index: i,
                        expected: T::REPR,
                        got: kind,
                    }
                    .into(),
                    None,
                    None,
                    None,
                ));
            }
        }
        Ok(res)
    }
    pub fn run(mut self) -> Result<(), ContextualizedScrError> {
        let input_data = std::mem::take(&mut self.input_data);
        let sess = self.build_session()?;
        let job = sess.construct_main_chain_job(input_data);
        sess.run(job);
        Ok(())
    }
    pub fn run_collect_output(
        self,
    ) -> Result<RecordSet, ContextualizedScrError> {
        // add operation to collect output into record set
        // similar to string sink
        todo!();
    }
}

impl ContextBuilder {
    pub fn push_custom(mut self, v: CustomDataBox, run_length: usize) -> Self {
        self.input_data.push_custom(v, run_length, true, false);
        self
    }
    pub fn push_str(mut self, v: &str, run_length: usize) -> Self {
        self.input_data.push_str(v, run_length, true, false);
        self
    }
    pub fn push_string(mut self, v: String, run_length: usize) -> Self {
        self.input_data.push_string(v, run_length, true, false);
        self
    }
    pub fn push_int(mut self, v: i64, run_length: usize) -> Self {
        self.input_data.push_int(v, run_length, true, false);
        self
    }
    pub fn push_bytes(mut self, v: &[u8], run_length: usize) -> Self {
        self.input_data.push_bytes(v, run_length, true, false);
        self
    }
    pub fn push_null(mut self, run_length: usize) -> Self {
        self.input_data.push_null(run_length, true);
        self
    }
}

impl ContextBuilder {
    pub fn set_max_thread_count(mut self, j: usize) -> Self {
        self.opts.max_threads.force_set(j, None);
        self
    }
    pub fn set_batch_size(mut self, bs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain as usize]
            .default_batch_size
            .force_set(bs, None);
        self
    }
    pub fn set_stream_buffer_size(mut self, sbs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain as usize]
            .stream_buffer_size
            .force_set(sbs, None);
        self
    }
    pub fn set_stream_size_threshold(mut self, sbs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain as usize]
            .stream_size_threshold
            .force_set(sbs, None);
        self
    }
}
