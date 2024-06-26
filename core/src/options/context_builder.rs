use std::{borrow::Cow, sync::Arc};

use crate::{
    cli::{call_expr::Span, parse_cli, CliOptions},
    context::{Context, SessionData},
    extension::ExtensionRegistry,
    operators::{
        aggregator::create_op_aggregate,
        field_value_sink::{create_op_field_value_sink, FieldValueSinkHandle},
        operator::{OperatorData, OperatorDataId},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    record_data::{
        custom_data::CustomDataBox, field_data::FixedSizeFieldValueType,
        field_value::FieldValue, push_interface::PushInterface,
        record_set::RecordSet,
    },
    scr_error::{CollectTypeMissmatch, ContextualizedScrError, ScrError},
};

use super::{
    operator_base_options::OperatorBaseOptions,
    session_options::SessionOptions,
};

#[derive(Default)]
pub struct ContextBuilder {
    opts: SessionOptions,
    input_data: RecordSet,
}

impl ContextBuilder {
    pub fn from_cli_args<'a>(
        opts: &CliOptions,
        args: impl IntoIterator<Item = impl Into<&'a [u8]>>,
    ) -> Result<Self, ContextualizedScrError> {
        let args = args.into_iter().map(|arg| arg.into().to_owned()).collect();
        Ok(Self::from_session_opts(parse_cli(opts, args)?))
    }
    pub fn from_cli_arg_strings<'a>(
        opts: &CliOptions,
        args: impl IntoIterator<Item = impl Into<&'a str>>,
    ) -> Result<Self, ContextualizedScrError> {
        Self::from_cli_args(
            opts,
            args.into_iter().map(|v| v.into().as_bytes()),
        )
    }
    pub fn from_extensions(extensions: Arc<ExtensionRegistry>) -> Self {
        Self::from_session_opts(SessionOptions::with_extensions(extensions))
    }
    pub fn from_session_opts(opts: SessionOptions) -> Self {
        Self {
            opts,
            input_data: RecordSet::default(),
        }
    }

    pub fn error_to_string(&self, err: &ScrError) -> String {
        err.contextualize_message(
            self.opts.cli_args.as_deref(),
            None,
            None,
            None,
        )
    }

    pub fn ref_add_op_with_opts(
        &mut self,
        op_data: OperatorData,
        argname: Option<Cow<'static, str>>,
        label: Option<Cow<'static, str>>,
        append_mode: bool,
        transparent_mode: bool,
        output_is_atom: bool,
    ) -> OperatorDataId {
        self.opts.add_op(
            OperatorBaseOptions {
                argname: argname.unwrap_or_else(|| op_data.default_op_name()),
                label,
                span: Span::Generated,
                transparent_mode,
                append_mode,
                output_is_atom,
            },
            op_data,
        )
    }
    pub fn add_op_with_opts(
        mut self,
        op_data: OperatorData,
        argname: Option<Cow<'static, str>>,
        label: Option<Cow<'static, str>>,
        append_mode: bool,
        transparent_mode: bool,
        output_is_atom: bool,
    ) -> Self {
        self.ref_add_op_with_opts(
            op_data,
            argname,
            label,
            append_mode,
            transparent_mode,
            output_is_atom,
        );
        self
    }
    pub fn add_label(mut self, label: String) -> Self {
        self.opts.add_chain(label);
        self
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
    pub fn ref_add_op(&mut self, op_data: OperatorData) -> OperatorDataId {
        self.ref_add_op_with_opts(op_data, None, None, false, false, false)
    }
    pub fn add_op(mut self, op_data: OperatorData) -> Self {
        self.ref_add_op(op_data);
        self
    }
    pub fn add_op_aggregate_with_opts(
        mut self,
        argname: Option<Cow<'static, str>>,
        label: Option<Cow<'static, str>>,
        append_mode: bool,
        transparent_mode: bool,
        output_is_atom: bool,
        sub_ops: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        self.ref_add_op_with_opts(
            create_op_aggregate(sub_ops),
            argname,
            label,
            append_mode,
            transparent_mode,
            output_is_atom,
        );
        self
    }
    pub fn add_op_aggregate(
        self,
        sub_ops: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        self.add_op_aggregate_with_opts(
            None, None, false, false, false, sub_ops,
        )
    }

    pub fn add_op_with_label(
        self,
        op_data: OperatorData,
        label: Cow<'static, str>,
    ) -> Self {
        self.add_op_with_opts(op_data, None, Some(label), false, false, false)
    }
    pub fn add_op_appending(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, true, false, false)
    }
    pub fn add_op_transparent(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, false, true, false)
    }
    pub fn add_op_transparent_appending(self, op_data: OperatorData) -> Self {
        self.add_op_with_opts(op_data, None, None, true, true, false)
    }
    pub fn set_input(mut self, rs: RecordSet) -> Self {
        self.input_data = rs;
        self
    }
    pub fn build_session(self) -> Result<SessionData, ContextualizedScrError> {
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
        self.opts.max_threads.force_set(j, Span::Generated);
        self
    }
    pub fn set_batch_size(mut self, bs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain]
            .default_batch_size
            .force_set(bs, Span::Generated);
        self
    }
    pub fn set_stream_buffer_size(mut self, sbs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain]
            .stream_buffer_size
            .force_set(sbs, Span::Generated);
        self
    }
    pub fn set_stream_size_threshold(mut self, sbs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain]
            .stream_size_threshold
            .force_set(sbs, Span::Generated);
        self
    }
}
