use std::{path::Path, sync::Arc};

use once_cell::sync::Lazy;

use crate::{
    cli::call_expr::{Argument, Span},
    context::{Context, SessionData},
    extension::ExtensionRegistry,
    operators::{
        aggregator::{create_op_aggregate, create_op_aggregate_appending},
        field_value_sink::{create_op_field_value_sink, FieldValueSinkHandle},
        key::create_op_key_with_op,
        operator::OperatorData,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    record_data::{
        custom_data::CustomDataBox, field_data::FixedSizeFieldValueType,
        field_value::FieldValue, push_interface::PushInterface,
        record_set::RecordSet,
    },
    scr_error::{CollectTypeMissmatch, ContextualizedScrError, ScrError},
    utils::index_vec::IndexVec,
};

use super::{
    chain_settings::{
        ChainSetting, SettingBatchSize, SettingConversionError,
        SettingDebugLog, SettingMaxThreads, SettingStreamBufferSize,
        SettingStreamSizeThreshold,
    },
    session_setup::{ScrSetupOptions, SessionSetupData},
};

pub struct ContextBuilder {
    pub setup_data: SessionSetupData,
    pub input_data: RecordSet,
}

pub static EMPTY_EXTENSION_REGISTRY: Lazy<Arc<ExtensionRegistry>> =
    Lazy::new(|| Arc::new(ExtensionRegistry::default()));

impl ContextBuilder {
    pub fn without_exts() -> Self {
        Self::with_exts(EMPTY_EXTENSION_REGISTRY.clone())
    }
    pub fn with_exts(extensions: Arc<ExtensionRegistry>) -> Self {
        Self::with_opts(ScrSetupOptions::with_extensions(extensions))
    }
    pub fn with_opts(opts: ScrSetupOptions) -> Self {
        Self {
            setup_data: SessionSetupData::new(opts),
            input_data: RecordSet::default(),
        }
    }

    pub fn from_arguments(
        opts: ScrSetupOptions,
        cli_args: Option<Vec<Vec<u8>>>,
        args: Vec<Argument>,
    ) -> Result<Self, ContextualizedScrError> {
        let mut sess = SessionSetupData::new(opts);
        sess.cli_args = cli_args.map(IndexVec::from);
        sess.process_arguments(args)
            .map_err(|e| sess.contextualize_error(e))?;

        Ok(Self {
            setup_data: sess,
            input_data: RecordSet::default(),
        })
    }

    pub fn from_cli_args(
        opts: ScrSetupOptions,
        args: Vec<Vec<u8>>,
    ) -> Result<Self, ContextualizedScrError> {
        let mut sess = SessionSetupData::new(opts);
        sess.process_cli_args(args)
            .map_err(|e| sess.contextualize_error(e))?;
        Ok(Self {
            setup_data: sess,
            input_data: RecordSet::default(),
        })
    }
    pub fn from_cli_arg_strings<'a>(
        opts: ScrSetupOptions,
        args: impl IntoIterator<Item = impl Into<&'a str>>,
    ) -> Result<Self, ContextualizedScrError> {
        let args = args
            .into_iter()
            .map(|v| v.into().as_bytes().to_vec())
            .collect();
        Self::from_cli_args(opts, args)
    }

    pub fn error_to_string(&self, err: &ScrError) -> String {
        err.contextualize_message(
            self.setup_data.cli_args.as_deref(),
            None,
            None,
            None,
        )
    }

    pub fn add_op(mut self, op_data: OperatorData) -> Self {
        self.setup_data.setup_op_generated(op_data).unwrap();
        self
    }

    pub fn add_op_with_key(
        mut self,
        key: impl Into<String>,
        op_data: OperatorData,
    ) -> Self {
        self.setup_data
            .setup_op_generated(create_op_key_with_op(key.into(), op_data))
            .unwrap();
        self
    }

    pub fn add_ops(
        mut self,
        operations: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        for op_data in operations {
            self.setup_data.setup_op_generated(op_data).unwrap();
        }
        self
    }

    pub fn add_ops_with_spans(
        mut self,
        operations: impl IntoIterator<Item = (OperatorData, Span)>,
    ) -> Self {
        for (op_data, span) in operations {
            self.setup_data
                .setup_op_from_data(
                    op_data,
                    self.setup_data.curr_chain,
                    self.setup_data
                        .direct_chain_offset(self.setup_data.curr_chain),
                    span,
                )
                .unwrap();
        }
        self
    }

    pub fn add_chain(mut self, label: String) -> Self {
        let chain = self
            .setup_data
            .add_subchain(self.setup_data.curr_chain, Some(label));
        self.setup_data.curr_chain = chain;
        self
    }

    pub fn add_op_aggregate(
        mut self,
        sub_ops: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        self.setup_data
            .setup_op_generated(create_op_aggregate(sub_ops))
            .unwrap();
        self
    }

    pub fn add_op_aggregate_appending(
        mut self,
        sub_ops: impl IntoIterator<Item = OperatorData>,
    ) -> Self {
        self.setup_data
            .setup_op_generated(create_op_aggregate_appending(sub_ops))
            .unwrap();
        self
    }

    pub fn set_input(mut self, rs: RecordSet) -> Self {
        self.input_data = rs;
        self
    }
    pub fn build_session(
        mut self,
    ) -> Result<SessionData, ContextualizedScrError> {
        self.setup_data.build_session_take().map_err(|e| {
            ContextualizedScrError::from_scr_error(
                e,
                None,
                None,
                Some(&self.setup_data),
                None,
            )
        })
    }
    pub fn build(self) -> Result<Context, ContextualizedScrError> {
        Ok(Context::new(Arc::new(self.build_session()?)))
    }
    pub fn run_collect_stringified(
        mut self,
    ) -> Result<Vec<String>, ContextualizedScrError> {
        let sink = StringSinkHandle::default();
        self.setup_data
            .setup_op_generated(create_op_string_sink(&sink))
            .unwrap();
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

    // TODO: add a run function that allows consuming an iterator
    pub fn run_collect(
        mut self,
    ) -> Result<Vec<FieldValue>, ContextualizedScrError> {
        let sink = FieldValueSinkHandle::default();
        self.setup_data
            .setup_op_generated(create_op_field_value_sink(&sink))
            .map_err(|e| self.setup_data.contextualize_error(e))?;
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
    pub fn set_chain_setting<S: ChainSetting>(
        mut self,
        value: S::Type,
    ) -> Result<Self, SettingConversionError> {
        S::assign(
            &mut self.setup_data.scope_mgr,
            &self.setup_data.chain_setting_names,
            self.setup_data.chains[self.setup_data.curr_chain].scope_id,
            value,
            Span::Generated,
        )?;
        Ok(self)
    }
    pub fn set_max_thread_count(
        self,
        j: usize,
    ) -> Result<Self, SettingConversionError> {
        self.set_chain_setting::<SettingMaxThreads>(j)
    }
    pub fn set_batch_size(
        self,
        bs: usize,
    ) -> Result<Self, SettingConversionError> {
        self.set_chain_setting::<SettingBatchSize>(bs)
    }
    pub fn set_stream_buffer_size(
        self,
        sbs: usize,
    ) -> Result<Self, SettingConversionError> {
        self.set_chain_setting::<SettingStreamBufferSize>(sbs)
    }
    pub fn set_stream_size_threshold(
        self,
        sst: usize,
    ) -> Result<Self, SettingConversionError> {
        self.set_chain_setting::<SettingStreamSizeThreshold>(sst)
    }
    pub fn set_debug_log_path(
        self,
        path: impl AsRef<Path>,
    ) -> Result<Self, SettingConversionError> {
        self.set_chain_setting::<SettingDebugLog>(Some(
            path.as_ref().to_owned(),
        ))
    }
}
