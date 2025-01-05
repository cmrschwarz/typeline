use crate::{
    chain::{Chain, ChainId},
    cli::{
        call_expr::{Argument, CliArgIdx, Span},
        cli_args_into_arguments_iter, parse_cli_raw, parse_operator_data,
        CliArgumentError, MissingArgumentsError,
    },
    context::{SessionData, SessionSettings},
    extension::ExtensionRegistry,
    liveness_analysis::OpOutputIdx,
    operators::{
        field_value_sink::{create_op_field_value_sink, FieldValueSinkHandle},
        file_reader::create_op_stdin,
        nop::OpNop,
        operator::{
            Operator, OperatorBase, OperatorDataId, OperatorId,
            OperatorOffsetInChain,
        },
        print::{create_op_print_with_opts, PrintOptions},
        success_updater::create_op_success_updator,
        utils::writable::WritableTarget,
    },
    options::chain_settings::SettingTypeConverter,
    record_data::scope_manager::{ScopeManager, DEFAULT_SCOPE_ID},
    typeline_error::{
        ChainSetupError, ContextualizedTypelineError, TypelineError,
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        index_vec::IndexVec,
        indexing_type::IndexingType,
        maybe_text::MaybeTextRef,
        string_store::{StringStore, StringStoreEntry},
    },
};
use std::{
    borrow::Cow,
    collections::HashMap,
    num::NonZeroUsize,
    os::unix::ffi::OsStrExt,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use super::chain_settings::{
    chain_settings_list, ChainSetting, ChainSettingNames,
    SettingActionListCleanupFrequency, SettingBatchSize,
    SettingDebugBreakOnStep, SettingDebugLog, SettingDebugLogNoApply,
    SettingDebugLogStepMin, SettingMaxThreads,
};

#[derive(Clone)]
pub struct SessionSetupOptions {
    pub extensions: Arc<ExtensionRegistry>,
    pub last_cli_output: Option<FieldValueSinkHandle>,
    pub output_storage: Option<FieldValueSinkHandle>,

    pub deny_threading: bool,
    pub allow_repl: bool,
    pub start_with_stdin: bool,
    pub print_output: bool,
    pub add_success_updator: bool,

    // useful if this comes from the cli, not the repl,
    // in which case this is the executable name
    pub skip_first_cli_arg: bool,
}

impl SessionSetupOptions {
    pub fn with_extensions(extensions: Arc<ExtensionRegistry>) -> Self {
        SessionSetupOptions {
            extensions,
            output_storage: None,
            last_cli_output: None,
            allow_repl: false,
            deny_threading: false,
            skip_first_cli_arg: false,
            start_with_stdin: false,
            print_output: false,
            add_success_updator: false,
        }
    }
    pub fn without_extensions() -> Self {
        SessionSetupOptions::with_extensions(Arc::new(
            ExtensionRegistry::default(),
        ))
    }
}

#[derive(Default)]
pub struct SessionSetupSettings {
    pub deny_threading: bool,
    pub allow_repl: bool,
    pub start_with_stdin: bool,
    pub print_output: bool,
    pub add_success_updator: bool,
    pub skipped_first_cli_arg: bool,
    pub repl: Option<bool>,
    pub exit_repl: Option<bool>,
    pub last_cli_output: Option<FieldValueSinkHandle>,
    pub output_storage: Option<FieldValueSinkHandle>,
}

pub struct SessionSetupData {
    pub setup_settings: SessionSetupSettings,
    pub scope_mgr: ScopeManager,
    pub curr_chain: ChainId,
    pub chains: IndexVec<ChainId, Chain>,
    pub chain_labels: HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    pub operator_bases: IndexVec<OperatorId, OperatorBase>,
    pub operator_data: IndexVec<OperatorDataId, Box<dyn Operator>>,
    pub string_store: StringStore,
    pub cli_args: Option<IndexVec<CliArgIdx, Vec<u8>>>,
    pub chain_setting_names: ChainSettingNames,
    pub extensions: Arc<ExtensionRegistry>,
}

macro_rules! ENV_VAR_DEBUG_LOG {
    () => {
        "TYPELINE_DEBUG_LOG_PATH"
    };
}
macro_rules! ENV_VAR_DEBUG_LOG_NO_APPLY {
    () => {
        "TYPELINE_DEBUG_LOG_NO_APPLY"
    };
}
macro_rules! ENV_VAR_DEBUG_LOG_STEP_MIN {
    () => {
        "TYPELINE_DEBUG_LOG_STEP_MIN"
    };
}
macro_rules! ENV_VAR_DEBUG_BREAK_ON_STEP {
    () => {
        "TYPELINE_DEBUG_BREAK_ON_STEP"
    };
}
macro_rules! ENV_VAR_ACTION_LIST_CLEANUP_FREQUENCY {
    () => {
        "TYPELINE_ACTION_LIST_CLEANUP_FREQUENCY"
    };
}

impl SessionSetupSettings {
    pub fn new(opts: &SessionSetupOptions) -> Self {
        Self {
            deny_threading: opts.deny_threading,
            allow_repl: opts.allow_repl,
            start_with_stdin: opts.start_with_stdin,
            print_output: opts.print_output,
            add_success_updator: opts.add_success_updator,
            skipped_first_cli_arg: opts.skip_first_cli_arg,
            repl: None,
            exit_repl: None,
            last_cli_output: opts.last_cli_output.clone(),
            output_storage: opts.output_storage.clone(),
        }
    }
}

impl SessionSetupData {
    pub fn new(opts: SessionSetupOptions) -> Self {
        let mut res = Self {
            setup_settings: SessionSetupSettings::new(&opts),
            scope_mgr: ScopeManager::default(),
            curr_chain: ChainId::ZERO,
            chains: index_vec![Chain {
                label: None,
                operators: IndexVec::new(),
                subchains: IndexVec::new(),
                scope_id: DEFAULT_SCOPE_ID,
                parent: None,
                subchain_idx: None,
            }],
            chain_labels: HashMap::default(),
            operator_bases: IndexVec::new(),
            operator_data: IndexVec::new(),
            string_store: StringStore::default(),
            cli_args: None,
            extensions: opts.extensions,
            chain_setting_names: [StringStoreEntry::MAX_VALUE;
                chain_settings_list::COUNT],
        };

        struct IndexInitializer<'a>(&'a mut SessionSetupData);

        impl<'a> chain_settings_list::ApplyForEach for IndexInitializer<'a> {
            fn apply<T: chain_settings_list::TypeList + ChainSetting>(
                &mut self,
            ) {
                self.0.chain_setting_names[T::INDEX] =
                    self.0.string_store.intern_static(T::NAME);
            }
        }
        chain_settings_list::for_each(&mut IndexInitializer(&mut res));

        if opts.start_with_stdin {
            let op_data = create_op_stdin(1);
            res.setup_op_generated(op_data).unwrap();
        }
        res
    }

    pub fn from_arguments(
        opts: SessionSetupOptions,
        args: Vec<Argument>,
    ) -> Result<Self, TypelineError> {
        let mut sess = Self::new(opts);
        sess.process_arguments(args)?;
        Ok(sess)
    }

    pub fn process_cli_args(
        &mut self,
        args: Vec<Vec<u8>>,
    ) -> Result<(), TypelineError> {
        self.cli_args = Some(IndexVec::from(args));
        let arguments = parse_cli_raw(&mut cli_args_into_arguments_iter(
            self.cli_args.as_mut().unwrap().iter().map(|v| &**v),
        ))?;
        self.process_arguments(arguments)
    }

    pub fn has_no_commands(&self) -> bool {
        self.operator_bases.len()
            == usize::from(self.setup_settings.start_with_stdin)
    }

    pub fn process_arguments(
        &mut self,
        args: impl IntoIterator<Item = Argument>,
    ) -> Result<(), TypelineError> {
        for arg in args {
            let span = arg.span;
            let op = parse_operator_data(self, arg)?;
            self.setup_op_from_data(
                op,
                self.curr_chain,
                self.direct_chain_offset(self.curr_chain),
                span,
            )?;
        }
        if self.has_no_commands() {
            return Err(TypelineError::MissingArgumentsError(
                MissingArgumentsError,
            ));
        }
        Ok(())
    }

    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
    }

    fn get_debug_setting_value<S: ChainSetting>(
        &self,
        env_var_name: &'static str,
        ct_env_value: Option<&str>,
    ) -> Result<(S::Type, Span), CliArgumentError> {
        if let Some((res, span)) = S::lookup(
            &self.scope_mgr,
            &self.chain_setting_names,
            self.chains[ChainId::ZERO].scope_id,
        ) {
            return Ok((
                res.map_err(|e| CliArgumentError::new_s(e.message, span))?,
                span,
            ));
        }

        if let Some(value) = std::env::var_os(env_var_name) {
            let span = Span::EnvVar {
                compile_time: false,
                var_name: env_var_name,
            };
            return Ok((
                S::Converter::convert_to_type_from_maybe_text(
                    MaybeTextRef::from_bytes_try_str(value.as_bytes()),
                )
                .map_err(|e| CliArgumentError::new_s(e.message, span))?,
                span,
            ));
        }

        if let Some(ct_env_arg) = ct_env_value {
            let span = Span::EnvVar {
                compile_time: true,
                var_name: env_var_name,
            };
            return Ok((
                S::Converter::convert_to_type_from_maybe_text(
                    MaybeTextRef::Text(ct_env_arg),
                )
                .map_err(|e| CliArgumentError::new_s(e.message, span))?,
                span,
            ));
        }
        Ok((S::DEFAULT, Span::Builtin))
    }

    fn build_session_settings(
        &mut self,
    ) -> Result<SessionSettings, CliArgumentError> {
        let (mut debug_log_path, debug_log_path_span) = self
            .get_debug_setting_value::<SettingDebugLog>(
                ENV_VAR_DEBUG_LOG!(),
                option_env!(ENV_VAR_DEBUG_LOG!()),
            )?;

        if let Some(path) = debug_log_path.as_ref() {
            if path.as_os_str().is_empty() {
                debug_log_path = None;
            }
        }

        let (debug_log_no_apply, _) = self
            .get_debug_setting_value::<SettingDebugLogNoApply>(
                ENV_VAR_DEBUG_LOG_NO_APPLY!(),
                option_env!(ENV_VAR_DEBUG_LOG_NO_APPLY!()),
            )?;

        let (action_list_cleanup_frequency, _) =
            self.get_debug_setting_value::<SettingActionListCleanupFrequency>(
                ENV_VAR_ACTION_LIST_CLEANUP_FREQUENCY!(),
                option_env!(ENV_VAR_ACTION_LIST_CLEANUP_FREQUENCY!()),
            )?;

        let (debug_log_step_min, _) = self
            .get_debug_setting_value::<SettingDebugLogStepMin>(
                ENV_VAR_DEBUG_LOG_STEP_MIN!(),
                option_env!(ENV_VAR_DEBUG_LOG_STEP_MIN!()),
            )?;

        let (debug_break_on_step, _) = self
            .get_debug_setting_value::<SettingDebugBreakOnStep>(
                ENV_VAR_DEBUG_BREAK_ON_STEP!(),
                option_env!(ENV_VAR_DEBUG_BREAK_ON_STEP!()),
            )?;

        if debug_log_path.is_some() && !cfg!(feature = "debug_log") {
            return Err(CliArgumentError::new(
                "Debug log not supported. Please compile with --features=debug_log",
                debug_log_path_span,
            ));
        }

        let max_threads = if self.setup_settings.deny_threading {
            1
        } else {
            let mt_setting = self
                .lookup_initial_chain_setting::<SettingMaxThreads>(
                    ChainId::ZERO,
                );
            if mt_setting == 0 {
                std::thread::available_parallelism()
                    .unwrap_or(NonZeroUsize::new(1).unwrap())
                    .get()
            } else {
                mt_setting
            }
        };

        Ok(SessionSettings {
            chain_setting_names: self.chain_setting_names,
            max_threads,
            debug_log_path,
            debug_log_no_apply,
            debug_log_step_min,
            debug_break_on_step,
            action_list_cleanup_frequency,
            repl: self.setup_settings.repl.unwrap_or(false),
            last_cli_output: self.setup_settings.last_cli_output.clone(),
            skipped_first_cli_arg: self.setup_settings.skipped_first_cli_arg,
        })
    }

    pub fn contextualize_error(
        &self,
        e: TypelineError,
    ) -> ContextualizedTypelineError {
        ContextualizedTypelineError::from_typeline_error(
            e,
            None,
            None,
            Some(self),
            None,
        )
    }

    pub fn build_session_take(
        &mut self,
    ) -> Result<SessionData, TypelineError> {
        if !self.has_no_commands() {
            if let Some(storage) = &self.setup_settings.output_storage {
                let field_value_sink =
                    create_op_field_value_sink(storage.clone());
                self.setup_op_generated(field_value_sink)?;
            }
            if self.setup_settings.print_output {
                let op_data = create_op_print_with_opts(
                    WritableTarget::Stdout,
                    PrintOptions {
                        ignore_nulls: true,
                        propagate_errors: true,
                    },
                );
                self.setup_op_generated(op_data)?;
            }
            if self.setup_settings.add_success_updator {
                let op_data = create_op_success_updator();
                self.setup_op_generated(op_data)?;
            }
        }
        use std::mem::take;

        let settings = self.build_session_settings()?;
        self.validate_chains()?;

        let mut sess = SessionData {
            chains: take(&mut self.chains),
            scope_mgr: take(&mut self.scope_mgr),
            settings,
            operator_data: take(&mut self.operator_data),
            operator_bases: take(&mut self.operator_bases),
            chain_labels: take(&mut self.chain_labels),
            extensions: take(&mut self.extensions),
            string_store: RwLock::new(take(&mut self.string_store)),
            cli_args: take(&mut self.cli_args),
            success: AtomicBool::new(true),
        };
        sess.compute_liveness();
        Ok(sess)
    }

    pub fn validate_chains(&mut self) -> Result<(), ChainSetupError> {
        if self.chains[ChainId::ZERO].operators.is_empty() {
            return Err(ChainSetupError::new(
                "main chain must contain at least one operator",
                ChainId::ZERO,
            ));
        }

        struct SettingsValidator<'a> {
            sm: &'a mut ScopeManager,
            csn: &'a ChainSettingNames,
            chain: &'a Chain,
            chain_id: ChainId,
        }

        impl<'a> chain_settings_list::ApplyTryFold for SettingsValidator<'a> {
            type Value = ();
            type Error = ChainSetupError;

            fn apply<S: ChainSetting>(
                &mut self,
                _value: Self::Value,
            ) -> Result<Self::Value, Self::Error> {
                if let Some((Err(e), _span)) =
                    S::lookup(self.sm, self.csn, self.chain.scope_id)
                {
                    Err(ChainSetupError {
                        message: Cow::Owned(e.message),
                        chain_id: self.chain_id,
                    })
                } else {
                    Ok(())
                }
            }
        }

        for (chain_id, chain) in self.chains.iter_enumerated() {
            chain_settings_list::try_fold(
                &mut SettingsValidator {
                    sm: &mut self.scope_mgr,
                    csn: &self.chain_setting_names,
                    chain,
                    chain_id,
                },
                (),
            )?;
        }
        Ok(())
    }

    pub fn add_op_data(
        &mut self,
        op_data: Box<dyn Operator>,
    ) -> OperatorDataId {
        self.operator_data.push_get_id(op_data)
    }

    pub fn lookup_initial_chain_setting<S: ChainSetting>(
        &mut self,
        chain_id: ChainId,
    ) -> S::Type {
        let scope_id = self.chains[chain_id].scope_id;
        S::lookup(&self.scope_mgr, &self.chain_setting_names, scope_id)
            .and_then(|(v, _span)| v.ok())
            .unwrap_or(S::DEFAULT)
    }

    pub fn add_op(
        &mut self,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> OperatorId {
        let batch_size =
            self.lookup_initial_chain_setting::<SettingBatchSize>(chain_id);
        let op_id = self.operator_bases.push_get_id(OperatorBase {
            op_data_id,
            chain_id,
            offset_in_chain,
            desired_batch_size: batch_size,
            span,
            outputs_start: OpOutputIdx::MAX_VALUE,
            outputs_end: OpOutputIdx::MAX_VALUE,
        });
        match offset_in_chain {
            OperatorOffsetInChain::Direct(offset) => {
                debug_assert_eq!(
                    offset,
                    self.chains[chain_id].operators.next_idx()
                );
                self.chains[chain_id].operators.push(op_id);
            }
            OperatorOffsetInChain::AggregationMember(_, _) => (),
        }
        op_id
    }

    pub fn add_op_direct(
        &mut self,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        span: Span,
    ) -> OperatorId {
        let offset_in_chain = OperatorOffsetInChain::Direct(
            self.chains[chain_id].operators.next_idx(),
        );
        self.add_op(op_data_id, chain_id, offset_in_chain, span)
    }

    pub fn setup_op(
        &mut self,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        let mut op_data = std::mem::replace(
            &mut self.operator_data[op_data_id],
            Box::new(OpNop::default()),
        );
        op_data.setup(self, op_data_id, chain_id, offset_in_chain, span)?;
        let op_id = op_data.setup(
            self,
            op_data_id,
            chain_id,
            offset_in_chain,
            span,
        )?;
        self.operator_data[op_data_id] = op_data;
        Ok(op_id)
    }

    pub fn setup_op_from_data(
        &mut self,
        mut op_data: Box<dyn Operator>,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        let op_data_id =
            self.operator_data.push_get_id(Box::new(OpNop::default()));
        let op_id = op_data.setup(
            self,
            op_data_id,
            chain_id,
            offset_in_chain,
            span,
        )?;
        self.operator_data[op_data_id] = op_data;
        Ok(op_id)
    }

    pub fn direct_chain_offset(
        &self,
        chain_id: ChainId,
    ) -> OperatorOffsetInChain {
        OperatorOffsetInChain::Direct(
            self.chains[chain_id].operators.next_idx(),
        )
    }

    pub fn setup_op_generated(
        &mut self,
        op_data: Box<dyn Operator>,
    ) -> Result<OperatorId, TypelineError> {
        let offset_in_chain = self.direct_chain_offset(self.curr_chain);
        self.setup_op_from_data(
            op_data,
            self.curr_chain,
            offset_in_chain,
            Span::Generated,
        )
    }

    pub fn setup_ops_with_spans(
        &mut self,
        operations: impl IntoIterator<Item = (Box<dyn Operator>, Span)>,
    ) -> Result<(), TypelineError> {
        for (op_data, span) in operations {
            self.setup_op_from_data(
                op_data,
                self.curr_chain,
                self.direct_chain_offset(self.curr_chain),
                span,
            )?;
        }
        Ok(())
    }

    pub fn add_subchain(
        &mut self,
        parent_id: ChainId,
        label: Option<String>,
    ) -> ChainId {
        let subchain_idx = self.chains[parent_id].subchains.next_idx();
        let chain_name = label.map(|v| self.string_store.intern_moved(v));

        let subchain_chain_id = self.chains.push_get_id(Chain {
            parent: Some(parent_id),
            subchain_idx: Some(subchain_idx),
            label: chain_name,
            operators: IndexVec::new(),
            subchains: IndexVec::new(),
            scope_id: self
                .scope_mgr
                .add_scope(Some(self.chains[parent_id].scope_id)),
        });
        self.chains[parent_id].subchains.push(subchain_chain_id);

        if let Some(chain_name) = chain_name {
            self.chain_labels.insert(chain_name, subchain_chain_id);
        }

        subchain_chain_id
    }

    pub fn setup_subchain(
        &mut self,
        chain_id: ChainId,
        subchain_data: impl IntoIterator<Item = (Box<dyn Operator>, Span)>,
    ) -> Result<ChainId, TypelineError> {
        let subchain_id = self.add_subchain(chain_id, None);
        for (op, span) in subchain_data {
            self.setup_op_from_data(
                op,
                subchain_id,
                OperatorOffsetInChain::Direct(
                    self.chains[subchain_id].operators.next_idx(),
                ),
                span,
            )?;
        }
        Ok(subchain_id)
    }

    pub fn setup_subchain_generated(
        &mut self,
        chain_id: ChainId,
        subchain_data: impl IntoIterator<Item = Box<dyn Operator>>,
    ) -> Result<ChainId, TypelineError> {
        self.setup_subchain(
            chain_id,
            subchain_data.into_iter().map(|op| (op, Span::Generated)),
        )
    }

    pub fn parse_argument(
        &mut self,
        arg: Argument,
    ) -> Result<Box<dyn Operator>, TypelineError> {
        parse_operator_data(self, arg)
    }

    pub fn setup_subchain_from_args(
        &mut self,
        chain_id: ChainId,
        args: Vec<Argument>,
    ) -> Result<ChainId, TypelineError> {
        let sc_id = self.add_subchain(chain_id, None);
        for arg in args {
            let span = arg.span;
            let op = self.parse_argument(arg)?;
            self.setup_op_from_data(
                op,
                sc_id,
                self.direct_chain_offset(chain_id),
                span,
            )?;
        }
        Ok(sc_id)
    }

    pub fn get_chain_setting<S: ChainSetting>(
        &mut self,
        chain_id: ChainId,
    ) -> S::Type {
        S::lookup(
            &self.scope_mgr,
            &self.chain_setting_names,
            self.chains[chain_id].scope_id,
        )
        .and_then(|(res, _span)| res.ok())
        .unwrap_or(S::DEFAULT)
    }
}
