use std::{
    collections::HashMap,
    num::NonZeroUsize,
    path::PathBuf,
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use once_cell::sync::Lazy;

use crate::{
    chain::{Chain, ChainId},
    cli::{call_expr::Span, CliArgumentError},
    context::{SessionData, SessionSettings, SessionSetupData},
    extension::ExtensionRegistry,
    operators::operator::{OperatorData, OperatorDataId},
    scr_error::{ContextualizedScrError, ScrError},
    utils::{
        identity_hasher::BuildIdentityHasher,
        index_vec::{IndexSlice, IndexVec},
        indexing_type::{IndexingType, IndexingTypeRange},
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::{
    chain_options::ChainOptions,
    operator_base_options::{
        OperatorBaseOptions, OperatorBaseOptionsInterned,
    },
    setting::{CliArgIdx, Setting},
};

pub struct SessionOptions {
    pub debug_log_path: Setting<PathBuf>,
    pub max_threads: Setting<usize>,
    pub any_threaded_operations: bool,
    pub repl: Setting<bool>,
    pub exit_repl: Setting<bool>,
    pub(crate) string_store: StringStore,
    pub(crate) operator_base_opts:
        IndexVec<OperatorDataId, OperatorBaseOptionsInterned>,
    pub(crate) operator_data: IndexVec<OperatorDataId, OperatorData>,
    pub(crate) chains: IndexVec<ChainId, ChainOptions>,
    pub(crate) curr_chain: ChainId,
    pub(crate) allow_repl: bool,
    pub cli_args: Option<IndexVec<CliArgIdx, Vec<u8>>>,
    // needed for reporting the intuitive index in error messages
    pub skipped_first_cli_arg: bool,
    pub extensions: Arc<ExtensionRegistry>,
}
macro_rules! DEBUG_LOG_ENV_VAR_CONST {
    () => {
        "SCR_DEBUG_LOG_PATH"
    };
}
static DEBUG_LOG_ENV_VAR: &str = DEBUG_LOG_ENV_VAR_CONST!();

static EMPTY_EXTENSION_REGISTRY: Lazy<Arc<ExtensionRegistry>> =
    Lazy::new(|| Arc::new(ExtensionRegistry::default()));
static DEFAULT_CONTEXT_OPTIONS: Lazy<SessionOptions> =
    Lazy::new(|| SessionOptions {
        max_threads: Setting::new_v(0),
        debug_log_path: Setting::new(
            option_env!(DEBUG_LOG_ENV_VAR_CONST!()).map(|p| {
                PathBuf::from_str(p)
                    .expect("valid compile time specified debug log path")
            }),
            Span::EnvVar {
                compile_time: true,
                var_name: &DEBUG_LOG_ENV_VAR,
            },
        ),
        repl: Setting::new_v(false),
        exit_repl: Setting::new_v(false),
        chains: IndexVec::new(),
        operator_base_opts: IndexVec::new(),
        operator_data: IndexVec::new(),
        string_store: StringStore::default(),
        cli_args: None,
        skipped_first_cli_arg: false,
        curr_chain: ChainId::zero(),
        any_threaded_operations: false,
        extensions: Arc::clone(&EMPTY_EXTENSION_REGISTRY),
        allow_repl: true,
    });

impl Default for SessionOptions {
    fn default() -> Self {
        Self::with_extensions(EMPTY_EXTENSION_REGISTRY.clone())
    }
}

impl SessionOptions {
    pub fn with_extensions(extensions: Arc<ExtensionRegistry>) -> Self {
        Self {
            max_threads: Setting::default(),
            repl: Setting::default(),
            debug_log_path: Setting::default(),
            exit_repl: Setting::default(),
            chains: IndexVec::from(vec![ChainOptions::default()]),
            curr_chain: ChainId::zero(),
            operator_base_opts: IndexVec::new(),
            operator_data: IndexVec::new(),
            string_store: StringStore::default(),
            cli_args: None,
            skipped_first_cli_arg: false,
            any_threaded_operations: false,
            extensions,
            allow_repl: cfg!(feature = "repl"),
        }
    }
}

impl SessionOptions {
    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
    }
    pub fn add_op_from_interned_opts(
        &mut self,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data: OperatorData,
    ) -> OperatorDataId {
        let op_id = self.operator_base_opts.push_get_id(op_base_opts_interned);
        self.operator_data.push(op_data);
        self.chains[self.curr_chain].operators.push(op_id);
        op_id
    }
    pub fn add_op(
        &mut self,
        op_base_opts: OperatorBaseOptions,
        op_data: OperatorData,
    ) -> OperatorDataId {
        let opts_interned = op_base_opts.intern(&mut self.string_store);
        self.add_op_from_interned_opts(opts_interned, op_data)
    }
    pub fn add_chain(&mut self, label: String) {
        let new_chain_id = self.chains.next_idx();
        let curr_chain = &mut self.chains[self.curr_chain];
        let new_chain = ChainOptions {
            parent: curr_chain.parent,
            label: Some(self.string_store.intern_moved(label)),
            ..Default::default()
        };
        //   let op_base = OperatorBaseOptions::new(
        // self.string_store.intern_cloned("jump"),
        // None,
        // false,
        // false,
        // None,
        // );
        // self.add_op(op_base, create_op_call_eager(new_chain_id));
        self.curr_chain = new_chain_id;
        self.chains.push(new_chain);
    }

    fn build_session_settings(
        &self,
    ) -> Result<SessionSettings, CliArgumentError> {
        let (mut debug_log_path, span) =
            if let Some(path) = self.debug_log_path.get() {
                (Some(path), self.debug_log_path.span)
            } else if let Some(path) = std::env::var_os(DEBUG_LOG_ENV_VAR) {
                (
                    Some(PathBuf::from(path)),
                    Span::EnvVar {
                        compile_time: false,
                        var_name: &DEBUG_LOG_ENV_VAR,
                    },
                )
            } else {
                (
                    DEFAULT_CONTEXT_OPTIONS.debug_log_path.get(),
                    DEFAULT_CONTEXT_OPTIONS.debug_log_path.span,
                )
            };
        if let Some(path) = &debug_log_path {
            if path.as_os_str().is_empty() {
                debug_log_path = None;
            }
        }

        if debug_log_path.is_some() && !cfg!(feature = "debug_log") {
            return Err(CliArgumentError::new(
                "Debug log not supported. Please compile with --features=debug_log",
                span,
            )
            .into());
        }

        let max_threads = if self.any_threaded_operations {
            let mt_setting = self
                .max_threads
                .value
                .unwrap_or(DEFAULT_CONTEXT_OPTIONS.max_threads.unwrap());
            if mt_setting == 0 {
                std::thread::available_parallelism()
                    .unwrap_or(NonZeroUsize::new(1).unwrap())
                    .get()
            } else {
                mt_setting
            }
        } else {
            1
        };

        Ok(SessionSettings {
            max_threads,
            repl: self.repl.unwrap_or(DEFAULT_CONTEXT_OPTIONS.repl.unwrap()),
            skipped_first_cli_arg: self.skipped_first_cli_arg,
            debug_log_path,
        })
    }

    fn build_session_chains(&self) -> IndexVec<ChainId, Chain> {
        let mut chains = IndexVec::with_capacity(self.chains.len());
        for cn_id in IndexingTypeRange::from_zero(self.chains.next_idx()) {
            let parent = if cn_id == ChainId::zero() {
                None
            } else {
                let p: &mut Chain = &mut chains[self.chains[cn_id].parent];
                p.subchains.push(cn_id);
                Some(&*p)
            };
            let chain_opts = &self.chains[cn_id];
            let chain = Chain {
                label: chain_opts.label,
                settings: chain_opts
                    .build_chain_settings(parent.map(|p| &p.settings)),
                operators: IndexVec::new(),
                subchains: IndexVec::new(),
            };
            chains.push(chain);
        }
        chains
    }

    pub fn build_chain_labels(
        chains: &IndexSlice<ChainId, Chain>,
    ) -> HashMap<StringStoreEntry, ChainId, BuildIdentityHasher> {
        let mut chain_labels = HashMap::default();
        for chain_id in
            IndexingTypeRange::new(ChainId::zero()..chains.next_idx())
        {
            if let Some(label) = chains[chain_id].label {
                chain_labels.insert(label, chain_id);
            }
        }
        chain_labels
    }

    fn contextualize_error(&self, e: ScrError) -> ContextualizedScrError {
        ContextualizedScrError::from_scr_error(
            e.into(),
            None,
            None,
            Some(&self),
            None,
        )
    }

    pub fn build_session(
        mut self,
    ) -> Result<SessionData, ContextualizedScrError> {
        let settings = self
            .build_session_settings()
            .map_err(|e| self.contextualize_error(e.into()))?;

        let chains = self.build_session_chains();
        let chain_labels = Self::build_chain_labels(&chains);

        let mut sess_setup_data = SessionSetupData {
            chains,
            settings,
            chain_labels,
            operator_bases: IndexVec::new(),
            operator_data: IndexVec::from(Vec::from(self.operator_data)),
            string_store: self.string_store,
            extensions: self.extensions,
        };

        let mut error = None;

        if error.is_none() {
            for (chain_id, chain_opts) in self.chains.iter_enumerated() {
                error = sess_setup_data
                    .setup_chain(
                        chain_id,
                        chain_opts.operators.iter().map(|&op_id| {
                            (self.operator_base_opts[op_id], op_id)
                        }),
                    )
                    .err()
                    .map(Into::into);
                if error.is_some() {
                    break;
                }
            }
        }

        if error.is_none() {
            error = sess_setup_data.validate_chains().err().map(Into::into);
        }

        if let Some(e) = error {
            // moving back into context options
            sess_setup_data
                .operator_data
                .truncate_len(self.operator_base_opts.len());
            self.operator_data =
                IndexVec::from(Vec::from(sess_setup_data.operator_data));
            self.extensions = sess_setup_data.extensions;
            self.string_store = sess_setup_data.string_store;
            return Err(self.contextualize_error(e));
        }

        let mut sess = SessionData {
            chains: sess_setup_data.chains,
            settings: sess_setup_data.settings,
            operator_data: sess_setup_data.operator_data,
            operator_bases: sess_setup_data.operator_bases,
            chain_labels: sess_setup_data.chain_labels,
            extensions: sess_setup_data.extensions,
            string_store: RwLock::new(sess_setup_data.string_store),
            cli_args: self.cli_args,
            success: AtomicBool::new(true),
        };
        sess.compute_liveness();
        Ok(sess)
    }
}
