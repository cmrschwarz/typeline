use std::{
    collections::HashMap,
    num::NonZeroUsize,
    path::PathBuf,
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use once_cell::sync::Lazy;

use crate::{
    chain::{Chain, ChainId, SubchainIndex},
    context::{SessionData, SessionSettings, SessionSetupData},
    extension::ExtensionRegistry,
    operators::{
        foreach::OpForeach,
        fork::OpFork,
        forkcat::OpForkCat,
        operator::{
            Operator, OperatorData, OperatorDataId, OperatorOffsetInChain,
        },
    },
    scr_error::ContextualizedScrError,
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

static EMPTY_EXTENSION_REGISTRY: Lazy<Arc<ExtensionRegistry>> =
    Lazy::new(|| Arc::new(ExtensionRegistry::default()));
static DEFAULT_CONTEXT_OPTIONS: Lazy<SessionOptions> =
    Lazy::new(|| SessionOptions {
        max_threads: Setting::new_v(0),
        debug_log_path: Setting::new_opt(
            option_env!("SCR_DEBUG_LOG_PATH")
                .map(|p| PathBuf::from_str(p).expect("valid debug log path")),
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
        let op_id = self.operator_data.next_idx();
        self.operator_base_opts.push(op_base_opts_interned);
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
    pub fn on_operator_subchains_ended(
        op: &mut OperatorData,
        scs_end: SubchainIndex,
    ) {
        match op {
            OperatorData::Fork(OpFork { subchains_end, .. })
            | OperatorData::Foreach(OpForeach { subchains_end, .. })
            | OperatorData::ForkCat(OpForkCat { subchains_end, .. }) => {
                *subchains_end = scs_end;
            }
            OperatorData::Custom(op) => op.on_subchains_added(scs_end),
            OperatorData::MultiOp(op) => op.on_subchains_added(scs_end),
            OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::ToStr(_)
            | OperatorData::Count(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Next(_)
            | OperatorData::Nop(_)
            | OperatorData::NopCopy(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::Sequence(_)
            | OperatorData::Aggregator(_) => (),
        }
    }

    fn build_session_settings(&self) -> SessionSettings {
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

        SessionSettings {
            max_threads,
            repl: self.repl.unwrap_or(DEFAULT_CONTEXT_OPTIONS.repl.unwrap()),
            skipped_first_cli_arg: self.skipped_first_cli_arg,
            debug_log_path: self
                .debug_log_path
                .get()
                .or(DEFAULT_CONTEXT_OPTIONS.debug_log_path.get()),
        }
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

    pub fn build_session(
        mut self,
    ) -> Result<SessionData, ContextualizedScrError> {
        let settings = self.build_session_settings();
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

        let mut res = None;

        'chains: for (chain_id, chain_opts) in self.chains.iter_enumerated() {
            for &op_data_id in &chain_opts.operators {
                res = sess_setup_data
                    .setup_for_op_data_id(
                        chain_id,
                        OperatorOffsetInChain::Direct(
                            sess_setup_data.chains[chain_id]
                                .operators
                                .next_idx(),
                        ),
                        self.operator_base_opts[op_data_id],
                        op_data_id,
                    )
                    .err();

                if res.is_some() {
                    break 'chains;
                }
            }
        }

        if let Some(e) = res {
            // moving back into context options
            sess_setup_data
                .operator_data
                .truncate_len(self.operator_base_opts.len());
            self.operator_data =
                IndexVec::from(Vec::from(sess_setup_data.operator_data));
            self.extensions = sess_setup_data.extensions;
            self.string_store = sess_setup_data.string_store;
            return Err(ContextualizedScrError::from_scr_error(
                e.into(),
                None,
                None,
                Some(&self),
                None,
            ));
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
