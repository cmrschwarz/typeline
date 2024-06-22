use std::{
    borrow::Cow,
    collections::HashMap,
    num::NonZeroUsize,
    path::PathBuf,
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use once_cell::sync::Lazy;

use crate::{
    chain::{Chain, ChainId, SubchainIndex},
    context::{SessionData, SessionSettings},
    extension::ExtensionRegistry,
    liveness_analysis::{self, LivenessData},
    operators::{
        errors::OperatorSetupError,
        foreach::OpForeach,
        fork::OpFork,
        forkcat::OpForkCat,
        operator::{
            Operator, OperatorData, OperatorId, OperatorOffsetInChain,
        },
    },
    scr_error::{
        result_into, ChainSetupError, ContextualizedScrError, ScrError,
    },
    utils::{
        index_vec::IndexVec,
        indexing_type::{IndexingType, IndexingTypeRange},
        string_store::StringStore,
    },
};

use super::{
    argument::{Argument, CliArgIdx},
    chain_options::{ChainOptions, DEFAULT_CHAIN_OPTIONS},
    operator_base_options::OperatorBaseOptions,
};

pub struct SessionOptions {
    pub debug_log_path: Argument<PathBuf>,
    pub max_threads: Argument<usize>,
    pub any_threaded_operations: bool,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub(crate) string_store: StringStore,
    pub(crate) operator_base_options:
        IndexVec<OperatorId, OperatorBaseOptions>,
    pub(crate) operator_data: IndexVec<OperatorId, OperatorData>,
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
        max_threads: Argument::new_v(0),
        debug_log_path: Argument::new_opt(
            option_env!("SCR_DEBUG_LOG_PATH")
                .map(|p| PathBuf::from_str(p).expect("valid debug log path")),
        ),
        repl: Argument::new_v(false),
        exit_repl: Argument::new_v(false),
        chains: IndexVec::new(),
        operator_base_options: IndexVec::new(),
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
            max_threads: Argument::default(),
            repl: Argument::default(),
            debug_log_path: Argument::default(),
            exit_repl: Argument::default(),
            chains: IndexVec::from(vec![ChainOptions::default()]),
            curr_chain: ChainId::zero(),
            operator_base_options: IndexVec::new(),
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
    pub fn init_op(&mut self, op_id: OperatorId, add_to_chain: bool) {
        let op_base_opts = &mut self.operator_base_options[op_id];
        let op_data = &mut self.operator_data[op_id];
        op_base_opts.op_id = Some(op_id);
        op_base_opts.desired_batch_size = self.chains[self.curr_chain]
            .default_batch_size
            .get()
            .unwrap_or(DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap());
        let chain_opts = &mut self.chains[self.curr_chain];
        op_base_opts.chain_id = Some(self.curr_chain);
        if add_to_chain {
            op_base_opts.offset_in_chain = chain_opts.operators.next_idx();
            chain_opts.operators.push(op_id);
        } else {
            op_base_opts.offset_in_chain =
                chain_opts.operators.last_idx().unwrap();
        }
        let mut op_data_ext = std::mem::take(op_data);
        op_data_ext.on_op_added(self, op_id, add_to_chain);
        self.operator_data[op_id] = op_data_ext;
    }
    pub fn add_op_uninit(
        &mut self,
        op_base_opts: OperatorBaseOptions,
        op_data: OperatorData,
    ) -> OperatorId {
        let op_id = self.operator_data.next_idx();
        self.operator_base_options.push(op_base_opts);
        self.operator_data.push(op_data);
        op_id
    }
    pub fn add_op(
        &mut self,
        op_base_opts: OperatorBaseOptions,
        op_data: OperatorData,
    ) -> OperatorId {
        let op_id = self.add_op_uninit(op_base_opts, op_data);
        self.init_op(op_id, true);
        op_id
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
    pub fn verify_bounds(sess: &SessionData) -> Result<(), ScrError> {
        let offset_max =
            <OperatorOffsetInChain as IndexingType>::IndexBaseType::MAX
                as usize;
        if sess.operator_bases.len() >= offset_max {
            return Err(OperatorSetupError {
                message: Cow::Owned(format!(
                    "cannot have more than {} operators",
                    offset_max - 1
                )),
                op_id: OperatorId::new(
                    <OperatorId as IndexingType>::IndexBaseType::MAX,
                ),
            }
            .into());
        }
        const MAX_CHAIN_ID: usize =
            <ChainId as IndexingType>::IndexBaseType::MAX as usize;
        if sess.chains.len() >= MAX_CHAIN_ID {
            return Err(ChainSetupError {
                message: Cow::Owned(format!(
                    "cannot have more than {} chains",
                    MAX_CHAIN_ID - 1
                )),
                chain_id: ChainId::from_usize(MAX_CHAIN_ID),
            }
            .into());
        }
        Ok(())
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
            | OperatorData::End(_)
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
    pub fn setup_operator(
        sess: &mut SessionData,
        chain_id: ChainId,
        op_id: OperatorId,
    ) -> Result<(), OperatorSetupError> {
        let mut op_data = std::mem::take(&mut sess.operator_data[op_id]);
        let res = op_data.setup(sess, chain_id, op_id);
        sess.operator_data[op_id] = op_data;
        res
    }
    pub fn setup_operators(
        sess: &mut SessionData,
    ) -> Result<(), OperatorSetupError> {
        for op_id in sess.operator_bases.indices() {
            let op_base = &mut sess.operator_bases[op_id];
            let Some(chain_id) = op_base.chain_id else {
                continue;
            };
            Self::setup_operator(sess, chain_id, op_id)?;
        }
        Ok(())
    }
    pub fn validate_chain(
        sess: &SessionData,
        chain_id: ChainId,
    ) -> Result<(), ChainSetupError> {
        let chain = &sess.chains[chain_id];
        let mut message = "";
        if chain.operators.is_empty() && !sess.settings.repl {
            message = "chain must have at least one operation";
        } else if chain.settings.default_batch_size == 0 {
            message = "default batch size cannot be zero";
        } else if chain.settings.stream_buffer_size == 0 {
            message = "stream buffer size cannot be zero";
        }
        if !message.is_empty() {
            return Err(ChainSetupError::new(message, chain_id));
        }
        Ok(())
    }
    pub fn setup_chain_labels(sess: &mut SessionData) {
        for chain_id in
            IndexingTypeRange::new(ChainId::zero()..sess.chains.next_idx())
        {
            if let Some(label) = sess.chains[chain_id].label {
                sess.chain_labels.insert(label, chain_id);
            }
        }
    }
    pub fn setup_chains(sess: &SessionData) -> Result<(), ChainSetupError> {
        for cn_id in
            IndexingTypeRange::new(ChainId::zero()..sess.chains.next_idx())
        {
            Self::validate_chain(sess, cn_id)?;
        }
        Ok(())
    }
    pub fn setup_op_liveness(
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        let mut op_data = std::mem::take(&mut sess.operator_data[op_id]);
        op_data.on_liveness_computed(sess, ld, op_id);
        sess.operator_data[op_id] = op_data;
    }
    pub fn compute_liveness(sess: &mut SessionData) {
        let ld = liveness_analysis::compute_liveness_data(sess);
        for op_id in
            IndexingTypeRange::from_zero(sess.operator_bases.next_idx())
        {
            Self::setup_op_liveness(sess, &ld, op_id);
        }
    }
    pub fn build_session(
        mut self,
    ) -> Result<SessionData, ContextualizedScrError> {
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

        let mut chains = IndexVec::with_capacity(self.chains.len());
        for cn_id in
            IndexingTypeRange::new(ChainId::zero()..self.chains.next_idx())
        {
            let parent = if cn_id == ChainId::zero() {
                None
            } else {
                let p: &mut Chain = &mut chains[self.chains[cn_id].parent];
                p.subchains.push(cn_id);
                Some(&*p)
            };
            let chain = self.chains[cn_id].build_chain(parent);
            chains.push(chain);
        }

        let mut sess = SessionData {
            settings: SessionSettings {
                max_threads,
                repl: self
                    .repl
                    .unwrap_or(DEFAULT_CONTEXT_OPTIONS.repl.unwrap()),
                skipped_first_cli_arg: self.skipped_first_cli_arg,
                debug_log_path: self
                    .debug_log_path
                    .get()
                    .or(DEFAULT_CONTEXT_OPTIONS.debug_log_path.get()),
            },
            chains,
            operator_data: self.operator_data,
            operator_bases: self
                .operator_base_options
                .iter()
                .map(OperatorBaseOptions::build)
                .collect(),
            cli_args: self.cli_args,
            chain_labels: HashMap::default(),
            string_store: RwLock::new(self.string_store),
            extensions: self.extensions,
            success: AtomicBool::new(true),
        };
        SessionOptions::setup_chain_labels(&mut sess);
        let res = SessionOptions::verify_bounds(&sess)
            .and(result_into(SessionOptions::setup_operators(&mut sess)))
            .and(result_into(SessionOptions::setup_chains(&sess)));
        if let Err(e) = res {
            // moving back into context options
            self.string_store = sess.string_store.into_inner().unwrap();
            self.operator_data = sess.operator_data;
            self.cli_args = sess.cli_args;
            self.extensions = sess.extensions;
            return Err(ContextualizedScrError::from_scr_error(
                e,
                None,
                None,
                Some(&self),
                None,
            ));
        }
        Self::compute_liveness(&mut sess);
        Ok(sess)
    }
}
