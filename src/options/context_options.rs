use std::num::NonZeroUsize;

use bstring::BString;

use crate::{
    chain::ChainId,
    context::{Context, SessionData},
    document::Document,
    operations::{
        operator_base::{OperatorBase, OperatorId, OperatorSetupError},
        operator_data::OperatorData,
        setup_operator,
    },
    selenium::SeleniumVariant,
    string_store::StringStore,
};

use super::{
    argument::Argument, chain_options::ChainOptions, operator_base_options::OperatorBaseOptions,
};

//TODO: refactor this into SessionOptions

pub struct ContextOptions {
    pub max_worker_threads: Argument<usize>,
    pub print_help: Argument<bool>,
    pub print_version: Argument<bool>,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub install_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub update_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub documents: Vec<Document>,
    pub(crate) string_store: Option<StringStore>,
    pub(crate) operator_base_options: Vec<OperatorBaseOptions>,
    pub(crate) operator_data: Vec<OperatorData>,
    pub(crate) chains: Vec<ChainOptions>,
    pub(crate) curr_chain: ChainId,
    pub cli_args: Option<Vec<BString>>,
}

impl Default for ContextOptions {
    fn default() -> Self {
        Self {
            max_worker_threads: Default::default(),
            print_help: Default::default(),
            print_version: Default::default(),
            repl: Default::default(),
            exit_repl: Default::default(),
            install_selenium_drivers: Default::default(),
            update_selenium_drivers: Default::default(),
            chains: vec![ChainOptions::default()],
            documents: Default::default(),
            curr_chain: Default::default(),
            operator_base_options: Default::default(),
            operator_data: Default::default(),
            string_store: Default::default(),
            cli_args: Default::default(),
        }
    }
}

const DEFAULT_CONTEXT_OPTIONS: ContextOptions = ContextOptions {
    max_worker_threads: Argument::new(0),
    print_help: Argument::new(false),
    print_version: Argument::new(false),
    repl: Argument::new(false),
    exit_repl: Argument::new(false),
    install_selenium_drivers: Vec::new(),
    update_selenium_drivers: Vec::new(),
    chains: Vec::new(),
    operator_base_options: Vec::new(),
    operator_data: Vec::new(),
    documents: Vec::new(),
    string_store: None,
    cli_args: None,
    curr_chain: 0,
};

impl ContextOptions {
    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
    }
    pub fn string_store_mut(&mut self) -> &mut StringStore {
        self.string_store.get_or_insert_with(Default::default)
    }
    pub fn add_op(&mut self, mut op_base_opts: OperatorBaseOptions, op_data: OperatorData) {
        op_base_opts.curr_chain = Some(self.curr_chain);
        op_base_opts.op_id = Some(self.operator_data.len() as OperatorId);
        self.operator_base_options.push(op_base_opts);
        self.operator_data.push(op_data);
        //TODO: where do we update the current chain e.g. in case of split?
    }
    pub fn set_current_chain(&mut self, chain_id: ChainId) {
        self.curr_chain = chain_id;
        if self.chains.len() <= chain_id as usize {
            self.chains
                .resize(chain_id as usize + 1, Default::default());
        }
    }
    pub fn build_context(mut self) -> Result<Context, (ContextOptions, OperatorSetupError)> {
        let max_worker_threads = NonZeroUsize::try_from(
            self.max_worker_threads
                .value
                .unwrap_or(DEFAULT_CONTEXT_OPTIONS.max_worker_threads.unwrap()),
        )
        .unwrap_or_else(|_| {
            std::thread::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::try_from(1).unwrap())
        });
        //TODO: handle chainspec and operator duplication
        let mut sd = SessionData {
            max_worker_threads,
            is_repl: self.repl.unwrap_or(DEFAULT_CONTEXT_OPTIONS.repl.unwrap()),
            documents: self.documents,
            chains: self.chains.iter().map(|c| c.build_chain()).collect(),
            operator_data: self.operator_data,
            operator_bases: self
                .operator_base_options
                .iter()
                .map(|obo| OperatorBase {
                    argname: obo.argname,
                    label: obo.label,
                    cli_arg_idx: obo.cli_arg_idx,
                    chain_id: obo.curr_chain.unwrap(),
                    offset_in_chain: u32::MAX, //set during setup
                })
                .collect(),
            cli_args: self.cli_args,
            string_store: self.string_store.unwrap_or_default(),
        };
        for i in 0..sd.operator_data.len() {
            if let Err(e) = setup_operator(&mut sd, i as OperatorId) {
                //moving back into context options
                self.documents = sd.documents;
                self.string_store = Some(sd.string_store);
                self.operator_data = sd.operator_data;
                self.cli_args = sd.cli_args;
                return Err((self, e));
            }
        }
        Ok(Context::new(sd))
    }
}
