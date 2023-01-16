use std::num::NonZeroUsize;

use crate::{
    chain::ChainId,
    context::{Context, ContextData},
    document::Document,
    operations::{Operation, OperationId, OperationOffsetInChain},
    selenium::SeleniumVariant,
};

use super::{argument::Argument, chain_options::ChainOptions, chain_spec::ChainSpec};

#[derive(Clone)]
pub struct ContextOptions {
    pub parallel_jobs: Argument<usize>,
    pub print_help: Argument<bool>,
    pub print_version: Argument<bool>,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub install_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub update_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub(crate) chains: Vec<ChainOptions>,
    pub(crate) operations: Vec<Box<dyn Operation>>,
    pub(crate) documents: Vec<Document>,
    pub(crate) curr_chain: ChainId,
}

impl Default for ContextOptions {
    fn default() -> Self {
        Self {
            parallel_jobs: Default::default(),
            print_help: Default::default(),
            print_version: Default::default(),
            repl: Default::default(),
            exit_repl: Default::default(),
            install_selenium_drivers: Default::default(),
            update_selenium_drivers: Default::default(),
            chains: vec![ChainOptions::default()],
            operations: Default::default(),
            documents: Default::default(),
            curr_chain: Default::default(),
        }
    }
}

const DEFAULT_CONTEXT_OPTIONS: ContextOptions = ContextOptions {
    parallel_jobs: Argument::new(0),
    print_help: Argument::new(false),
    print_version: Argument::new(false),
    repl: Argument::new(false),
    exit_repl: Argument::new(false),
    install_selenium_drivers: Vec::new(),
    update_selenium_drivers: Vec::new(),
    chains: Vec::new(),
    operations: Vec::new(),
    documents: Vec::new(),
    curr_chain: 0,
};

impl ContextOptions {
    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
    }
    pub fn add_op(&mut self, op: Box<dyn Operation>) -> &mut ContextOptions {
        let op_id = self.operations.len() as OperationId;
        self.operations.push(op);
        self.operations.last_mut().unwrap().curr_chain = Some(self.curr_chain);
        self.chains[self.curr_chain].operations.push(op_id);
        self
    }
    pub fn set_current_chain(&mut self, chain_id: ChainId) -> &mut ContextOptions {
        self.curr_chain = chain_id;
        if self.chains.len() <= chain_id {
            self.chains
                .resize(chain_id as usize + 1, Default::default());
        }
        self
    }
    pub fn build_context(self) -> Context {
        let parallel_jobs = NonZeroUsize::try_from(
            self.parallel_jobs
                .value
                .unwrap_or(DEFAULT_CONTEXT_OPTIONS.parallel_jobs.unwrap()),
        )
        .unwrap_or_else(|_| {
            std::thread::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::try_from(1).unwrap())
        });
        Context::new(ContextData {
            parallel_jobs,
            documents: self.documents,
            chains: self.chains.into_iter().map(|c| c.build_chain()).collect(),
            operations: self.operations,
        })
    }
}
