use std::num::NonZeroUsize;

use crate::{
    chain::ChainId,
    context::{Context, ContextData},
    document::Document,
    operations::Operation,
    selenium::SeleniumVariant,
};

use super::{argument::Argument, chain_options::ChainOptions};

#[derive(Clone, Default)]
pub struct ContextOptions {
    pub parallel_jobs: Argument<usize>,
    pub print_help: Argument<bool>,
    pub print_version: Argument<bool>,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub install_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub update_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub chains: Vec<ChainOptions>,
    pub operations: Vec<Box<dyn Operation>>,
    pub documents: Vec<Document>,
    pub curr_chain: ChainId, //so we can use this as a builder
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
