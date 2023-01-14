use crossbeam::deque::Injector;

use crate::{
    chain::{Chain, ChainId},
    cli::CliArgument,
    context::{Context, ContextData, Session, SessionData},
    document::Document,
    encoding::TextEncoding,
    operations::{Operation, OperationId},
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
};
use std::{
    collections::VecDeque,
    ffi::OsString,
    num::NonZeroUsize,
    sync::{Arc, Condvar, Mutex},
};
use std::{ops::Deref, sync::atomic::AtomicUsize};

#[derive(Clone)]
pub struct Argument<T: Clone> {
    value: Option<T>,
    arg: Option<CliArgument>,
}

impl<T: Clone> Default for Argument<T> {
    fn default() -> Self {
        Self {
            value: None,
            arg: None,
        }
    }
}

impl<T: Clone> Argument<T> {
    const fn new(t: T) -> Self {
        Self {
            value: Some(t),
            arg: None,
        }
    }
    const fn new_with_arg(t: T, arg: CliArgument) -> Self {
        Self {
            value: Some(t),
            arg: Some(arg),
        }
    }
}

impl<T: Clone> Deref for Argument<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub enum ChainSpec {}
pub enum RangeSpec {}
#[derive(Clone, Default)]
pub struct ChainOptions {
    pub default_text_encoding: Argument<TextEncoding>,
    pub prefer_parent_text_encoding: Argument<bool>,
    pub force_text_encoding: Argument<bool>,
    pub selenium_variant: Argument<Option<SeleniumVariant>>,
    pub selenium_download_strategy: Argument<SeleniumDownloadStrategy>,
    pub parent: ChainId,
    pub subchains: Vec<ChainId>,
    pub operations: Vec<OperationId>,
}
const DEFAULT_CHAIN_OPTIONS: ChainOptions = ChainOptions {
    default_text_encoding: Argument::new(TextEncoding::UTF8),
    prefer_parent_text_encoding: Argument::new(false),
    force_text_encoding: Argument::new(false),
    selenium_variant: Argument::new(None),
    selenium_download_strategy: Argument::new(SeleniumDownloadStrategy::Scr),
    parent: 0,
    subchains: Vec::new(),
    operations: Vec::new(),
};
impl ChainOptions {
    fn build_chain(self) -> Chain {
        Chain {
            default_text_encoding: self
                .default_text_encoding
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.default_text_encoding.unwrap()),
            prefer_parent_text_encoding: self
                .prefer_parent_text_encoding
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.prefer_parent_text_encoding.unwrap()),
            force_text_encoding: self
                .force_text_encoding
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.force_text_encoding.unwrap()),
            selenium_download_strategy: self
                .selenium_download_strategy
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.selenium_download_strategy.unwrap()),
            operations: self.operations,
            subchains: self.subchains,
            aggregation_targets: Vec::new(),
        }
    }
}

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
    //can't use VecDeque here because new it's new is not a const fn
    pub documents: Vec<Document>,
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
