use std::{borrow::Cow, num::NonZeroUsize};

use bstr::BString;
use lazy_static::lazy_static;

use crate::{
    chain::ChainId,
    context::{Context, SessionData},
    field_data::record_set::RecordSet,
    operations::{
        errors::{ChainSetupError, OperatorSetupError},
        file_reader::setup_op_file_reader,
        format::setup_op_format,
        key::setup_op_key,
        operator::{OperatorBase, OperatorData, OperatorId, OperatorOffsetInChain},
        regex::setup_op_regex,
    },
    scr_error::{result_into, ScrError},
    selenium::SeleniumVariant,
    utils::string_store::StringStore,
};

use super::{
    argument::Argument, chain_options::ChainOptions, operator_base_options::OperatorBaseOptions,
};

//TODO: refactor this into SessionOptions

pub struct ContextOptions {
    pub max_worker_threads: Argument<usize>,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub install_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub update_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub input_data: RecordSet,
    pub(crate) string_store: StringStore,
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
            repl: Default::default(),
            exit_repl: Default::default(),
            install_selenium_drivers: Default::default(),
            update_selenium_drivers: Default::default(),
            chains: vec![ChainOptions::default()],
            input_data: Default::default(),
            curr_chain: Default::default(),
            operator_base_options: Default::default(),
            operator_data: Default::default(),
            string_store: Default::default(),
            cli_args: Default::default(),
        }
    }
}

lazy_static! {
    static ref DEFAULT_CONTEXT_OPTIONS: ContextOptions = ContextOptions {
        max_worker_threads: Argument::new(1),
        repl: Argument::new(false),
        exit_repl: Argument::new(false),
        install_selenium_drivers: Vec::new(),
        update_selenium_drivers: Vec::new(),
        chains: Vec::new(),
        operator_base_options: Vec::new(),
        operator_data: Vec::new(),
        input_data: Default::default(),
        string_store: StringStore::default(),
        cli_args: None,
        curr_chain: 0,
    };
}

impl ContextOptions {
    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
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
    pub fn verify_bounds(sess: &mut SessionData) -> Result<(), ScrError> {
        if sess.operator_bases.len() >= OperatorOffsetInChain::MAX as usize {
            return Err(OperatorSetupError {
                message: Cow::Owned(
                    format!(
                        "cannot have more than {} operators",
                        OperatorOffsetInChain::MAX - 1
                    )
                    .to_owned(),
                ),
                op_id: OperatorOffsetInChain::MAX,
            }
            .into());
        }
        if sess.chains.len() >= ChainId::MAX as usize {
            return Err(ChainSetupError {
                message: Cow::Owned(
                    format!("cannot have more than {} chains", ChainId::MAX - 1).to_owned(),
                ),
                chain_id: ChainId::MAX,
            }
            .into());
        }
        return Ok(());
    }
    pub fn setup_operators(sess: &mut SessionData) -> Result<(), OperatorSetupError> {
        for i in 0..sess.operator_bases.len() {
            let op = &mut sess.operator_bases[i];
            let chain = &mut sess.chains[op.chain_id as usize];
            op.offset_in_chain = chain.operations.len() as OperatorOffsetInChain;
            chain.operations.push(i as OperatorId);
            match &mut sess.operator_data[i] {
                OperatorData::Regex(op) => setup_op_regex(&mut sess.string_store, op)?,
                OperatorData::Format(op) => setup_op_format(&mut sess.string_store, op)?,
                OperatorData::Key(op) => setup_op_key(&mut sess.string_store, op)?,
                OperatorData::FileReader(op) => setup_op_file_reader(chain, op)?,
                OperatorData::StringSink(_)
                | OperatorData::Split(_)
                | OperatorData::Sequence(_)
                | OperatorData::DataInserter(_)
                | OperatorData::Print => (),
            }
        }
        Ok(())
    }
    pub fn setup_chains(sess: &mut SessionData) -> Result<(), ChainSetupError> {
        for i in 0..sess.chains.len() {
            let chain = &mut sess.chains[i];
            if chain.operations.is_empty() {
                return Err(ChainSetupError::new(
                    "chain must habe at least one operation",
                    i as ChainId,
                ));
            }
        }
        Ok(())
    }
    pub fn build_context(mut self) -> Result<Context, (ContextOptions, ScrError)> {
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
            input_data: self.input_data,
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
            string_store: self.string_store,
        };
        let res = ContextOptions::verify_bounds(&mut sd)
            .and(result_into(ContextOptions::setup_operators(&mut sd)))
            .and(result_into(ContextOptions::setup_chains(&mut sd)));
        if let Err(e) = res {
            //moving back into context options
            self.input_data = sd.input_data;
            self.string_store = sd.string_store;
            self.operator_data = sd.operator_data;
            self.cli_args = sd.cli_args;
            return Err((self, e));
        }
        Ok(Context::new(sd))
    }
}
