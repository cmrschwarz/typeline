use std::{borrow::Cow, num::NonZeroUsize};

use lazy_static::lazy_static;

use crate::{
    chain::{compute_field_livenses, Chain, ChainId, INVALID_CHAIN_ID},
    context::{Session, SessionSettings},
    operators::{
        call::{create_op_call_eager, setup_op_call},
        call_concurrent::setup_op_call_concurrent,
        errors::OperatorSetupError,
        file_reader::setup_op_file_reader,
        format::setup_op_format,
        key::setup_op_key,
        operator::{OperatorBase, OperatorData, OperatorId, OperatorOffsetInChain},
        regex::setup_op_regex,
        select::setup_op_select,
    },
    scr_error::{result_into, ChainSetupError, ContextualizedScrError, ScrError},
    selenium::SeleniumVariant,
    utils::string_store::StringStore,
};

use super::{
    argument::Argument, chain_options::ChainOptions, operator_base_options::OperatorBaseOptions,
};

//TODO: refactor this into SessionOptions

#[derive(Clone)]
pub struct SessionOptions {
    pub max_threads: Argument<usize>,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub install_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub update_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub(crate) string_store: StringStore,
    pub(crate) operator_base_options: Vec<OperatorBaseOptions>,
    pub(crate) operator_data: Vec<OperatorData>,
    pub(crate) chains: Vec<ChainOptions>,
    pub(crate) curr_chain: ChainId,
    pub cli_args: Option<Vec<Vec<u8>>>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            max_threads: Default::default(),
            repl: Default::default(),
            exit_repl: Default::default(),
            install_selenium_drivers: Default::default(),
            update_selenium_drivers: Default::default(),
            chains: vec![ChainOptions::default()],
            curr_chain: Default::default(),
            operator_base_options: Default::default(),
            operator_data: Default::default(),
            string_store: Default::default(),
            cli_args: Default::default(),
        }
    }
}

lazy_static! {
    static ref DEFAULT_CONTEXT_OPTIONS: SessionOptions = SessionOptions {
        max_threads: Argument::new_v(1),
        repl: Argument::new_v(false),
        exit_repl: Argument::new_v(false),
        install_selenium_drivers: Vec::new(),
        update_selenium_drivers: Vec::new(),
        chains: Vec::new(),
        operator_base_options: Vec::new(),
        operator_data: Vec::new(),
        string_store: StringStore::default(),
        cli_args: None,
        curr_chain: 0,
    };
}

impl SessionOptions {
    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
    }
    pub fn add_op(&mut self, mut op_base_opts: OperatorBaseOptions, op_data: OperatorData) {
        op_base_opts.op_id = Some(self.operator_data.len() as OperatorId);
        op_base_opts.chain_id = Some(self.curr_chain);
        match &op_data {
            OperatorData::Call(_) => (),
            OperatorData::CallConcurrent(_) => (),
            OperatorData::Print(_) => (),
            OperatorData::Count(_) => (),
            OperatorData::Cast(_) => (),
            OperatorData::Key(_) => (),
            OperatorData::Select(_) => (),
            OperatorData::Regex(_) => (),
            OperatorData::Format(_) => (),
            OperatorData::StringSink(_) => (),
            OperatorData::FileReader(_) => (),
            OperatorData::Literal(_) => (),
            OperatorData::Sequence(_) => (),
            OperatorData::Join(_) => (),
            OperatorData::Fork(_) => {
                let mut new_chain = ChainOptions::default();
                new_chain.parent = self.curr_chain;
                self.curr_chain = self.chains.len() as ChainId;
                self.chains.push(new_chain);
            }
            OperatorData::Next(_) => {
                let mut new_chain = ChainOptions::default();
                new_chain.parent = self.chains[self.curr_chain as usize].parent;
                self.curr_chain = self.chains.len() as ChainId;
                self.chains.push(new_chain);
                op_base_opts.chain_id = None;
            }
            OperatorData::Up(up) => {
                for _ in 0..up.step.get() {
                    self.curr_chain = self.chains[self.curr_chain as usize].parent;
                }
                op_base_opts.chain_id = None;
            }
        }
        self.operator_base_options.push(op_base_opts);
        self.operator_data.push(op_data);
    }
    pub fn add_label(&mut self, label: String) {
        let new_chain_id = self.chains.len() as ChainId;
        let curr_chain = &mut self.chains[self.curr_chain as usize];
        let mut new_chain = ChainOptions::default();
        new_chain.parent = curr_chain.parent;
        new_chain.label = Some(self.string_store.intern_moved(label));
        let op_base = OperatorBaseOptions::new(
            self.string_store.intern_cloned("jump"),
            None,
            false,
            false,
            None,
        );
        self.add_op(op_base, create_op_call_eager(new_chain_id));
        self.curr_chain = new_chain_id;
        self.chains.push(new_chain);
    }
    pub fn verify_bounds(sess: &mut Session) -> Result<(), ScrError> {
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
    pub fn setup_operators(sess: &mut Session) -> Result<(), OperatorSetupError> {
        for i in 0..sess.operator_bases.len() {
            let op_id = i as OperatorId;
            let op_base = &mut sess.operator_bases[i];
            if op_base.chain_id == INVALID_CHAIN_ID {
                continue;
            }
            let chain = &mut sess.chains[op_base.chain_id as usize];
            op_base.offset_in_chain = chain.operators.len() as OperatorOffsetInChain;
            chain.operators.push(op_id);
            match &mut sess.operator_data[i] {
                OperatorData::Regex(op) => setup_op_regex(&mut sess.string_store, op)?,
                OperatorData::Format(op) => setup_op_format(&mut sess.string_store, op)?,
                OperatorData::Key(op) => setup_op_key(&mut sess.string_store, op)?,
                OperatorData::Select(op) => setup_op_select(&mut sess.string_store, op)?,
                OperatorData::FileReader(op) => setup_op_file_reader(chain, op)?,
                OperatorData::StringSink(_) => (),
                OperatorData::Fork(_) => (),
                OperatorData::Cast(_) => (),
                OperatorData::Count(_) => (),
                OperatorData::Sequence(_) => (),
                OperatorData::Literal(_) => (),
                OperatorData::Print(_) => (),
                OperatorData::Join(_) => (),
                OperatorData::Next(_) => unreachable!(),
                OperatorData::Up(_) => unreachable!(),
                OperatorData::Call(op) => {
                    setup_op_call(&sess.chain_labels, &mut sess.string_store, op, op_id)?
                }
                OperatorData::CallConcurrent(op) => setup_op_call_concurrent(
                    &sess.settings,
                    &sess.chain_labels,
                    &mut sess.string_store,
                    op,
                    op_id,
                )?,
            }
        }
        Ok(())
    }
    pub fn validate_chain(sess: &Session, chain_id: ChainId) -> Result<(), ChainSetupError> {
        let chain = &sess.chains[chain_id as usize];
        let mut message = "";
        if chain.operators.is_empty() && !sess.settings.repl {
            message = "chain must habe at least one operation";
        } else if chain.settings.default_batch_size == 0 {
            message = "default batch size cannot be zero";
        } else if chain.settings.stream_buffer_size == 0 {
            message = "stream buffer size cannot be zero";
        }
        if message != "" {
            return Err(ChainSetupError::new(message, chain_id));
        }
        Ok(())
    }
    pub fn setup_chain_labels(sess: &mut Session) {
        for i in 0..sess.chains.len() {
            if let Some(label) = sess.chains[i].label {
                sess.chain_labels.insert(label, i as ChainId);
            }
        }
    }
    pub fn setup_chains(sess: &mut Session) -> Result<(), ChainSetupError> {
        for i in 0..sess.chains.len() {
            Self::validate_chain(sess, i as ChainId)?;
        }
        compute_field_livenses(sess);
        Ok(())
    }
    pub fn build_session(self) -> Result<Session, ContextualizedScrError> {
        self.build_session_raw().map_err(|(opts, err)| {
            ContextualizedScrError::from_scr_error(err, None, Some(&opts), None)
        })
    }
    pub fn build_session_raw(mut self) -> Result<Session, (SessionOptions, ScrError)> {
        let mut max_threads = self
            .max_threads
            .value
            .unwrap_or(DEFAULT_CONTEXT_OPTIONS.max_threads.unwrap());
        if max_threads == 0 {
            max_threads = std::thread::available_parallelism()
                .unwrap_or(NonZeroUsize::new(1).unwrap())
                .get();
        }

        let mut chains = Vec::with_capacity(self.chains.len());
        for i in 0..self.chains.len() {
            let parent = if i == 0 {
                None
            } else {
                let p: &mut Chain = &mut chains[self.chains[i].parent as usize];
                p.subchains.push(i as ChainId);
                Some(&*p)
            };
            let chain = self.chains[i].build_chain(parent);
            chains.push(chain);
        }

        let mut sess = Session {
            settings: SessionSettings {
                max_threads,
                repl: self.repl.unwrap_or(DEFAULT_CONTEXT_OPTIONS.repl.unwrap()),
            },
            chains,
            operator_data: self.operator_data,
            operator_bases: self
                .operator_base_options
                .iter()
                .map(|obo| {
                    OperatorBase {
                        argname: obo.argname,
                        label: obo.label,
                        cli_arg_idx: obo.cli_arg_idx,
                        chain_id: obo.chain_id.unwrap_or(INVALID_CHAIN_ID),
                        offset_in_chain: u32::MAX, //set during setup
                        append_mode: obo.append_mode,
                        transparent_mode: obo.transparent_mode,
                    }
                })
                .collect(),
            cli_args: self.cli_args,
            chain_labels: Default::default(),
            string_store: self.string_store,
        };
        SessionOptions::setup_chain_labels(&mut sess);
        let res = SessionOptions::verify_bounds(&mut sess)
            .and(result_into(SessionOptions::setup_operators(&mut sess)))
            .and(result_into(SessionOptions::setup_chains(&mut sess)));
        if let Err(e) = res {
            //moving back into context options
            self.string_store = sess.string_store;
            self.operator_data = sess.operator_data;
            self.cli_args = sess.cli_args;
            return Err((self, e));
        }
        Ok(sess)
    }
}
