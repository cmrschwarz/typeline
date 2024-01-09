use std::{
    borrow::Cow,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
};

use lazy_static::lazy_static;

use crate::{
    chain::{Chain, ChainId},
    context::{SessionData, SessionSettings},
    extension::ExtensionRegistry,
    liveness_analysis::{self, LivenessData},
    operators::{
        aggregator::setup_op_aggregator,
        call::setup_op_call,
        call_concurrent::{
            setup_op_call_concurrent, setup_op_call_concurrent_liveness_data,
        },
        end::setup_op_end,
        errors::OperatorSetupError,
        file_reader::setup_op_file_reader,
        foreach::{setup_op_foreach, OpForeach},
        fork::{setup_op_fork, setup_op_fork_liveness_data, OpFork},
        forkcat::{
            setup_op_forkcat, setup_op_forkcat_liveness_data, OpForkCat,
        },
        format::setup_op_format,
        key::setup_op_key,
        nop::{setup_op_nop, OpNop},
        nop_copy::on_op_nop_copy_liveness_computed,
        operator::{OperatorData, OperatorId, OperatorOffsetInChain},
        regex::setup_op_regex,
        select::{setup_op_select, setup_op_select_liveness_data},
        sequence::setup_op_sequence_concurrent_liveness_data,
    },
    scr_error::{
        result_into, ChainSetupError, ContextualizedScrError, ScrError,
    },
    selenium::SeleniumVariant,
    utils::string_store::StringStore,
};

use super::{
    argument::Argument,
    chain_options::{ChainOptions, DEFAULT_CHAIN_OPTIONS},
    operator_base_options::OperatorBaseOptions,
};

pub struct SessionOptions {
    pub max_threads: Argument<usize>,
    pub any_threaded_operations: bool,
    pub repl: Argument<bool>,
    pub exit_repl: Argument<bool>,
    pub install_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub update_selenium_drivers: Vec<Argument<SeleniumVariant>>,
    pub(crate) string_store: StringStore,
    pub(crate) operator_base_options: Vec<OperatorBaseOptions>,
    pub(crate) operator_data: Vec<OperatorData>,
    pub(crate) chains: Vec<ChainOptions>,
    pub(crate) curr_chain: ChainId,
    pub(crate) allow_repl: bool,
    pub cli_args: Option<Vec<Vec<u8>>>,
    pub extensions: Arc<ExtensionRegistry>,
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
            any_threaded_operations: false,
            extensions: Arc::clone(&EMPTY_EXTENSION_REGISTRY),
            allow_repl: true,
        }
    }
}

lazy_static! {
    static ref EMPTY_EXTENSION_REGISTRY: Arc<ExtensionRegistry> =
        Arc::new(ExtensionRegistry::default());
    static ref DEFAULT_CONTEXT_OPTIONS: SessionOptions = SessionOptions {
        max_threads: Argument::new_v(0),
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
        any_threaded_operations: false,
        extensions: Arc::clone(&EMPTY_EXTENSION_REGISTRY),
        allow_repl: true,
    };
}

impl SessionOptions {
    pub fn get_current_chain(&mut self) -> ChainId {
        self.curr_chain
    }
    pub fn init_op(&mut self, op_id: OperatorId, add_to_chain: bool) {
        let op_idx = op_id as usize;
        let op_base_opts = &mut self.operator_base_options[op_idx];
        let op_data = &mut self.operator_data[op_idx];
        op_base_opts.op_id = Some(op_id);
        op_base_opts.desired_batch_size = self.chains
            [self.curr_chain as usize]
            .default_batch_size
            .get()
            .unwrap_or(DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap());
        let chain_opts = &mut self.chains[self.curr_chain as usize];
        op_base_opts.chain_id = Some(self.curr_chain);
        if add_to_chain {
            op_base_opts.offset_in_chain =
                chain_opts.operators.len() as OperatorOffsetInChain;
            chain_opts.operators.push(op_id);
        } else {
            op_base_opts.offset_in_chain =
                chain_opts.operators.len() as OperatorOffsetInChain - 1;
        }
        match op_data {
            OperatorData::Call(_) => (),
            OperatorData::CallConcurrent(_) => {
                self.any_threaded_operations = true;
            }
            OperatorData::Print(_) => (),
            OperatorData::Count(_) => (),
            OperatorData::Cast(_) => (),
            OperatorData::Key(_) => (),
            OperatorData::Nop(_) => (),
            OperatorData::NopCopy(_) => (),
            OperatorData::Select(_) => (),
            OperatorData::Regex(_) => (),
            OperatorData::Format(_) => (),
            OperatorData::StringSink(_) => (),
            OperatorData::FieldValueSink(_) => (),
            OperatorData::FileReader(_) => (),
            OperatorData::Literal(_) => (),
            OperatorData::Sequence(_) => (),
            OperatorData::Join(_) => (),
            OperatorData::Foreach(OpForeach {
                subchains_start, ..
            })
            | OperatorData::Fork(OpFork {
                subchains_start, ..
            })
            | OperatorData::ForkCat(OpForkCat {
                subchains_start, ..
            }) => {
                *subchains_start =
                    self.chains[self.curr_chain as usize].subchain_count;
                self.chains[self.curr_chain as usize].subchain_count += 1;
                let new_chain = ChainOptions {
                    parent: self.curr_chain,
                    ..Default::default()
                };
                self.curr_chain = self.chains.len() as ChainId;
                self.chains.push(new_chain);
            }
            OperatorData::Next(_) => {
                if add_to_chain {
                    chain_opts.operators.pop();
                }
                let parent = self.chains[self.curr_chain as usize].parent;
                self.chains[parent as usize].subchain_count += 1;
                let new_chain = ChainOptions {
                    parent,
                    ..Default::default()
                };
                self.curr_chain = self.chains.len() as ChainId;
                self.chains.push(new_chain);
                op_base_opts.chain_id = None;
            }
            OperatorData::End(end) => {
                if add_to_chain {
                    chain_opts.operators.pop();
                }
                end.chain_id_before = self.curr_chain;
                // the parent of the root chain is itself
                self.curr_chain = self.chains[self.curr_chain as usize].parent;

                op_base_opts.chain_id = Some(self.curr_chain);
                end.subchain_count_after =
                    self.chains[self.curr_chain as usize].subchain_count;
                let subchain_count_after = end.subchain_count_after;
                if let Some(&op_id) =
                    self.chains[self.curr_chain as usize].operators.last()
                {
                    Self::on_operator_subchains_ended(
                        &mut self.operator_data[op_id as usize],
                        subchain_count_after,
                    );
                }
            }
            OperatorData::Aggregator(agg) => {
                for i in 0..agg.sub_ops.len() {
                    let OperatorData::Aggregator(agg) =
                        &mut self.operator_data[op_idx]
                    else {
                        unreachable!()
                    };
                    let sub_op_id = agg.sub_ops[i];
                    self.init_op(sub_op_id, false);
                }
            }
            OperatorData::Custom(_) => {
                let OperatorData::Custom(op) = std::mem::replace(
                    op_data,
                    OperatorData::Nop(OpNop::default()),
                ) else {
                    unreachable!()
                };
                op.on_op_added(self);
                self.operator_data[op_idx] = OperatorData::Custom(op);
            }
        }
    }
    pub fn add_op_uninit(
        &mut self,
        op_base_opts: OperatorBaseOptions,
        op_data: OperatorData,
    ) -> OperatorId {
        let op_id = self.operator_data.len() as OperatorId;
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
    pub fn add_label(&mut self, label: String) {
        let new_chain_id = self.chains.len() as ChainId;
        let curr_chain = &mut self.chains[self.curr_chain as usize];
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
        if sess.operator_bases.len() >= OperatorOffsetInChain::MAX as usize {
            return Err(OperatorSetupError {
                message: Cow::Owned(format!(
                    "cannot have more than {} operators",
                    OperatorOffsetInChain::MAX - 1
                )),
                op_id: OperatorOffsetInChain::MAX,
            }
            .into());
        }
        if sess.chains.len() >= ChainId::MAX as usize {
            return Err(ChainSetupError {
                message: Cow::Owned(format!(
                    "cannot have more than {} chains",
                    ChainId::MAX - 1
                )),
                chain_id: ChainId::MAX,
            }
            .into());
        }
        Ok(())
    }
    pub fn on_operator_subchains_ended(
        op: &mut OperatorData,
        sc_count_after: u32,
    ) {
        match op {
            OperatorData::Fork(OpFork { subchains_end, .. })
            | OperatorData::Foreach(OpForeach { subchains_end, .. })
            | OperatorData::ForkCat(OpForkCat { subchains_end, .. }) => {
                *subchains_end = sc_count_after;
            }
            OperatorData::Call(_) => (),
            OperatorData::CallConcurrent(_) => (),
            OperatorData::Cast(_) => (),
            OperatorData::Count(_) => (),
            OperatorData::Print(_) => (),
            OperatorData::Join(_) => (),
            OperatorData::Next(_) => (),
            OperatorData::End(_) => (),
            OperatorData::Nop(_) => (),
            OperatorData::NopCopy(_) => (),
            OperatorData::Key(_) => (),
            OperatorData::Select(_) => (),
            OperatorData::Regex(_) => (),
            OperatorData::Format(_) => (),
            OperatorData::StringSink(_) => (),
            OperatorData::FieldValueSink(_) => (),
            OperatorData::FileReader(_) => (),
            OperatorData::Literal(_) => (),
            OperatorData::Sequence(_) => (),
            OperatorData::Aggregator(_) => (),
            OperatorData::Custom(op) => op.on_subchains_added(sc_count_after),
        }
    }
    //we can't reborrow the whole Session because we want the locked string store
    pub fn setup_operator(
        sess: &mut SessionData,
        chain_id: ChainId,
        op_id: OperatorId,
    ) -> Result<(), OperatorSetupError> {
        let chain = &sess.chains[chain_id as usize];
        let op_base = &mut sess.operator_bases[op_id as usize];
        match &mut sess.operator_data[op_id as usize] {
            OperatorData::Regex(op) => {
                setup_op_regex(sess.string_store.get_mut().unwrap(), op)?
            }
            OperatorData::Format(op) => {
                setup_op_format(sess.string_store.get_mut().unwrap(), op)?
            }
            OperatorData::Key(op) => {
                setup_op_key(sess.string_store.get_mut().unwrap(), op)?
            }
            OperatorData::Select(op) => {
                setup_op_select(sess.string_store.get_mut().unwrap(), op)?
            }
            OperatorData::FileReader(op) => setup_op_file_reader(chain, op)?,
            OperatorData::StringSink(_) => (),
            OperatorData::FieldValueSink(_) => (),
            OperatorData::Fork(op) => {
                setup_op_fork(chain, op_base, op, op_id)?
            }
            OperatorData::Foreach(_) => {
                setup_op_foreach(sess, chain_id, op_id)?;
            }
            OperatorData::Nop(op) => setup_op_nop(chain, op_base, op, op_id)?,
            OperatorData::NopCopy(_) => (),
            OperatorData::ForkCat(op) => {
                setup_op_forkcat(chain, op_base, op, op_id)?
            }
            OperatorData::Cast(_) => (),
            OperatorData::Count(_) => (),
            OperatorData::Sequence(_) => (),
            OperatorData::Literal(_) => (),
            OperatorData::Print(_) => (),
            OperatorData::Join(_) => (),
            OperatorData::Next(_) => unreachable!(),
            OperatorData::End(op) => {
                setup_op_end(op, op_id)?;
            }
            OperatorData::Call(op) => setup_op_call(
                &sess.chain_labels,
                sess.string_store.get_mut().unwrap(),
                op,
                op_id,
            )?,
            OperatorData::CallConcurrent(op) => setup_op_call_concurrent(
                &sess.settings,
                &sess.chain_labels,
                sess.string_store.get_mut().unwrap(),
                op,
                op_id,
            )?,
            OperatorData::Custom(op) => op.setup(
                op_id,
                op_base,
                chain,
                &sess.settings,
                &sess.chain_labels,
                sess.string_store.get_mut().unwrap(),
            )?,
            OperatorData::Aggregator(_) => {
                setup_op_aggregator(sess, chain_id, op_id)?;
            }
        }
        Ok(())
    }
    pub fn setup_operators(
        sess: &mut SessionData,
    ) -> Result<(), OperatorSetupError> {
        for op_idx in 0..sess.operator_bases.len() {
            let op_base = &mut sess.operator_bases[op_idx];
            let chain_id = if let Some(cid) = op_base.chain_id {
                cid
            } else {
                continue;
            };
            Self::setup_operator(sess, chain_id, op_idx as OperatorId)?;
        }
        Ok(())
    }
    pub fn validate_chain(
        sess: &SessionData,
        chain_id: ChainId,
    ) -> Result<(), ChainSetupError> {
        let chain = &sess.chains[chain_id as usize];
        let mut message = "";
        if chain.operators.is_empty() && !sess.settings.repl {
            message = "chain must habe at least one operation";
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
        for i in 0..sess.chains.len() {
            if let Some(label) = sess.chains[i].label {
                sess.chain_labels.insert(label, i as ChainId);
            }
        }
    }
    pub fn setup_chains(sess: &SessionData) -> Result<(), ChainSetupError> {
        for i in 0..sess.chains.len() {
            Self::validate_chain(sess, i as ChainId)?;
        }
        Ok(())
    }
    pub fn setup_op_liveness(
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        let mut op_data = std::mem::replace(
            &mut sess.operator_data[op_id as usize],
            OperatorData::Nop(OpNop::default()),
        );
        match &mut op_data {
            OperatorData::Call(_) => (),
            OperatorData::CallConcurrent(op) => {
                setup_op_call_concurrent_liveness_data(op, op_id, ld)
            }
            OperatorData::Cast(_) => (),
            OperatorData::Nop(_) => (),
            OperatorData::NopCopy(op) => {
                on_op_nop_copy_liveness_computed(op, op_id, ld)
            }
            OperatorData::Count(_) => (),
            OperatorData::Print(_) => (),
            OperatorData::Join(_) => (),
            OperatorData::Foreach(_) => (),
            OperatorData::Fork(op) => {
                setup_op_fork_liveness_data(op, op_id, ld)
            }
            OperatorData::ForkCat(op) => {
                setup_op_forkcat_liveness_data(sess, op, op_id, ld)
            }
            OperatorData::Next(_) => (),
            OperatorData::End(_) => (),
            OperatorData::Key(_) => (),
            OperatorData::Select(op) => {
                setup_op_select_liveness_data(op, op_id, ld)
            }
            OperatorData::Regex(_) => (),
            OperatorData::Format(_) => (),
            OperatorData::StringSink(_) => (),
            OperatorData::FieldValueSink(_) => (),
            OperatorData::FileReader(_) => (),
            OperatorData::Literal(_) => (),
            OperatorData::Sequence(op) => {
                setup_op_sequence_concurrent_liveness_data(sess, op, op_id, ld)
            }
            OperatorData::Custom(op) => {
                op.on_liveness_computed(sess, op_id, ld)
            }
            OperatorData::Aggregator(agg) => {
                for &sc_id in &agg.sub_ops {
                    Self::setup_op_liveness(sess, ld, sc_id);
                }
            }
        }
        std::mem::swap(&mut sess.operator_data[op_id as usize], &mut op_data);
    }
    pub fn compute_liveness(sess: &mut SessionData) {
        let ld = liveness_analysis::compute_liveness_data(sess);
        for i in 0..sess.operator_bases.len() {
            let op_id = i as OperatorId;
            Self::setup_op_liveness(sess, &ld, op_id);
        }
    }
    pub fn build_session(
        mut self,
    ) -> Result<SessionData, ContextualizedScrError> {
        let mut max_threads;
        if !self.any_threaded_operations {
            max_threads = 1;
        } else {
            max_threads = self
                .max_threads
                .value
                .unwrap_or(DEFAULT_CONTEXT_OPTIONS.max_threads.unwrap());
            if max_threads == 0 {
                max_threads = std::thread::available_parallelism()
                    .unwrap_or(NonZeroUsize::new(1).unwrap())
                    .get();
            }
        }

        let mut chains = Vec::with_capacity(self.chains.len());
        for i in 0..self.chains.len() {
            let parent = if i == 0 {
                None
            } else {
                let p: &mut Chain =
                    &mut chains[self.chains[i].parent as usize];
                p.subchains.push(i as ChainId);
                Some(&*p)
            };
            let chain = self.chains[i].build_chain(parent);
            chains.push(chain);
        }

        let mut sess = SessionData {
            settings: SessionSettings {
                max_threads,
                repl: self
                    .repl
                    .unwrap_or(DEFAULT_CONTEXT_OPTIONS.repl.unwrap()),
            },
            chains,
            operator_data: self.operator_data,
            operator_bases: self
                .operator_base_options
                .iter()
                .map(|obo| obo.build())
                .collect(),
            cli_args: self.cli_args,
            chain_labels: Default::default(),
            string_store: RwLock::new(self.string_store),
            extensions: self.extensions,
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
                Some(&self),
                None,
            ));
        }
        Self::compute_liveness(&mut sess);
        Ok(sess)
    }
}
