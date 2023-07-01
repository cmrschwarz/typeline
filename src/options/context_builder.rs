use crate::{
    chain::ChainId, context::Context, operations::operator::OperatorData, scr_error::ScrError,
};

use super::{context_options::ContextOptions, operator_base_options::OperatorBaseOptions};

pub struct ContextBuilder {
    opts: Box<ContextOptions>,
}

impl Default for ContextBuilder {
    fn default() -> Self {
        ContextBuilder {
            opts: Box::new(ContextOptions::default()),
        }
    }
}

impl ContextBuilder {
    pub fn add_op_raw(mut self, op_base: OperatorBaseOptions, op_data: OperatorData) -> Self {
        self.opts.add_op(op_base, op_data);
        self
    }
    pub fn add_op(mut self, op_data: OperatorData) -> Self {
        let argname = self
            .opts
            .string_store
            .intern_cloned(op_data.default_op_name().as_str());
        let base = OperatorBaseOptions {
            argname: argname,
            label: None,
            chainspec: None,
            cli_arg_idx: None,
            curr_chain: None,
            op_id: None,
        };
        self.opts.add_op(base, op_data);
        self
    }

    pub fn set_current_chain(mut self, chain_id: ChainId) -> Self {
        self.opts.set_current_chain(chain_id);
        self
    }
    pub fn build(self) -> Result<Context, ScrError> {
        Ok(self.opts.build_context().map_err(|e| e.1)?)
    }
    pub fn run(self) -> Result<(), ScrError> {
        self.build()?.run()
    }
}
