use crate::{
    chain::ChainId,
    context::Context,
    field_data::{fd_push_interface::FDPushInterface, record_set::RecordSet},
    operations::operator::OperatorData,
    scr_error::ScrError,
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
    pub fn set_input(mut self, rs: RecordSet) -> Self {
        self.opts.input_data = rs;
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

impl ContextBuilder {
    pub fn push_str(mut self, v: &str, run_length: usize) -> Self {
        self.opts.input_data.push_str(v, run_length, true, false);
        self
    }
    pub fn push_string(mut self, v: String, run_length: usize) -> Self {
        self.opts.input_data.push_string(v, run_length, true, false);
        self
    }
    pub fn push_int(mut self, v: i64, run_length: usize) -> Self {
        self.opts.input_data.push_int(v, run_length, true, false);
        self
    }
    pub fn push_bytes(mut self, v: &[u8], run_length: usize) -> Self {
        self.opts.input_data.push_bytes(v, run_length, true, false);
        self
    }
    pub fn push_null(mut self, run_length: usize) -> Self {
        self.opts.input_data.push_null(run_length, true);
        self
    }
}
