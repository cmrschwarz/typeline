use crate::{
    context::Context,
    field_data::{push_interface::PushInterface, record_set::RecordSet},
    operations::operator::OperatorData,
    scr_error::ScrError,
};

use super::{
    chain_spec::ChainSpec, context_options::ContextOptions,
    operator_base_options::OperatorBaseOptions,
};

#[derive(Clone)]
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
    pub fn add_op_with_opts(
        mut self,
        op_data: OperatorData,
        argname: Option<&str>,
        label: Option<&str>,
        chainspec: Option<ChainSpec>,
        append_mode: bool,
    ) -> Self {
        let op_base = OperatorBaseOptions {
            argname: self
                .opts
                .string_store
                .intern_cloned(argname.unwrap_or(op_data.default_op_name().as_str())),
            label: label.map(|lbl| self.opts.string_store.intern_cloned(lbl)),
            chainspec,
            append_mode,
            cli_arg_idx: None,
            chain_id: None,
            op_id: None,
        };
        self.opts.add_op(op_base, op_data);
        self
    }
    pub fn add_op(self, op_data: OperatorData) -> Self {
        let argname = op_data.default_op_name();
        self.add_op_with_opts(op_data, Some(&argname), None, None, false)
    }
    pub fn add_op_appending(self, op_data: OperatorData) -> Self {
        let argname = op_data.default_op_name();
        self.add_op_with_opts(op_data, Some(&argname), None, None, true)
    }
    pub fn set_input(mut self, rs: RecordSet) -> Self {
        self.opts.input_data = rs;
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

impl ContextBuilder {
    pub fn set_batch_size(mut self, bs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain as usize]
            .default_batch_size
            .force_set(bs);
        self
    }
    pub fn set_stream_buffer_size(mut self, sbs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain as usize]
            .stream_buffer_size
            .force_set(sbs);
        self
    }
    pub fn set_stream_size_threshold(mut self, sbs: usize) -> Self {
        self.opts.chains[self.opts.curr_chain as usize]
            .stream_size_threshold
            .force_set(sbs);
        self
    }
}
