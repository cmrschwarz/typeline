use std::sync::Arc;

use crate::{
    context::{Context, Session},
    operators::operator::OperatorData,
    record_data::{push_interface::PushInterface, record_set::RecordSet},
    scr_error::ContextualizedScrError,
};

use super::{
    operator_base_options::OperatorBaseOptions,
    session_options::SessionOptions,
};

#[derive(Default, Clone)]
pub struct ContextBuilderData {
    opts: SessionOptions,
    input_data: RecordSet,
}

#[derive(Default, Clone)]
pub struct ContextBuilder {
    data: Box<ContextBuilderData>,
}

impl ContextBuilder {
    pub fn add_op_with_opts(
        mut self,
        op_data: OperatorData,
        argname: Option<&str>,
        label: Option<&str>,
        append_mode: bool,
        transparent_mode: bool,
    ) -> Self {
        let op_base = OperatorBaseOptions::new(
            self.data.opts.string_store.intern_cloned(
                argname.unwrap_or(op_data.default_op_name().as_str()),
            ),
            label.map(|lbl| self.data.opts.string_store.intern_cloned(lbl)),
            append_mode,
            transparent_mode,
            None,
        );
        self.data.opts.add_op(op_base, op_data);
        self
    }
    pub fn add_label(mut self, label: String) -> Self {
        self.data.opts.add_label(label);
        self
    }
    pub fn add_op(self, op_data: OperatorData) -> Self {
        let argname = op_data.default_op_name();
        self.add_op_with_opts(op_data, Some(&argname), None, false, false)
    }
    pub fn add_ops<const N: usize>(self, op_data: [OperatorData; N]) -> Self {
        let mut this = self;
        for op in op_data.into_iter() {
            this = this.add_op(op);
        }
        this
    }
    pub fn add_op_appending(self, op_data: OperatorData) -> Self {
        let argname = op_data.default_op_name();
        self.add_op_with_opts(op_data, Some(&argname), None, true, false)
    }
    pub fn add_op_transparent(self, op_data: OperatorData) -> Self {
        let argname = op_data.default_op_name();
        self.add_op_with_opts(op_data, Some(&argname), None, false, true)
    }
    pub fn add_op_transparent_appending(self, op_data: OperatorData) -> Self {
        let argname = op_data.default_op_name();
        self.add_op_with_opts(op_data, Some(&argname), None, true, true)
    }
    pub fn set_input(mut self, rs: RecordSet) -> Self {
        self.data.input_data = rs;
        self
    }
    pub fn build_session(self) -> Result<Session, ContextualizedScrError> {
        self.data.opts.build_session()
    }
    pub fn build(self) -> Result<Context, ContextualizedScrError> {
        Ok(Context::new(Arc::new(self.build_session()?)))
    }
    pub fn run(self) -> Result<(), ContextualizedScrError> {
        let sess = self.data.opts.build_session()?;
        if sess.settings.max_threads == 1 {
            sess.run_job_unthreaded(
                sess.construct_main_chain_job(self.data.input_data),
            )
        } else {
            let mut ctx = Context::new(Arc::new(sess));
            ctx.run_main_chain(self.data.input_data);
        }
        Ok(())
    }
    pub fn run_collect_output(
        self,
    ) -> Result<RecordSet, ContextualizedScrError> {
        // add operation to collect output into record set
        // similar to string sink
        todo!();
    }
}

impl ContextBuilder {
    pub fn push_str(mut self, v: &str, run_length: usize) -> Self {
        self.data.input_data.push_str(v, run_length, true, false);
        self
    }
    pub fn push_string(mut self, v: String, run_length: usize) -> Self {
        self.data.input_data.push_string(v, run_length, true, false);
        self
    }
    pub fn push_int(mut self, v: i64, run_length: usize) -> Self {
        self.data.input_data.push_int(v, run_length, true, false);
        self
    }
    pub fn push_bytes(mut self, v: &[u8], run_length: usize) -> Self {
        self.data.input_data.push_bytes(v, run_length, true, false);
        self
    }
    pub fn push_null(mut self, run_length: usize) -> Self {
        self.data.input_data.push_null(run_length, true);
        self
    }
}

impl ContextBuilder {
    pub fn set_max_thread_count(mut self, j: usize) -> Self {
        self.data.opts.max_threads.force_set(j, None);
        self
    }
    pub fn set_batch_size(mut self, bs: usize) -> Self {
        self.data.opts.chains[self.data.opts.curr_chain as usize]
            .default_batch_size
            .force_set(bs, None);
        self
    }
    pub fn set_stream_buffer_size(mut self, sbs: usize) -> Self {
        self.data.opts.chains[self.data.opts.curr_chain as usize]
            .stream_buffer_size
            .force_set(sbs, None);
        self
    }
    pub fn set_stream_size_threshold(mut self, sbs: usize) -> Self {
        self.data.opts.chains[self.data.opts.curr_chain as usize]
            .stream_size_threshold
            .force_set(sbs, None);
        self
    }
}
