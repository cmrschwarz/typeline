use crate::{
    chain::ChainId,
    context::Context,
    document::{Document, DocumentSource},
    operations::operation::Operation,
    scr_error::ScrError,
};

use super::{chain_spec::ChainSpec, context_options::ContextOptions};
use smallvec::smallvec;

pub struct ContextBuilder {
    opts: Box<ContextOptions>,
}

pub trait OperationOps {
    fn set_argname(self, argname: String) -> Self;
    fn set_label(self, label: String) -> Self;
    fn set_chainspec(self, chainspec: ChainSpec) -> Self;
}

impl<T: Operation> OperationOps for Box<T> {
    fn set_argname(mut self, argname: String) -> Self {
        self.base_mut().argname = argname;
        self
    }
    fn set_label(mut self, label: String) -> Self {
        self.base_mut().label = Some(label);
        self
    }
    fn set_chainspec(mut self, chainspec: ChainSpec) -> Self {
        self.base_mut().chainspec = Some(chainspec);
        self
    }
}

impl Default for ContextBuilder {
    fn default() -> Self {
        ContextBuilder {
            opts: Box::new(ContextOptions::default()),
        }
    }
}

impl ContextBuilder {
    pub fn add_op<OP: Operation + 'static>(mut self, op: OP) -> Self {
        self.opts.add_op(Box::new(op));
        self
    }
    pub fn add_op_box(mut self, op: Box<dyn Operation>) -> Self {
        self.opts.add_op(op);
        self
    }
    pub fn add_doc(mut self, doc_src: DocumentSource) -> Self {
        self.opts.documents.push(Document {
            source: doc_src,
            reference_point: None,
            target_chains: smallvec![self.opts.curr_chain],
        });
        self
    }
    pub fn add_doc_custom(mut self, doc: Document) -> Self {
        self.opts.documents.push(doc);
        self
    }
    pub fn set_current_chain(mut self, chain_id: ChainId) -> Self {
        self.opts.set_current_chain(chain_id);
        self
    }
    pub fn build(self) -> Result<Context, ScrError> {
        Ok(self.opts.build_context()?)
    }
    pub fn run(self) -> Result<(), ScrError> {
        let mut ctx = self.opts.build_context()?;
        ctx.run()
    }
}
