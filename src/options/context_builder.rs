use crate::{
    chain::ChainId,
    context::Context,
    document::{Document, DocumentSource},
    operations::Operation,
    scr_error::ScrError,
};

use super::context_options::ContextOptions;
use smallvec::smallvec;

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
    pub fn add_op<OP: Operation + 'static>(mut self, op: OP) -> Self {
        self.opts.add_op(Box::new(op));
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
        self.opts.build_context().map_err(|err| err.into())
    }
    pub fn run(self) -> Result<(), ScrError> {
        let mut ctx = self.opts.build_context()?;
        ctx.run()
    }
}
