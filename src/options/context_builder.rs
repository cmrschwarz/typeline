use crate::{
    chain::ChainId,
    context::Context,
    document::{Document, DocumentSource},
    operations::{operator_base::OperatorBase, operator_data::OperatorData},
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
    pub fn add_op(mut self, op_base: OperatorBase, op_data: OperatorData) -> Self {
        self.opts.add_op(op_base, op_data);
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
        Ok(self.opts.build_context().map_err(|e| e.1)?)
    }
    pub fn run(self) -> Result<(), ScrError> {
        self.build()?.run()
    }
}
