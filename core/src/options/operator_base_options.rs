use crate::{
    chain::ChainId,
    cli::call_expr::Span,
    liveness_analysis::OpOutputIdx,
    operators::operator::{OperatorBase, OperatorId, OperatorOffsetInChain},
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::argument::CliArgIdx;

#[derive(Clone)]
pub struct OperatorBaseOptions {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub span: Span,
    pub transparent_mode: bool,
    // all following fields are set by the context on add_op
    pub desired_batch_size: usize,
    pub chain_id: Option<ChainId>,
    pub op_id: Option<OperatorId>,
    pub offset_in_chain: OperatorOffsetInChain,
}

impl OperatorBaseOptions {
    pub fn new(
        argname: StringStoreEntry,
        label: Option<StringStoreEntry>,
        transparent_mode: bool,
        span: Span,
    ) -> OperatorBaseOptions {
        OperatorBaseOptions {
            argname,
            label,
            span,
            transparent_mode,
            desired_batch_size: 0,
            chain_id: None,
            op_id: None,
            offset_in_chain: OperatorOffsetInChain::zero(),
        }
    }
    pub fn from_name(argname: StringStoreEntry) -> OperatorBaseOptions {
        OperatorBaseOptions::new(argname, None, false, Span::Generated)
    }

    pub fn build(&self) -> OperatorBase {
        OperatorBase {
            argname: self.argname,
            label: self.label,
            span: self.span,
            chain_id: self.chain_id,
            transparent_mode: self.transparent_mode,
            desired_batch_size: self.desired_batch_size,
            offset_in_chain: self.offset_in_chain,
            // set during setup
            outputs_start: OpOutputIdx::zero(),
            outputs_end: OpOutputIdx::zero(),
        }
    }
}
