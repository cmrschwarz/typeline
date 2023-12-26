use crate::{
    chain::ChainId,
    operators::operator::{OperatorBase, OperatorId, OperatorOffsetInChain},
    utils::string_store::StringStoreEntry,
};

use super::argument::CliArgIdx;

#[derive(Clone)]
pub struct OperatorBaseOptions {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub append_mode: bool,
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
        append_mode: bool,
        transparent_mode: bool,
        cli_arg_idx: Option<CliArgIdx>,
    ) -> OperatorBaseOptions {
        OperatorBaseOptions {
            argname,
            label,
            cli_arg_idx,
            append_mode,
            transparent_mode,
            desired_batch_size: 0,
            chain_id: None,
            op_id: None,
            offset_in_chain: 0,
        }
    }
    pub fn from_name(argname: StringStoreEntry) -> OperatorBaseOptions {
        OperatorBaseOptions::new(argname, None, false, false, None)
    }

    pub fn build(&self) -> OperatorBase {
        OperatorBase {
            argname: self.argname,
            label: self.label,
            cli_arg_idx: self.cli_arg_idx,
            chain_id: self.chain_id,
            append_mode: self.append_mode,
            transparent_mode: self.transparent_mode,
            desired_batch_size: self.desired_batch_size,
            offset_in_chain: self.offset_in_chain,
            // set during setup
            outputs_start: 0,
            outputs_end: 0,
        }
    }
}
