use crate::{
    chain::ChainId, operators::operator::OperatorId,
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
        }
    }
}
