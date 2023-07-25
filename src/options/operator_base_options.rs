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
    pub chain_id: Option<ChainId>, // set by the context on add_op
    pub op_id: Option<OperatorId>, // set by the context on add_op
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
            chain_id: None,
            op_id: None,
        }
    }
}
