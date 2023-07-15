use crate::{
    chain::ChainId, operations::operator::OperatorId, utils::string_store::StringStoreEntry,
};

use super::{argument::CliArgIdx, chain_spec::ChainSpec};

#[derive(Clone)]
pub struct OperatorBaseOptions {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub chainspec: Option<ChainSpec>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub append_mode: bool,
    pub chain_id: Option<ChainId>, // set by the context on add_op
    pub op_id: Option<OperatorId>, // set by the context on add_op
}

impl OperatorBaseOptions {
    pub fn new(
        argname: StringStoreEntry,
        label: Option<StringStoreEntry>,
        chainspec: Option<ChainSpec>,
        append_mode: bool,
        cli_arg_idx: Option<CliArgIdx>,
    ) -> OperatorBaseOptions {
        OperatorBaseOptions {
            argname,
            label,
            chainspec,
            cli_arg_idx,
            append_mode,
            chain_id: None,
            op_id: None,
        }
    }
}
