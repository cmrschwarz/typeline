use crate::{chain::ChainId, options::argument::CliArgIdx, string_store::StringStoreEntry};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
}
