use crate::{
    chain::ChainId, context::SessionData, options::argument::CliArgIdx,
    string_store::StringStoreEntry,
};

use super::OperatorSetupError;

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
}

pub fn setup_operator(_sd: &mut SessionData, _op_id: OperatorId) -> Result<(), OperatorSetupError> {
    //TODO: typechecking
    Ok(())
}
