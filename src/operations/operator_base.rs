use std::borrow::Cow;

use crate::string_store::StringStoreEntry;
use thiserror::Error;

use crate::{
    chain::ChainId,
    options::{argument::CliArgIdx, chain_spec::ChainSpec},
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

#[derive(Error, Debug, Clone)]
#[error("{message}")]
pub struct OperatorCreationError {
    pub message: Cow<'static, str>,
    pub cli_arg_idx: Option<CliArgIdx>,
}

#[derive(Error, Debug, Clone)]
#[error("in op id {op_id}: {message}")]
pub struct OperatorSetupError {
    pub message: Cow<'static, str>,
    pub op_id: OperatorId,
}

#[derive(Error, Debug, Clone)]
#[error("in op id {0}: {message}", op_id)]
pub struct OperatorApplicationError {
    pub message: Cow<'static, str>,
    pub op_id: OperatorId,
}

impl OperatorCreationError {
    pub fn new(message: &'static str, cli_arg_idx: Option<CliArgIdx>) -> OperatorCreationError {
        OperatorCreationError {
            message: message.into(),
            cli_arg_idx,
        }
    }
}
impl OperatorSetupError {
    pub fn new(message: &'static str, op_id: OperatorId) -> OperatorSetupError {
        OperatorSetupError {
            message: Cow::Borrowed(message),
            op_id,
        }
    }
}

impl OperatorApplicationError {
    pub fn new(message: &'static str, op_id: OperatorId) -> OperatorApplicationError {
        OperatorApplicationError {
            message: Cow::Borrowed(message),
            op_id,
        }
    }
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
}
