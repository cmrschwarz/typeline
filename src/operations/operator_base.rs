use std::borrow::Cow;

use crate::string_store::StringStoreEntry;
use thiserror::Error;

use crate::{
    chain::ChainId,
    options::{argument::CliArgIdx, chain_spec::ChainSpec},
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

#[derive(Clone, Copy, Debug)]
pub struct OperatorRef {
    pub chain_id: ChainId,
    pub op_offset: OperatorOffsetInChain,
}

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
#[error("in op {0} of chain {1}: {message}", op_ref.op_offset, op_ref.chain_id)]
pub struct OperatorApplicationError {
    pub message: Cow<'static, str>,
    pub op_ref: OperatorRef,
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
    pub fn new(message: &'static str, op_ref: OperatorRef) -> OperatorApplicationError {
        OperatorApplicationError {
            message: Cow::Borrowed(message),
            op_ref,
        }
    }
}

impl OperatorRef {
    pub fn new(chain_id: ChainId, op_offset: OperatorOffsetInChain) -> Self {
        Self {
            chain_id,
            op_offset,
        }
    }
}

#[derive(Clone)]
pub struct OperatorBase {
    //TODO: argname and label will be interned later, store them as strings for now
    pub(crate) argname: StringStoreEntry,
    pub(crate) label: Option<StringStoreEntry>,
    pub(crate) chainspec: Option<ChainSpec>,
    pub(crate) cli_arg_idx: Option<CliArgIdx>,
    pub(crate) curr_chain: Option<ChainId>, // set by the context on add_op
    pub(crate) op_id: Option<OperatorId>,   // set by the context on add_op
}

impl OperatorBase {
    pub fn new(
        argname: StringStoreEntry,
        label: Option<StringStoreEntry>,
        chainspec: Option<ChainSpec>,
        cli_arg_idx: Option<CliArgIdx>,
    ) -> OperatorBase {
        OperatorBase {
            argname,
            label,
            chainspec,
            cli_arg_idx,
            curr_chain: None,
            op_id: None,
        }
    }
}
