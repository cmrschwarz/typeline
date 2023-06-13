use std::borrow::Cow;

use thiserror::Error;

use crate::options::argument::CliArgIdx;

use self::operator_base::OperatorId;

pub mod file_reader;
#[allow(dead_code)] //TODO
pub mod format;
pub mod operator_base;
pub mod operator_data;
pub mod print;
pub mod regex;
pub mod split;
pub mod transform_state;

#[derive(Error, Debug, Clone)]
#[error("{message}")]
pub struct OperatorCreationError {
    pub cli_arg_idx: Option<CliArgIdx>,
    pub message: Cow<'static, str>,
}

#[derive(Error, Debug, Clone)]
#[error("in op id {op_id}: {message}")]
pub struct OperatorSetupError {
    pub op_id: OperatorId,
    pub message: Cow<'static, str>,
}

#[derive(Error, Debug, Clone, PartialEq)]
#[error("in op id {0}: {message}", op_id)]
pub struct OperatorApplicationError {
    pub op_id: OperatorId,
    pub message: Cow<'static, str>,
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
