use std::borrow::Cow;

use thiserror::Error;

use crate::options::argument::CliArgIdx;

use super::operator::OperatorId;

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
    pub fn new(message: &'static str, cli_arg_idx: Option<CliArgIdx>) -> Self {
        Self {
            message: message.into(),
            cli_arg_idx,
        }
    }
    pub fn new_s(message: String, cli_arg_idx: Option<CliArgIdx>) -> Self {
        Self {
            message: message.into(),
            cli_arg_idx,
        }
    }
}
impl OperatorSetupError {
    pub fn new(message: &'static str, op_id: OperatorId) -> Self {
        Self {
            message: Cow::Borrowed(message),
            op_id,
        }
    }
    pub fn new_s(msg: String, op_id: OperatorId) -> Self {
        Self {
            message: Cow::Owned(msg),
            op_id,
        }
    }
}

impl OperatorApplicationError {
    pub fn new(message: &'static str, op_id: OperatorId) -> Self {
        Self {
            message: Cow::Borrowed(message),
            op_id,
        }
    }
    pub fn new_s(message: String, op_id: OperatorId) -> Self {
        Self {
            message: Cow::Owned(message),
            op_id,
        }
    }
}

pub fn io_error_to_op_error(op_id: OperatorId, err: std::io::Error) -> OperatorApplicationError {
    OperatorApplicationError {
        op_id,
        message: Cow::Owned(err.to_string()),
    }
}
