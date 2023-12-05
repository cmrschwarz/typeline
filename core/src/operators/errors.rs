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

// optimized layout because it's otherwise the bottleneck of the FieldValue
// enum
#[derive(Debug, Clone)]
pub enum OperatorApplicationError {
    Borrowed {
        op_id: OperatorId,
        message: &'static str,
    },
    Owned {
        op_id: OperatorId,
        message: Box<str>,
    },
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
        OperatorApplicationError::Borrowed { op_id, message }
    }
    pub fn new_s(message: String, op_id: OperatorId) -> Self {
        OperatorApplicationError::Owned {
            op_id,
            message: message.into_boxed_str(),
        }
    }
    pub fn op_id(&self) -> OperatorId {
        match self {
            OperatorApplicationError::Borrowed { op_id, .. } => *op_id,
            OperatorApplicationError::Owned { op_id, .. } => *op_id,
        }
    }
    pub fn message(&self) -> &str {
        match self {
            OperatorApplicationError::Borrowed { message, .. } => message,
            OperatorApplicationError::Owned { message, .. } => message,
        }
    }
}

impl std::fmt::Display for OperatorApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "in op id {}: {}",
            self.op_id(),
            self.message()
        ))
    }
}

impl std::error::Error for OperatorApplicationError {}
impl PartialEq for OperatorApplicationError {
    fn eq(&self, other: &Self) -> bool {
        self.message() == other.message() && self.op_id() == other.op_id()
    }
}

pub fn io_error_to_op_error(
    op_id: OperatorId,
    err: std::io::Error,
) -> OperatorApplicationError {
    OperatorApplicationError::new_s(err.to_string(), op_id)
}
