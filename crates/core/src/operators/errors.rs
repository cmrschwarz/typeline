use std::{borrow::Cow, cmp::Ordering};

use thiserror::Error;

use crate::cli::{call_expr::Span, CliArgumentError};

use super::operator::OperatorId;

#[must_use]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct OperatorCreationError {
    pub message: Cow<'static, str>,
    pub span: Span,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("in op id {op_id}: {message}")]
pub struct OperatorSetupError {
    pub op_id: OperatorId,
    pub message: Cow<'static, str>,
}

// optimized layout because it's otherwise the bottleneck of the FieldValue
// enum
#[derive(Debug, Clone, Eq)]
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
    pub fn new(message: &'static str, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
    pub fn new_s(message: String, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
}

impl From<CliArgumentError> for OperatorCreationError {
    fn from(e: CliArgumentError) -> Self {
        Self {
            message: e.message,
            span: e.span,
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

impl Default for OperatorApplicationError {
    fn default() -> Self {
        Self::Borrowed {
            op_id: OperatorId::default(),
            message: "",
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
            OperatorApplicationError::Borrowed { op_id, .. }
            | OperatorApplicationError::Owned { op_id, .. } => *op_id,
        }
    }
    pub fn message(&self) -> &str {
        match self {
            OperatorApplicationError::Borrowed { message, .. } => message,
            OperatorApplicationError::Owned { message, .. } => message,
        }
    }
    pub fn into_message_cow(self) -> Cow<'static, str> {
        match self {
            OperatorApplicationError::Borrowed { message, .. } => {
                Cow::Borrowed(message)
            }
            OperatorApplicationError::Owned { message, .. } => {
                Cow::from(message.into_string())
            }
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
impl PartialOrd for OperatorApplicationError {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.op_id() != other.op_id() {
            return None;
        }
        // TODO: decide what is right here.
        // the current thought process is: don't reorder errors,
        // the order that they are generated in is the right one
        if self.message() != other.message() {
            return None;
        }
        Some(Ordering::Equal)
    }
}

pub fn io_error_to_op_error(
    op_id: OperatorId,
    err: &std::io::Error,
) -> OperatorApplicationError {
    OperatorApplicationError::new_s(err.to_string(), op_id)
}
