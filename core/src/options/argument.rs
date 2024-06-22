use std::{error::Error, fmt};

use crate::{
    cli::call_expr::Span, utils::debuggable_nonmax::DebuggableNonMaxU32,
};

pub type CliArgIdx = DebuggableNonMaxU32;

#[derive(Clone, derive_more::Deref)]
pub struct Argument<T: Clone> {
    #[deref]
    pub value: Option<T>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgumentReassignmentError {
    pub prev_assignment_span: Span,
    pub reassignment_span: Span,
}
pub const ARGUMENT_REASSIGNMENT_ERROR_MESSAGE: &str = "option was already set";

impl fmt::Display for ArgumentReassignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(ARGUMENT_REASSIGNMENT_ERROR_MESSAGE)
    }
}
impl Error for ArgumentReassignmentError {}

impl<T: Clone> Default for Argument<T> {
    fn default() -> Self {
        Self {
            value: None,
            span: Span::Builtin,
        }
    }
}

impl<T: Clone> Argument<T> {
    pub const fn new(value: T, span: Span) -> Self {
        Self {
            value: Some(value),
            span,
        }
    }
    pub const fn new_v(value: T) -> Self {
        Self {
            value: Some(value),
            span: Span::Generated,
        }
    }
    pub const fn new_opt(value: Option<T>) -> Self {
        Self {
            value,
            span: Span::Generated,
        }
    }
    pub fn set(
        &mut self,
        value: T,
        span: Span,
    ) -> Result<(), ArgumentReassignmentError> {
        if self.value.is_some() {
            return Err(ArgumentReassignmentError {
                reassignment_span: span,
                prev_assignment_span: self.span,
            });
        }
        self.value = Some(value);
        self.span = span;
        Ok(())
    }
    pub fn force_set(&mut self, value: T, span: Span) {
        self.value = Some(value);
        self.span = span;
    }
    pub fn get(&self) -> Option<T> {
        self.value.clone()
    }
    pub fn unwrap(&self) -> T {
        self.value.as_ref().unwrap().clone()
    }
}
