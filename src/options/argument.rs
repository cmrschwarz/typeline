use std::{error::Error, fmt, ops::Deref};

pub type CliArgIdx = u32;

#[derive(Clone)]
pub struct Argument<T: Clone> {
    pub value: Option<T>,
    pub cli_arg_idx: Option<CliArgIdx>,
}

#[derive(Debug)]
pub struct ArgumentReassignmentError {
    pub message: &'static str,
    pub cli_arg_idx: Option<CliArgIdx>,
}
impl fmt::Display for ArgumentReassignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl Error for ArgumentReassignmentError {}

impl<T: Clone> Default for Argument<T> {
    fn default() -> Self {
        Self {
            value: None,
            cli_arg_idx: None,
        }
    }
}

impl<T: Clone> Deref for Argument<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl<T: Clone> Argument<T> {
    pub const fn new(t: T) -> Self {
        Self {
            value: Some(t),
            cli_arg_idx: None,
        }
    }
    pub const fn new_with_arg_idx(t: T, cli_arg_idx: CliArgIdx) -> Self {
        Self {
            value: Some(t),
            cli_arg_idx: Some(cli_arg_idx),
        }
    }
    pub fn set(&mut self, value: T) -> Result<(), ArgumentReassignmentError> {
        if self.value.is_some() {
            return Err(ArgumentReassignmentError {
                message: "attempted to reassign value of option",
                cli_arg_idx: self.cli_arg_idx,
            });
        }
        self.value = Some(value);
        Ok(())
    }
}
