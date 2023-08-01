use std::{error::Error, fmt, ops::Deref};

pub type CliArgIdx = u32;

#[derive(Clone)]
pub struct Argument<T: Clone> {
    pub value: Option<T>,
    pub cli_arg_idx: Option<CliArgIdx>,
}

#[derive(Debug, Clone)]
pub struct ArgumentReassignmentError {
    pub prev_cli_arg_idx: Option<CliArgIdx>,
    pub cli_arg_idx: Option<CliArgIdx>,
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
    pub const fn new(value: T, cli_arg_idx: Option<CliArgIdx>) -> Self {
        Self {
            value: Some(value),
            cli_arg_idx,
        }
    }
    pub const fn new_v(value: T) -> Self {
        Self {
            value: Some(value),
            cli_arg_idx: None,
        }
    }
    pub fn set(
        &mut self,
        value: T,
        cli_arg_idx: Option<CliArgIdx>,
    ) -> Result<(), ArgumentReassignmentError> {
        if self.value.is_some() {
            return Err(ArgumentReassignmentError {
                cli_arg_idx,
                prev_cli_arg_idx: self.cli_arg_idx,
            });
        }
        self.value = Some(value);
        self.cli_arg_idx = cli_arg_idx;
        Ok(())
    }
    pub fn force_set(&mut self, value: T, cli_arg_idx: Option<CliArgIdx>) {
        self.value = Some(value);
        self.cli_arg_idx = cli_arg_idx;
    }
    pub fn get(&self) -> Option<T> {
        self.value.clone()
    }
    pub fn unwrap(&self) -> T {
        self.value.as_ref().unwrap().clone()
    }
}
