use std::{error::Error, ffi::OsString, fmt, ops::Deref};

use crate::xstr::XString;

#[derive(Clone, Debug)]
pub struct CliArgument {
    pub arg_index: u32,
    pub arg_str: XString,
}

#[derive(Clone)]
pub struct Argument<T: Clone> {
    pub value: Option<T>,
    pub cli_arg: Option<CliArgument>,
}

#[derive(Debug)]
pub struct ArgumentReassignmentError {
    pub message: &'static str,
    pub cli_arg: Option<CliArgument>,
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
            cli_arg: None,
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
            cli_arg: None,
        }
    }
    pub const fn new_with_arg(t: T, cli_arg: CliArgument) -> Self {
        Self {
            value: Some(t),
            cli_arg: Some(cli_arg),
        }
    }
    pub fn set(&mut self, value: T) -> Result<(), ArgumentReassignmentError> {
        if self.value.is_some() {
            return Err(ArgumentReassignmentError {
                message: "attempted to reassign value of option",
                cli_arg: self.cli_arg.clone(),
            });
        }
        self.value = Some(value);
        Ok(())
    }
}
