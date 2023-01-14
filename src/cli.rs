use crate::options::ContextOptions;
use std::{error::Error, ffi::OsString};

pub fn parse_cli_from_env() -> ContextOptions {
    let args: Vec<_> = std::env::args_os().collect();
    parse_cli(&args)
}

pub fn parse_cli(args: &[OsString]) -> ContextOptions {
    Default::default()
}
