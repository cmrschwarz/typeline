#![allow(dead_code, unused)] // HACK
extern crate crossbeam;
extern crate markup5ever_rcdom;
extern crate smallvec;
extern crate bstring;
#[macro_use()]
extern crate lazy_static;

mod argument;
mod chain;
mod cli;
mod context;
mod document;
mod encoding;
mod operations;
mod options;
mod selenium;
mod transform;

use std::{process::ExitCode, io::Write, os::unix::prelude::OsStrExt};

use cli::parse_cli_from_env;

use crate::options::ContextOptions;

#[tokio::main]
async fn main() -> ExitCode {
    let mut stderr = std::io::stderr();
    if std::env::args_os().len() < 2 {
        eprintln!("[ERROR]: missing arguments, consider supplying --help");
        return ExitCode::FAILURE;
    }
    let ctx_opts = parse_cli_from_env();
    match ctx_opts {
        Err(err) => {
            eprintln!("[ERROR]: {}", err);
            ExitCode::FAILURE
        }
        Ok(ctx_opts) => {
            let mut ctx = ctx_opts.build_context();
            ctx.run();
            ExitCode::SUCCESS
        }
    }
}
