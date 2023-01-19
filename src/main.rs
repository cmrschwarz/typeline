#![allow(dead_code, unused)] // HACK
extern crate bstring;
extern crate crossbeam;
extern crate markup5ever_rcdom;
extern crate num;
extern crate regex;
#[macro_use()]
extern crate smallvec;
#[macro_use()]
extern crate lazy_static;

mod chain;
mod cli;
mod context;
mod document;
mod encoding;
mod operations;
mod options;
mod plattform;
mod selenium;

use std::{io::Write, os::unix::prelude::OsStrExt, process::ExitCode};

use cli::parse_cli_from_env;
use context::Context;
use operations::{print::OpPrint, OperationCloneBox, OperationOps};
use options::context_options::ContextOptions;


#[tokio::main]
async fn main() -> ExitCode {
    let mut ctx_opts = ContextOptions::default();
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
