#![allow(dead_code, unused)] // HACK
extern crate crossbeam;
extern crate markup5ever_rcdom;
extern crate regex;
extern crate smallvec;
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
mod xstr;

use std::process::ExitCode;

use cli::parse_cli_from_env;

use crate::options::ContextOptions;

#[tokio::main]
async fn main() -> ExitCode {
    let ctx_opts = parse_cli_from_env();
    match ctx_opts {
        Err(err) => {
            println!("[ERROR]: {}", err);
            ExitCode::FAILURE
        }
        Ok(ctx_opts) => {
            let mut ctx = ctx_opts.build_context();
            ctx.run();
            ExitCode::SUCCESS
        }
    }
}
