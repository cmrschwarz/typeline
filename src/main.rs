
#![allow(dead_code, unused)] // HACK
extern crate smallvec;
extern crate markup5ever_rcdom;
extern crate crossbeam;

mod transform;
mod document;
mod context;
mod chain;
mod operations;
mod selenium;
mod encoding;
mod options;
mod cli;

use cli::parse_cli_from_env;

use crate::options::ContextOptions;


#[tokio::main]
async fn main() -> Result<(), String> {
    let ctx_opts = parse_cli_from_env();
    let mut ctx = ctx_opts.build_context();
    ctx.run();
    Ok(())
}
