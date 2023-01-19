use std::process::ExitCode;
use scr::{cli::parse_cli_from_env, scr_error::ScrError};


#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args_os().len() < 2 {
        eprintln!("[ERROR]: missing arguments, consider supplying --help");
        return ExitCode::FAILURE;
    }
    let res = parse_cli_from_env().map_err(ScrError::from)
        .and_then(|ctx_opts|ctx_opts.build_context().map_err(ScrError::from))
        .and_then(|mut ctx|ctx.run());
    match res {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[ERROR]: {}", err);
            ExitCode::FAILURE
        }
    }
}
