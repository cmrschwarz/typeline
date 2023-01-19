use std::process::ExitCode;
use scr::cli::parse_cli_from_env;


#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args_os().len() < 2 {
        eprintln!("[ERROR]: missing arguments, consider supplying --help");
        return ExitCode::FAILURE;
    }
    let ctx_opts = parse_cli_from_env();
    match ctx_opts {
        Ok(ctx_opts) => {
            match ctx_opts.build_context().and_then(|mut ctx|ctx.run()) {
                Ok(_) => ExitCode::SUCCESS,
                Err(err) => {
                    eprintln!("[ERROR]: {}", err);
                    ExitCode::FAILURE
                }
            }
        }
        Err(err) => {
            eprintln!("[ERROR]: {}", err);
            ExitCode::FAILURE
        }
    }
}
