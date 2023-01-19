use std::process::ExitCode;
use scr::{cli::{collect_env_args, parse_cli}, scr_error::ScrError};


#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args_os().len() < 2 {
        eprintln!("[ERROR]: missing arguments, consider supplying --help");
        return ExitCode::FAILURE;
    }
    let args = collect_env_args().map_err(ScrError::from);
    let ctx_opts = args.as_ref().map_err(|e|e.clone())
        .and_then(|args|parse_cli(&args).map_err(ScrError::from));
    let mut ctx =
        ctx_opts.clone().and_then(|ctx_opts|ctx_opts.clone().build_context().map_err(ScrError::from));
    let result = match &mut ctx {
        Ok(ctx) => ctx.run(),
        Err(err) => Err(err.clone())
    };
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!(
                "[ERROR]: {}",
                err.contextualize_message(
                    args.as_ref().ok().map(|vec|vec.as_slice()),
                    ctx_opts.as_ref().ok(),
                    ctx.as_ref().ok()
                )
            );
            ExitCode::FAILURE
        }
    }
}
