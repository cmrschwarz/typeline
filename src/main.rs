use scr::{
    cli::{collect_env_args, parse_cli},
    scr_error::ScrError,
};
use std::process::ExitCode;

fn run() -> Result<(), String> {
    let args = collect_env_args()
        .map_err(|e| ScrError::from(e).contextualize_message(None, None, None))?;

    let ctx_opts = match parse_cli(args) {
        Err((_args, ScrError::PrintInfoAndExitError(e))) => {
            println!("{e}");
            return Ok(());
        }
        Err((args, e)) => {
            return Err(ScrError::from(e)
                .contextualize_message(Some(&args), None, None)
                .into())
        }
        Ok(opts) => opts,
    };
    let mut ctx = ctx_opts
        .build_context()
        .map_err(|(opts, e)| ScrError::from(e).contextualize_message(None, Some(&opts), None))?;
    ctx.perform_jobs().map_err(|e| {
        e.contextualize_message(ctx.curr_session_data.cli_args.as_ref(), None, Some(&ctx))
    })?;
    ctx.terminate();
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[ERROR]: {err}");
            ExitCode::FAILURE
        }
    }
}
