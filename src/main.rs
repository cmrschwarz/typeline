use scr::{
    cli::{collect_env_args, parse_cli},
    scr_error::ScrError,
};
use std::{process::ExitCode, borrow::Cow};

fn run() -> Result<(), Cow<'static, str>>{
    if std::env::args_os().len() < 2 {
        return Err(Cow::Borrowed("missing arguments, consider supplying --help"));
    }
    let args = collect_env_args().map_err(|e|ScrError::from(e).contextualize_message(None, None, None))?;
    let ctx_opts = parse_cli(&args).map_err(|e|ScrError::from(e).contextualize_message(Some(&args), None, None))?;
    let mut ctx = ctx_opts.build_context().map_err(|e|ScrError::from(e.1).contextualize_message(Some(&args), Some(&e.0), None))?;
    ctx.perform_jobs().map_err(|e|e.contextualize_message(Some(&args), None, Some(&ctx)))?;
    ctx.terminate();
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[ERROR]: {err}");
            ExitCode::FAILURE
        }
    }
}
