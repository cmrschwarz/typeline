use scr::{
    cli::{collect_env_args, parse_cli},
    field_data::record_set::RecordSet,
    scr_error::ScrError,
};
use std::process::ExitCode;

fn run() -> Result<(), String> {
    let args = collect_env_args()
        .map_err(|e| ScrError::from(e).contextualize_message(None, None, None))?;

    let sess_opts = match parse_cli(args) {
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
    let sess = sess_opts
        .build_session()
        .map_err(|(opts, e)| ScrError::from(e).contextualize_message(None, Some(&opts), None))?;

    let job = sess.construct_main_chain_job(RecordSet::default());
    sess.run(job);
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
