use scr::{
    cli::{collect_env_args, parse_cli},
    context::Context,
    field_data::record_set::RecordSet,
    options::session_options::SessionOptions,
    scr_error::ScrError,
};
use std::{process::ExitCode, sync::Arc};

fn run() -> Result<(), String> {
    let args = collect_env_args().map_err(|e| {
        ScrError::from(e).contextualize_message(None, None, None)
    })?;

    let sess = match parse_cli(args, true)
        .and_then(|sess_opts| sess_opts.build_session())
    {
        Ok(sess) => sess,
        Err(e) => match e.err {
            ScrError::MissingArgumentsError(_) => {
                let mut sess_opts = SessionOptions::default();
                sess_opts.repl.set(true, None).unwrap();
                sess_opts.build_session().unwrap()
            }
            ScrError::PrintInfoAndExitError(_) => {
                println!("{}", e.contextualized_message);
                return Ok(());
            }
            _ => return Err(e.contextualized_message),
        },
    };

    if sess.repl_requested() {
        Context::new(Arc::new(sess)).run_repl();
    } else {
        let job = sess.construct_main_chain_job(RecordSet::default());
        sess.run(job);
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}
