use scr::{
    cli::collect_env_args,
    context::{Context, Session},
    field_data::record_set::RecordSet,
    scr_error::ScrError,
};
use std::{process::ExitCode, sync::Arc};

fn run() -> Result<(), String> {
    let args = collect_env_args()
        .map_err(|e| ScrError::from(e).contextualize_message(None, None, None))?;

    let sess = match Session::from_cli_args_stringify_error(args) {
        Ok(sess) => sess,
        Err(e) => {
            if e.message_is_info_text {
                println!("{}", e.message);
                return Ok(());
            }
            return Err(e.message);
        }
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
