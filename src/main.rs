use scr::{
    build_extension_registry,
    cli::{collect_env_args, parse_cli, CliOptions},
    context::Context,
    options::session_options::SessionOptions,
    record_data::record_set::RecordSet,
    scr_error::ScrError,
};
use std::{process::ExitCode, sync::Arc};

fn run() -> Result<bool, String> {
    let repl = cfg!(feature = "repl");
    let cli_opts = CliOptions {
        allow_repl: repl,
        start_with_stdin: true,
        skip_first_arg: true,
        print_output: true,
        add_success_updator: true,
    };

    let args = collect_env_args().map_err(|e| {
        ScrError::from(e).contextualize_message(
            None,
            Some(&cli_opts),
            None,
            None,
        )
    })?;
    let extensions = build_extension_registry();

    let sess = match parse_cli(args, cli_opts, extensions)
        .and_then(|sess_opts| sess_opts.build_session())
    {
        Ok(sess) => sess,
        Err(e) => match e.err {
            ScrError::MissingArgumentsError(_) if repl => {
                let mut sess_opts = SessionOptions::default();
                sess_opts.repl.set(true, None).unwrap();
                sess_opts.build_session().unwrap()
            }
            ScrError::PrintInfoAndExitError(_) => {
                println!("{}", e.contextualized_message);
                return Ok(true);
            }
            _ => return Err(e.contextualized_message),
        },
    };

    #[cfg(feature = "repl")]
    if sess.repl_requested() {
        let sess = Arc::new(sess);
        Context::new(sess.clone()).run_repl(cli_opts);
        return Ok(sess.get_success());
    }
    let job = sess.construct_main_chain_job(RecordSet::default());
    if sess.settings.max_threads == 1 {
        sess.run_job_unthreaded(job);
        return Ok(sess.get_success());
    }
    let sess = Arc::new(sess);
    Context::new(sess.clone()).run_job(job);
    Ok(sess.get_success())
}

fn main() -> ExitCode {
    match run() {
        Ok(success) => {
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}
