use ref_cast::RefCast;
use scr::{
    cli::{call_expr::Span, collect_env_args, parse_cli_args_form_vec},
    context::Context,
    options::session_setup::{ScrSetupOptions, SessionSetupData},
    record_data::record_set::RecordSet,
    scr_error::ScrError,
    utils::index_vec::IndexSlice,
    DEFAULT_EXTENSION_REGISTRY,
};
use std::{process::ExitCode, sync::Arc};

fn run() -> Result<bool, String> {
    let repl = cfg!(feature = "repl");

    let cli_opts = ScrSetupOptions {
        allow_repl: repl,
        skip_first_cli_arg: true,
        print_output: true,
        add_success_updator: true,
        deny_threading: false,
        start_with_stdin: true,
        extensions: DEFAULT_EXTENSION_REGISTRY.clone(),
    };

    let args = collect_env_args().map_err(|e| {
        ScrError::from(e).contextualize_message(
            None,
            Some(&cli_opts),
            None,
            None,
        )
    })?;

    let arguments =
        parse_cli_args_form_vec(&args, cli_opts.skip_first_cli_arg).map_err(
            |e| {
                e.contextualize_message(
                    Some(IndexSlice::ref_cast(&*args)),
                    Some(&cli_opts),
                    None,
                    None,
                )
            },
        )?;

    let mut sess = SessionSetupData::new(cli_opts.clone());

    match sess.process_arguments(arguments) {
        Ok(()) => (),
        Err(e) => match e {
            ScrError::MissingArgumentsError(_) if repl => {
                sess.setup_settings.repl.set(true, Span::Builtin).unwrap();
            }
            ScrError::PrintInfoAndExitError(_) => {
                println!(
                    "{}",
                    sess.contextualize_error(e).contextualized_message
                );
                return Ok(true);
            }
            _ => {
                return Err(sess.contextualize_error(e).contextualized_message)
            }
        },
    };

    let sess = sess
        .build_session_take()
        .map_err(|e| sess.contextualize_error(e).contextualized_message)?;

    #[cfg(feature = "repl")]
    if sess.settings.repl {
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
