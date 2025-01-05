use ref_cast::RefCast;
use std::{process::ExitCode, sync::Arc};
use typeline::{
    cli::{collect_env_args, parse_cli_args_form_vec},
    context::Context,
    operators::field_value_sink::FieldValueSinkHandle,
    options::session_setup::{SessionSetupData, SessionSetupOptions},
    record_data::record_set::RecordSet,
    typeline_error::TypelineError,
    utils::index_slice::IndexSlice,
    DEFAULT_EXTENSION_REGISTRY,
};

fn run() -> Result<bool, String> {
    let repl = cfg!(feature = "repl");

    let mut setup_opts = SessionSetupOptions {
        allow_repl: repl,
        output_storage: None,
        last_cli_output: None,
        skip_first_cli_arg: true,
        print_output: true,
        add_success_updator: true,
        deny_threading: false,
        start_with_stdin: true,
        extensions: DEFAULT_EXTENSION_REGISTRY.clone(),
    };

    let args = collect_env_args().map_err(|e| {
        TypelineError::from(e).contextualize_message(
            None,
            Some(&setup_opts),
            None,
            None,
        )
    })?;

    let arguments =
        parse_cli_args_form_vec(&args, setup_opts.skip_first_cli_arg)
            .map_err(|e| {
                e.contextualize_message(
                    Some(IndexSlice::ref_cast(&*args)),
                    Some(&setup_opts),
                    None,
                    None,
                )
            })?;

    let mut sess = SessionSetupData::new(setup_opts.clone());

    match sess.process_arguments(arguments) {
        Ok(()) => (),
        Err(e) => match e {
            TypelineError::MissingArgumentsError(_) if repl => {
                sess.setup_settings.repl = Some(true);
                setup_opts.output_storage =
                    Some(FieldValueSinkHandle::default());
            }
            TypelineError::PrintInfoAndExitError(_) => {
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
        Context::new(sess.clone()).run_repl(setup_opts);
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
