use ref_cast::RefCast;
use std::{
    fs::File,
    io::{Read, Write},
    process::ExitCode,
    sync::Arc,
};
use typeline::{
    self,
    cli::{
        collect_env_args, parse_cli_args_from_bytes, parse_cli_args_from_vec,
    },
    context::Context,
    operators::field_value_sink::FieldValueSinkHandle,
    options::session_setup::{SessionSetupData, SessionSetupOptions},
    record_data::{
        formattable::{Formattable, FormattingContext},
        record_set::RecordSet,
    },
    typeline_error::TypelineError,
    utils::{index_slice::IndexSlice, text_write::TextWriteIoAdapter},
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

    let mut cli_data =
        parse_cli_args_from_vec(&args, setup_opts.skip_first_cli_arg)
            .map_err(|e| {
                e.contextualize_message(
                    Some(IndexSlice::ref_cast(&*args)),
                    Some(&setup_opts),
                    None,
                    None,
                )
            })?;

    if cli_data.repl {
        setup_opts.output_storage = Some(FieldValueSinkHandle::new_rle());
    }
    if cli_data.debug_ast {
        let mut fc = FormattingContext::default();
        let mut out = std::io::stderr().lock();
        _ = out.write(b"[");
        for (i, arg) in cli_data.args.iter().enumerate() {
            if i > 0 {
                _ = out.write(b", ");
            }
            _ = arg.format(&mut fc, &mut TextWriteIoAdapter(&mut out));
        }
        _ = out.write(b"]\n");
    }

    let mut sess = SessionSetupData::new(setup_opts.clone());

    if cli_data.repl {
        sess.setup_settings.repl = Some(true);
    }

    insert_tlrc(&setup_opts, &mut cli_data)?;

    match sess.process_arguments(cli_data.args) {
        Ok(()) => (),
        Err(e) => match e {
            TypelineError::MissingArgumentsError(_) if repl => {
                if sess.setup_settings.repl.is_none() {
                    sess.setup_settings.repl = Some(true);
                    setup_opts.output_storage =
                        Some(FieldValueSinkHandle::new_rle());
                    // make sure this is in setup_opts and the sess setup data
                    // TODO: refactor this mess
                    sess.setup_settings.output_storage =
                        setup_opts.output_storage.clone();
                }
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

fn insert_tlrc(
    setup_opts: &SessionSetupOptions,
    cli_data: &mut typeline::cli::CliArgumentData,
) -> Result<(), String> {
    let mut buf = Vec::new();
    let rc_source = if let Some(rc_path) = std::env::var_os("TYPELINE_RC") {
        let mut f = File::open(rc_path)
            .map_err(|e| format!("Error opening rc file: {e}"))?;

        f.read_to_end(&mut buf)
            .map_err(|e| format!("Error reading rc file: {e}"))?;
        &*buf
    } else {
        include_bytes!("./tlrc")
    };
    let mut data = parse_cli_args_from_bytes(rc_source).map_err(|e| {
        e.contextualize_message(None, Some(setup_opts), None, None)
    })?;
    data.args.extend(std::mem::take(&mut cli_data.args));
    cli_data.args = data.args;
    Ok(())
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
