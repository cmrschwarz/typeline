use crate::{
    chain::BufferingMode,
    extension::ExtensionRegistry,
    operators::{
        aggregator::{
            create_op_aggregate, create_op_aggregator_append_leader,
        },
        call::parse_op_call,
        call_concurrent::parse_op_call_concurrent,
        count::parse_op_count,
        end::parse_op_end,
        errors::OperatorCreationError,
        file_reader::{create_op_stdin, parse_op_file_reader, parse_op_stdin},
        foreach::parse_op_foreach,
        fork::parse_op_fork,
        forkcat::parse_op_forkcat,
        format::parse_op_format,
        join::parse_op_join,
        key::parse_op_key,
        literal::{
            parse_op_bytes, parse_op_error, parse_op_int,
            parse_op_literal_zst, parse_op_str, parse_op_tyson,
            parse_op_tyson_value, Literal,
        },
        next::parse_op_next,
        nop::parse_op_nop,
        operator::{OperatorData, OperatorId},
        print::{create_op_print_with_opts, parse_op_print, PrintOptions},
        regex::parse_op_regex,
        select::parse_op_select,
        sequence::{parse_op_seq, OpSequenceMode},
        success_updater::create_op_success_updator,
        to_str::parse_op_to_str,
        utils::writable::WritableTarget,
    },
    options::{
        argument::CliArgIdx, operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions,
    },
    record_data::field_value::FieldValueKind,
    scr_error::{ContextualizedScrError, ReplDisabledError, ScrError},
    utils::{index_vec::IndexSlice, indexing_type::IndexingType},
};
pub mod call_expr;
use bstr::ByteSlice;

use call_expr::{OperatorCallExpr, Span};
use once_cell::sync::Lazy;
use ref_cast::RefCast;

use std::{
    borrow::Cow,
    fmt::Display,
    path::PathBuf,
    str::{from_utf8, FromStr},
    sync::Arc,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Default)]
pub struct CliOptions {
    pub allow_repl: bool,
    // useful if this comes from the cli, not the repl,
    // in which case this is the executable name
    pub skip_first_arg: bool,
    pub start_with_stdin: bool,
    pub print_output: bool,
    pub add_success_updator: bool,
}

#[must_use]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("{message}")] // TODO: display span aswell
pub struct CliArgumentError {
    pub message: Cow<'static, str>,
    pub span: Span,
}

struct CliArgument<'a> {
    value: &'a [u8],
    span: Span,
}

impl CliArgumentError {
    pub fn new(message: &'static str, span: Span) -> Self {
        Self {
            message: Cow::Borrowed(message),
            span,
        }
    }
    pub fn new_s(message: String, span: Span) -> Self {
        Self {
            message: Cow::Owned(message),
            span,
        }
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PrintInfoAndExitError {
    Help(Cow<'static, str>),
    Version,
}

impl PrintInfoAndExitError {
    pub fn get_message(&self) -> String {
        format!("{self}")
    }
}

impl Display for PrintInfoAndExitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrintInfoAndExitError::Help(help_text) => f.write_str(help_text),
            PrintInfoAndExitError::Version => print_version(f),
        }
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub struct MissingArgumentsError;

impl Display for MissingArgumentsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "missing arguments, consider supplying --help")
    }
}

static TRUTHY_REGEX: Lazy<regex::bytes::Regex> = Lazy::new(|| {
    regex::bytes::RegexBuilder::new("^true|tru|tr|t|yes|ye|y|1$")
        .case_insensitive(true)
        .build()
        .unwrap()
});
static FALSY_REGEX: Lazy<regex::bytes::Regex> = Lazy::new(|| {
    regex::bytes::RegexBuilder::new("^false|fal|fa|f|no|n|0$")
        .case_insensitive(true)
        .build()
        .unwrap()
});
static LABEL_REGEX: Lazy<regex::bytes::Regex> = Lazy::new(|| {
    regex::bytes::RegexBuilder::new(
        r"^(?<label>\p{XID_Start}\p{XID_Continue}*):$",
    )
    .build()
    .unwrap()
});
static CLI_ARG_REGEX: Lazy<regex::bytes::Regex> = Lazy::new(|| {
    regex::bytes::RegexBuilder::new(
        r"^(?<modes>(?:(?<append_mode>\+)|(?<transparent_mode>_))*)(?<argname>[^@=]+)(@(?<label>[^@=]+))?(=(?<value>(?:.|[\r\n])*))?$"
    ).build()
    .unwrap()
});

fn try_parse_bool(val: &[u8]) -> Option<bool> {
    if TRUTHY_REGEX.is_match(val) {
        return Some(true);
    }
    if FALSY_REGEX.is_match(val) {
        return Some(false);
    }
    None
}

fn print_version(f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    write!(f, "scr {VERSION}")?;
    Ok(())
}

fn try_parse_as_context_opt(
    ctx_opts: &mut SessionOptions,
    expr: &OperatorCallExpr,
) -> Result<bool, ScrError> {
    const MAIN_HELP_PAGE: &str = include_str!("help_sections/main.txt");
    let mut matched = false;
    if ["--version", "-v"].contains(&expr.op_name) {
        return Err(PrintInfoAndExitError::Version.into());
    }
    if ["--help", "-h", "help", "h"].contains(&expr.op_name) {
        let text = if let Some(value) = expr.require_at_most_one_param()? {
            let section = String::from_utf8_lossy(value);
            match section.trim().to_lowercase().as_ref() {
                "cast" => include_str!("help_sections/cast.txt"),
                "format" | "f" => include_str!("help_sections/format.txt"),
                "help" | "h" => include_str!("help_sections/help.txt"),
                "join" | "j" => include_str!("help_sections/join.txt"),
                "main" => MAIN_HELP_PAGE,
                "print" | "p" => include_str!("help_sections/print.txt"),
                "regex" | "r" => include_str!("help_sections/regex.txt"),
                "types" | "int" | "str" | "~str" | "bytes" | "~bytes"
                | "error" | "~error" | "null" | "undefined" | "array"
                | "object" | "integer" | "float" | "rational" => {
                    include_str!("help_sections/types.txt")
                }
                _ => {
                    return Err(OperatorCreationError {
                        message: format!("no help section for '{section}'")
                            .into(),
                        span: expr.span,
                    }
                    .into())
                }
            }
        } else {
            MAIN_HELP_PAGE
        };

        return Err(PrintInfoAndExitError::Help(text.into()).into());
    }
    if expr.op_name == "tc" {
        ctx_opts
            .max_threads
            .set(expr.require_single_usize_param()?, expr.span)?;
        matched = true;
    }
    if expr.op_name == "repl" {
        let enabled = expr.require_single_bool_arg_or_default(true)?;
        if !ctx_opts.allow_repl && enabled {
            return Err(ReplDisabledError {
                message: "REPL mode is not allowed",
                span: expr.span,
            }
            .into());
        }
        ctx_opts.repl.set(enabled, expr.span)?;
        matched = true;
    }
    if expr.op_name == "exit" {
        if !ctx_opts.allow_repl {
            return Err(ReplDisabledError {
                message: "exit cannot be requested outside of repl mode",
                span: expr.span,
            }
            .into());
        }
        let enabled = expr.require_single_bool_arg_or_default(true)?;
        ctx_opts.exit_repl.set(enabled, expr.span)?;
        matched = true;
    }
    if matched && expr.label.is_some() {
        return Err(CliArgumentError::new(
            "cannot specify label for global argument",
            expr.span,
        )
        .into());
    }
    Ok(matched)
}

fn try_parse_as_chain_opt(
    ctx_opts: &mut SessionOptions,
    arg: &OperatorCallExpr,
) -> Result<bool, ScrError> {
    let chain = &mut ctx_opts.chains[ctx_opts.curr_chain];
    match arg.op_name {
        "debug_log" => {
            let val = arg.require_single_param()?;
            match val.to_str().ok().and_then(|v| PathBuf::from_str(v).ok()) {
                Some(path) => {
                    ctx_opts.debug_log_path.set(path, arg.span)?;
                }
                None => {
                    return Err(CliArgumentError::new(
                        "invalid path for debug log",
                        arg.span,
                    )
                    .into());
                }
            }
        }
        "denc" => {
            let _val = arg.require_single_string_param()?;
            todo!("parse text encoding");
        }
        "ppenc" => {
            let ppte = arg.require_single_bool_arg_or_default(true)?;
            chain.prefer_parent_text_encoding.set(ppte, arg.span)?;
        }
        "fenc" => {
            let fte = arg.require_single_bool_arg_or_default(true)?;
            chain.force_text_encoding.set(fte, arg.span)?;
        }
        "fpm" => {
            let fpm = arg.require_single_bool_arg_or_default(true)?;
            chain.floating_point_math.set(fpm, arg.span)?;
        }
        "prr" => {
            let prr = arg.require_single_bool_arg_or_default(true)?;
            chain.print_rationals_raw.set(prr, arg.span)?;
        }
        "bs" => {
            let bs = arg.require_single_usize_param()?;
            chain.default_batch_size.set(bs, arg.span)?;
        }
        "sbs" => {
            let bs = arg.require_single_usize_param()?;
            chain.stream_buffer_size.set(bs, arg.span)?;
        }
        "sst" => {
            let bs = arg.require_single_usize_param()?;
            chain.stream_size_threshold.set(bs, arg.span)?;
        }
        "lb" => {
            let mut buffering_mode = BufferingMode::LineBuffer;
            if !arg.args.is_empty() {
                let val = arg.require_single_string_param()?;
                buffering_mode = if let Some(v) =
                    try_parse_bool(val.as_bytes())
                {
                    if v {
                        BufferingMode::LineBuffer
                    } else {
                        BufferingMode::BlockBuffer
                    }
                } else {
                    let res = match val {
                        "stdin" => Some(BufferingMode::LineBufferStdin),
                        "tty" => Some(BufferingMode::LineBufferIfTTY),
                        "stdin-if-tty" => {
                            Some(BufferingMode::LineBufferStdinIfTTY)
                        }
                        _ => None,
                    };
                    if let Some(bm) = res {
                        bm
                    } else {
                        return Err(CliArgumentError{
                            message: Cow::Owned(
                                format!(
                                    "unknown line buffering mode '{val}', options are yes, no, stdin, tty, and stdin-if-tty"
                                )
                            ),
                            span: arg.span
                        }.into());
                    }
                }
            }
            chain.buffering_mode.set(buffering_mode, arg.span)?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn try_parse_operator_data(
    ctx_opts: &mut SessionOptions,
    expr: &OperatorCallExpr,
) -> Result<Option<OperatorData>, OperatorCreationError> {
    Ok(Some(match expr.op_name {
        "int" => parse_op_int(expr)?,
        "bytes" => parse_op_bytes(expr, false)?,
        "~bytes" => parse_op_bytes(expr, true)?,
        "str" => parse_op_str(expr, false)?,
        "~str" => parse_op_str(expr, true)?,
        "object" => parse_op_tyson(expr, FieldValueKind::Object, ctx_opts)?,
        "array" => parse_op_tyson(expr, FieldValueKind::Array, ctx_opts)?,
        "float" => parse_op_tyson(expr, FieldValueKind::Float, ctx_opts)?,
        "rational" => {
            parse_op_tyson(expr, FieldValueKind::Rational, ctx_opts)?
        }
        "v" | "value" | "tyson" => parse_op_tyson_value(expr, Some(ctx_opts))?,
        "error" => parse_op_error(expr, false)?,
        "~error" => parse_op_error(expr, true)?,
        "null" => parse_op_literal_zst(expr, Literal::Null)?,
        "undefined" => parse_op_literal_zst(expr, Literal::Undefined)?,
        "to_str" => parse_op_to_str(expr)?,
        "join" | "j" => parse_op_join(expr)?,
        "r" | "regex" => parse_op_regex(expr)?,
        "print" | "p" => parse_op_print(expr)?,
        "format" | "f" => parse_op_format(expr)?,
        "file" => parse_op_file_reader(expr)?,
        "stdin" | "in" => parse_op_stdin(expr)?,
        "key" => parse_op_key(expr)?,
        "select" => parse_op_select(expr)?,
        "seq" => parse_op_seq(expr, OpSequenceMode::Sequence, false)?,
        "seqn" => parse_op_seq(expr, OpSequenceMode::Sequence, true)?,
        "enum" => parse_op_seq(expr, OpSequenceMode::Enum, false)?,
        "enumn" => parse_op_seq(expr, OpSequenceMode::Enum, true)?,
        "enum-u" => parse_op_seq(expr, OpSequenceMode::EnumUnbounded, false)?,
        "enumn-u" => parse_op_seq(expr, OpSequenceMode::EnumUnbounded, true)?,
        "count" => parse_op_count(&expr)?,
        "nop" | "scr" => parse_op_nop(&expr)?,
        "fork" => parse_op_fork(&expr)?,
        "foreach" | "fe" => parse_op_foreach(expr)?,
        "forkcat" | "fc" => parse_op_forkcat(expr)?,
        "call" | "c" => parse_op_call(expr)?,
        "callcc" | "cc" => parse_op_call_concurrent(expr)?,
        "next" | "n" => parse_op_next(expr)?,
        "end" | "e" => parse_op_end(expr)?,
        _ => {
            for e in &ctx_opts.extensions.extensions {
                if let Some(op) = e.try_match_cli_argument(ctx_opts, expr)? {
                    return Ok(Some(op));
                }
            }
            return Ok(None);
        }
    }))
}
pub fn add_op_from_arg_and_op_data_uninit(
    ctx_opts: &mut SessionOptions,
    arg: &OperatorCallExpr,
    op_data: OperatorData,
) -> OperatorId {
    let argname = ctx_opts.string_store.intern_cloned(arg.op_name);
    let label = arg.label.map(|l| ctx_opts.string_store.intern_cloned(l));
    ctx_opts.add_op_uninit(
        OperatorBaseOptions::new(
            argname,
            label,
            arg.transparent_mode,
            arg.span,
        ),
        op_data,
    )
}
pub fn parse_cli_argument_parts(
    args: &[Vec<u8>],
    arg_idx: &mut usize,
) -> Result<OperatorCallExpr, CliArgumentError> {
    let arg_str = &args[*arg_idx];
    let span = Span::CliArg {
        start: arg_idx,
        end: CliArgIdx::from_usize(*arg_idx + 1),
        offset_start: 0,
        offset_end: arg_str.len() as u16,
    };

    let Some(arg_match) = CLI_ARG_REGEX.captures(&arg_str) else {
        return Err(CliArgumentError::new("invalid argument syntax", span));
    };
    let argname = from_utf8(arg_match.name("argname").unwrap().as_bytes())
        .map_err(|_| {
            CliArgumentError::new("argument name must be valid UTF-8", span)
        })?;
    if let Some(modes) = arg_match.name("modes") {
        let modes_str = modes.as_bytes();
        if modes_str.len() >= 2
            || (modes_str.len() == 2 && modes_str[0] == modes_str[1])
        {
            return Err(CliArgumentError::new(
                "operator modes cannot be specified twice",
                span,
            ));
        }
    }
    let label = if let Some(lbl) = arg_match.name("label") {
        Some(from_utf8(lbl.as_bytes()).map_err(|_| {
            CliArgumentError::new("label must be valid UTF-8", span)
        })?)
    } else {
        None
    };
    Ok(OperatorCallExpr {
        op_name: argname,
        args: arg_match.name("value").map(|v| v.as_bytes()),
        label,
        span,
        append_mode: arg_match.name("append_mode").is_some(),
        transparent_mode: arg_match.name("transparent_mode").is_some(),
    })
}

pub fn try_parse_label(ctx_opts: &mut SessionOptions, arg_str: &[u8]) -> bool {
    if let Some(m) = LABEL_REGEX.captures(arg_str) {
        ctx_opts.add_label(
            m.name("label")
                .unwrap()
                .as_bytes()
                .to_str()
                .unwrap() // we know this is valid utf-8 because of the regex match
                .to_owned(),
        );
        return true;
    }
    false
}

pub fn parse_cli_retain_args(
    args: &[Vec<u8>],
    cli_opts: CliOptions,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, ScrError> {
    assert!(
        !cli_opts.allow_repl || cfg!(feature = "repl"),
        "the 'repl' feature of this crate is disabled"
    );

    // primarily for skipping the executable name
    let mut arg_idx = usize::from(cli_opts.skip_first_arg);

    if args.len() <= arg_idx {
        return Err(MissingArgumentsError.into());
    }
    let mut ctx_opts = SessionOptions::with_extensions(extensions);
    ctx_opts.allow_repl = cli_opts.allow_repl;

    let mut curr_aggregate = Vec::new();
    let mut last_non_append_op_id = None;
    let mut curr_op_appendable = true;
    if cli_opts.start_with_stdin {
        let op_base_opts = OperatorBaseOptions::new(
            ctx_opts.string_store.intern_cloned("stdin"),
            None,
            false,
            Span::Generated,
        );
        let op_data = create_op_stdin(1);
        last_non_append_op_id = Some(ctx_opts.add_op(op_base_opts, op_data));
    }
    while arg_idx < args.len() {
        let arg_str = &args[arg_idx];

        arg_idx += 1;

        if try_parse_label(&mut ctx_opts, arg_str) {
            continue;
        }

        let arg = parse_cli_argument_parts(args, &mut arg_idx)?;
        if let Some(op_data) = try_parse_operator_data(&mut ctx_opts, &arg)? {
            let prev_op_appendable = curr_op_appendable;
            curr_op_appendable = op_data.can_be_appended();
            let op_id = add_op_from_arg_and_op_data_uninit(
                &mut ctx_opts,
                &arg,
                op_data,
            );
            if !arg.append_mode || !prev_op_appendable {
                if !curr_aggregate.is_empty() {
                    let op_data = create_op_aggregate(std::mem::take(
                        &mut curr_aggregate,
                    ));
                    let op_base = OperatorBaseOptions::from_name(
                        ctx_opts
                            .string_store
                            .intern_cloned(&op_data.default_op_name()),
                    );
                    ctx_opts.add_op(op_base, op_data);
                }
                if let Some(pred) = last_non_append_op_id {
                    ctx_opts.init_op(pred, true);
                }
                if arg.append_mode {
                    let (op_base_opts, op_data) =
                        create_op_aggregator_append_leader(&mut ctx_opts);
                    curr_aggregate
                        .push(ctx_opts.add_op_uninit(op_base_opts, op_data));
                    curr_aggregate.push(op_id);
                    last_non_append_op_id = None;
                    continue;
                }
                last_non_append_op_id = Some(op_id);
                continue;
            }
            if curr_aggregate.is_empty() {
                if let Some(pred) = last_non_append_op_id {
                    curr_aggregate.push(pred);
                    last_non_append_op_id = None;
                } else {
                    let (op_base_opts, op_data) =
                        create_op_aggregator_append_leader(&mut ctx_opts);
                    curr_aggregate
                        .push(ctx_opts.add_op_uninit(op_base_opts, op_data));
                }
            }
            curr_aggregate.push(op_id);
            continue;
        }

        if try_parse_as_context_opt(&mut ctx_opts, &arg)? {
            continue;
        }
        if try_parse_as_chain_opt(&mut ctx_opts, &arg)? {
            continue;
        }
        return Err(CliArgumentError {
            message: format!("unknown operator '{}'", arg.op_name).into(),
            span: arg.span,
        }
        .into());
    }
    if !curr_aggregate.is_empty() {
        let op_data = create_op_aggregate(std::mem::take(&mut curr_aggregate));
        let op_base = OperatorBaseOptions::from_name(
            ctx_opts
                .string_store
                .intern_cloned(&op_data.default_op_name()),
        );
        ctx_opts.add_op(op_base, op_data);
    }
    if let Some(pred) = last_non_append_op_id {
        ctx_opts.init_op(pred, true);
    }
    if cli_opts.print_output {
        let op_data = create_op_print_with_opts(
            WritableTarget::Stdout,
            PrintOptions {
                ignore_nulls: true,
                propagate_errors: true,
            },
        );
        let op_base_opts = OperatorBaseOptions::new(
            ctx_opts
                .string_store
                .intern_cloned(&op_data.default_op_name()),
            None,
            false,
            Span::Generated,
        );
        ctx_opts.add_op(op_base_opts, op_data);
    }
    if cli_opts.add_success_updator {
        let op_data = create_op_success_updator();
        let op_base_opts = OperatorBaseOptions::new(
            ctx_opts
                .string_store
                .intern_cloned(&op_data.default_op_name()),
            None,
            false,
            Span::Generated,
        );
        ctx_opts.add_op(op_base_opts, op_data);
    }
    Ok(ctx_opts)
}

pub fn parse_cli_raw(
    args: Vec<Vec<u8>>,
    cli_opts: CliOptions,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, (Vec<Vec<u8>>, ScrError)> {
    match parse_cli_retain_args(&args, cli_opts, extensions) {
        Ok(mut ctx) => {
            ctx.cli_args = Some(args.into());
            Ok(ctx)
        }
        Err(e) => Err((args, e)),
    }
}

pub fn parse_cli(
    args: Vec<Vec<u8>>,
    cli_opts: CliOptions,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, ContextualizedScrError> {
    parse_cli_raw(args, cli_opts, extensions).map_err(|(args, err)| {
        ContextualizedScrError::from_scr_error(
            err,
            Some(IndexSlice::ref_cast(&args)),
            Some(&cli_opts),
            None,
            None,
        )
    })
}

pub fn collect_env_args() -> Result<Vec<Vec<u8>>, CliArgumentError> {
    #[cfg(unix)]
    {
        Ok(std::env::args_os()
            .map(std::os::unix::prelude::OsStringExt::into_vec)
            .collect::<Vec<Vec<u8>>>())
    }
    #[cfg(windows)]
    {
        let mut args = Vec::new();
        for (i, arg) in std::env::args_os().enumerate() {
            if let Some(arg) = arg.to_str() {
                args.push(Vec::<u8>::from(arg));
            } else {
                return Err(CliArgumentError::new(
                    "failed to parse byte sequence as unicode",
                    i as CliArgIdx,
                ));
            }
        }
        Ok(args)
    }
}

pub fn parse_cli_from_env(
    cli_opts: CliOptions,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, ContextualizedScrError> {
    let args = collect_env_args().map_err(|e| {
        ContextualizedScrError::from_scr_error(
            e.into(),
            None,
            Some(&cli_opts),
            None,
            None,
        )
    })?;
    parse_cli(args, cli_opts, extensions)
}
