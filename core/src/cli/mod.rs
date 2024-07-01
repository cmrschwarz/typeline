use crate::{
    chain::BufferingMode,
    extension::ExtensionRegistry,
    operators::{
        call::parse_op_call,
        call_concurrent::parse_op_call_concurrent,
        count::parse_op_count,
        errors::{OperatorCreationError, OperatorParsingError},
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
        nop::parse_op_nop,
        operator::OperatorData,
        print::{create_op_print_with_opts, parse_op_print, PrintOptions},
        regex::parse_op_regex,
        select::parse_op_select,
        sequence::{parse_op_seq, SequenceMode},
        success_updater::create_op_success_updator,
        to_str::parse_op_to_str,
        utils::writable::WritableTarget,
    },
    options::{
        operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions,
    },
    record_data::field_value::FieldValueKind,
    scr_error::{ContextualizedScrError, ReplDisabledError, ScrError},
    utils::{cow_to_str, index_vec::IndexSlice, slice_cow},
};
pub mod call_expr;
pub mod call_expr_iter;
use bstr::ByteSlice;

use call_expr::{
    Argument, ArgumentValue, CallExpr, CallExprEndKind, Label, Span,
};
use once_cell::sync::Lazy;
use ref_cast::RefCast;
use unicode_ident::{is_xid_continue, is_xid_start};

use std::{
    borrow::Cow, fmt::Display, iter::Peekable, path::PathBuf, str::FromStr,
    sync::Arc,
};
use thiserror::Error;

#[derive(Clone)]
pub struct CliOptions {
    pub allow_repl: bool,
    // useful if this comes from the cli, not the repl,
    // in which case this is the executable name
    pub skip_first_arg: bool,
    pub start_with_stdin: bool,
    pub print_output: bool,
    pub add_success_updator: bool,
    pub extensions: Arc<ExtensionRegistry>,
}

#[derive(Default)]
pub struct CallExprHead<'a> {
    append_mode: bool,
    transparent_mode: bool,
    op_name: Cow<'a, str>,
    label: Option<Label<'a>>,
    colon_found: bool,
    dashed_arg: Option<Argument<'a>>,
    equals_arg: Option<Argument<'a>>,
}

pub struct CallExprParseResult<'a> {
    pub head: CallExprHead<'a>,
    pub end_kind: CallExprEndKind,
    pub expr_span: Span,
}

#[must_use]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("{message}")] // TODO: display span aswell
pub struct CliArgumentError {
    pub message: Cow<'static, str>,
    pub span: Span,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PrintInfoAndExitError {
    Help(Cow<'static, str>),
    Version,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub struct MissingArgumentsError;

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

impl CliOptions {
    pub fn with_extensions(extensions: Arc<ExtensionRegistry>) -> Self {
        CliOptions {
            extensions,
            allow_repl: false,
            skip_first_arg: false,
            start_with_stdin: false,
            print_output: false,
            add_success_updator: false,
        }
    }
    pub fn without_extensions() -> Self {
        CliOptions::with_extensions(Arc::new(ExtensionRegistry::default()))
    }
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

impl Display for MissingArgumentsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "missing arguments, consider supplying --help")
    }
}

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

fn try_parse_as_special_op(expr: &CallExpr) -> Result<bool, ScrError> {
    if ["--version", "-v", "version"].contains(&&*expr.op_name) {
        expr.reject_args()?;
        return Err(PrintInfoAndExitError::Version.into());
    }
    if ["--help", "-h", "help", "h"].contains(&&*expr.op_name) {
        const MAIN_HELP_PAGE: &str = include_str!("help_sections/main.txt");
        let text = if let Some(value) = expr.require_at_most_one_arg()? {
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
    Ok(false)
}

fn try_parse_as_setting(
    ctx_opts: &mut SessionOptions,
    expr: &CallExpr,
) -> Result<bool, ScrError> {
    let chain = &mut ctx_opts.chains[ctx_opts.curr_chain];

    match &*expr.op_name {
        "exit" => {
            if !ctx_opts.allow_repl {
                return Err(ReplDisabledError {
                    message: "exit cannot be requested outside of repl mode",
                    span: expr.span,
                }
                .into());
            }
            let enabled = expr.require_at_most_one_bool_arg()?.unwrap_or(true);
            ctx_opts.exit_repl.set(enabled, expr.span)?;
        }
        "repl" => {
            let enabled = expr.require_at_most_one_bool_arg()?.unwrap_or(true);
            if !ctx_opts.allow_repl && enabled {
                return Err(ReplDisabledError {
                    message: "REPL mode is not allowed",
                    span: expr.span,
                }
                .into());
            }
            ctx_opts.repl.set(enabled, expr.span)?;
        }
        "tc" => {
            ctx_opts
                .max_threads
                .set(expr.require_single_number_param()?, expr.span)?;
        }
        "debug_log" => {
            let val = expr.require_single_arg()?;
            match val.to_str().ok().and_then(|v| PathBuf::from_str(v).ok()) {
                Some(path) => {
                    ctx_opts.debug_log_path.set(path, expr.span)?;
                }
                None => {
                    return Err(CliArgumentError::new(
                        "invalid path for debug log",
                        expr.span,
                    )
                    .into());
                }
            }
        }
        "denc" => {
            let _val = expr.require_single_string_arg()?;
            todo!("parse text encoding");
        }
        "ppenc" => {
            let ppte = expr.require_at_most_one_bool_arg()?.unwrap_or(true);
            chain.prefer_parent_text_encoding.set(ppte, expr.span)?;
        }
        "fenc" => {
            let fte = expr.require_at_most_one_bool_arg()?.unwrap_or(true);
            chain.force_text_encoding.set(fte, expr.span)?;
        }
        "fpm" => {
            let fpm = expr.require_at_most_one_bool_arg()?.unwrap_or(true);
            chain.floating_point_math.set(fpm, expr.span)?;
        }
        "prr" => {
            let prr = expr.require_at_most_one_bool_arg()?.unwrap_or(true);
            chain.print_rationals_raw.set(prr, expr.span)?;
        }
        "bs" => {
            let bs = expr.require_single_number_param()?;
            chain.default_batch_size.set(bs, expr.span)?;
        }
        "sbs" => {
            let bs = expr.require_single_number_param()?;
            chain.stream_buffer_size.set(bs, expr.span)?;
        }
        "sst" => {
            let bs = expr.require_single_number_param()?;
            chain.stream_size_threshold.set(bs, expr.span)?;
        }
        "lb" => {
            let mut buffering_mode = BufferingMode::LineBuffer;
            if !expr.args.is_empty() {
                let val = expr.require_single_string_arg()?;
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
                            span: expr.span
                        }.into());
                    }
                }
            }
            chain.buffering_mode.set(buffering_mode, expr.span)?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn parse_op_def(
    _ctx_opts: &SessionOptions,
    _expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    todo!()
}

pub fn parse_operator_data<'a>(
    sess_opts: &mut SessionOptions,
    expr: CallExpr,
) -> Result<OperatorData, OperatorParsingError<'a>> {
    Ok(match &*expr.op_name {
        "def" => parse_op_def(sess_opts, &expr)?,
        "int" => parse_op_int(&expr)?,
        "bytes" => parse_op_bytes(&expr, false)?,
        "~bytes" => parse_op_bytes(&expr, true)?,
        "str" => parse_op_str(&expr, false)?,
        "~str" => parse_op_str(&expr, true)?,
        "object" => parse_op_tyson(&expr, FieldValueKind::Object, sess_opts)?,
        "array" => parse_op_tyson(&expr, FieldValueKind::Array, sess_opts)?,
        "float" => parse_op_tyson(&expr, FieldValueKind::Float, sess_opts)?,
        "rational" => {
            parse_op_tyson(&expr, FieldValueKind::Rational, sess_opts)?
        }
        "v" | "value" | "tyson" => {
            parse_op_tyson_value(&expr, Some(sess_opts))?
        }
        "error" => parse_op_error(&expr, false)?,
        "~error" => parse_op_error(&expr, true)?,
        "null" => parse_op_literal_zst(&expr, Literal::Null)?,
        "undefined" => parse_op_literal_zst(&expr, Literal::Undefined)?,
        "to_str" => parse_op_to_str(&expr)?,
        "join" | "j" => parse_op_join(&expr)?,
        "r" | "regex" => parse_op_regex(&expr)?,
        "print" | "p" => parse_op_print(&expr)?,
        "format" | "f" => parse_op_format(&expr)?,
        "file" => parse_op_file_reader(&expr)?,
        "stdin" | "in" => parse_op_stdin(&expr)?,
        "key" => parse_op_key(&expr)?,
        "select" => parse_op_select(&expr)?,
        "seq" => parse_op_seq(&expr, SequenceMode::Sequence, false)?,
        "seqn" => parse_op_seq(&expr, SequenceMode::Sequence, true)?,
        "enum" => parse_op_seq(&expr, SequenceMode::Enum, false)?,
        "enumn" => parse_op_seq(&expr, SequenceMode::Enum, true)?,
        "enum-u" => parse_op_seq(&expr, SequenceMode::EnumUnbounded, false)?,
        "enumn-u" => parse_op_seq(&expr, SequenceMode::EnumUnbounded, true)?,
        "count" => parse_op_count(&expr)?,
        "nop" | "scr" => parse_op_nop(&expr)?,
        "fork" => parse_op_fork(sess_opts, expr)?,
        "foreach" | "fe" => parse_op_foreach(sess_opts, expr)?,
        "forkcat" | "fc" => parse_op_forkcat(sess_opts, expr)?,
        "call" | "c" => parse_op_call(&expr)?,
        "callcc" | "cc" => parse_op_call_concurrent(&expr)?,
        _ => {
            let mut expr = expr;
            let ext_registry = sess_opts.extensions.clone();
            for ext in &ext_registry.extensions {
                match ext.parse_call_expr(sess_opts, expr) {
                    Ok(op_data) => return Ok(op_data),
                    Err(OperatorParsingError::CreationFailed(e)) => {
                        return Err(OperatorParsingError::CreationFailed(e));
                    }
                    Err(OperatorParsingError::UnknownOperator(e)) => {
                        expr = e;
                    }
                }
            }
            return Err(OperatorParsingError::CreationFailed(
                expr.error_invalid_operator().into(),
            ));
        }
    })
}

fn expect_equals_after_colon(
    char: char,
    start: usize,
    end: usize,
    span: Span,
) -> Result<(), CliArgumentError> {
    if char != '=' {
        return Err(CliArgumentError::new(
            "expected `=` or whitespace after `:`",
            span.subslice(0, 1, start, end),
        ));
    }
    Ok(())
}

fn require_list_start_as_separate_arg(
    arg: &[u8],
    arg_span: Span,
) -> Result<(), CliArgumentError> {
    if arg.len() > 1 {
        return Err(CliArgumentError::new(
            "list start `[` must be a separate argument",
            arg_span,
        ));
    }
    Ok(())
}

pub fn complain_if_dashed_arg<'a>(
    src: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
    because_block: bool,
) -> Result<(), CliArgumentError> {
    if let Some(arg) = src.peek() {
        if let ArgumentValue::Plain(argv) = &arg.value {
            if argv.starts_with(b"-") {
                return Err(CliArgumentError::new_s(
                    format!(
                        "dashed argument found after end of {}",
                        if because_block { "block" } else { "list" }
                    ),
                    arg.span,
                ));
            }
        }
    };
    Ok(())
}

pub fn gobble_cli_args_while_dashed<'a>(
    src: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
    args: &mut Vec<Argument<'a>>,
) -> Option<Span> {
    let mut final_span = None;
    while let Some(arg) = src.peek() {
        let ArgumentValue::Plain(argv) = &arg.value else {
            break;
        };
        if !argv.starts_with(b"-") {
            break;
        }
        final_span = Some(arg.span);
        args.push(src.next().unwrap());
    }
    final_span
}

pub fn gobble_cli_args_until_end<'a>(
    src: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
    push_end: bool,
    args: &mut Vec<Argument<'a>>,
    block_start: Span,
) -> Result<Span, CliArgumentError> {
    while let Some(arg) = src.peek() {
        let ArgumentValue::Plain(argv) = &arg.value else {
            args.push(src.next().unwrap());
            continue;
        };
        if *argv == "]".as_bytes() {
            return Err(CliArgumentError::new(
                "found list end `]` while waiting for `end` to terminate block",
                arg.span
            ));
        }
        if *argv == "end".as_bytes() {
            let arg = src.next().unwrap();
            let arg_span = arg.span;
            if push_end {
                args.push(arg);
            }
            return Ok(arg_span);
        }

        let _res = parse_call_expr_raw(src, None, true, args)?;
    }
    Err(CliArgumentError::new(
        "unterminated block expression: eof reached while searching for `end`",
        block_start,
    ))
}

pub fn error_unterminated_list(list_start: Span) -> CliArgumentError {
    CliArgumentError::new(
        "unterminated list: eof reached while searching for `]`",
        list_start,
    )
}

pub fn gobble_cli_args_until_bracket_close<'a>(
    src: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
    list_start: Span,
    args: &mut Vec<Argument<'a>>,
) -> Result<Span, CliArgumentError> {
    while let Some(arg) = src.next() {
        let ArgumentValue::Plain(argv) = &arg.value else {
            args.push(arg);
            continue;
        };
        if *argv == "]".as_bytes() {
            return Ok(arg.span);
        }
        if *argv != "[".as_bytes() {
            args.push(arg.clone());
            continue;
        }
        let mut list_args = Vec::new();
        let list_end = gobble_cli_args_until_bracket_close(
            src,
            arg.span,
            &mut list_args,
        )?;
        args.push(Argument {
            value: ArgumentValue::List(list_args),
            span: arg.span.span_until(list_end).unwrap(),
        })
    }
    Err(error_unterminated_list(list_start))
}

pub fn cli_args_into_arguments_iter(
    args: &[Vec<u8>],
) -> Peekable<impl Iterator<Item = Argument<'_>>> {
    args.iter()
        .enumerate()
        .map(|(i, v)| Argument {
            value: ArgumentValue::Plain(Cow::Borrowed(v)),
            span: Span::from_single_arg(i, v.len()),
        })
        .peekable()
}

pub fn error_arg_start_is_list(span: Span) -> CliArgumentError {
    CliArgumentError::new("expression cannot start with a list", span)
}

pub fn parse_call_expr_head<'a>(
    argv: &Cow<'a, [u8]>,
    arg_span: Span,
) -> Result<CallExprHead<'a>, CliArgumentError> {
    let mut append_mode = false;
    let mut transparent_mode = false;

    let mut i = 0;

    if argv[i] == b'+' {
        append_mode = true;
        i += 1;
    }

    if argv[i] == b'_' {
        transparent_mode = true;
        i += 1;
    }

    if transparent_mode && argv[i] == b'+' {
        return Err(CliArgumentError::new(
            "append mode `+` must be specified before transparent mode `_`",
            arg_span.subslice(0, 1, i, i + 1),
        ));
    }

    let op_start = i;
    let mut op_end = i;

    let mut equals_found = false;
    let mut label_found = false;
    let mut dash_found = false;
    let mut colon_found = false;
    let mut label_is_atom = false;
    let mut first_opname_char_found = false;

    for (mut start, mut end, char) in argv[op_start..].char_indices() {
        start += op_start;
        end += op_start;
        i = end;
        if colon_found {
            expect_equals_after_colon(char, start, end, arg_span)?;
            equals_found = true;
            break;
        }
        match char {
            '@' => {
                label_found = true;
                break;
            }
            '%' => {
                label_found = true;
                label_is_atom = true;
                break;
            }
            '=' => {
                equals_found = true;
                break;
            }
            ':' => {
                colon_found = true;
                continue;
            }
            '-' => {
                dash_found = true;
                break;
            }
            _ => (),
        }
        if !first_opname_char_found {
            if !is_xid_start(char) {
                return Err(CliArgumentError::new_s(
                    format!(
                    "invalid character '{char}' to start operator identifier"
                ),
                    arg_span.subslice(0, 1, start, end),
                ));
            }
            first_opname_char_found = true;
        } else if !is_xid_continue(char) {
            return Err(CliArgumentError::new_s(
                format!("invalid character '{char}' in operator identifier"),
                arg_span.subslice(0, 1, start, end),
            ));
        }
        op_end = end;
    }
    let op_name = cow_to_str(slice_cow(argv, op_start..op_end))
        .expect("op_name was checked to be valid utf-8");

    let mut label = None;

    let label_kind = if label_is_atom { "label" } else { "atom" };
    let mut label_end = i;
    if label_found {
        let mut label_start_found = false;
        let label_start = i;
        for (mut start, mut end, char) in argv[label_start..].char_indices() {
            start += label_start;
            end += label_start;
            i = end;
            if colon_found {
                expect_equals_after_colon(char, start, end, arg_span)?;
                equals_found = true;
                break;
            }
            match char {
                '=' => {
                    equals_found = true;
                    break;
                }
                ':' => {
                    colon_found = true;
                    continue;
                }
                '-' => {
                    dash_found = true;
                    break;
                }
                _ => (),
            }
            if !label_start_found {
                if !is_xid_start(char) {
                    return Err(CliArgumentError::new_s(
                        format!(
                            "invalid character '{char}' to start {label_kind} identifier"
                        ),
                        arg_span.subslice(0, 1, start, end),
                    ));
                }
                label_start_found = true;
            } else if !is_xid_continue(char) {
                return Err(CliArgumentError::new_s(
                    format!("invalid character '{char}' in {label_kind} identifier"),
                    arg_span.subslice(0, 1, start, end),
                ));
            }
            label_end = end;
        }
        label = Some(Label {
            value: cow_to_str(slice_cow(argv, label_start..label_end))
                .expect("op_name was checked to be valid utf-8"),
            is_atom: label_is_atom,
            span: arg_span.subslice(0, 1, label_start, label_end),
        });
    }

    let mut dashed_arg = None;

    if dash_found {
        let squished_arg_start = i;
        let mut squished_arg_end = i;
        for (mut start, mut end, char) in
            argv[squished_arg_start..].char_indices()
        {
            start += squished_arg_start;
            end += squished_arg_start;
            i = end;
            if colon_found {
                expect_equals_after_colon(char, start, end, arg_span)?;
                equals_found = true;
                break;
            }
            if char == ':' {
                colon_found = true;
                continue;
            }
            if char == '=' {
                equals_found = true;
                break;
            }
            squished_arg_end = end;
        }

        dashed_arg = Some(Argument {
            value: ArgumentValue::Plain(slice_cow(
                argv,
                squished_arg_start..squished_arg_end,
            )),
            span: arg_span.subslice(
                0,
                1,
                squished_arg_start,
                squished_arg_end,
            ),
        });
    }

    let mut equals_arg = None;

    if equals_found {
        equals_arg = Some(Argument {
            value: ArgumentValue::Plain(slice_cow(argv, i..)),
            span: arg_span.subslice(0, 1, i, argv.len()),
        })
    };

    Ok(CallExprHead {
        append_mode,
        transparent_mode,
        op_name,
        label,
        colon_found,
        dashed_arg,
        equals_arg,
    })
}

pub fn parse_call_expr_raw<'a>(
    src: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
    mut list_start_span: Option<Span>,
    push_args_raw: bool,
    args: &mut Vec<Argument<'a>>,
) -> Result<Option<CallExprParseResult<'a>>, CliArgumentError> {
    let Some(mut arg) = src.next() else {
        return Ok(None);
    };

    let start_span = arg.span;

    let mut argv = match arg.value {
        ArgumentValue::List(_) => {
            if let Some(lss) = list_start_span {
                return Err(error_arg_start_is_list(lss));
            }
            return parse_call_expr_raw(
                src,
                Some(start_span),
                push_args_raw,
                args,
            );
        }
        ArgumentValue::Plain(v) => v,
    };

    let mut is_plain_list = false;

    let mut arg_span = start_span;

    if argv[0] == b'[' {
        require_list_start_as_separate_arg(&argv, arg_span)?;
        if let Some(lss) = list_start_span {
            return Err(error_arg_start_is_list(lss));
        }
        list_start_span = Some(start_span);
        is_plain_list = true;

        arg = src
            .next()
            .ok_or_else(|| error_unterminated_list(start_span))?;

        let ArgumentValue::Plain(next_argv) = arg.value else {
            return Err(error_arg_start_is_list(arg.span));
        };
        argv = next_argv;
        arg_span = arg.span;
    }

    let head = parse_call_expr_head(&argv, arg_span)?;

    if push_args_raw {
        args.push(Argument {
            value: ArgumentValue::Plain(argv.clone()),
            span: arg_span,
        });
    } else {
        if let Some(dash_arg) = &head.dashed_arg {
            args.push(dash_arg.clone());
        }
        if let Some(equals_arg) = &head.equals_arg {
            args.push(equals_arg.clone());
        }
    };

    let mut end_span =
        gobble_cli_args_while_dashed(src, args).unwrap_or(arg_span);

    let mut end_kind = CallExprEndKind::Inline;

    if head.colon_found {
        let list_end_span =
            gobble_cli_args_until_end(src, push_args_raw, args, arg_span)?;

        complain_if_dashed_arg(src, true)?;

        end_kind = CallExprEndKind::End(list_end_span);

        end_span = list_end_span;
    }

    if is_plain_list {
        let args_len_before = args.len();

        let presumably_end_span =
            src.peek().map(|a| a.span).unwrap_or_default();

        let list_end =
            gobble_cli_args_until_bracket_close(src, start_span, args)?;

        if head.colon_found && args.len() != args_len_before {
            return Err(CliArgumentError::new(
                "arguments cannot continue after `end` of block expression",
                presumably_end_span,
            ));
        }

        complain_if_dashed_arg(src, false)?;

        end_kind = CallExprEndKind::ClosingBracket(list_end);

        end_span = list_end;
    } else if list_start_span.is_some() {
        args.extend(src);
    }

    let expr_span = start_span.span_until(end_span).unwrap();

    Ok(Some(CallExprParseResult {
        head,
        end_kind,
        expr_span,
    }))
}

pub fn parse_call_expr<'a>(
    src: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
) -> Result<Option<CallExpr<'a>>, CliArgumentError> {
    let mut args = Vec::new();

    let parse_res = parse_call_expr_raw(src, None, false, &mut args)?;

    Ok(parse_res.map(|res| CallExpr {
        append_mode: res.head.append_mode,
        transparent_mode: res.head.transparent_mode,
        op_name: res.head.op_name,
        label: res.head.label,
        args,
        end_kind: res.end_kind,
        span: res.expr_span,
    }))
}

pub fn parse_cli_retain_args<'a>(
    cli_opts: &CliOptions,
    args: &mut Peekable<impl Iterator<Item = Argument<'a>>>,
) -> Result<SessionOptions, ScrError> {
    assert!(
        !cli_opts.allow_repl || cfg!(feature = "repl"),
        "the 'repl' feature of this crate is disabled"
    );

    // primarily for skipping the executable name
    if cli_opts.skip_first_arg {
        args.next();
    }

    if args.peek().is_none() {
        return Err(MissingArgumentsError.into());
    }
    let mut ctx_opts =
        SessionOptions::with_extensions(cli_opts.extensions.clone());
    ctx_opts.allow_repl = cli_opts.allow_repl;

    if cli_opts.start_with_stdin {
        let op_base_opts = OperatorBaseOptions::new(
            "stdin".into(),
            None,
            false,
            false,
            false,
            Span::Generated,
        );
        let op_data = create_op_stdin(1);
        ctx_opts.add_op(op_base_opts, op_data);
    }
    while let Some(expr) = parse_call_expr(args)? {
        if let Some(label) = &expr.label {
            if expr.op_name.is_empty() && label.is_atom {
                todo!("settings");
            }
        }

        if try_parse_as_special_op(&expr)? {
            continue;
        }
        if try_parse_as_setting(&mut ctx_opts, &expr)? {
            continue;
        }

        let op_base_opts =
            expr.op_base_options_interned(&mut ctx_opts.string_store);

        let op_data = match parse_operator_data(&mut ctx_opts, expr) {
            Ok(op_data) => op_data,
            Err(OperatorParsingError::CreationFailed(e)) => {
                return Err(e.into());
            }
            Err(OperatorParsingError::UnknownOperator(e)) => {
                return Err(e.error_invalid_operator().into());
            }
        };

        ctx_opts.add_op_from_interned_opts(op_base_opts, op_data);
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
            op_data.default_op_name(),
            None,
            false,
            false,
            false,
            Span::Generated,
        );
        ctx_opts.add_op(op_base_opts, op_data);
    }
    if cli_opts.add_success_updator {
        let op_data = create_op_success_updator();
        let op_base_opts = OperatorBaseOptions::new(
            op_data.default_op_name(),
            None,
            false,
            false,
            false,
            Span::Generated,
        );
        ctx_opts.add_op(op_base_opts, op_data);
    }
    Ok(ctx_opts)
}

pub fn parse_cli_raw(
    args: Vec<Vec<u8>>,
    cli_opts: &CliOptions,
) -> Result<SessionOptions, (Vec<Vec<u8>>, ScrError)> {
    let mut args_mapper = cli_args_into_arguments_iter(&args);
    match parse_cli_retain_args(cli_opts, &mut args_mapper) {
        Ok(mut ctx) => {
            drop(args_mapper);
            ctx.cli_args = Some(args.into());
            Ok(ctx)
        }
        Err(e) => {
            drop(args_mapper);
            Err((args, e))
        }
    }
}

pub fn parse_cli(
    cli_opts: &CliOptions,
    args: Vec<Vec<u8>>,
) -> Result<SessionOptions, ContextualizedScrError> {
    parse_cli_raw(args, cli_opts).map_err(|(args, err)| {
        ContextualizedScrError::from_scr_error(
            err,
            Some(IndexSlice::ref_cast(&args)),
            Some(cli_opts),
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
    cli_opts: &CliOptions,
) -> Result<SessionOptions, ContextualizedScrError> {
    let args = collect_env_args().map_err(|e| {
        ContextualizedScrError::from_scr_error(
            e.into(),
            None,
            Some(cli_opts),
            None,
            None,
        )
    })?;
    parse_cli(cli_opts, args)
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use crate::cli::{
        call_expr::{
            Argument, ArgumentValue, CallExpr, CallExprEndKind, Span,
        },
        cli_args_into_arguments_iter, parse_call_expr,
    };

    #[test]
    fn test_parse_call_expr() {
        let src = vec!["seq=10".as_bytes().to_owned()];
        let expr = parse_call_expr(&mut cli_args_into_arguments_iter(&src))
            .unwrap()
            .unwrap();
        assert_eq!(
            expr,
            CallExpr {
                append_mode: false,
                transparent_mode: false,
                op_name: "seq".into(),
                label: None,
                args: vec![Argument {
                    value: ArgumentValue::Plain(Cow::Borrowed(b"10")),
                    span: Span::from_single_arg_with_offset(0, 4, 6),
                }],
                end_kind: CallExprEndKind::Inline,
                span: Span::from_single_arg(0, 6)
            }
        )
    }
}
