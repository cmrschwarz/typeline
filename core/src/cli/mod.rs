use crate::{
    chain::BufferingMode,
    extension::ExtensionRegistry,
    operators::{
        call::parse_op_call,
        call_concurrent::parse_op_call_concurrent,
        count::parse_op_count,
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
        operator::OperatorData,
        print::{create_op_print_with_opts, parse_op_print, PrintOptions},
        regex::parse_op_regex,
        select::parse_op_select,
        sequence::{parse_op_seq, OpSequenceMode},
        success_updater::create_op_success_updator,
        to_str::parse_op_to_str,
        utils::writable::WritableTarget,
    },
    options::{
        operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions, setting::CliArgIdx,
    },
    record_data::field_value::FieldValueKind,
    scr_error::{ContextualizedScrError, ReplDisabledError, ScrError},
    utils::{index_vec::IndexSlice, indexing_type::IndexingType},
};
pub mod call_expr;
use bstr::ByteSlice;

use call_expr::{Argument, ArgumentValue, CallExpr, CallExprEnd, Label, Span};
use once_cell::sync::Lazy;
use ref_cast::RefCast;
use unicode_ident::{is_xid_continue, is_xid_start};

use std::{borrow::Cow, fmt::Display, path::PathBuf, str::FromStr, sync::Arc};
use thiserror::Error;

#[derive(Clone, Default)]
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

#[must_use]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("{message}")] // TODO: display span aswell
pub struct CliArgumentError {
    pub message: Cow<'static, str>,
    pub span: Span,
}

impl CliOptions {
    pub fn with_extensions(extensions: Arc<ExtensionRegistry>) -> Self {
        CliOptions {
            extensions,
            ..CliOptions::default()
        }
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
    if ["--version", "-v", "version"].contains(&expr.op_name) {
        expr.reject_args()?;
        return Err(PrintInfoAndExitError::Version.into());
    }
    if ["--help", "-h", "help", "h"].contains(&expr.op_name) {
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

    match expr.op_name {
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
    _ctx_opts: &mut SessionOptions,
    _expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    todo!()
}

fn try_parse_operator_data(
    ctx_opts: &mut SessionOptions,
    expr: &CallExpr,
) -> Result<Option<OperatorData>, OperatorCreationError> {
    Ok(Some(match expr.op_name {
        "def" => parse_op_def(ctx_opts, expr)?,
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
        "count" => parse_op_count(expr)?,
        "nop" | "scr" => parse_op_nop(expr)?,
        "fork" => parse_op_fork(expr)?,
        "foreach" | "fe" => parse_op_foreach(expr)?,
        "forkcat" | "fc" => parse_op_forkcat(expr)?,
        "call" | "c" => parse_op_call(expr)?,
        "callcc" | "cc" => parse_op_call_concurrent(expr)?,
        "next" | "n" => parse_op_next(expr)?,
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
    arg_idx: usize,
) -> Result<(), CliArgumentError> {
    if arg.len() > 1 {
        return Err(CliArgumentError::new(
            "list start `[` must be a separate argument",
            Span::CliArg {
                start: CliArgIdx::from_usize(arg_idx),
                end: CliArgIdx::from_usize(arg_idx + 1),
                offset_start: 1,
                offset_end: arg.len() as u16,
            },
        ));
    }
    Ok(())
}

pub fn complain_if_dashed_arg(
    src: &[Vec<u8>],
    src_idx: usize,
    because_block: bool,
) -> Result<(), CliArgumentError> {
    if let Some(arg) = src.get(src_idx) {
        if arg.starts_with(b"-") {
            return Err(CliArgumentError::new_s(
                format!(
                    "dashed argument found after end of {}",
                    if because_block { "block" } else { "list" }
                ),
                Span::from_single_arg(src_idx, arg.len()),
            ));
        }
    };
    Ok(())
}

pub fn gobble_cli_args_while_dashed<'a>(
    src: &'a [Vec<u8>],
    src_idx: &mut usize,
    args: &mut Vec<Argument<'a>>,
) -> Option<Span> {
    let mut i = *src_idx;
    let mut final_span = None;
    while i < src.len() {
        let arg = &src[i];
        if !arg.starts_with(b"-") {
            break;
        }
        let span = Span::from_single_arg(i, arg.len());
        args.push(Argument {
            value: ArgumentValue::Plain(arg),
            span,
        });
        final_span = Some(span);
        i += 1;
    }
    *src_idx = i;
    final_span
}

pub fn gobble_cli_args_until_end<'a>(
    src: &'a [Vec<u8>],
    src_idx: &mut usize,
    args: &mut Vec<Argument<'a>>,
    block_start: Span,
) -> Result<Span, CliArgumentError> {
    let mut i = *src_idx;
    while i < src.len() {
        let arg = &src[i];
        if arg == "]".as_bytes() {
            return Err(CliArgumentError::new(
                "found list end `]` while waiting for `end` to terminate block",
                Span::from_single_arg(i, arg.len()),
            ));
        }
        if arg == "end".as_bytes() {
            return Ok(Span::from_single_arg(i, arg.len()));
        }
        let mut append = false;
        let mut transparent = false;
        let mut name = "";
        let mut label = None;
        let mut end = CallExprEnd::Inline;
        let mut span = Span::Generated;
        parse_call_expr_raw(
            src,
            &mut i,
            true,
            &mut append,
            &mut transparent,
            &mut name,
            &mut label,
            args,
            &mut end,
            &mut span,
        )?;
    }
    *src_idx = i;
    Err(CliArgumentError::new(
        "unterminated block expression: eof reached while searching for `end`",
        block_start,
    ))
}

pub fn gobble_cli_args_until_bracket_close<'a>(
    src: &'a [Vec<u8>],
    list_start: Span,
    src_idx: &mut usize,
    args: &mut Vec<Argument<'a>>,
) -> Result<Span, CliArgumentError> {
    let mut i = *src_idx;
    while i < src.len() {
        let arg = &src[i];
        let span = Span::CliArg {
            start: CliArgIdx::from_usize(i),
            end: CliArgIdx::from_usize(i + 1),
            offset_start: 0,
            offset_end: arg.len() as u16,
        };
        if arg == "]".as_bytes() {
            return Ok(span);
        }
        if arg != "[".as_bytes() {
            args.push(Argument {
                value: ArgumentValue::Plain(arg),
                span,
            });
            i += 1;
            continue;
        }
        let mut list_args = Vec::new();
        let list_end = gobble_cli_args_until_bracket_close(
            src,
            span,
            &mut i,
            &mut list_args,
        )?;
        args.push(Argument {
            value: ArgumentValue::List(list_args),
            span: span.span_until(list_end).unwrap(),
        })
    }
    *src_idx = i;
    Err(CliArgumentError::new(
        "unterminated list: eof reached while searching for `]`",
        list_start,
    ))
}

pub fn parse_call_expr_raw<'a>(
    src: &'a [Vec<u8>],
    src_idx: &mut usize,
    push_args_raw: bool,

    append_mode: &mut bool,
    transparent_mode: &mut bool,
    op_name: &mut &'a str,
    label: &mut Option<Label<'a>>,
    args: &mut Vec<Argument<'a>>,
    end: &mut CallExprEnd,
    expr_span: &mut Span,
) -> Result<(), CliArgumentError> {
    let mut arg = &src[*src_idx];

    let mut is_list = false;

    let start_span = Span::from_single_arg(*src_idx, arg.len());
    let mut arg_span = start_span;

    if arg[0] == b'[' {
        require_list_start_as_separate_arg(arg, *src_idx)?;
        is_list = true;
        *src_idx += 1;
        arg = &src[*src_idx];
        arg_span = Span::from_single_arg(*src_idx, arg.len());
    }

    let mut i = 0;

    if arg[i] == b'+' {
        *append_mode = true;
        i += 1;
    }

    if arg[i] == b'_' {
        *transparent_mode = true;
        i += 1;
    }

    if *transparent_mode && arg[i] == b'+' {
        return Err(CliArgumentError::new(
            "append mode `+` must be specified before transparent mode `_`",
            arg_span.subslice(0, 1, i, i + 1),
        ));
    }

    let mut first_found = false;
    let mut label_found = false;
    let mut label_is_atom = false;
    let mut colon_found = false;
    let mut equals_found = false;
    let mut dash_found = false;
    let op_start = i;
    let mut op_end = i;

    for (start, end, char) in arg[op_start..].char_indices() {
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
        if !first_found {
            if !is_xid_start(char) {
                return Err(CliArgumentError::new_s(
                    format!(
                    "invalid character '{char}' to start operator identifier"
                ),
                    arg_span.subslice(0, 1, start, end),
                ));
            }
            first_found = true;
        } else if !is_xid_continue(char) {
            return Err(CliArgumentError::new_s(
                format!("invalid character '{char}' in operator identifier"),
                arg_span.subslice(0, 1, start, end),
            ));
        }
        op_end = end;
    }
    *op_name = arg[op_start..op_end]
        .to_str()
        .expect("op_name was checked to be valid utf-8");

    let label_kind = if label_is_atom { "label" } else { "atom" };
    let mut label_end = i;
    if label_found {
        let mut label_start_found = false;
        let label_start = i;
        for (start, end, char) in arg[i..].char_indices() {
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
        *label = Some(Label {
            value: arg[label_start..label_end]
                .to_str()
                .expect("op_name was checked to be valid utf-8"),
            is_atom: label_is_atom,
            span: arg_span.subslice(0, 1, label_start, label_end),
        });
    }

    if dash_found {
        let squished_arg_start = i;
        let mut squished_arg_end = i;
        for (start, end, char) in arg[i..].char_indices() {
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

        if !push_args_raw {
            args.push(Argument {
                value: ArgumentValue::Plain(
                    &arg[squished_arg_start..squished_arg_end],
                ),
                span: arg_span.subslice(
                    0,
                    1,
                    squished_arg_start,
                    squished_arg_end,
                ),
            });
        }
    }

    if equals_found && !push_args_raw {
        args.push(Argument {
            value: ArgumentValue::Plain(&arg[i..]),
            span: arg_span.subslice(0, 1, i, arg.len()),
        });
    }

    if push_args_raw {
        args.push(Argument {
            value: ArgumentValue::Plain(arg),
            span: arg_span,
        });
    }

    *src_idx += 1;

    let mut end_span =
        gobble_cli_args_while_dashed(src, src_idx, args).unwrap_or(arg_span);

    if colon_found {
        let mut block_args = Vec::new();
        let list_end_span = gobble_cli_args_until_end(
            src,
            src_idx,
            &mut block_args,
            arg_span,
        )?;

        complain_if_dashed_arg(src, *src_idx, true)?;

        *end = CallExprEnd::End(list_end_span);

        end_span = list_end_span;
    }

    if is_list {
        let index_before_list_end = *src_idx;
        let list_end =
            gobble_cli_args_until_end(src, src_idx, args, start_span)?;

        if colon_found && index_before_list_end + 1 != *src_idx {
            return Err(CliArgumentError::new(
                "arguments cannot continue after `end` of block expression",
                Span::from_single_arg(
                    index_before_list_end,
                    src[index_before_list_end].len(),
                ),
            ));
        }

        complain_if_dashed_arg(src, *src_idx, false)?;

        *end = CallExprEnd::ClosingBracket(list_end);

        end_span = list_end;
    }

    *expr_span = start_span.span_until(end_span).unwrap();

    Ok(())
}

pub fn parse_call_expr<'a>(
    src: &'a [Vec<u8>],
    src_idx: &mut usize,
) -> Result<CallExpr<'a>, CliArgumentError> {
    let mut op_name = "";
    let mut args = Vec::new();
    let mut label = None;
    let mut span = Span::Generated;
    let mut append_mode = false;
    let mut transparent_mode = false;
    let mut end = CallExprEnd::Inline;

    parse_call_expr_raw(
        src,
        src_idx,
        false,
        &mut append_mode,
        &mut transparent_mode,
        &mut op_name,
        &mut label,
        &mut args,
        &mut end,
        &mut span,
    )?;

    Ok(CallExpr {
        append_mode,
        transparent_mode,
        op_name,
        label,
        args,
        end,
        span,
    })
}

pub fn parse_cli_retain_args(
    cli_opts: &CliOptions,
    args: &[Vec<u8>],
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
    while arg_idx < args.len() {
        let expr = parse_call_expr(args, &mut arg_idx)?;

        if let Some(label) = expr.label {
            if expr.op_name.is_empty() && label.is_atom {
                todo!("settings");
            }
        }

        if let Some(op_data) = try_parse_operator_data(&mut ctx_opts, &expr)? {
            let op_base_opts =
                expr.op_base_options_interned(&mut ctx_opts.string_store);
            ctx_opts.add_op_from_interned_opts(op_base_opts, op_data);
            continue;
        }

        if try_parse_as_special_op(&expr)? {
            continue;
        }
        if try_parse_as_setting(&mut ctx_opts, &expr)? {
            continue;
        }
        return Err(CliArgumentError {
            message: format!("unknown operator '{}'", expr.op_name).into(),
            span: expr.span,
        }
        .into());
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
    match parse_cli_retain_args(cli_opts, &args) {
        Ok(mut ctx) => {
            ctx.cli_args = Some(args.into());
            Ok(ctx)
        }
        Err(e) => Err((args, e)),
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
    use crate::cli::{
        call_expr::{Argument, ArgumentValue, CallExpr, CallExprEnd, Span},
        parse_call_expr,
    };

    #[test]
    fn test_parse_call_expr() {
        let src = vec!["seq=10".as_bytes().to_owned()];
        let expr = parse_call_expr(&src, &mut 0).unwrap();
        assert_eq!(
            expr,
            CallExpr {
                append_mode: false,
                transparent_mode: false,
                op_name: "seq",
                label: None,
                args: vec![Argument {
                    value: ArgumentValue::Plain(b"10"),
                    span: Span::from_single_arg_with_offset(0, 4, 6),
                }],
                end: CallExprEnd::Inline,
                span: Span::from_single_arg(0, 6)
            }
        )
    }
}
