use crate::{
    operators::{
        atom::parse_op_atom,
        call::parse_op_call,
        call_concurrent::parse_op_call_concurrent,
        count::parse_op_count,
        errors::OperatorCreationError,
        file_reader::{parse_op_file_reader, parse_op_stdin},
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
        print::parse_op_print,
        regex::parse_op_regex,
        select::parse_op_select,
        sequence::{parse_op_seq, SequenceMode},
        to_str::parse_op_to_str,
    },
    options::session_setup::SessionSetupData,
    record_data::{
        array::Array,
        field_value::{FieldValue, FieldValueKind, Object},
        scope_manager::{ScopeId, DEFAULT_SCOPE_ID},
    },
    scr_error::ScrError,
    tyson::TysonParser,
    utils::maybe_text::MaybeText,
};
pub mod call_expr;
use bstr::ByteSlice;

use call_expr::{Argument, CallExpr, CallExprEndKind, Label, Span};
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use unicode_ident::{is_xid_continue, is_xid_start};

use std::{borrow::Cow, fmt::Display, iter::Peekable};
use thiserror::Error;

#[derive(Default)]
pub struct CallExprHead {
    op_name: String,
    op_name_span: Span,
    colon_found: bool,
    label: Option<Label>,
    equals_arg: Option<Argument>,
    dashed_arg: Option<IndexMap<String, FieldValue>>,
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

pub fn try_parse_bool(val: &[u8]) -> Option<bool> {
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

fn try_parse_as_special_op<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
) -> Result<bool, ScrError> {
    let Some((arg, _start_span)) = src.peek() else {
        return Ok(false);
    };
    if [b"--version" as &[u8], b"-v", b"version"].contains(arg) {
        return Err(PrintInfoAndExitError::Version.into());
    }
    if [b"--help" as &[u8], b"-h", b"help", b"h"].contains(arg) {
        let (_, start_span) = src.next().unwrap();

        const MAIN_HELP_PAGE: &str = include_str!("help_sections/main.txt");
        let section = if let Some((help_section_arg, span)) = src.next() {
            let section_name = String::from_utf8_lossy(help_section_arg);
            match section_name.trim().to_lowercase().as_ref() {
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
                        message: format!(
                            "no help section for '{section_name}'"
                        )
                        .into(),
                        span: start_span.span_until(span).unwrap(),
                    }
                    .into())
                }
            }
        } else {
            MAIN_HELP_PAGE
        };
        return Err(PrintInfoAndExitError::Help(section.into()).into());
    }
    Ok(false)
}

// TODO: rewrite using scopes
#[cfg(any())]
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
            let val = expr.require_single_plaintext_arg()?;
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

pub fn parse_operator_data(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let mut expr = CallExpr::from_argument_mut(&mut arg)?;

    Ok(match expr.op_name {
        "atom" => parse_op_atom(sess, &mut expr)?,
        "int" => parse_op_int(&expr)?,
        "bytes" => parse_op_bytes(&mut arg, false)?,
        "~bytes" => parse_op_bytes(&mut arg, true)?,
        "str" => parse_op_str(&expr, false)?,
        "~str" => parse_op_str(&expr, true)?,
        "object" => parse_op_tyson(sess, &expr, FieldValueKind::Object)?,
        "array" => parse_op_tyson(sess, &expr, FieldValueKind::Array)?,
        "float" => parse_op_tyson(sess, &expr, FieldValueKind::Float)?,
        "v" | "value" | "tyson" => parse_op_tyson_value(&expr, Some(sess))?,
        "error" => parse_op_error(&expr, false)?,
        "~error" => parse_op_error(&expr, true)?,
        "null" => parse_op_literal_zst(&expr, Literal::Null)?,
        "undefined" => parse_op_literal_zst(&expr, Literal::Undefined)?,
        "to_str" => parse_op_to_str(sess, expr)?,
        "join" | "j" => parse_op_join(&expr)?,
        "r" | "regex" => parse_op_regex(sess, expr)?,
        "print" | "p" => parse_op_print(&expr)?,
        "format" | "f" => parse_op_format(&expr)?,
        "file" => parse_op_file_reader(sess, expr)?,
        "stdin" | "in" => parse_op_stdin(sess, expr)?,
        "key" => parse_op_key(sess, arg)?,
        "select" => parse_op_select(&expr)?,
        "seq" => parse_op_seq(&expr, SequenceMode::Sequence, false)?,
        "seqn" => parse_op_seq(&expr, SequenceMode::Sequence, true)?,
        "enum" => parse_op_seq(&expr, SequenceMode::Enum, false)?,
        "enumn" => parse_op_seq(&expr, SequenceMode::Enum, true)?,
        "enum-u" => parse_op_seq(&expr, SequenceMode::EnumUnbounded, false)?,
        "enumn-u" => parse_op_seq(&expr, SequenceMode::EnumUnbounded, true)?,
        "count" => parse_op_count(&expr)?,
        "nop" | "scr" => parse_op_nop(&expr)?,
        "fork" => parse_op_fork(arg)?,
        "foreach" | "fe" => parse_op_foreach(sess, arg)?,
        "forkcat" | "fc" => parse_op_forkcat(sess, arg)?,
        "call" | "c" => parse_op_call(&expr)?,
        "callcc" | "cc" => parse_op_call_concurrent(&expr)?,
        _ => {
            let ext_registry = sess.extensions.clone();
            for ext in &ext_registry.extensions {
                if let Some(op_data) = ext.parse_call_expr(sess, &mut arg)? {
                    return Ok(op_data);
                }
            }
            return Err(CallExpr::from_argument(&arg)?
                .error_invalid_operator()
                .into());
        }
    })
}

fn require_list_start_as_separate_arg(
    arg: &[u8],
    i: usize,
    arg_span: Span,
) -> Result<(), CliArgumentError> {
    if arg.len() > i + 1 {
        return Err(CliArgumentError::new(
            "list start `[` must be followed by whitespace",
            arg_span.subslice_offsets(i, arg.len()),
        ));
    }
    Ok(())
}

fn parse_list_end_with_label(
    argv: &[u8],
    arg_span: Span,
) -> Result<Option<(String, Span)>, CliArgumentError> {
    if argv.len() == 1 {
        return Ok(None);
    }
    if argv[1] != b'@' {
        return Err(CliArgumentError::new(
            "list end `]` must be followed by whitespace or '@'",
            arg_span,
        ));
    }
    let label_span = arg_span.subslice_offsets(2, argv.len());
    let label = argv[2..]
        .to_str()
        .map_err(|_| {
            CliArgumentError::new("label must be valid utf-8", label_span)
        })?
        .to_owned();

    Ok(Some((label, label_span)))
}

pub fn complain_if_dashed_arg_after_block<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
    because_block: bool,
) -> Result<(), CliArgumentError> {
    if let Some((arg, span)) = src.peek() {
        if arg.starts_with(b"-") {
            return Err(CliArgumentError::new_s(
                format!(
                    "dashed argument found after end of {}",
                    if because_block { "block" } else { "list" }
                ),
                *span,
            ));
        }
    };
    Ok(())
}

pub fn gobble_cli_args_while_dashed_or_eq<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
    args: &mut Vec<Argument>,
    source_scope: ScopeId,
    target: &mut Option<IndexMap<String, FieldValue>>,
) -> Result<Option<Span>, CliArgumentError> {
    let mut final_span = None;
    while let Some((arg, _span)) = src.peek() {
        let dash = arg.starts_with(b"-") && arg.len() > 1;
        let eq = arg.starts_with(b"=");
        if !dash && !eq {
            break;
        }
        let (arg, span) = src.next().unwrap();
        final_span = Some(span);
        if dash {
            parse_dashed_arg(
                arg,
                source_scope,
                span,
                target.get_or_insert_with(IndexMap::new),
            )?;
            continue;
        }
        args.push(Argument {
            value: FieldValue::from_maybe_text(MaybeText::from_bytes_try_str(
                &arg[1..],
            )),
            span,
            source_scope,
            end_kind: None,
        });
    }
    Ok(final_span)
}

pub fn parse_block_until_end<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
    args: &mut Vec<Argument>,
    block_start: Span,
    source_scope: ScopeId,
) -> Result<Span, CliArgumentError> {
    // TODO: support aggregates

    while let Some((argv, span)) = src.peek() {
        if argv.first() == Some(&b']') && (argv.len() == 1 || argv[1] == b'@')
        {
            return Err(CliArgumentError::new(
                "found list end `]` while waiting for `end` to terminate block",
                *span
            ));
        }
        if argv == b"end" {
            let (_, end_span) = src.next().unwrap();
            return Ok(end_span);
        }
        let Some(expr) = parse_call_expr(src, source_scope)? else {
            break;
        };
        args.push(expr.arg);
    }
    Err(CliArgumentError::new(
        "unterminated block expression: end of input reached while searching for `end`",
        block_start,
    ))
}

pub fn error_unterminated_list(list_start: Span) -> CliArgumentError {
    CliArgumentError::new(
        "unterminated list: eof reached while searching for `]`",
        list_start,
    )
}

pub fn cli_args_into_arguments_iter<'a>(
    args: impl IntoIterator<Item = &'a [u8]>,
) -> Peekable<impl Iterator<Item = (&'a [u8], Span)>> {
    args.into_iter()
        .enumerate()
        .map(|(i, arg)| (arg, Span::from_cli_arg(i, i + 1, 0, arg.len())))
        .peekable()
}

pub fn error_arg_start_is_list(span: Span) -> CliArgumentError {
    CliArgumentError::new("expression cannot start with a list", span)
}

pub fn parse_call_expr_head(
    argv: &[u8],
    offset: usize,
    arg_span: Span,
    source_scope: ScopeId,
) -> Result<CallExprHead, CliArgumentError> {
    let mut i = offset;
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
                    arg_span.subslice_offsets(start, end),
                ));
            }
            first_opname_char_found = true;
        } else if !is_xid_continue(char) {
            return Err(CliArgumentError::new_s(
                format!("invalid character '{char}' in operator identifier"),
                arg_span.subslice_offsets(start, end),
            ));
        }
        op_end = end;
    }
    let op_name = argv[op_start..op_end]
        .to_str()
        .expect("op_name was checked to be valid utf-8")
        .to_owned();

    let op_name_span = arg_span.subslice_offsets(op_start, op_end);

    let mut label = None;

    let label_kind = if label_is_atom { "label" } else { "atom" };
    let mut label_end = i;
    if label_found {
        let label_start = i;
        for (_start, mut end, char) in argv[label_start..].char_indices() {
            end += label_start;
            i = end;
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
            label_end = end;
        }
        label = Some(Label {
            value: argv[label_start..label_end]
                .to_str()
                .map_err(|_| {
                    CliArgumentError::new_s(
                        format!("{label_kind} must be valid utf-8"),
                        arg_span.subslice_offsets(label_start, label_end),
                    )
                })?
                .to_owned(),
            is_atom: label_is_atom,
            span: arg_span.subslice_offsets(label_start, label_end),
        });
    }

    let mut dashed_arg = None;

    if dash_found {
        let squished_arg_start = i;
        let mut squished_arg_end = argv.len();
        for (_start, mut end, char) in
            argv[squished_arg_start..].char_indices()
        {
            end += squished_arg_start;
            i = end;
            if char == '=' {
                squished_arg_end = i - 1;
                equals_found = true;
                break;
            }
        }

        let mut target = IndexMap::new();
        parse_dashed_arg(
            &argv[squished_arg_start - 1..squished_arg_end],
            source_scope,
            arg_span.subslice_offsets(squished_arg_start - 1, 0),
            &mut target,
        )?;

        dashed_arg = Some(target);
    }

    let mut equals_arg = None;

    if equals_found {
        equals_arg = Some(Argument {
            value: FieldValue::from_maybe_text(MaybeText::from_bytes_try_str(
                &argv[i..],
            )),
            span: arg_span.subslice_offsets(i, argv.len()),
            source_scope,
            end_kind: None,
        })
    };

    Ok(CallExprHead {
        op_name,
        op_name_span,
        colon_found,
        label,
        equals_arg,
        dashed_arg,
    })
}

pub fn parse_single_arg_value(arg: &[u8]) -> FieldValue {
    if TysonParser::<&'static [u8]>::is_number_start(arg[0]) {
        let mut tp = TysonParser::new(&arg[1..], true, None);
        if let Ok(number) = tp.parse_number(arg[0]) {
            return number;
        }
    };

    FieldValue::from_maybe_text(MaybeText::from_bytes_try_str(arg))
}

struct ExprModes {
    setting: bool,
    append_mode: bool,
    transparent_mode: bool,
}

fn parse_modes(
    argv: &[u8],
    span: Span,
) -> Result<(ExprModes, usize), CliArgumentError> {
    let mut append_mode = false;
    let mut transparent_mode = false;

    let mut i = 0;

    if argv.get(i) == Some(&b'%') {
        return Ok((
            ExprModes {
                setting: true,
                append_mode: false,
                transparent_mode: false,
            },
            1,
        ));
    }

    if argv.get(i) == Some(&b'+') {
        append_mode = true;
        i += 1;
    }

    if argv.get(i) == Some(&b'_') {
        transparent_mode = true;
        i += 1;
    }

    if !append_mode && transparent_mode && argv.get(i) == Some(&b'+') {
        return Err(CliArgumentError::new(
            "append mode `+` must be specified before transparent mode `_`",
            span.subslice_offsets(i, i + 1),
        ));
    }

    Ok((
        ExprModes {
            append_mode,
            transparent_mode,
            setting: false,
        },
        i,
    ))
}

pub fn wrap_expr_in_transparent(arg: Argument) -> Argument {
    Argument {
        span: arg.span,
        source_scope: arg.source_scope,
        end_kind: Some(CallExprEndKind::SpecialBuiltin),
        value: FieldValue::Array(Array::Argument(vec![
            Argument::generated_from_name("transparent", arg.source_scope),
            arg,
        ])),
    }
}

pub fn wrap_expr_in_key(
    key: String,
    label_span: Span,
    arg: Argument,
) -> Argument {
    let source_scope = arg.source_scope;
    Argument {
        span: arg.span,
        source_scope,
        end_kind: Some(CallExprEndKind::SpecialBuiltin),
        value: FieldValue::Array(Array::Argument(vec![
            Argument::generated_from_name("key", source_scope),
            Argument::from_field_value(
                FieldValue::Text(key),
                label_span,
                source_scope,
            ),
            arg,
        ])),
    }
}

pub fn create_nop_arg(source_scope: ScopeId) -> Argument {
    Argument::generated_from_field_value(
        FieldValue::Array(Array::Argument(vec![
            Argument::generated_from_name("nop", source_scope),
        ])),
        source_scope,
    )
}

pub fn parse_list_after_start<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
    start_span: Span,
    source_scope: ScopeId,
) -> Result<Argument, CliArgumentError> {
    let mut args = Vec::new();
    let mut append_group_start = None;
    while let Some((argv, span)) = src.next() {
        if argv.first() == Some(&b']') {
            let label = parse_list_end_with_label(argv, span)?;
            let list = Argument {
                value: FieldValue::Array(Array::Argument(args)),
                span: start_span.span_until(span).unwrap(),
                source_scope,
                end_kind: Some(CallExprEndKind::End(span)),
            };
            let Some((label, label_span)) = label else {
                return Ok(list);
            };
            return Ok(Argument::generated_from_field_value(
                FieldValue::Array(Array::Argument(vec![
                    Argument::generated_from_name("label", source_scope),
                    Argument {
                        value: FieldValue::Text(label),
                        span: label_span,
                        source_scope,
                        end_kind: None,
                    },
                    list,
                ])),
                source_scope,
            ));
        }

        let (modes, i) = parse_modes(argv, span)?;

        let mut arg = if argv.get(i) == Some(&b'[') {
            require_list_start_as_separate_arg(argv, i, span)?;
            parse_list_after_start(src, span, source_scope)?
        } else {
            Argument {
                value: parse_single_arg_value(&argv[i..]),
                span,
                source_scope,
                end_kind: None,
            }
        };

        if modes.transparent_mode {
            arg = wrap_expr_in_transparent(arg);
        }

        if !modes.append_mode {
            if let Some(append_group_start) = append_group_start {
                let mut args_group = Vec::new();
                args_group.push(Argument::generated_from_name(
                    "aggregate",
                    source_scope,
                ));
                args_group.extend(args.drain(append_group_start..));
                args.push(Argument::generated_from_field_value(
                    FieldValue::Array(Array::Argument(args_group)),
                    source_scope,
                ));
            }
        }

        if modes.append_mode && append_group_start.is_none() {
            append_group_start = Some(args.len());
            if args.is_empty() {
                args.push(create_nop_arg(source_scope));
            }
        }

        args.push(arg);
    }
    Err(error_unterminated_list(start_span))
}

pub struct ParsedExpr {
    pub arg: Argument,
    pub append_mode: bool,
}

pub fn parse_dashed_arg(
    argv: &[u8],
    source_scope: ScopeId,
    span: Span,
    target: &mut IndexMap<String, FieldValue>,
) -> Result<(), CliArgumentError> {
    debug_assert_eq!(argv[0], b'-');
    let mut i = 1;
    let mut starts_with_dash = true;
    loop {
        if argv.len() == i {
            return Err(CliArgumentError::new(
                "leading dash in argument must be followed by flag",
                span,
            ));
        }
        let arg_start = i;
        let mut key = argv;
        let mut value = FieldValue::Null;
        if argv[i] == b'-' {
            if let Some(colon_idx) = argv[i + 1..].find_char(':') {
                let colon_idx = i + 1 + colon_idx;
                key = &argv[..colon_idx];
                value = parse_single_arg_value(&argv[colon_idx + 1..]);
                i = argv.len();
            };
        } else {
            key = &argv[i..=i];
            i += 1;
            if let Some(b':') = argv.get(i) {
                i += 1;
                let v_start = i;
                while i < argv.len() {
                    if argv[i] == b'-' {
                        break;
                    }
                    i += 1;
                }
                value = parse_single_arg_value(&argv[v_start..i]);
            }
        }
        let Ok(key) = key.to_str() else {
            return Err(CliArgumentError::new(
                "double dash argument must be valid utf-8",
                span.subslice_offsets(arg_start, arg_start + key.len()),
            ));
        };
        let key = format!("-{key}");
        let value = FieldValue::Argument(Box::new(Argument {
            value,
            span: span.subslice_offsets(
                arg_start - usize::from(starts_with_dash),
                i,
            ),
            source_scope,
            end_kind: None,
        }));
        if let Some(_prev) = target.insert(key.to_string(), value) {
            return Err(CliArgumentError::new_s(
                format!("dashed argument name '{key}' specified twice"),
                span.subslice_offsets(arg_start, arg_start + key.len()),
            ));
        }
        if i == argv.len() {
            return Ok(());
        }
        starts_with_dash = argv[i] == b'-';
        i += usize::from(starts_with_dash);
    }
}

pub fn parse_call_expr<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
    source_scope: ScopeId,
) -> Result<Option<ParsedExpr>, CliArgumentError> {
    let Some((argv, arg_span)) = src.next() else {
        return Ok(None);
    };

    let (modes, i) = parse_modes(argv, arg_span)?;

    if argv.get(i) == Some(&b'[') {
        require_list_start_as_separate_arg(argv, i, arg_span)?;
        let mut arg = parse_list_after_start(src, arg_span, source_scope)?;
        if modes.transparent_mode {
            arg = wrap_expr_in_transparent(arg);
        }
        return Ok(Some({
            ParsedExpr {
                arg,
                append_mode: modes.append_mode,
            }
        }));
    }

    let mut args = Vec::new();

    if modes.setting {
        args.push(Argument {
            value: FieldValue::Text("atom".to_string()),
            span: arg_span.subslice_offsets(0, 1),
            source_scope,
            end_kind: None,
        })
    }

    let mut head =
        parse_call_expr_head(argv.as_bytes(), i, arg_span, source_scope)?;

    args.push(Argument {
        value: FieldValue::Text(head.op_name),
        span: head.op_name_span,
        source_scope,
        end_kind: None,
    });
    let dash_arg_pos = args.len();
    let dash_arg_found = head.dashed_arg.is_some();

    if dash_arg_found {
        args.push(Argument {
            value: FieldValue::Undefined,
            span: Span::FlagsObject,
            source_scope,
            end_kind: None,
        });
    }
    if let Some(equals_arg) = &head.equals_arg {
        // TODO: dash dash arg
        args.push(equals_arg.clone());
    }

    let mut end_span = gobble_cli_args_while_dashed_or_eq(
        src,
        &mut args,
        source_scope,
        &mut head.dashed_arg,
    )?
    .unwrap_or(arg_span);

    if let Some(dashed) = head.dashed_arg {
        if modes.setting {
            return Err(CliArgumentError::new(
                "dashed arguments not allowed in atom oerator",
                arg_span,
            ));
        }
        if !dash_arg_found {
            args.insert(
                dash_arg_pos,
                Argument {
                    value: FieldValue::Undefined,
                    span: Span::FlagsObject,
                    source_scope,
                    end_kind: None,
                },
            )
        }
        args[dash_arg_pos].value =
            FieldValue::Object(Box::new(Object::KeysStored(dashed)));
    }

    let mut end_kind = CallExprEndKind::Inline;

    if head.colon_found {
        let list_end_span =
            parse_block_until_end(src, &mut args, arg_span, source_scope)?;

        complain_if_dashed_arg_after_block(src, true)?;

        end_kind = CallExprEndKind::End(list_end_span);

        end_span = list_end_span;
    }

    let expr_span = arg_span.span_until(end_span).unwrap();

    let mut arg = Argument {
        value: FieldValue::Array(Array::Argument(args)),
        span: expr_span,
        end_kind: Some(end_kind),
        source_scope,
    };

    if let Some(label) = head.label {
        if label.is_atom {
            todo!()
        };
        arg = wrap_expr_in_key(label.value, label.span, arg);
    }

    if modes.transparent_mode {
        arg = wrap_expr_in_transparent(arg);
    }

    Ok(Some(ParsedExpr {
        arg,
        append_mode: modes.append_mode,
    }))
}

pub fn parse_cli_raw<'a>(
    src: &mut Peekable<impl Iterator<Item = (&'a [u8], Span)>>,
) -> Result<Vec<Argument>, ScrError> {
    let mut args = Vec::new();

    let mut aggregation_start = None;

    let scope_id = DEFAULT_SCOPE_ID;

    loop {
        if try_parse_as_special_op(src)? {
            continue;
        }

        let Some(expr) = parse_call_expr(src, scope_id)? else {
            break;
        };

        if expr.append_mode && aggregation_start.is_none() {
            aggregation_start = Some(args.len());
            if args.is_empty() {
                args.push(create_nop_arg(scope_id));
            }
        }

        if !expr.append_mode {
            if let Some(agg_start) = aggregation_start.take() {
                let mut agg_args = Vec::new();
                agg_args.push(Argument::generated_from_name(
                    "aggregation",
                    scope_id,
                ));
                agg_args.extend(args.drain(agg_start..));
                agg_args.push(expr.arg);
                args.push(Argument::generated_from_field_value(
                    FieldValue::Array(Array::Argument(agg_args)),
                    scope_id,
                ));
                continue;
            }
        }
        args.push(expr.arg);
    }
    Ok(args)
}

pub fn parse_cli_args<'a>(
    src: impl IntoIterator<Item = &'a [u8]>,
    skip_first: bool,
) -> Result<Vec<Argument>, ScrError> {
    parse_cli_raw(
        &mut src
            .into_iter()
            .enumerate()
            .map(|(i, arg)| (arg, Span::from_cli_arg(i, i + 1, 0, arg.len())))
            .skip(usize::from(skip_first))
            .peekable(),
    )
}

pub fn parse_cli_args_form_vec<'a>(
    src: impl IntoIterator<Item = &'a Vec<u8>>,
    skip_first: bool,
) -> Result<Vec<Argument>, ScrError> {
    parse_cli_raw(
        &mut src
            .into_iter()
            .enumerate()
            .map(|(i, arg)| {
                (&**arg, Span::from_cli_arg(i, i + 1, 0, arg.len()))
            })
            .skip(usize::from(skip_first))
            .peekable(),
    )
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

#[cfg(test)]
mod test {

    use indexmap::indexmap;

    use crate::{
        cli::{
            call_expr::{Argument, CallExprEndKind, Span},
            cli_args_into_arguments_iter, parse_call_expr,
        },
        record_data::{
            array::Array,
            field_value::{FieldValue, Object},
            scope_manager::DEFAULT_SCOPE_ID,
        },
    };

    #[test]
    fn equals_parsed_as_str() {
        let src = ["seq=10".as_bytes().to_owned()];
        let expr = parse_call_expr(
            &mut cli_args_into_arguments_iter(src.iter().map(|v| &**v)),
            DEFAULT_SCOPE_ID,
        )
        .unwrap()
        .unwrap();
        let cli_arg = Span::from_single_arg(0, 6);
        assert!(!expr.append_mode);
        assert_eq!(
            expr.arg,
            Argument {
                value: FieldValue::Array(Array::Argument(vec![
                    Argument {
                        value: FieldValue::Text("seq".to_string()),
                        span: cli_arg.reoffset(0, 3),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    },
                    Argument {
                        value: FieldValue::Text("10".into()),
                        span: cli_arg.reoffset(4, 6),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    }
                ])),
                span: cli_arg,
                source_scope: DEFAULT_SCOPE_ID,
                end_kind: Some(CallExprEndKind::Inline)
            },
        )
    }

    #[test]
    fn listified_value_parsed_as_int() {
        let src = ["[", "seq", "10", "]"];
        let expr = parse_call_expr(
            &mut cli_args_into_arguments_iter(
                src.into_iter().map(str::as_bytes),
            ),
            DEFAULT_SCOPE_ID,
        )
        .unwrap()
        .unwrap();
        assert!(!expr.append_mode);
        assert_eq!(
            expr.arg,
            Argument {
                value: FieldValue::Array(Array::Argument(vec![
                    Argument {
                        value: FieldValue::Text("seq".to_string()),
                        span: Span::from_single_arg(1, 3),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    },
                    Argument {
                        value: FieldValue::Int(10),
                        span: Span::from_single_arg(2, 2),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    }
                ])),
                span: Span::from_cli_arg(0, 4, 0, 1),
                source_scope: DEFAULT_SCOPE_ID,
                end_kind: Some(CallExprEndKind::End(Span::from_single_arg(
                    3, 1
                )))
            },
        )
    }

    #[test]
    fn test_parse_call_expr_separate_args() {
        let src = ["seq:-a:3=5", "-b", "-c:5", "=10", "asdf", "end"];
        let expr = parse_call_expr(
            &mut cli_args_into_arguments_iter(
                src.into_iter().map(str::as_bytes),
            ),
            DEFAULT_SCOPE_ID,
        )
        .unwrap()
        .unwrap();
        assert!(!expr.append_mode);
        assert_eq!(
            expr.arg,
            Argument {
                value: FieldValue::Array(Array::Argument(vec![
                    Argument {
                        value: FieldValue::Text("seq".to_string()),
                        span: Span::from_single_arg(0, 3),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    },
                    Argument {
                        value: FieldValue::Object(Box::new(
                            Object::KeysStored(indexmap! {
                                "-a".into() => FieldValue::Argument(Box::new(Argument {
                                    value: FieldValue::Int(3),
                                    span: Span::from_single_arg_with_offset(0, 4, 8),
                                    source_scope: DEFAULT_SCOPE_ID,
                                    end_kind: None
                                })),
                                "-b".into() => FieldValue::Argument(Box::new(Argument {
                                    value: FieldValue::Null,
                                    span: Span::from_single_arg(1, 2),
                                    source_scope: DEFAULT_SCOPE_ID,
                                    end_kind: None
                                })),
                                "-c".into() => FieldValue::Argument(Box::new(Argument {
                                    value: FieldValue::Int(5),
                                    span: Span::from_single_arg(2, 4),
                                    source_scope: DEFAULT_SCOPE_ID,
                                    end_kind: None
                                })),
                            })
                        )),
                        span: Span::Generated,
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    },
                    Argument {
                        value: FieldValue::Text("5".into()),
                        span: Span::from_single_arg_with_offset(0, 9, 10),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    },
                    Argument {
                        value: FieldValue::Text("10".into()),
                        span: Span::from_single_arg(3, 3),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: None
                    },
                    Argument {
                        value: FieldValue::Array(Array::Argument(vec![
                            Argument {
                                value: FieldValue::Text("asdf".into()),
                                span: Span::from_single_arg(4, 4),
                                source_scope: DEFAULT_SCOPE_ID,
                                end_kind: None
                            }
                        ])),
                        span: Span::from_single_arg(4, 4),
                        source_scope: DEFAULT_SCOPE_ID,
                        end_kind: Some(CallExprEndKind::Inline),
                    }
                ])),
                span: Span::from_cli_arg(0, 6, 0, 3),
                source_scope: DEFAULT_SCOPE_ID,
                end_kind: Some(CallExprEndKind::End(Span::from_single_arg(
                    5, 3
                )))
            },
        )
    }
}
