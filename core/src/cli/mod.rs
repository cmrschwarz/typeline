use crate::{
    chain::BufferingMode,
    extension::ExtensionRegistry,
    operators::{
        aggregator::{
            create_op_aggregate, create_op_aggregator_append_leader,
        },
        call::parse_op_call,
        call_concurrent::parse_op_call_concurrent,
        cast::{argument_matches_op_cast, parse_op_cast},
        count::parse_op_count,
        end::parse_op_end,
        errors::OperatorCreationError,
        file_reader::{argument_matches_op_file_reader, parse_op_file_reader},
        foreach::parse_op_foreach,
        fork::parse_op_fork,
        forkcat::parse_op_forkcat,
        format::parse_op_format,
        join::{argument_matches_op_join, parse_op_join},
        key::parse_op_key,
        literal::{argument_matches_op_literal, parse_op_literal},
        next::parse_op_next,
        nop::parse_op_nop,
        nop_copy::parse_op_nop_copy,
        operator::{OperatorData, OperatorId},
        print::parse_op_print,
        regex::{parse_op_regex, try_match_regex_cli_argument},
        select::parse_op_select,
        sequence::{parse_op_seq, OpSequenceMode},
    },
    options::{
        argument::CliArgIdx, operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions,
    },
    scr_error::{ContextualizedScrError, ReplDisabledError, ScrError},
    utils::int_string_conversions::{
        parse_int_with_units, parse_int_with_units_from_bytes,
    },
};
use bstr::ByteSlice;

use lazy_static::lazy_static;
use num::{FromPrimitive, PrimInt};

use std::{
    borrow::Cow,
    fmt::Display,
    str::{from_utf8, FromStr},
    sync::Arc,
};
use thiserror::Error;

#[must_use]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("in cli arg {cli_arg_idx}: {message}")]
pub struct CliArgumentError {
    pub message: Cow<'static, str>,
    pub cli_arg_idx: CliArgIdx,
}

impl CliArgumentError {
    pub fn new(message: &'static str, cli_arg_idx: CliArgIdx) -> Self {
        Self {
            message: Cow::Borrowed(message),
            cli_arg_idx,
        }
    }
    pub fn new_s(message: String, cli_arg_idx: CliArgIdx) -> Self {
        Self {
            message: Cow::Owned(message),
            cli_arg_idx,
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

#[derive(Clone, Debug)]
pub struct CliArgument<'a> {
    pub idx: CliArgIdx,
    pub value: &'a [u8],
}

#[derive(Clone)]
pub struct ParsedCliArgumentParts<'a> {
    pub argname: &'a str,
    pub value: Option<&'a [u8]>,
    pub label: Option<&'a str>,
    pub cli_arg: CliArgument<'a>,
    pub append_mode: bool,
    pub transparent_mode: bool,
}

lazy_static! {
    static ref TRUTHY_REGEX: regex::bytes::Regex =
        regex::bytes::RegexBuilder::new("^true|tru|tr|t|yes|ye|y|1$")
            .case_insensitive(true)
            .build()
            .unwrap();
    static ref FALSY_REGEX: regex::bytes::Regex =
        regex::bytes::RegexBuilder::new("^false|fal|fa|f|no|n|0$")
            .case_insensitive(true)
            .build()
            .unwrap();
    static ref LABEL_REGEX: regex::bytes::Regex = regex::bytes::RegexBuilder::new(
        r"^(?<label>\p{XID_Start}\p{XID_Continue}*):$"
    ).build().unwrap();
    static ref CLI_ARG_REGEX: regex::bytes::Regex = regex::bytes::RegexBuilder::new(
        r"^(?<modes>(?:(?<append_mode>\+)|(?<transparent_mode>_))*)(?<argname>[^@=]+)(@(?<label>[^@=]+))?(=(?<value>(?:.|[\r\n])*))?$"
    ).build()
    .unwrap();
}

pub fn reject_operator_argument(
    argname: &str,
    value: Option<&[u8]>,
    cli_arg_idx: Option<CliArgIdx>,
) -> Result<(), OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new_s(
            format!("operator `{argname}` does not take an argument"),
            cli_arg_idx,
        ));
    }
    Ok(())
}

pub fn parse_arg_value_as_str<'a>(
    argname: &str,
    value: Option<&'a [u8]>,
    cli_arg_idx: Option<CliArgIdx>,
) -> Result<&'a str, OperatorCreationError> {
    let Some(value) = value else {
        return Err(OperatorCreationError::new_s(
            format!("missing argument for operator `{argname}`"),
            cli_arg_idx,
        ));
    };
    let value_str = value.to_str().map_err(|_| {
        OperatorCreationError::new_s(
            format!("failed to parse `{argname}` parameter (invalid utf-8)",),
            cli_arg_idx,
        )
    })?;
    Ok(value_str)
}

pub fn parse_arg_value_as_number<I>(
    argname: &str,
    value: Option<&[u8]>,
    cli_arg_idx: Option<CliArgIdx>,
) -> Result<I, OperatorCreationError>
where
    I: PrimInt
        + Display
        + FromPrimitive
        + FromStr<Err = std::num::ParseIntError>,
{
    let value_str =
        parse_arg_value_as_str(argname, value, cli_arg_idx)?.trim();
    let v = parse_int_with_units::<I>(value_str).map_err(|msg| {
        OperatorCreationError::new_s(
            format!(
                "failed to parse `{argname}` parameter as an integer: {msg}",
            ),
            cli_arg_idx,
        )
    })?;
    Ok(v)
}

impl ParsedCliArgumentParts<'_> {
    pub fn reject_value(&self) -> Result<(), OperatorCreationError> {
        reject_operator_argument(
            self.argname,
            self.value,
            Some(self.cli_arg.idx),
        )
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

fn try_parse_bool_arg_or_default(
    val: Option<&[u8]>,
    default: bool,
    cli_arg_idx: CliArgIdx,
) -> Result<bool, CliArgumentError> {
    if let Some(val) = val {
        if let Some(b) = try_parse_bool(val) {
            Ok(b)
        } else {
            Err(CliArgumentError::new(
                "failed to parse as bool",
                cli_arg_idx,
            ))
        }
    } else {
        Ok(default)
    }
}
fn try_parse_usize_arg(
    val: &[u8],
    cli_arg_idx: CliArgIdx,
) -> Result<usize, CliArgumentError> {
    match parse_int_with_units_from_bytes::<usize>(val) {
        Ok(v) => Ok(v),
        Err(msg) => Err(CliArgumentError::new_s(
            format!("failed to parse as an integer: {msg}"),
            cli_arg_idx,
        )),
    }
}

fn print_version(f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    write!(f, "scr {VERSION}")?;
    Ok(())
}

fn try_parse_as_context_opt(
    ctx_opts: &mut SessionOptions,
    arg: &ParsedCliArgumentParts,
) -> Result<bool, ScrError> {
    const MAIN_HELP_PAGE: &str = include_str!("help_sections/main.txt");
    let mut matched = false;
    let arg_idx = Some(arg.cli_arg.idx);
    if ["--version", "-v"].contains(&arg.argname) {
        return Err(PrintInfoAndExitError::Version.into());
    }
    if ["--help", "-h", "help", "h"].contains(&arg.argname) {
        let text = if let Some(v) = arg.value {
            let section = String::from_utf8_lossy(v);
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
                    return Err(CliArgumentError {
                        message: format!("no help section for '{section}'")
                            .into(),
                        cli_arg_idx: arg.cli_arg.idx,
                    }
                    .into())
                }
            }
        } else {
            MAIN_HELP_PAGE
        };

        return Err(PrintInfoAndExitError::Help(text.into()).into());
    }
    if arg.argname == "tc" {
        if let Some(val) = arg.value {
            ctx_opts
                .max_threads
                .set(try_parse_usize_arg(val, arg.cli_arg.idx)?, arg_idx)?;
        } else {
            return Err(CliArgumentError::new(
                "missing thread count argument",
                arg.cli_arg.idx,
            )
            .into());
        }
        matched = true;
    }
    if arg.argname == "repl" {
        let enabled =
            try_parse_bool_arg_or_default(arg.value, true, arg.cli_arg.idx)?;
        if !ctx_opts.allow_repl && enabled {
            return Err(ReplDisabledError {
                cli_arg_idx: Some(arg.cli_arg.idx),
                message: "REPL mode is not allowed",
            }
            .into());
        }
        ctx_opts.repl.set(enabled, arg_idx)?;
        matched = true;
    }
    if arg.argname == "exit" {
        if !ctx_opts.allow_repl {
            return Err(ReplDisabledError {
                cli_arg_idx: Some(arg.cli_arg.idx),
                message: "exit cannot be requested outside of repl mode",
            }
            .into());
        }
        let enabled =
            try_parse_bool_arg_or_default(arg.value, true, arg.cli_arg.idx)?;
        ctx_opts.exit_repl.set(enabled, arg_idx)?;
        matched = true;
    }
    if matched && arg.label.is_some() {
        return Err(CliArgumentError::new(
            "cannot specify label for global argument",
            arg.cli_arg.idx,
        )
        .into());
    }
    Ok(matched)
}

fn try_parse_as_chain_opt(
    ctx_opts: &mut SessionOptions,
    arg: &ParsedCliArgumentParts,
) -> Result<bool, ScrError> {
    let chain = &mut ctx_opts.chains[ctx_opts.curr_chain as usize];
    let arg_idx = Some(arg.cli_arg.idx);
    match arg.argname {
        "denc" => {
            if let Some(_val) = &arg.value {
                todo!("parse text encoding");
            } else {
                return Err(CliArgumentError::new(
                    "missing argument for default text encoding",
                    arg.cli_arg.idx,
                )
                .into());
            }
        }
        "ppenc" => {
            let ppte = try_parse_bool_arg_or_default(
                arg.value,
                true,
                arg.cli_arg.idx,
            )?;
            chain.prefer_parent_text_encoding.set(ppte, arg_idx)?;
        }
        "fenc" => {
            let fte = try_parse_bool_arg_or_default(
                arg.value,
                true,
                arg.cli_arg.idx,
            )?;
            chain.force_text_encoding.set(fte, arg_idx)?;
        }
        "fpm" => {
            let fpm = try_parse_bool_arg_or_default(
                arg.value,
                true,
                arg.cli_arg.idx,
            )?;
            chain.floating_point_math.set(fpm, arg_idx)?;
        }
        "prr" => {
            let prr = try_parse_bool_arg_or_default(
                arg.value,
                true,
                arg.cli_arg.idx,
            )?;
            chain.print_rationals_raw.set(prr, arg_idx)?;
        }
        "bs" => {
            if let Some(val) = arg.value {
                let bs = try_parse_usize_arg(val, arg.cli_arg.idx)?;
                chain.default_batch_size.set(bs, arg_idx)?;
            } else {
                return Err(CliArgumentError::new(
                    "missing argument for batch size",
                    arg.cli_arg.idx,
                )
                .into());
            }
        }
        "sbs" => {
            if let Some(val) = arg.value {
                let bs = try_parse_usize_arg(val, arg.cli_arg.idx)?;
                chain.stream_buffer_size.set(bs, arg_idx)?;
            } else {
                return Err(CliArgumentError::new(
                    "missing argument for stream buffer size",
                    arg.cli_arg.idx,
                )
                .into());
            }
        }
        "sst" => {
            if let Some(val) = arg.value {
                let bs = try_parse_usize_arg(val, arg.cli_arg.idx)?;
                chain.stream_size_threshold.set(bs, arg_idx)?;
            } else {
                return Err(CliArgumentError::new(
                    "missing argument for stream size threshold",
                    arg.cli_arg.idx,
                )
                .into());
            }
        }
        "lb" => {
            let buffering_mode = if let Some(val) = arg.value {
                if let Some(v) = try_parse_bool(val) {
                    if v {
                        BufferingMode::LineBuffer
                    } else {
                        BufferingMode::BlockBuffer
                    }
                } else {
                    let res = if let Ok(val) = val.to_str() {
                        match val {
                            "stdin" => Some(BufferingMode::LineBufferStdin),
                            "tty" => Some(BufferingMode::LineBufferIfTTY),
                            "stdin-if-tty" => {
                                Some(BufferingMode::LineBufferStdinIfTTY)
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    if let Some(bm) = res {
                        bm
                    } else {
                        return Err(CliArgumentError{
                            message: Cow::Owned(format!("unknown line buffering mode '{}', options are yes, no, stdin, tty, and stdin-if-tty", String::from_utf8_lossy(val))),
                            cli_arg_idx: arg.cli_arg.idx
                        }.into());
                    }
                }
            } else {
                BufferingMode::LineBuffer
            };
            chain.buffering_mode.set(buffering_mode, arg_idx)?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn try_parse_operator_data(
    ctx_opts: &mut SessionOptions,
    arg: &ParsedCliArgumentParts,
    args: &[Vec<u8>],
    next_arg_idx: &mut usize,
) -> Result<Option<OperatorData>, OperatorCreationError> {
    let idx = Some(arg.cli_arg.idx);
    if let Some(opts) = try_match_regex_cli_argument(arg.argname, idx)? {
        return Ok(Some(parse_op_regex(arg.value, idx, opts)?));
    }
    if argument_matches_op_cast(arg.argname, arg.value) {
        return Ok(Some(parse_op_cast(arg.argname, arg.value, idx)?));
    }
    if argument_matches_op_literal(arg.argname) {
        return Ok(Some(parse_op_literal(
            arg.argname,
            arg.value,
            idx,
            ctx_opts,
        )?));
    }
    if argument_matches_op_file_reader(arg.argname) {
        return Ok(Some(parse_op_file_reader(arg.argname, arg.value, idx)?));
    }
    if argument_matches_op_join(arg.argname) {
        return Ok(Some(parse_op_join(arg.argname, arg.value, idx)?));
    }
    if let Some(op) = match arg.argname {
        "print" | "p" => Some(parse_op_print(arg.value, idx)?),
        "format" | "f" => Some(parse_op_format(arg.value, idx)?),
        "key" => Some(parse_op_key(arg.value, idx)?),
        "select" => Some(parse_op_select(arg.value, idx)?),
        "seq" => Some(parse_op_seq(arg, OpSequenceMode::Sequence, false)?),
        "seqn" => Some(parse_op_seq(arg, OpSequenceMode::Sequence, true)?),
        "enum" => Some(parse_op_seq(arg, OpSequenceMode::Enum, false)?),
        "enumn" => Some(parse_op_seq(arg, OpSequenceMode::Enum, true)?),
        "enum-u" => {
            Some(parse_op_seq(arg, OpSequenceMode::EnumUnbounded, false)?)
        }
        "enumn-u" => {
            Some(parse_op_seq(arg, OpSequenceMode::EnumUnbounded, true)?)
        }
        "count" => Some(parse_op_count(arg.value, idx)?),
        "nop" | "scr" => Some(parse_op_nop(arg.value, idx)?),
        "nop-c" => Some(parse_op_nop_copy(arg.value, idx)?),
        "fork" => Some(parse_op_fork(arg.value, idx)?),
        "foreach" | "fe" => Some(parse_op_foreach(arg.value, idx)?),
        "forkcat" | "fc" => Some(parse_op_forkcat(arg.value, idx)?),
        "call" | "c" => Some(parse_op_call(arg.value, idx)?),
        "callcc" | "cc" => Some(parse_op_call_concurrent(arg.value, idx)?),
        "next" | "n" => Some(parse_op_next(arg.value, idx)?),
        "end" | "e" => Some(parse_op_end(arg.value, idx)?),
        _ => None,
    } {
        return Ok(Some(op));
    }
    for e in &ctx_opts.extensions.extensions {
        if let Some(op) =
            e.try_match_cli_argument(ctx_opts, arg, args, next_arg_idx)?
        {
            return Ok(Some(op));
        }
    }
    Ok(None)
}
pub fn add_op_from_arg_and_op_data_uninit(
    ctx_opts: &mut SessionOptions,
    arg: &ParsedCliArgumentParts,
    op_data: OperatorData,
) -> OperatorId {
    let argname = ctx_opts.string_store.intern_cloned(arg.argname);
    let label = arg.label.map(|l| ctx_opts.string_store.intern_cloned(l));
    ctx_opts.add_op_uninit(
        OperatorBaseOptions::new(
            argname,
            label,
            arg.transparent_mode,
            Some(arg.cli_arg.idx),
        ),
        op_data,
    )
}
pub fn parse_cli_argument_parts(
    arg: CliArgument,
) -> Result<ParsedCliArgumentParts, CliArgumentError> {
    let Some(arg_match) = CLI_ARG_REGEX.captures(arg.value) else {
        return Err(CliArgumentError::new("invalid argument syntax", arg.idx));
    };
    let argname = from_utf8(arg_match.name("argname").unwrap().as_bytes())
        .map_err(|_| {
            CliArgumentError::new("argument name must be valid UTF-8", arg.idx)
        })?;
    if let Some(modes) = arg_match.name("modes") {
        let modes_str = modes.as_bytes();
        if modes_str.len() >= 2
            || (modes_str.len() == 2 && modes_str[0] == modes_str[1])
        {
            return Err(CliArgumentError::new(
                "operator modes cannot be specified twice",
                arg.idx,
            ));
        }
    }
    let label = if let Some(lbl) = arg_match.name("label") {
        Some(from_utf8(lbl.as_bytes()).map_err(|_| {
            CliArgumentError::new("label must be valid UTF-8", arg.idx)
        })?)
    } else {
        None
    };
    Ok(ParsedCliArgumentParts {
        argname,
        value: arg_match.name("value").map(|v| v.as_bytes()),
        label,
        cli_arg: arg,
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
    allow_repl: bool,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, ScrError> {
    assert!(
        !allow_repl || cfg!(feature = "repl"),
        "the 'repl' feature of this crate is disabled"
    );

    if args.is_empty() {
        return Err(MissingArgumentsError.into());
    }
    let mut ctx_opts = SessionOptions {
        allow_repl,
        extensions,
        ..Default::default()
    };
    let mut arg_idx = 1; // skip executable name
    let mut curr_aggregate = Vec::new();
    let mut last_non_append_op_id = None;
    let mut curr_op_appendable = true;
    while arg_idx < args.len() {
        let arg_str = &args[arg_idx];
        arg_idx += 1;

        let cli_arg = CliArgument {
            idx: arg_idx as CliArgIdx + 1,
            value: arg_str,
        };

        if try_parse_label(&mut ctx_opts, arg_str) {
            continue;
        }

        let arg = parse_cli_argument_parts(cli_arg)?;
        if let Some(op_data) =
            try_parse_operator_data(&mut ctx_opts, &arg, args, &mut arg_idx)?
        {
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
            message: format!("unknown operator '{}'", arg.argname).into(),
            cli_arg_idx: arg.cli_arg.idx,
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
    Ok(ctx_opts)
}

pub fn parse_cli_raw(
    args: Vec<Vec<u8>>,
    allow_repl: bool,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, (Vec<Vec<u8>>, ScrError)> {
    match parse_cli_retain_args(&args, allow_repl, extensions) {
        Ok(mut ctx) => {
            ctx.cli_args = Some(args);
            Ok(ctx)
        }
        Err(e) => Err((args, e)),
    }
}

pub fn parse_cli(
    args: Vec<Vec<u8>>,
    allow_repl: bool,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, ContextualizedScrError> {
    parse_cli_raw(args, allow_repl, extensions).map_err(|(args, err)| {
        ContextualizedScrError::from_scr_error(err, Some(&args), None, None)
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
        let args = Vec::new();
        for (i, arg) in std::env::args_os().enumerate() {
            if let (Some(arg)) = arg.to_str() {
                args.push(Vec::<u8>::from(arg));
            } else {
                return Err(CliArgumentError::new(
                    "failed to parse byte sequence as unicode".to_owned(),
                    CliArgument {
                        arg_index: i + 1,
                        arg_str: Vec::<u8>::from(
                            arg.to_string_lossy().as_bytes(),
                        ),
                    },
                ));
            }
        }
        Ok(args)
    }
}

pub fn parse_cli_from_env(
    allow_repl: bool,
    extensions: Arc<ExtensionRegistry>,
) -> Result<SessionOptions, ContextualizedScrError> {
    let args = collect_env_args().map_err(|e| {
        ContextualizedScrError::from_scr_error(e.into(), None, None, None)
    })?;
    parse_cli(args, allow_repl, extensions)
}
