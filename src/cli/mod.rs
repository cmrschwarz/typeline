use crate::{
    chain::BufferingMode,
    operators::{
        call::parse_op_call,
        call_concurrent::parse_op_call_concurrent,
        count::parse_op_count,
        errors::OperatorCreationError,
        file_reader::{argument_matches_op_file_reader, parse_op_file_reader},
        fork::parse_op_fork,
        format::parse_op_format,
        join::{argument_matches_op_join, parse_op_join},
        key::parse_op_key,
        literal::{argument_matches_op_literal, parse_op_literal},
        next::parse_op_next,
        operator::OperatorData,
        print::parse_op_print,
        regex::{parse_op_regex, RegexOptions},
        select::parse_op_select,
        sequence::parse_op_seq,
        up::parse_op_up,
    },
    options::{
        argument::CliArgIdx, operator_base_options::OperatorBaseOptions,
        session_options::SessionOptions,
    },
    scr_error::{ContextualizedScrError, ReplDisabledError, ScrError},
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
    utils::int_units::parse_int_with_units_from_bytes,
};
use bstr::ByteSlice;

use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};

use std::{borrow::Cow, fmt::Display, str::from_utf8};
use thiserror::Error;

#[derive(Error, Debug, Clone)]
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

#[derive(Error, Debug, Clone)]
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
            PrintInfoAndExitError::Help(help_text) => f.write_str(&help_text),
            PrintInfoAndExitError::Version => print_version(f),
        }
    }
}

#[derive(Error, Debug, Clone)]
pub struct MissingArgumentsError;

impl Display for MissingArgumentsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "missing arguments, consider supplying --help")
    }
}

#[derive(Clone, Debug)]
pub struct CliArgument<'a> {
    pub idx: CliArgIdx,
    pub value: &'a Vec<u8>,
}

#[derive(Clone)]
pub struct ParsedCliArgument<'a> {
    argname: &'a str,
    value: Option<&'a [u8]>,
    label: Option<&'a str>,
    cli_arg: CliArgument<'a>,
    append_mode: bool,
    transparent_mode: bool,
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
        r#"^(?<label>\p{XID_Start}\p{XID_Continue}*):$"#
    ).build().unwrap();
    static ref CLI_ARG_REGEX: regex::bytes::Regex = regex::bytes::RegexBuilder::new(
        r#"^(?<modes>(?:(?<append_mode>\+)|(?<transparent_mode>_))*)(?<argname>[^@=]+)(@(?<label>[^@=]+))?(=(?<value>(?:.|[\r\n])*))?$"#
    ).build()
    .unwrap();


    static ref REGEX_CLI_ARG_REGEX: Regex =
        RegexBuilder::new("^r((?<a>a)|(?<b>b)|(?<d>d)|(?<i>i)|(?<l>l)|(?<m>m)|(?<o>o)|(?<u>u))*$")
            .case_insensitive(true)
            .build()
            .unwrap();

    //static ref REGEX_CLI_ARG_REGEX: Regex =
    //    RegexBuilder::new("^r((?<i>i))*$")
    //        .case_insensitive(true)
    //        .build()
    //        .unwrap();
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

fn try_parse_selenium_variant(
    _value: Option<&[u8]>,
    _cli_arg: &CliArgument,
) -> Result<Option<SeleniumVariant>, CliArgumentError> {
    todo!()
}
fn try_parse_selenium_download_strategy(
    _value: Option<&[u8]>,
    _cli_arg: &CliArgument,
) -> Result<SeleniumDownloadStrategy, CliArgumentError> {
    todo!()
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
            format!("failed to parse as integer: {msg}"),
            cli_arg_idx,
        )),
    }
}

fn print_version(f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    write!(f, "scr {}", VERSION)?;
    Ok(())
}

fn try_parse_as_context_opt(
    ctx_opts: &mut SessionOptions,
    arg: &ParsedCliArgument,
    allow_repl: bool,
) -> Result<bool, ScrError> {
    let mut matched = false;
    let arg_idx = Some(arg.cli_arg.idx);
    if ["--version", "-v"].contains(&&*arg.argname) {
        return Err(PrintInfoAndExitError::Version.into());
    }
    if ["--help", "-h", "help", "h"].contains(&&*arg.argname) {
        let text = if let Some(v) = arg.value {
            let section = v.to_str_lossy();
            match section.as_ref() {
                "f" => include_str!("help_sections/format.txt"),
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
            include_str!("help_sections/main.txt")
        };

        return Err(PrintInfoAndExitError::Help(text.into()).into());
    }
    if arg.argname == "j" {
        if let Some(val) = arg.value.as_deref() {
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
        matched = true
    }
    if arg.argname == "repl" {
        let enabled =
            try_parse_bool_arg_or_default(arg.value, true, arg.cli_arg.idx)?;
        if !allow_repl && enabled {
            return Err(ReplDisabledError {
                cli_arg_idx: Some(arg.cli_arg.idx),
                message: "REPL mode is not allowed",
            }
            .into());
        }
        ctx_opts.repl.set(enabled, arg_idx)?;
        matched = true
    }
    if arg.argname == "exit" {
        if !allow_repl {
            return Err(ReplDisabledError {
                cli_arg_idx: Some(arg.cli_arg.idx),
                message: "exit cannot be requested outside of repl mode",
            }
            .into());
        }
        let enabled =
            try_parse_bool_arg_or_default(arg.value, true, arg.cli_arg.idx)?;
        ctx_opts.exit_repl.set(enabled, arg_idx)?;
        matched = true
    }
    if matched {
        if arg.label.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify label for global argument",
                arg.cli_arg.idx,
            )
            .into());
        }
    }
    return Ok(matched);
}

fn try_parse_as_chain_opt(
    ctx_opts: &mut SessionOptions,
    arg: &ParsedCliArgument,
) -> Result<bool, ScrError> {
    let chain = &mut ctx_opts.chains[ctx_opts.curr_chain as usize];
    let arg_idx = Some(arg.cli_arg.idx);
    match arg.argname {
        "sel" => {
            let sv = try_parse_selenium_variant(
                arg.value.as_deref(),
                &arg.cli_arg,
            )?;
            chain.selenium_variant.set(sv, arg_idx)?;
            return Ok(true);
        }
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
                arg.value.as_deref(),
                true,
                arg.cli_arg.idx,
            )?;
            chain.prefer_parent_text_encoding.set(ppte, arg_idx)?;
        }
        "fenc" => {
            let fte = try_parse_bool_arg_or_default(
                arg.value.as_deref(),
                true,
                arg.cli_arg.idx,
            )?;
            chain.force_text_encoding.set(fte, arg_idx)?;
        }
        "sds" => {
            let sds = try_parse_selenium_download_strategy(
                arg.value.as_deref(),
                &arg.cli_arg,
            )?;
            chain.selenium_download_strategy.set(sds, arg_idx)?;
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
            let buffering_mode = if let Some(val) = arg.value.as_deref() {
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
                            message: Cow::Owned(format!("unknown line buffering mode '{}', options are yes, no, stdin, tty, and stdin-if-tty", val.to_str_lossy())),
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

fn parse_operation(
    argname: &str,
    value: Option<&[u8]>,
    idx: Option<CliArgIdx>,
) -> Result<Option<OperatorData>, OperatorCreationError> {
    if let Some(c) = REGEX_CLI_ARG_REGEX.captures(argname) {
        let mut opts = RegexOptions::default();
        let mut unicode_mode = false;
        if c.name("a").is_some() {
            opts.ascii_mode = true;
        }
        if c.name("b").is_some() {
            opts.binary_mode = true;
        }
        if c.name("d").is_some() {
            opts.dotall = true;
        }
        if c.name("i").is_some() {
            opts.case_insensitive = true;
        }
        if c.name("l").is_some() {
            opts.line_based = true;
        }
        if c.name("m").is_some() {
            opts.multimatch = true;
        }
        if c.name("n").is_some() {
            opts.non_mandatory = true;
        }
        if c.name("o").is_some() {
            opts.overlapping = true;
        }
        if c.name("u").is_some() {
            unicode_mode = true;
        }

        if opts.ascii_mode && unicode_mode {
            return Err(OperatorCreationError::new(
                "[a]scii and [u]nicode mode on regex are mutually exclusive",
                idx,
            ));
        }
        if opts.binary_mode && !unicode_mode {
            opts.ascii_mode = true;
        }
        return Ok(Some(OperatorData::Regex(parse_op_regex(
            value, idx, opts,
        )?)));
    }

    if argument_matches_op_literal(argname) {
        return Ok(Some(parse_op_literal(argname, value, idx)?));
    }
    if argument_matches_op_file_reader(argname) {
        return Ok(Some(parse_op_file_reader(argname, value, idx)?));
    }
    if argument_matches_op_join(argname) {
        return Ok(Some(parse_op_join(argname, value, idx)?));
    }
    Ok(match argname {
        "p" => Some(parse_op_print(value, idx)?),
        "f" => Some(parse_op_format(value, idx)?),
        "key" => Some(parse_op_key(value, idx)?),
        "select" => Some(parse_op_select(value, idx)?),
        "seq" => Some(parse_op_seq(value, false, false, idx)?),
        "seqn" => Some(parse_op_seq(value, false, true, idx)?),
        "enum" => Some(parse_op_seq(value, true, false, idx)?),
        "enumn" => Some(parse_op_seq(value, true, true, idx)?),
        "count" => Some(parse_op_count(value, idx)?),

        "fork" => Some(parse_op_fork(value, idx)?),
        "call" => Some(parse_op_call(value, idx)?),
        "callcc" => Some(parse_op_call_concurrent(value, idx)?),
        "next" => Some(parse_op_next(value, idx)?),
        "up" => Some(parse_op_up(value, idx)?),
        _ => None,
    })
}

fn try_parse_as_operation<'a>(
    ctx_opts: &mut SessionOptions,
    arg: ParsedCliArgument<'a>,
) -> Result<Option<ParsedCliArgument<'a>>, CliArgumentError> {
    let op_data =
        parse_operation(&arg.argname, arg.value, Some(arg.cli_arg.idx))
            .map_err(|oce| CliArgumentError {
                message: oce.message,
                cli_arg_idx: arg.cli_arg.idx,
            })?;
    if let Some(op_data) = op_data {
        let argname = ctx_opts.string_store.intern_cloned(arg.argname);
        let label = arg.label.map(|l| ctx_opts.string_store.intern_cloned(l));
        ctx_opts.add_op(
            OperatorBaseOptions::new(
                argname,
                label,
                arg.append_mode,
                arg.transparent_mode,
                Some(arg.cli_arg.idx),
            ),
            op_data,
        );
        Ok(None)
    } else {
        Ok(Some(arg))
    }
}

pub fn parse_cli_retain_args(
    args: &Vec<Vec<u8>>,
    allow_repl: bool,
) -> Result<SessionOptions, ScrError> {
    if args.is_empty() {
        return Err(MissingArgumentsError.into());
    }
    let mut ctx_opts = SessionOptions::default();
    for (i, arg_str) in args.iter().enumerate() {
        let cli_arg = CliArgument {
            idx: i as CliArgIdx + 1,
            value: arg_str,
        };
        if let Some(m) = LABEL_REGEX.captures(&arg_str) {
            ctx_opts.add_label(
                m.name("label")
                    .unwrap()
                    .as_bytes()
                    .to_str()
                    .unwrap() // we know this is valid utf-8 because of the regex match
                    .to_owned(),
            );
            continue;
        }
        if let Some(m) = CLI_ARG_REGEX.captures(&arg_str) {
            let argname = from_utf8(m.name("argname").unwrap().as_bytes())
                .map_err(|_| {
                    CliArgumentError::new(
                        "argument name must be valid UTF-8",
                        cli_arg.idx,
                    )
                })?;
            if let Some(modes) = m.name("modes") {
                let modes_str = modes.as_bytes();
                if modes_str.len() >= 2
                    || (modes_str.len() == 2 && modes_str[0] == modes_str[1])
                {
                    return Err(CliArgumentError::new(
                        "operator modes cannot be specified twice",
                        cli_arg.idx,
                    )
                    .into());
                }
            }
            let label = if let Some(lbl) = m.name("label") {
                Some(from_utf8(lbl.as_bytes()).map_err(|_| {
                    CliArgumentError::new(
                        "label must be valid UTF-8",
                        cli_arg.idx,
                    )
                })?)
            } else {
                None
            };

            let arg = ParsedCliArgument {
                argname,
                value: m.name("value").map(|v| <&[u8]>::from(v.as_bytes())),
                label: label,
                cli_arg: cli_arg,
                append_mode: m.name("append_mode").is_some(),
                transparent_mode: m.name("transparent_mode").is_some(),
            };
            if try_parse_as_context_opt(&mut ctx_opts, &arg, allow_repl)? {
                continue;
            }
            if try_parse_as_chain_opt(&mut ctx_opts, &arg)? {
                continue;
            }
            if let Some(arg) = try_parse_as_operation(&mut ctx_opts, arg)? {
                return Err(CliArgumentError {
                    message: format!("unknown operator '{}'", arg.argname)
                        .into(),
                    cli_arg_idx: arg.cli_arg.idx,
                }
                .into());
            }
        } else {
            return Err(CliArgumentError::new(
                "invalid argument syntax",
                cli_arg.idx,
            )
            .into());
        }
    }
    return Ok(ctx_opts);
}
pub fn parse_cli_raw(
    args: Vec<Vec<u8>>,
    allow_repl: bool,
) -> Result<SessionOptions, (Vec<Vec<u8>>, ScrError)> {
    match parse_cli_retain_args(&args, allow_repl) {
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
) -> Result<SessionOptions, ContextualizedScrError> {
    parse_cli_raw(args, allow_repl).map_err(|(args, err)| {
        ContextualizedScrError::from_scr_error(err, Some(&args), None, None)
    })
}

pub fn collect_env_args() -> Result<Vec<Vec<u8>>, CliArgumentError> {
    #[cfg(unix)]
    {
        Ok(std::env::args_os()
            .skip(1)
            .map(|s| std::os::unix::prelude::OsStringExt::into_vec(s).into())
            .collect::<Vec<Vec<u8>>>())
    }
    #[cfg(windows)]
    {
        let args = Vec::new();
        for (i, arg) in std::env::args_os().skip(1).enumerate() {
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
) -> Result<SessionOptions, ContextualizedScrError> {
    let args = collect_env_args().map_err(|e| {
        ContextualizedScrError::from_scr_error(e.into(), None, None, None)
    })?;
    parse_cli(args, allow_repl)
}
