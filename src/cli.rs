use crate::chain::BufferingMode;
use crate::operations::data_inserter::{parse_op_bytes, parse_op_int, parse_op_str};
use crate::operations::errors::OperatorCreationError;
use crate::operations::file_reader::{parse_op_file, parse_op_stdin};
use crate::operations::format::parse_op_format;
use crate::operations::key::parse_op_key;
use crate::operations::operator::OperatorData;
use crate::operations::print::parse_op_print;
use crate::operations::regex::{parse_op_regex, RegexOptions};
use crate::operations::sequence::{parse_op_seq, SequenceMode};
use crate::operations::split::parse_op_split;
use crate::scr_error::ScrError;
use crate::{
    options::{
        argument::{ArgumentReassignmentError, CliArgIdx},
        chain_options::ChainOptions,
        chain_spec::ChainSpec,
        context_options::ContextOptions,
        operator_base_options::OperatorBaseOptions,
    },
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
};
use bstr::{BStr, BString, ByteSlice};
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
}

impl From<ArgumentReassignmentError> for CliArgumentError {
    fn from(v: ArgumentReassignmentError) -> Self {
        CliArgumentError {
            message: Cow::Borrowed(v.message),
            cli_arg_idx: v.cli_arg_idx.unwrap(),
        }
    }
}

#[derive(Error, Debug, Clone)]
pub enum PrintInfoAndExitError {
    Help,
    Version,
}

impl Display for PrintInfoAndExitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrintInfoAndExitError::Help => print_help(f),
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
    pub value: &'a BString,
}

#[derive(Clone)]
pub struct ParsedCliArgument<'a> {
    argname: &'a str,
    value: Option<&'a BStr>,
    label: Option<&'a str>,
    chainspec: Option<ChainSpec>,
    cli_arg: CliArgument<'a>,
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
    static ref CLI_ARG_REGEX: regex::bytes::Regex = regex::bytes::RegexBuilder::new(
        r#"^(?<argname>[^\s@:=]+)(@(?<label>[^\s@:=]+))?(?<chainspec>:[^\s@:=]+)?(=(?<value>(?:.|[\r\n])*))?$"#
    ).build()
    .unwrap();


    static ref REGEX_CLI_ARG_REGEX: Regex =
        RegexBuilder::new("^r((?<i>i)|(?<m>m)|(?<l>l)|(?<d>d)|(?<b>b)|(?<a>a)|(?<u>u))*$")
            .case_insensitive(true)
            .build()
            .unwrap();

    //static ref REGEX_CLI_ARG_REGEX: Regex =
    //    RegexBuilder::new("^r((?<i>i))*$")
    //        .case_insensitive(true)
    //        .build()
    //        .unwrap();
}

fn try_parse_bool(val: &BStr) -> Option<bool> {
    if TRUTHY_REGEX.is_match(val.as_bytes()) {
        return Some(true);
    }
    if FALSY_REGEX.is_match(val.as_bytes()) {
        return Some(false);
    }
    None
}

fn try_parse_selenium_variant(
    _value: Option<&BStr>,
    _cli_arg: &CliArgument,
) -> Result<Option<SeleniumVariant>, CliArgumentError> {
    todo!()
}
fn try_parse_selenium_download_strategy(
    _value: Option<&BStr>,
    _cli_arg: &CliArgument,
) -> Result<SeleniumDownloadStrategy, CliArgumentError> {
    todo!()
}

fn try_parse_bool_arg_or_default(
    val: Option<&BStr>,
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
fn try_parse_usize_arg(val: &BStr, cli_arg_idx: CliArgIdx) -> Result<usize, CliArgumentError> {
    if let Some(b) = val.to_str().ok().and_then(|v| v.parse::<usize>().ok()) {
        Ok(b)
    } else {
        Err(CliArgumentError::new(
            "failed to parse as unsigned integer",
            cli_arg_idx,
        ))
    }
}

fn print_help(f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "scr [OPTIONS]")?;
    Ok(())
}
fn print_version(f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    write!(f, "scr {}", VERSION)?;
    Ok(())
}

fn try_parse_as_context_opt(
    ctx_opts: &mut ContextOptions,
    arg: &ParsedCliArgument,
) -> Result<bool, ScrError> {
    let mut matched = false;
    if ["--help", "-h"].contains(&&*arg.argname) {
        return Err(PrintInfoAndExitError::Help.into());
    }
    if ["--version", "-v"].contains(&&*arg.argname) {
        return Err(PrintInfoAndExitError::Version.into());
    }
    if arg.argname == "help" {
        let print_help =
            try_parse_bool_arg_or_default(arg.value.as_deref(), true, arg.cli_arg.idx)?;
        if print_help {
            return Err(PrintInfoAndExitError::Help.into());
        }
        matched = true;
    }
    if arg.argname == "version" {
        let print_version =
            try_parse_bool_arg_or_default(arg.value.as_deref(), true, arg.cli_arg.idx)?;
        if print_version {
            return Err(PrintInfoAndExitError::Version.into());
        }
        matched = true;
    }
    if arg.argname == "j" {
        if let Some(val) = arg.value.as_deref() {
            ctx_opts
                .max_worker_threads
                .set(try_parse_usize_arg(val, arg.cli_arg.idx)?)
                .map_err(|e| ScrError::from(CliArgumentError::from(e)))?;
        } else {
            return Err(
                CliArgumentError::new("missing thread count argument", arg.cli_arg.idx).into(),
            );
        }
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
        if arg.chainspec.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify chain range for global argument",
                arg.cli_arg.idx,
            )
            .into());
        }
    }
    return Ok(matched);
}

fn try_parse_as_chain_opt(
    ctx_opts: &mut ContextOptions,
    arg: &ParsedCliArgument,
) -> Result<bool, CliArgumentError> {
    fn apply_to_chains<F>(
        ctx_opts: &mut ContextOptions,
        arg: &ParsedCliArgument,
        f: F,
    ) -> Result<bool, CliArgumentError>
    where
        F: FnOnce(&mut ChainOptions) -> Result<(), ArgumentReassignmentError>,
    {
        assert!(arg.chainspec.is_none()); //TODO
        f(&mut ctx_opts.chains[ctx_opts.curr_chain as usize])
            .map_err(|e| CliArgumentError::from(e))?;
        Ok(true)
    }
    if "selenium".starts_with(&arg.argname) {
        let sv = try_parse_selenium_variant(arg.value.as_deref(), &arg.cli_arg)?;
        return apply_to_chains(ctx_opts, arg, |c| c.selenium_variant.set(sv));
    }
    match arg.argname {
        "dte" => {
            if let Some(_val) = &arg.value {
                todo!("parse text encoding");
            } else {
                Err(CliArgumentError::new(
                    "missing argument for default text encoding",
                    arg.cli_arg.idx,
                ))
            }
        }
        "ppte" => {
            let ppte = try_parse_bool_arg_or_default(arg.value.as_deref(), true, arg.cli_arg.idx)?;
            apply_to_chains(ctx_opts, arg, |c| c.prefer_parent_text_encoding.set(ppte))
        }
        "fte" => {
            let fte = try_parse_bool_arg_or_default(arg.value.as_deref(), true, arg.cli_arg.idx)?;
            apply_to_chains(ctx_opts, arg, |c| c.force_text_encoding.set(fte))
        }
        "sds" => {
            let sds = try_parse_selenium_download_strategy(arg.value.as_deref(), &arg.cli_arg)?;
            apply_to_chains(ctx_opts, arg, |c| c.selenium_download_strategy.set(sds))
        }
        "bs" => {
            if let Some(val) = arg.value {
                let bs = try_parse_usize_arg(val, arg.cli_arg.idx)?;
                apply_to_chains(ctx_opts, arg, |c| c.default_batch_size.set(bs))
            } else {
                Err(CliArgumentError::new(
                    "missing argument for batch size",
                    arg.cli_arg.idx,
                ))
            }
        }
        "sbs" => {
            if let Some(val) = arg.value {
                let bs = try_parse_usize_arg(val, arg.cli_arg.idx)?;
                apply_to_chains(ctx_opts, arg, |c| c.stream_buffer_size.set(bs))
            } else {
                Err(CliArgumentError::new(
                    "missing argument for stream buffer size",
                    arg.cli_arg.idx,
                ))
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
                            "stdin-if-tty" => Some(BufferingMode::LineBufferStdinIfTTY),
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
                        });
                    }
                }
            } else {
                BufferingMode::LineBuffer
            };
            apply_to_chains(ctx_opts, arg, |c| c.buffering_mode.set(buffering_mode))
        }
        _ => Ok(false),
    }
}

fn parse_operation(
    argname: &str,
    value: Option<&BStr>,
    idx: Option<CliArgIdx>,
) -> Result<Option<OperatorData>, OperatorCreationError> {
    if let Some(c) = REGEX_CLI_ARG_REGEX.captures(argname) {
        let mut opts = RegexOptions::default();
        let mut binary_regex = false;
        let mut unicode_mode = false;
        if c.name("l").is_some() {
            opts.line_based = true;
        }
        if c.name("i").is_some() {
            opts.case_insensitive = true;
        }
        if c.name("m").is_some() {
            opts.multimatch = true;
        }
        if c.name("d").is_some() {
            opts.dotall = true;
        }
        if c.name("a").is_some() {
            opts.ascii_mode = true;
        }
        if c.name("u").is_some() {
            unicode_mode = true;
        }
        if c.name("b").is_some() {
            binary_regex = true;
        }
        if opts.ascii_mode && unicode_mode {
            return Err(OperatorCreationError::new(
                "[a]scii and [u]nicode mode are mutually exclusive on any regex",
                idx,
            ));
        }
        if binary_regex {
            if !unicode_mode {
                opts.ascii_mode = true;
            }
            todo!();
        }
        return Ok(Some(OperatorData::Regex(parse_op_regex(value, idx, opts)?)));
    }
    let append = &argname[0..1] == "+";
    Ok(match argname {
        "s" | "split" => Some(parse_op_split(value, idx)?),
        "p" | "print" => Some(parse_op_print(value, idx)?),
        "f" | "fmt" | "format" => Some(parse_op_format(value, idx)?),
        "key" => Some(parse_op_key(value, idx)?),

        "+file" | "file" => Some(parse_op_file(value, append, idx)?),

        "seq" => Some(parse_op_seq(value, SequenceMode::Default, idx)?),
        "+seq" => Some(parse_op_seq(value, SequenceMode::Append, idx)?),
        "enum" => Some(parse_op_seq(value, SequenceMode::Enumerate, idx)?),

        "+url" | "url" => todo!(),
        "+str" | "str" => Some(parse_op_str(value, append, idx)?),
        "+int" | "int" => Some(parse_op_int(value, append, idx)?),
        "+bytes" | "bytes" => Some(parse_op_bytes(value, append, idx)?),
        "+stdin" | "stdin" => Some(parse_op_stdin(value, append, idx)?),
        _ => None,
    })
}

fn try_parse_as_operation<'a>(
    ctx_opts: &mut ContextOptions,
    arg: ParsedCliArgument<'a>,
) -> Result<Option<ParsedCliArgument<'a>>, CliArgumentError> {
    let op_data =
        parse_operation(&arg.argname, arg.value, Some(arg.cli_arg.idx)).map_err(|oce| {
            CliArgumentError {
                message: oce.message,
                cli_arg_idx: arg.cli_arg.idx,
            }
        })?;
    if let Some(op_data) = op_data {
        let argname = ctx_opts.string_store.intern_cloned(arg.argname);
        let label = arg.label.map(|l| ctx_opts.string_store.intern_cloned(l));
        ctx_opts.add_op(
            OperatorBaseOptions::new(argname, label, arg.chainspec, Some(arg.cli_arg.idx)),
            op_data,
        );
        Ok(None)
    } else {
        Ok(Some(arg))
    }
}

pub fn parse_cli_retain_args(args: &Vec<BString>) -> Result<ContextOptions, ScrError> {
    if args.is_empty() {
        return Err(MissingArgumentsError.into());
    }
    let mut ctx_opts = ContextOptions::default();
    for (i, arg_str) in args.iter().enumerate() {
        let cli_arg = CliArgument {
            idx: i as CliArgIdx + 1,
            value: arg_str,
        };
        if let Some(m) = CLI_ARG_REGEX.captures(arg_str.as_bytes()) {
            let argname = from_utf8(m.name("argname").unwrap().as_bytes()).map_err(|_| {
                CliArgumentError::new("argument name must be valid UTF-8", cli_arg.idx)
            })?;
            let label =
                if let Some(lbl) = m.name("label") {
                    Some(from_utf8(lbl.as_bytes()).map_err(|_| {
                        CliArgumentError::new("label must be valid UTF-8", cli_arg.idx)
                    })?)
                } else {
                    None
                };

            let arg = ParsedCliArgument {
                argname,
                value: m.name("value").map(|v| <&BStr>::from(v.as_bytes())),
                label: label,
                chainspec: None, //m.group("chainspec"); // TODO
                cli_arg: cli_arg,
            };
            if try_parse_as_context_opt(&mut ctx_opts, &arg)? {
                continue;
            }
            if try_parse_as_chain_opt(&mut ctx_opts, &arg)? {
                continue;
            }
            if let Some(arg) = try_parse_as_operation(&mut ctx_opts, arg)? {
                return Err(CliArgumentError {
                    message: format!("unknown argument name '{}'", arg.argname).into(),
                    cli_arg_idx: arg.cli_arg.idx,
                }
                .into());
            }
        } else {
            return Err(CliArgumentError::new("invalid argument syntax", cli_arg.idx).into());
        }
    }
    return Ok(ctx_opts);
}
pub fn parse_cli(args: Vec<BString>) -> Result<ContextOptions, (Vec<BString>, ScrError)> {
    match parse_cli_retain_args(&args) {
        Ok(mut ctx) => {
            ctx.cli_args = Some(args);
            Ok(ctx)
        }
        Err(e) => Err((args, e)),
    }
}

pub fn collect_env_args() -> Result<Vec<BString>, CliArgumentError> {
    #[cfg(unix)]
    {
        Ok(std::env::args_os()
            .skip(1)
            .map(|s| std::os::unix::prelude::OsStringExt::into_vec(s).into())
            .collect::<Vec<BString>>())
    }
    #[cfg(windows)]
    {
        let args = Vec::new();
        for (i, arg) in std::env::args_os().skip(1).enumerate() {
            if let (Some(arg)) = arg.to_str() {
                args.push(BString::from(arg));
            } else {
                return Err(CliArgumentError::new(
                    "failed to parse byte sequence as unicode".to_owned(),
                    CliArgument {
                        arg_index: i + 1,
                        arg_str: BString::from(arg.to_string_lossy().as_bytes()),
                    },
                ));
            }
        }
        Ok(args)
    }
}

pub fn parse_cli_from_env() -> Result<ContextOptions, ScrError> {
    let args = collect_env_args()?;
    parse_cli(args).map_err(|(_, e)| e)
}
