use crate::{
    document::{Document, DocumentSource},
    operations::{
        control_flow_ops::parse_split_op,
        operator_base::{OperatorBase, OperatorCreationError},
        operator_data::OperatorData,
        regex::parse_regex_op,
    },
    options::{
        argument::{ArgumentReassignmentError, CliArgIdx},
        chain_options::ChainOptions,
        chain_spec::ChainSpec,
        context_options::ContextOptions,
        operator_base_options::OperatorBaseOptions,
    },
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
};
use bstring::{bstr, BString};
use lazy_static::lazy_static;
use smallvec::smallvec;
use std::{borrow::Cow, ffi::OsStr, path::PathBuf, str::from_utf8};
use thiserror::Error;
use url::Url;
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

impl Into<CliArgumentError> for ArgumentReassignmentError {
    fn into(self) -> CliArgumentError {
        CliArgumentError {
            message: Cow::Borrowed(self.message),
            cli_arg_idx: self.cli_arg_idx.unwrap(),
        }
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
    value: Option<&'a bstr>,
    label: Option<&'a str>,
    chainspec: Option<ChainSpec>,
    cli_arg: CliArgument<'a>,
}

lazy_static! {
    static ref TRUTHY_REGEX: regex::Regex = regex::RegexBuilder::new("true|tru|tr|t|yes|ye|y|1")
        .case_insensitive(true)
        .build()
        .unwrap();
    static ref FALSY_REGEX: regex::Regex = regex::RegexBuilder::new("false|fal|fa|f|no|n|0")
        .case_insensitive(true)
        .build()
        .unwrap();
    static ref CLI_ARG_REGEX: regex::bytes::Regex = regex::bytes::Regex::new(
        r#"(?P<argname>[^\s@:=]+)(@(?P<label>[^\s@:=]+))?(?P<chainspec>:[^\s@:=]+)?(=(?P<value>.*))?"#
    )
    .unwrap();
}

pub fn print_help() {
    print!("scr [OPTIONS]"); //TODO
}

fn try_parse_bool(val: &bstr) -> Option<bool> {
    if let Ok(val) = val.to_str() {
        if TRUTHY_REGEX.is_match(val) {
            return Some(true);
        }
        if FALSY_REGEX.is_match(val) {
            return Some(false);
        }
    }
    None
}

fn try_parse_document_source(
    argname: &str,
    value: Option<&bstr>,
    cli_arg_idx: CliArgIdx,
) -> Result<Option<DocumentSource>, CliArgumentError> {
    match argname {
        "url" => {
            if let Some(value) = value {
                let url = Url::parse(from_utf8(value.as_bytes()).map_err(|_| {
                    CliArgumentError::new(
                        "str argument must be valid UTF-8, consider using bytes=...",
                        cli_arg_idx,
                    )
                })?)
                .map_err(|_| CliArgumentError::new("failed to parse url argument", cli_arg_idx))?;
                Ok(Some(DocumentSource::Url(url)))
            } else {
                Err(CliArgumentError::new("missing value for url", cli_arg_idx))
            }
        }
        "str" => {
            if let Some(value) = value {
                Ok(Some(DocumentSource::String(
                    from_utf8(value.as_bytes())
                        .map_err(|_| {
                            CliArgumentError::new(
                                "str argument must be valid UTF-8, consider using bytes=...",
                                cli_arg_idx,
                            )
                        })?
                        .to_owned(),
                )))
            } else {
                Err(CliArgumentError::new(
                    "missing value argument for str",
                    cli_arg_idx,
                ))
            }
        }
        "bytes" => {
            if let Some(value) = value {
                Ok(Some(DocumentSource::Bytes(BString::from(value))))
            } else {
                Err(CliArgumentError::new(
                    "missing value argument for bytes",
                    cli_arg_idx,
                ))
            }
        }
        "file" => {
            if let Some(value) = value {
                #[cfg(unix)]
                {
                    let path = PathBuf::from(
                        <OsStr as std::os::unix::prelude::OsStrExt>::from_bytes(value.as_bytes()),
                    );
                    Ok(Some(DocumentSource::File(path)))
                }
                #[cfg(windows)]
                {
                    let path = PathBuf::from(value.to_str().map_err(|_| {
                        CliArgumentError::new(
                            "failed to parse file path argument as unicode",
                            cli_arg_idx,
                        )
                    })?);
                    Ok(Some(DocumentSource::File(path)))
                }
            } else {
                Err(CliArgumentError::new(
                    "missing path argument for file",
                    cli_arg_idx,
                ))
            }
        }
        "stdin" => {
            if value.is_some() {
                Err(CliArgumentError::new(
                    "stdin does not take arguments",
                    cli_arg_idx,
                ))
            } else {
                Ok(Some(DocumentSource::Stdin))
            }
        }
        _ => Ok(None),
    }
}

fn try_parse_selenium_variant(
    _value: Option<&bstr>,
    _cli_arg: &CliArgument,
) -> Result<Option<SeleniumVariant>, CliArgumentError> {
    todo!()
}
fn try_parse_selenium_download_strategy(
    _value: Option<&bstr>,
    _cli_arg: &CliArgument,
) -> Result<SeleniumDownloadStrategy, CliArgumentError> {
    todo!()
}

fn try_parse_bool_arg_or_default(
    val: Option<&bstr>,
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
fn try_parse_usize_arg(val: &bstr, cli_arg_idx: CliArgIdx) -> Result<usize, CliArgumentError> {
    if let Ok(b) = val.parse::<usize>() {
        Ok(b)
    } else {
        Err(CliArgumentError::new(
            "failed to parse as unsigned integer",
            cli_arg_idx,
        ))
    }
}

fn try_parse_as_context_opt(
    ctx_opts: &mut ContextOptions,
    arg: &ParsedCliArgument,
) -> Result<bool, CliArgumentError> {
    let mut matched = false;
    if ["--help", "-h"].contains(&&*arg.argname) {
        ctx_opts.print_help.set(true).map_err(|e| e.into())?;
        matched = true;
    }
    if ["--version", "-v"].contains(&&*arg.argname) {
        ctx_opts.print_help.set(true).map_err(|e| e.into())?;
        matched = true;
    }

    if arg.argname == "help" {
        ctx_opts
            .print_help
            .set(try_parse_bool_arg_or_default(
                arg.value.as_deref(),
                true,
                arg.cli_arg.idx,
            )?)
            .map_err(|e| e.into())?;
        matched = true
    }
    if arg.argname == "version" {
        ctx_opts
            .print_version
            .set(try_parse_bool_arg_or_default(
                arg.value.as_deref(),
                true,
                arg.cli_arg.idx,
            )?)
            .map_err(|e| e.into())?;
        matched = true
    }
    if arg.argname == "j" {
        if let Some(val) = arg.value.as_deref() {
            ctx_opts
                .max_worker_threads
                .set(try_parse_usize_arg(val, arg.cli_arg.idx)?)
                .map_err(|e| e.into())?;
        } else {
            return Err(CliArgumentError::new(
                "missing thread count argument",
                arg.cli_arg.idx,
            ));
        }

        matched = true
    }
    if matched {
        if arg.label.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify label for global argument",
                arg.cli_arg.idx,
            ));
        }
        if arg.chainspec.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify chain range for global argument",
                arg.cli_arg.idx,
            ));
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
        f(&mut ctx_opts.chains[ctx_opts.curr_chain as usize]).map_err(|e| e.into())?;
        Ok(true)
    }

    if arg.argname == "dte" {
        if let Some(_val) = &arg.value {
            todo!("parse text encoding");
        } else {
            return Err(CliArgumentError::new(
                "missing argument for default text encoding",
                arg.cli_arg.idx,
            ));
        }
    }
    if arg.argname == "ppte" {
        let ppte = try_parse_bool_arg_or_default(arg.value.as_deref(), true, arg.cli_arg.idx)?;
        return apply_to_chains(ctx_opts, arg, |c| c.prefer_parent_text_encoding.set(ppte));
    }
    if arg.argname == "fte" {
        let fte = try_parse_bool_arg_or_default(arg.value.as_deref(), true, arg.cli_arg.idx)?;
        return apply_to_chains(ctx_opts, arg, |c| c.force_text_encoding.set(fte));
    }
    if "selenium".starts_with(&arg.argname) {
        let sv = try_parse_selenium_variant(arg.value.as_deref(), &arg.cli_arg)?;
        return apply_to_chains(ctx_opts, arg, |c| c.selenium_variant.set(sv));
    }
    if arg.argname == "sds" {
        let sds = try_parse_selenium_download_strategy(arg.value.as_deref(), &arg.cli_arg)?;
        return apply_to_chains(ctx_opts, arg, |c| c.selenium_download_strategy.set(sds));
    }
    return Ok(false);
}

fn try_parse_as_doc<'a>(
    ctx_opts: &mut ContextOptions,
    arg: ParsedCliArgument<'a>,
) -> Result<Option<ParsedCliArgument<'a>>, CliArgumentError> {
    let doc_source =
        try_parse_document_source(&arg.argname, arg.value.as_deref(), arg.cli_arg.idx)?;
    if let Some(doc_source) = doc_source {
        if arg.label.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify label for global argument",
                arg.cli_arg.idx,
            ));
        }
        assert!(arg.chainspec.is_none()); //TODO
        ctx_opts.documents.push(Document {
            source: doc_source,
            reference_point: None,
            target_chains: smallvec![ctx_opts.curr_chain],
        });
        Ok(None)
    } else {
        Ok(Some(arg))
    }
}

fn parse_operation(
    argname: &str,
    value: Option<&bstr>,
    idx: Option<CliArgIdx>,
) -> Result<Option<OperatorData>, OperatorCreationError> {
    Ok(match argname {
        "r" | "re" | "regex" => Some(OperatorData::Regex(parse_regex_op(value, idx)?)),
        "s" | "split" => Some(OperatorData::Split(parse_split_op(value, idx)?)),
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
        let argname = ctx_opts.string_store_mut().intern_cloned(arg.argname);
        let label = arg
            .label
            .map(|l| ctx_opts.string_store_mut().intern_cloned(l));
        ctx_opts.add_op(
            OperatorBaseOptions::new(argname, label, arg.chainspec, Some(arg.cli_arg.idx)),
            op_data,
        );
        Ok(None)
    } else {
        Ok(Some(arg))
    }
}

pub fn parse_cli_retain_args(args: &Vec<BString>) -> Result<ContextOptions, CliArgumentError> {
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
                value: m.name("value").map(|v| <&bstr>::from(v.as_bytes())),
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
            if let Some(arg) = try_parse_as_doc(&mut ctx_opts, arg)? {
                if let Some(arg) = try_parse_as_operation(&mut ctx_opts, arg)? {
                    return Err(CliArgumentError {
                        message: format!("unknown argument name '{}'", arg.argname).into(),
                        cli_arg_idx: arg.cli_arg.idx,
                    });
                }
            }
        } else {
            return Err(CliArgumentError::new("invalid argument", cli_arg.idx));
        }
    }
    return Ok(ctx_opts);
}
pub fn parse_cli(args: Vec<BString>) -> Result<ContextOptions, (Vec<BString>, CliArgumentError)> {
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

pub fn parse_cli_from_env() -> Result<ContextOptions, CliArgumentError> {
    let args = collect_env_args()?;
    parse_cli(args).map_err(|(_, e)| e)
}
