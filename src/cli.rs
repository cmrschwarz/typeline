use crate::{
    argument::{ArgumentReassignmentError, CliArgument},
    chain::ChainId,
    document::{Document, DocumentReferencePoint, DocumentSource},
    operations::BUILTIN_OPERATIONS_CATALOG,
    options::{ChainOptions, ChainSpec, ContextOptions},
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
    xstr::{XStr, XString},
};
use lazy_static::{__Deref, lazy_static};
use regex::Regex;
use std::{collections::btree_map::Range, error::Error, fmt, ops::RangeFull, str::from_utf8};
#[derive(Debug)]
pub struct CliArgumentError {
    message: &'static str,
    cli_arg: CliArgument,
}
impl fmt::Display for CliArgumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "in cli arg {} '{}': {}",
            self.cli_arg.arg_index,
            self.cli_arg.arg_str.to_string_lossy(),
            self.message
        )
    }
}
impl Error for CliArgumentError {}

impl Into<CliArgumentError> for ArgumentReassignmentError {
    fn into(self) -> CliArgumentError {
        CliArgumentError {
            message: self.message,
            cli_arg: self.cli_arg.unwrap(),
        }
    }
}

impl CliArgumentError {
    pub fn new(message: &'static str, cli_arg: CliArgument) -> Self {
        Self { message, cli_arg }
    }
}

struct CliArgParsed {
    argname: String,
    value: Option<XString>,
    label: Option<String>,
    chainspec: Option<ChainSpec>,
    cli_arg: CliArgument,
}

pub fn print_help() {
    print!("scr [OPTIONS]"); //TODO
}

fn try_parse_bool(val: &XStr) -> Option<bool> {
    todo!()
}

fn try_parse_document_source(
    argname: &str,
    value: Option<&XStr>,
) -> Result<Option<DocumentSource>, CliArgumentError> {
    todo!()
}

fn try_parse_selenium_variant(
    value: Option<&XStr>,
    cli_arg: &CliArgument,
) -> Result<Option<SeleniumVariant>, CliArgumentError> {
    todo!()
}
fn try_parse_selenium_download_strategy(
    value: Option<&XStr>,
    cli_arg: &CliArgument,
) -> Result<SeleniumDownloadStrategy, CliArgumentError> {
    todo!()
}

fn try_parse_bool_arg_or_default(
    val: Option<&XStr>,
    default: bool,
    cli_arg: &CliArgument,
) -> Result<bool, CliArgumentError> {
    if let Some(val) = val {
        if let Some(b) = try_parse_bool(val) {
            Ok(b)
        } else {
            Err(CliArgumentError {
                message: "failed to parse as bool",
                cli_arg: cli_arg.clone(),
            })
        }
    } else {
        Ok(default)
    }
}

fn try_parse_as_context_opt(
    ctx_opts: &mut ContextOptions,
    arg: &CliArgParsed,
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
                &arg.cli_arg,
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
                &arg.cli_arg,
            )?)
            .map_err(|e| e.into())?;
        matched = true
    }
    if matched {
        if arg.label.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify label for global argument",
                arg.cli_arg.clone(),
            ));
        }
        if arg.chainspec.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify chain range for global argument",
                arg.cli_arg.clone(),
            ));
        }
    }
    return Ok(matched);
}

fn try_parse_as_doc(
    ctx_opts: &mut ContextOptions,
    arg: &CliArgParsed,
) -> Result<bool, CliArgumentError> {
    let doc_source = try_parse_document_source(&arg.argname, arg.value.as_deref())?;
    if let Some(doc_source) = doc_source {
        if arg.label.is_some() {
            return Err(CliArgumentError::new(
                "cannot specify label for global argument",
                arg.cli_arg.clone(),
            ));
        }
        assert!(arg.chainspec.is_none()); //TODO
        ctx_opts.documents.push(Document {
            source: doc_source,
            reference_point: None,
            target_chains: vec![ctx_opts.curr_chain],
        });
        Ok(true)
    } else {
        Ok(false)
    }
}
fn try_parse_as_chain_opt(
    ctx_opts: &mut ContextOptions,
    arg: &CliArgParsed,
) -> Result<bool, CliArgumentError> {
    fn apply_to_chains<F>(
        ctx_opts: &mut ContextOptions,
        arg: &CliArgParsed,
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
        if let Some(val) = &arg.value {
            todo!("parse text encoding");
        } else {
            return Err(CliArgumentError::new(
                "missing argument for default text encoding",
                arg.cli_arg.clone(),
            ));
        }
    }
    if arg.argname == "ppte" {
        let ppte = try_parse_bool_arg_or_default(arg.value.as_deref(), true, &arg.cli_arg)?;
        return apply_to_chains(ctx_opts, arg, |c| c.prefer_parent_text_encoding.set(ppte));
    }
    if arg.argname == "fte" {
        let fte = try_parse_bool_arg_or_default(arg.value.as_deref(), true, &arg.cli_arg)?;
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

fn try_parse_as_transform(
    ctx_opts: &mut ContextOptions,
    arg: &CliArgParsed,
) -> Result<bool, CliArgumentError> {
    for tf in &BUILTIN_OPERATIONS_CATALOG {
        let name_matches = tf.name_matches;
        if !name_matches(&&*arg.argname) {
            continue;
        }
        let create = tf.create;
        let tf_inst = create(
            &ctx_opts,
            arg.label.clone().unwrap_or_else(|| arg.argname.clone()),
            arg.value.clone(),
            ctx_opts.curr_chain,
            arg.chainspec.clone(),
        );

        return Ok(true);
    }
    return Ok(false);
}

lazy_static! {
    static ref CLI_ARG_REGEX: regex::bytes::Regex = regex::bytes::Regex::new("(?P<argname>[a-zA-Z_]+)(@(?P<label>[a-zA-Z_]+))?(?P<chainspec>[/0-9a-zA-Z-^]+)?(=(?P<value>.*))?").unwrap();
}

pub fn parse_cli(args: &[XString]) -> Result<ContextOptions, CliArgumentError> {
    let mut ctx_opts = ContextOptions::default();
    for (i, arg_str) in args.iter().enumerate() {
        let mut cli_arg = CliArgument {
            arg_index: i as u32 + 1,
            arg_str: arg_str.clone(),
        };
        if let Some(m) = CLI_ARG_REGEX.captures(arg_str.as_bytes()) {
            let argname = from_utf8(m.name("argname").unwrap().as_bytes())
                .map_err(|_| {
                    CliArgumentError::new("argument name must be valid UTF-8", cli_arg.clone())
                })?
                .to_owned();
            let label = if let Some(lbl) = m.name("label") {
                Some(
                    from_utf8(lbl.as_bytes())
                        .map_err(|_| {
                            CliArgumentError::new("label must be valid UTF-8", cli_arg.clone())
                        })?
                        .to_owned(),
                )
            } else {
                None
            };

            let mut arg = CliArgParsed {
                argname: argname,
                value: m.name("label").map(|l| XString::from(l.as_bytes())),
                label: label,
                chainspec: None, //m.group("chainspec"); // TODO
                cli_arg: cli_arg,
            };

            let succ_ctx = try_parse_as_context_opt(&mut ctx_opts, &arg)?;
            let succ_doc = try_parse_as_doc(&mut ctx_opts, &arg)?;
            let succ_co = try_parse_as_chain_opt(&mut ctx_opts, &arg)?;
            let succ_tf = try_parse_as_transform(&mut ctx_opts, &arg)?;
            let succ_sum = (succ_ctx as u8 + succ_doc as u8 + succ_co as u8 + succ_tf as u8);
            if succ_sum == 1 {
                continue;
            }
            if succ_sum > 1 {
                return Err(CliArgumentError::new(
                    "ambiguous argument name",
                    arg.cli_arg,
                ));
            } else {
                return Err(CliArgumentError::new(
                    "ambiguous argument name",
                    arg.cli_arg,
                ));
            }
        } else {
            return Err(CliArgumentError::new("invalid argument", cli_arg));
        }
    }
    return Ok(ctx_opts);
}

pub fn parse_cli_from_env() -> Result<ContextOptions, CliArgumentError> {
    let args: Vec<XString> = std::env::args_os().map(|s| s.into()).collect();
    parse_cli(&args)
}
