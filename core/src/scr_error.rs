use std::{borrow::Cow, fmt::Debug};

use thiserror::Error;

use crate::{
    chain::ChainId,
    cli::{
        call_expr::Span, CliArgumentError, CliOptions, MissingArgumentsError,
        PrintInfoAndExitError,
    },
    context::SessionData,
    operators::{
        errors::{
            OperatorApplicationError, OperatorCreationError,
            OperatorSetupError,
        },
        operator::OperatorId,
    },
    options::{
        argument::{
            ArgumentReassignmentError, CliArgIdx,
            ARGUMENT_REASSIGNMENT_ERROR_MESSAGE,
        },
        session_options::SessionOptions,
    },
    record_data::{field_data::FieldValueRepr, field_value::FieldValueKind},
    utils::{index_vec::IndexSlice, indexing_type::IndexingType},
};

#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("in chain {chain_id}: {message}")]
pub struct ChainSetupError {
    pub chain_id: ChainId,
    pub message: Cow<'static, str>,
}

impl ChainSetupError {
    pub fn new(message: &'static str, chain_id: ChainId) -> Self {
        Self {
            message: message.into(),
            chain_id,
        }
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct ReplDisabledError {
    pub message: &'static str,
    pub span: Span,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("failed to collect {expected} as {got} (element index {index})")]
pub struct CollectTypeMissmatch {
    pub index: usize,
    pub expected: FieldValueRepr,
    pub got: FieldValueKind,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ScrError {
    #[error(transparent)]
    PrintInfoAndExitError(#[from] PrintInfoAndExitError),

    #[error(transparent)]
    ReplDisabledError(#[from] ReplDisabledError),

    #[error(transparent)]
    ArgumentReassignmentError(#[from] ArgumentReassignmentError),

    #[error(transparent)]
    MissingArgumentsError(#[from] MissingArgumentsError),

    #[error(transparent)]
    CliArgumentError(#[from] CliArgumentError),

    #[error(transparent)]
    OperationCreationError(#[from] OperatorCreationError),

    #[error(transparent)]
    OperationSetupError(#[from] OperatorSetupError),

    #[error(transparent)]
    ChainSetupError(#[from] ChainSetupError),

    #[error(transparent)]
    OperationApplicationError(#[from] OperatorApplicationError),

    #[error(transparent)]
    CollectTypeMissmatch(#[from] CollectTypeMissmatch),
}
#[derive(Error, Debug, Clone)]
#[error("{contextualized_message}")]
pub struct ContextualizedScrError {
    pub contextualized_message: String,
    pub err: ScrError,
}

impl ContextualizedScrError {
    pub fn from_scr_error(
        err: ScrError,
        args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
        cli_opts: Option<&CliOptions>,
        ctx_opts: Option<&SessionOptions>,
        sess: Option<&SessionData>,
    ) -> Self {
        Self {
            contextualized_message: err
                .contextualize_message(args, cli_opts, ctx_opts, sess),
            err,
        }
    }
}

impl From<ContextualizedScrError> for ScrError {
    fn from(value: ContextualizedScrError) -> Self {
        value.err
    }
}

pub fn result_into<T, E, EFrom: Into<E>>(
    result: Result<T, EFrom>,
) -> Result<T, E> {
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(e.into()),
    }
}
fn contextualize_span(
    msg: &str,
    args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
    span: Span,
    skipped_first_cli_arg: bool,
) -> String {
    let cli_arg_offset =
        CliArgIdx::from_usize(usize::from(!skipped_first_cli_arg));
    match span {
        Span::CliArg {
            start,
            end,
            offset_start,
            offset_end,
        } => {
            if start == end {
                if let Some(args) = args {
                    format!(
                        "in cli arg {} `{}`: {msg}",
                        start + cli_arg_offset,
                        String::from_utf8_lossy(
                            &args[start]
                                [offset_start as usize..offset_end as usize]
                        ),
                    )
                } else {
                    format!("in cli arg {}: {msg}", start + cli_arg_offset,)
                }
            } else {
                format!(
                    "in cli args {}:-{}: {msg}",
                    start + cli_arg_offset,
                    end + cli_arg_offset,
                )
            }
        }
        Span::MacroExpansion { op_id } => {
            format!("in macro expansion of op {op_id}: {msg}")
        }
        Span::Generated | Span::Builtin => msg.to_string(),
    }
}

fn was_first_cli_arg_skipped(
    cli_opts: Option<&CliOptions>,
    ctx_opts: Option<&SessionOptions>,
    sess: Option<&SessionData>,
) -> bool {
    cli_opts.map(|o| o.skip_first_arg).unwrap_or(
        ctx_opts.map(|o| o.skipped_first_cli_arg).unwrap_or(
            sess.map(|s| s.settings.skipped_first_cli_arg)
                .unwrap_or(true),
        ),
    )
}

fn contextualize_op_id(
    msg: &str,
    op_id: OperatorId,
    args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
    cli_opts: Option<&CliOptions>,
    ctx_opts: Option<&SessionOptions>,
    sess: Option<&SessionData>,
) -> String {
    let span = ctx_opts
        .map(|o| o.operator_base_options[op_id].span)
        .or_else(|| sess.map(|sess| sess.operator_bases[op_id].span));
    if let (Some(args), Some(span)) = (args, span) {
        let first_arg_skipped =
            was_first_cli_arg_skipped(cli_opts, ctx_opts, sess);
        contextualize_span(msg, Some(args), span, first_arg_skipped)
    } else if let Some(sess) = sess {
        let op_base = &sess.operator_bases[op_id];
        // TODO: stringify chain id
        format!(
            "in op {} '{}' of chain {}: {}",
            op_base.offset_in_chain,
            sess.string_store.read().unwrap().lookup(op_base.argname),
            op_base
                .chain_id
                .map(|id| id.to_string())
                .unwrap_or("<unknown chain>".to_owned()),
            msg
        )
    } else {
        format!("in global op id {op_id}: {msg}")
    }
}

impl ScrError {
    // PERF: could avoid allocations by taking a &impl Write
    pub fn contextualize_message(
        &self,
        args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
        cli_opts: Option<&CliOptions>,
        ctx_opts: Option<&SessionOptions>,
        sess: Option<&SessionData>,
    ) -> String {
        let first_arg_skipped =
            was_first_cli_arg_skipped(cli_opts, ctx_opts, sess);
        let args_gathered = args
            .or_else(|| {
                ctx_opts.and_then(|o| o.cli_args.as_ref().map(|a| &**a))
            })
            .or_else(|| {
                sess.and_then(|sess| sess.cli_args.as_ref().map(|a| &**a))
            });
        match self {
            ScrError::CliArgumentError(e) => contextualize_span(
                &e.message,
                args_gathered,
                e.span,
                first_arg_skipped,
            ),
            ScrError::ArgumentReassignmentError(e) => contextualize_span(
                ARGUMENT_REASSIGNMENT_ERROR_MESSAGE,
                args_gathered,
                e.reassignment_span,
                first_arg_skipped,
            ),
            ScrError::ReplDisabledError(e) => contextualize_span(
                e.message,
                args_gathered,
                e.span,
                first_arg_skipped,
            ),
            ScrError::ChainSetupError(e) => e.to_string(),
            ScrError::OperationCreationError(e) => e.message.to_string(),
            ScrError::OperationSetupError(e) => contextualize_op_id(
                &e.message,
                e.op_id,
                args_gathered,
                cli_opts,
                ctx_opts,
                sess,
            ),
            ScrError::OperationApplicationError(e) => contextualize_op_id(
                e.message(),
                e.op_id(),
                args_gathered,
                cli_opts,
                ctx_opts,
                sess,
            ),
            ScrError::PrintInfoAndExitError(e) => e.to_string(),
            ScrError::MissingArgumentsError(e) => e.to_string(),
            ScrError::CollectTypeMissmatch(e) => e.to_string(),
        }
    }
}
