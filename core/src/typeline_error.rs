use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    sync::Arc,
};

use thiserror::Error;

use crate::{
    chain::ChainId,
    cli::{
        call_expr::{CliArgIdx, Span},
        CliArgumentError, MissingArgumentsError, PrintInfoAndExitError,
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
        chain_settings::SettingConversionError,
        session_setup::{SessionSetupData, SetupOptions},
    },
    record_data::{field_data::FieldValueRepr, field_value::FieldValue},
    utils::{index_slice::IndexSlice, indexing_type::IndexingType},
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

#[derive(Error, Debug, Clone, PartialEq)]
pub struct CollectTypeMissmatch {
    pub index: usize,
    pub expected: FieldValueRepr,
    pub got: FieldValue,
}

impl Display for CollectTypeMissmatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "expected {}, got {}",
            self.expected,
            self.got.kind()
        ))
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum TypelineError {
    #[error(transparent)]
    PrintInfoAndExitError(#[from] PrintInfoAndExitError),

    #[error(transparent)]
    ReplDisabledError(#[from] ReplDisabledError),

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

    #[error(transparent)]
    SettingConversionError(#[from] SettingConversionError),
}
#[derive(Error, Debug, Clone)]
#[error("{contextualized_message}")]
pub struct ContextualizedTypelineError {
    pub contextualized_message: String,
    pub err: TypelineError,
}

impl ContextualizedTypelineError {
    pub fn from_typeline_error(
        err: TypelineError,
        args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
        cli_opts: Option<&SetupOptions>,
        setup_data: Option<&SessionSetupData>,
        sess: Option<&SessionData>,
    ) -> Self {
        Self {
            contextualized_message: err
                .contextualize_message(args, cli_opts, setup_data, sess),
            err,
        }
    }
}

impl From<ContextualizedTypelineError> for TypelineError {
    fn from(value: ContextualizedTypelineError) -> Self {
        value.err
    }
}

impl From<Arc<OperatorApplicationError>> for TypelineError {
    fn from(value: Arc<OperatorApplicationError>) -> Self {
        TypelineError::from(
            Arc::try_unwrap(value).unwrap_or_else(|v| (*v).clone()),
        )
    }
}

pub fn result_into<T, E, IntoE: Into<E>>(
    result: Result<T, IntoE>,
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
        Span::Generated | Span::Builtin | Span::FlagsObject => msg.to_string(),
        Span::EnvVar {
            compile_time,
            var_name,
        } => {
            format!(
                "in{}environment variable `{var_name}`: {msg}",
                if compile_time { " compile-time " } else { " " }
            )
        }
    }
}

fn was_first_cli_arg_skipped(
    cli_opts: Option<&SetupOptions>,
    setup_data: Option<&SessionSetupData>,
    sess: Option<&SessionData>,
) -> bool {
    cli_opts.map(|o| o.skip_first_cli_arg).unwrap_or(
        setup_data
            .map(|o| o.setup_settings.skipped_first_cli_arg)
            .unwrap_or(
                sess.map(|s| s.settings.skipped_first_cli_arg)
                    .unwrap_or(true),
            ),
    )
}

fn contextualize_op_id(
    msg: &str,
    op_id: OperatorId,
    args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
    cli_opts: Option<&SetupOptions>,
    setup_data: Option<&SessionSetupData>,
    sess: Option<&SessionData>,
) -> String {
    let span = sess.map(|sess| sess.operator_bases[op_id].span);
    if let (Some(args), Some(span)) = (args, span) {
        let first_arg_skipped =
            was_first_cli_arg_skipped(cli_opts, setup_data, sess);
        contextualize_span(msg, Some(args), span, first_arg_skipped)
    } else if let Some(sess) = sess {
        let op_base = &sess.operator_bases[op_id];
        let op_data = &sess.operator_data[op_base.op_data_id];
        // TODO: stringify chain id
        format!(
            "in op {} '{}' of chain {}: {}",
            // TODO: better message for aggregation members
            op_base.offset_in_chain.base_chain_offset(sess),
            op_data.default_name(),
            op_base.chain_id,
            msg
        )
    } else {
        format!("in global op id {op_id}: {msg}")
    }
}

impl TypelineError {
    // PERF: could avoid allocations by taking a &impl Write
    pub fn contextualize_message(
        &self,
        args: Option<&IndexSlice<CliArgIdx, Vec<u8>>>,
        cli_opts: Option<&SetupOptions>,
        setup_data: Option<&SessionSetupData>,
        sess: Option<&SessionData>,
    ) -> String {
        let first_arg_skipped =
            was_first_cli_arg_skipped(cli_opts, setup_data, sess);
        let args_gathered = args
            .or_else(|| setup_data.and_then(|o| o.cli_args.as_deref()))
            .or_else(|| sess.and_then(|sess| sess.cli_args.as_deref()));
        match self {
            TypelineError::CliArgumentError(e) => contextualize_span(
                &e.message,
                args_gathered,
                e.span,
                first_arg_skipped,
            ),
            TypelineError::ReplDisabledError(e) => contextualize_span(
                e.message,
                args_gathered,
                e.span,
                first_arg_skipped,
            ),
            TypelineError::ChainSetupError(e) => e.to_string(),
            TypelineError::OperationCreationError(e) => e.message.to_string(),
            TypelineError::SettingConversionError(e) => e.message.to_string(),
            TypelineError::OperationSetupError(e) => contextualize_op_id(
                &e.message,
                e.op_id,
                args_gathered,
                cli_opts,
                setup_data,
                sess,
            ),
            TypelineError::OperationApplicationError(e) => {
                contextualize_op_id(
                    e.message(),
                    e.op_id(),
                    args_gathered,
                    cli_opts,
                    setup_data,
                    sess,
                )
            }
            TypelineError::PrintInfoAndExitError(e) => e.to_string(),
            TypelineError::MissingArgumentsError(e) => e.to_string(),
            TypelineError::CollectTypeMissmatch(e) => e.to_string(),
        }
    }
}
