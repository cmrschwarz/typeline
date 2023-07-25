use std::{borrow::Cow, fmt::Debug};

use thiserror::Error;

use crate::{
    chain::ChainId,
    cli::{CliArgumentError, MissingArgumentsError, PrintInfoAndExitError},
    context::Session,
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
};

#[derive(Error, Debug, Clone)]
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

#[derive(Error, Debug, Clone)]
#[error("{message}")]
pub struct ReplDisabledError {
    pub message: &'static str,
    pub cli_arg_idx: Option<CliArgIdx>,
}

#[derive(Error, Debug, Clone)]
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
        args: Option<&Vec<Vec<u8>>>,
        ctx_opts: Option<&SessionOptions>,
        sess: Option<&Session>,
    ) -> Self {
        Self {
            contextualized_message: err
                .contextualize_message(args, ctx_opts, sess),
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
fn contextualize_cli_arg(
    msg: &str,
    args: Option<&Vec<Vec<u8>>>,
    cli_arg_idx: CliArgIdx,
) -> String {
    if let Some(args) = args {
        format!(
            "in cli arg {} `{}`: {}",
            cli_arg_idx,
            String::from_utf8_lossy(&args[cli_arg_idx as usize - 1]),
            msg
        )
    } else {
        msg.to_owned()
    }
}

fn contextualize_op_id(
    msg: &str,
    op_id: OperatorId,
    args: Option<&Vec<Vec<u8>>>,
    ctx_opts: Option<&SessionOptions>,
    sess: Option<&Session>,
) -> String {
    let cli_arg_id = ctx_opts
        .and_then(|o| o.operator_base_options[op_id as usize].cli_arg_idx)
        .or_else(|| {
            sess.and_then(|sess| {
                sess.operator_bases[op_id as usize].cli_arg_idx
            })
        });
    if let (Some(args), Some(cli_arg_id)) = (args, cli_arg_id) {
        contextualize_cli_arg(msg, Some(args), cli_arg_id)
    } else {
        if let Some(sess) = sess {
            let op_base = &sess.operator_bases[op_id as usize];
            //TODO: stringify chain id
            format!(
                "in op {} '{}' of chain {}: {}",
                op_base.offset_in_chain,
                sess.string_store.lookup(op_base.argname),
                op_base.chain_id,
                msg
            )
        } else {
            format!("in global op id {}: {}", op_id, msg)
        }
    }
}

impl ScrError {
    //TODO: avoid allocations by taking a &impl Write
    pub fn contextualize_message(
        &self,
        args: Option<&Vec<Vec<u8>>>,
        ctx_opts: Option<&SessionOptions>,
        sess: Option<&Session>,
    ) -> String {
        let args_gathered = args
            .or_else(|| ctx_opts.and_then(|o| o.cli_args.as_ref()))
            .or_else(|| sess.and_then(|sess| sess.cli_args.as_ref()));
        match self {
            ScrError::CliArgumentError(e) => {
                contextualize_cli_arg(&e.message, args_gathered, e.cli_arg_idx)
            }
            ScrError::ArgumentReassignmentError(e) => {
                if args_gathered.is_some()
                    && e.prev_cli_arg_idx.is_some()
                    && e.cli_arg_idx.is_some()
                {
                    let args = args_gathered.unwrap();
                    let arg = e.cli_arg_idx.unwrap();
                    let prev = e.prev_cli_arg_idx.unwrap();
                    format!(
                        "in cli arg {arg} `{}`: {ARGUMENT_REASSIGNMENT_ERROR_MESSAGE} (at cli arg {prev} `{}`)",
                        String::from_utf8_lossy(&args[arg as usize - 1]),
                        String::from_utf8_lossy(&args[prev as usize - 1]),
                    )
                } else if let Some(arg) = e.cli_arg_idx {
                    contextualize_cli_arg(
                        ARGUMENT_REASSIGNMENT_ERROR_MESSAGE,
                        args_gathered,
                        arg,
                    )
                } else {
                    return ARGUMENT_REASSIGNMENT_ERROR_MESSAGE.to_string();
                }
            }
            ScrError::ReplDisabledError(e) => {
                if let Some(cli_arg_idx) = e.cli_arg_idx {
                    contextualize_cli_arg(
                        &e.message,
                        args_gathered,
                        cli_arg_idx,
                    )
                } else {
                    return e.message.to_string();
                }
            }
            ScrError::ChainSetupError(e) => e.to_string(),
            ScrError::OperationCreationError(e) => e.message.to_string(),
            ScrError::OperationSetupError(e) => contextualize_op_id(
                &e.message,
                e.op_id,
                args_gathered,
                ctx_opts,
                sess,
            ),
            ScrError::OperationApplicationError(e) => contextualize_op_id(
                &e.message,
                e.op_id,
                args_gathered,
                ctx_opts,
                sess,
            ),
            ScrError::PrintInfoAndExitError(e) => format!("{}", e),
            ScrError::MissingArgumentsError(e) => format!("{}", e),
        }
    }
}
