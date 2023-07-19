use std::fmt::Debug;

use thiserror::Error;

use crate::{
    cli::{CliArgumentError, MissingArgumentsError, PrintInfoAndExitError},
    context::Session,
    operations::{
        errors::{
            ChainSetupError, OperatorApplicationError, OperatorCreationError, OperatorSetupError,
        },
        operator::OperatorId,
    },
    options::{argument::CliArgIdx, session_options::SessionOptions},
};

#[derive(Error, Debug, Clone)]
pub enum ScrError {
    #[error(transparent)]
    PrintInfoAndExitError(#[from] PrintInfoAndExitError),

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

pub fn result_into<T, E, EFrom: Into<E>>(result: Result<T, EFrom>) -> Result<T, E> {
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(e.into()),
    }
}
fn contextualize_cli_arg(msg: &str, args: Option<&Vec<Vec<u8>>>, cli_arg_idx: CliArgIdx) -> String {
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
        .or_else(|| sess.and_then(|sess| sess.operator_bases[op_id as usize].cli_arg_idx));
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
        self,
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
            ScrError::ChainSetupError(e) => e.to_string(),
            ScrError::OperationCreationError(e) => e.message.to_string(),
            ScrError::OperationSetupError(e) => {
                contextualize_op_id(&e.message, e.op_id, args_gathered, ctx_opts, sess)
            }
            ScrError::OperationApplicationError(e) => {
                contextualize_op_id(&e.message, e.op_id, args_gathered, ctx_opts, sess)
            }
            ScrError::PrintInfoAndExitError(e) => format!("{}", e),
            ScrError::MissingArgumentsError(e) => format!("{}", e),
        }
    }
}
