use std::fmt::Debug;

use bstring::BString;
use thiserror::Error;

use crate::{
    cli::CliArgumentError,
    context::Context,
    operations::{
        operation::{
            OperationApplicationError, OperationCreationError, OperationId, OperationRef,
            OperationSetupError,
        },
        transform::TransformApplicationError,
    },
    options::{argument::CliArgIdx, context_options::ContextOptions},
};

#[derive(Error, Debug, Clone)]
pub enum ScrError {
    #[error(transparent)]
    CliArgumentError(#[from] CliArgumentError),
    #[error(transparent)]
    OperationCreationError(#[from] OperationCreationError),
    #[error(transparent)]
    OperationSetupError(#[from] OperationSetupError),
    #[error(transparent)]
    OperationApplicationError(#[from] OperationApplicationError),
    #[error(transparent)]
    TransformApplicationError(#[from] TransformApplicationError),
}

fn contextualize_cli_arg(msg: &str, args: Option<&[BString]>, cli_arg_idx: CliArgIdx) -> String {
    if let Some(args) = args {
        format!(
            "in cli arg {} `{}`: {}",
            cli_arg_idx,
            args[cli_arg_idx as usize - 1].to_string_lossy(),
            msg
        )
    } else {
        format!("{}", msg)
    }
}

fn contextualize_op_id(
    msg: &str,
    op_id: OperationId,
    args: Option<&[BString]>,
    ctx_opts: Option<&ContextOptions>,
    ctx: Option<&Context>,
) -> String {
    let ops = ctx_opts
        .map(|o| &o.operations)
        .or_else(|| ctx.map(|c| &c.data.operations));
    let cli_arg_id = ops
        .map(|ops| ops[op_id as usize].base().cli_arg_idx)
        .unwrap_or_default();
    if let (Some(args), Some(cli_arg_id)) = (args, cli_arg_id) {
        contextualize_cli_arg(msg, Some(args), cli_arg_id)
    } else {
        format!("in global op id {}: {}", op_id, msg)
    }
}

fn contextualize_op_ref(
    msg: &str,
    op_ref: OperationRef,
    args: Option<&[BString]>,
    ctx_opts: Option<&ContextOptions>,
    ctx: Option<&Context>,
) -> String {
    if let Some(ctx) = ctx {
        let op_id = ctx.data.chains[op_ref.chain_id as usize].operations[op_ref.op_offset as usize];
        contextualize_op_id(msg, op_id, args, ctx_opts, Some(ctx))
    } else {
        format!(
            "in op {} of chain {}: {}",
            op_ref.op_offset, op_ref.chain_id, msg
        )
    }
}

impl ScrError {
    pub fn contextualize_message(
        &self,
        args: Option<&[BString]>,
        ctx_opts: Option<&ContextOptions>,
        ctx: Option<&Context>,
    ) -> String {
        match self {
            ScrError::CliArgumentError(e) => contextualize_cli_arg(&e.message, args, e.cli_arg_idx),
            ScrError::OperationCreationError(e) => e.message.clone(),
            ScrError::OperationSetupError(e) => {
                contextualize_op_id(&e.message, e.op_id, args, ctx_opts, ctx)
            }
            ScrError::OperationApplicationError(e) => {
                contextualize_op_id(&e.message, e.op_id, args, ctx_opts, ctx)
            }
            ScrError::TransformApplicationError(e) => {
                contextualize_op_ref(&e.message, e.op_ref, args, ctx_opts, ctx)
            }
        }
    }
}
