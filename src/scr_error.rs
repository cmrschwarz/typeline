use std::fmt::Debug;

use bstring::BString;
use thiserror::Error;

use crate::{
    cli::CliArgumentError,
    context::Context,
    operations::{
        operator_base::OperatorId, OperatorApplicationError, OperatorCreationError,
        OperatorSetupError,
    },
    options::{argument::CliArgIdx, context_options::ContextOptions},
};

#[derive(Error, Debug, Clone)]
pub enum ScrError {
    #[error(transparent)]
    CliArgumentError(#[from] CliArgumentError),
    #[error(transparent)]
    OperationCreationError(#[from] OperatorCreationError),
    #[error(transparent)]
    OperationSetupError(#[from] OperatorSetupError),
    #[error(transparent)]
    OperationApplicationError(#[from] OperatorApplicationError),
}

fn contextualize_cli_arg(msg: &str, args: Option<&Vec<BString>>, cli_arg_idx: CliArgIdx) -> String {
    if let Some(args) = args {
        format!(
            "in cli arg {} `{}`: {}",
            cli_arg_idx,
            args[cli_arg_idx as usize - 1].to_string_lossy(),
            msg
        )
    } else {
        msg.to_owned()
    }
}

fn contextualize_op_id(
    msg: &str,
    op_id: OperatorId,
    args: Option<&Vec<BString>>,
    ctx_opts: Option<&ContextOptions>,
    ctx: Option<&Context>,
) -> String {
    let cli_arg_id = ctx_opts
        .and_then(|o| o.operator_base_options[op_id as usize].cli_arg_idx)
        .or_else(|| {
            ctx.and_then(|c| c.curr_session_data.operator_bases[op_id as usize].cli_arg_idx)
        });
    if let (Some(args), Some(cli_arg_id)) = (args, cli_arg_id) {
        contextualize_cli_arg(msg, Some(args), cli_arg_id)
    } else {
        if let Some(sess) = ctx.map(|c| &*c.curr_session_data) {
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
    pub fn contextualize_message(
        self,
        args: Option<&Vec<BString>>,
        ctx_opts: Option<&ContextOptions>,
        ctx: Option<&Context>,
    ) -> String {
        let args_gathered = args
            .or_else(|| ctx_opts.and_then(|o| o.cli_args.as_ref()))
            .or_else(|| ctx.and_then(|c| c.curr_session_data.cli_args.as_ref()));
        match self {
            ScrError::CliArgumentError(e) => {
                contextualize_cli_arg(&e.message, args_gathered, e.cli_arg_idx)
            }
            ScrError::OperationCreationError(e) => e.message.to_string(),
            ScrError::OperationSetupError(e) => {
                contextualize_op_id(&e.message, e.op_id, args_gathered, ctx_opts, ctx)
            }
            ScrError::OperationApplicationError(e) => {
                contextualize_op_id(&e.message, e.op_id, args_gathered, ctx_opts, ctx)
            }
        }
    }
}
