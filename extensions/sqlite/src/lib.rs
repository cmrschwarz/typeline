use scr_core::{
    cli::OperatorCallExpr,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};

extern crate scr_core;

#[derive(Default)]
pub struct SqliteExtension {}

impl Extension for SqliteExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        _arg: &OperatorCallExpr,
        _args: &[Vec<u8>],
        _next_arg_idx: &mut usize,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        Ok(None)
    }
}
