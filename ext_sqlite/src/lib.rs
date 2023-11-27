use scr_core::{
    cli::ParsedCliArgument,
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
        _ctx_opts: &mut SessionOptions,
        _arg: &ParsedCliArgument,
        _args: &[Vec<u8>],
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        Ok(None)
    }
}
