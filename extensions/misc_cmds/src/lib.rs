use scr_core::{
    cli::ParsedCliArgument,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
    smallbox,
};
use sum::OpSum;

extern crate scr_core;

pub mod sum;

#[derive(Default)]
pub struct MiscCmdsExtension {}

impl Extension for MiscCmdsExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        arg: &ParsedCliArgument,
        _args: &[Vec<u8>],
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        if arg.argname == "sum" {
            arg.reject_value()?;
            return Ok(Some(OperatorData::Custom(
                smallbox![OpSum::default()],
            )));
        }
        Ok(None)
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
