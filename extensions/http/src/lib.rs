use scr_core::{
    cli::ParsedCliArgument,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};

extern crate scr_core;

#[derive(Default)]
pub struct HttpExtension {}

impl Extension for HttpExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        arg: &ParsedCliArgument,
        _args: &[Vec<u8>],
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        if arg.argname == "GET" {
            arg.reject_value()?;
            todo!();
            // return Ok(Some(OperatorData::Custom(smallbox![
            //     OpHTTPRequest::default()
            // ])));
        }
        Ok(None)
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
