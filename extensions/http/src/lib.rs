pub mod http;
pub mod tls_client;
pub mod url;

use http::OpHttpRequest;
use scr_core::{
    cli::ParsedCliArgument,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
    smallbox,
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
        if arg.argname == "GET" || arg.argname == "http-get" {
            arg.reject_value()?;
            return Ok(Some(OperatorData::Custom(smallbox![
                OpHttpRequest::default()
            ])));
        }
        Ok(None)
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
