pub mod http;
pub mod tls_client;
pub mod url;

use http::create_op_GET;
use scr_core::{
    cli::OperatorCallExpr,
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
        arg: &OperatorCallExpr,
        _args: &[Vec<u8>],
        _next_arg_idx: &mut usize,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        if arg.op_name == "GET" || arg.op_name == "http-get" {
            arg.reject_value()?;
            return Ok(Some(create_op_GET()));
        }
        Ok(None)
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
