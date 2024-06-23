pub mod http;
pub mod tls_client;
pub mod url;

use http::create_op_GET;
use scr_core::{
    cli::call_expr::CallExpr,
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
        expr: &CallExpr,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        if expr.op_name == "GET" || expr.op_name == "http-get" {
            expr.reject_args()?;
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
