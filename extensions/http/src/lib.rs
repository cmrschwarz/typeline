pub mod http;
pub mod tls_client;
pub mod url;

use http::create_op_GET;
use scr_core::{
    cli::call_expr::CallExpr,
    extension::Extension,
    operators::{errors::OperatorParsingError, operator::OperatorData},
    options::session_options::SessionOptions,
};

#[derive(Default)]
pub struct HttpExtension {}

impl Extension for HttpExtension {
    fn parse_call_expr<'a>(
        &self,
        _ctx_opts: &mut SessionOptions,
        expr: CallExpr<'a>,
    ) -> Result<OperatorData, OperatorParsingError<'a>> {
        if expr.op_name == "GET" || expr.op_name == "http-get" {
            expr.reject_args()?;
            return Ok(create_op_GET());
        }
        Err(OperatorParsingError::UnknownOperator(expr))
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_http".into()
    }
}
