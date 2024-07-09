pub mod http;
pub mod tls_client;
pub mod url;

use http::create_op_GET;
use scr_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_setup::SessionSetupData,
};

#[derive(Default)]
pub struct HttpExtension {}

impl Extension for HttpExtension {
    fn parse_call_expr(
        &self,
        _ctx_opts: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        let expr = CallExpr::from_argument(arg)?;
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

    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_http".into()
    }
}
