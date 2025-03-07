//! Python Extension for [`typeline`](https://crates.io/crates/typeline)

use py::build_op_py;
use typeline_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::operator::Operator,
    options::session_setup::SessionSetupData,
    typeline_error::TypelineError,
};

pub mod py;

#[derive(Default)]
pub struct PythonExtension {}

impl Extension for PythonExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "typeline_ext_python".into()
    }
    fn parse_call_expr(
        &self,
        sess: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<Box<dyn Operator>>, TypelineError> {
        let expr = CallExpr::from_argument(arg)?;
        if expr.op_name == "py" {
            let val = expr.require_single_string_arg_autoconvert(sess)?;
            return Ok(Some(build_op_py(val.to_string(), expr.span)?));
        }
        Ok(None)
    }
}
