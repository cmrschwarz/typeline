use py::build_op_py;
use scr_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_setup::SessionSetupData,
};

pub mod py;

#[derive(Default)]
pub struct PythonExtension {}

impl Extension for PythonExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_python".into()
    }
    fn parse_call_expr(
        &self,
        _ctx_opts: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        let expr = CallExpr::from_argument(arg)?;
        if expr.op_name == "py" {
            let val = expr.require_single_string_arg()?;
            return Ok(Some(build_op_py(val.to_owned(), expr.span)?));
        }
        if expr.op_name == "to_int" {
            expr.reject_args()?;
            return Ok(Some(build_op_py("int(_)".to_string(), expr.span)?));
        }
        Ok(None)
    }
}
