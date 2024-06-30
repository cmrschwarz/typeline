use py::build_op_py;
use scr_core::{
    cli::call_expr::CallExpr,
    extension::Extension,
    operators::{errors::OperatorParsingError, operator::OperatorData},
    options::session_options::SessionOptions,
};

pub mod py;

#[derive(Default)]
pub struct PythonExtension {}

impl Extension for PythonExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_python".into()
    }
    fn parse_call_expr<'a>(
        &self,
        _ctx_opts: &mut SessionOptions,
        expr: CallExpr<'a>,
    ) -> Result<OperatorData, OperatorParsingError<'a>> {
        if expr.op_name == "py" {
            let val = expr.require_single_string_arg()?;
            return Ok(build_op_py(val.to_owned(), expr.span)?);
        }
        if expr.op_name == "to_int" {
            expr.reject_args()?;
            return Ok(build_op_py("int(_)".to_string(), expr.span)?);
        }
        Err(OperatorParsingError::UnknownOperator(expr))
    }
}
