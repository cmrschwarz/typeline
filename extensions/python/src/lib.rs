use py::build_op_py;
use scr_core::{
    cli::call_expr::CallExpr,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};

extern crate scr_core;

pub mod py;

#[derive(Default)]
pub struct PythonExtension {}

impl Extension for PythonExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        expr: &CallExpr,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        if expr.op_name == "py" {
            let val = expr.require_single_string_arg()?;
            return build_op_py(val.to_owned(), expr.span).map(Some);
        }
        if expr.op_name == "to_int" {
            expr.reject_args()?;
            return build_op_py("int(_)".to_string(), expr.span).map(Some);
        }
        Ok(None)
    }
}
