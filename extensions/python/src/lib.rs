use py::build_op_py;
use scr_core::{
    cli::call_expr::{Argument, CallExpr, Span},
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
};

pub mod py;

#[derive(Default)]
pub struct PythonExtension {}

pub fn create_op_to_int_with_span(
    span: Span,
) -> Result<OperatorData, OperatorCreationError> {
    build_op_py("int(_)".to_string(), span)
}

pub fn create_op_to_int() -> Result<OperatorData, OperatorCreationError> {
    create_op_to_int_with_span(Span::Generated)
}

impl Extension for PythonExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_python".into()
    }
    fn parse_call_expr(
        &self,
        _ctx_opts: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<OperatorData>, ScrError> {
        let expr = CallExpr::from_argument(arg)?;
        if expr.op_name == "py" {
            let val = expr.require_single_string_arg()?;
            return Ok(Some(build_op_py(val.to_owned(), expr.span)?));
        }
        // HACK: this should obviously be an scr builtin, but for
        // now this is a fine workaround
        if expr.op_name == "to_int" {
            expr.reject_args()?;
            return Ok(Some(create_op_to_int_with_span(expr.span)?));
        }
        Ok(None)
    }
}
