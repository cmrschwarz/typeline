use csv::parse_op_csv;
use typeline_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::operator::Operator,
    options::session_setup::SessionSetupData,
    typeline_error::TypelineError,
};

pub mod csv;

#[derive(Default)]
pub struct CsvExtension {}

impl Extension for CsvExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "typeline_ext_csv".into()
    }
    fn parse_call_expr(
        &self,
        sess: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<Box<dyn Operator>>, TypelineError> {
        let expr = CallExpr::from_argument_mut(arg)?;
        if expr.op_name == "csv" {
            return parse_op_csv(sess, expr);
        }
        Ok(None)
    }
}
