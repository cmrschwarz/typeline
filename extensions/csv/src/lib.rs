use csv::parse_op_csv;
use scr_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::operator::OperatorData,
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
};

pub mod csv;

#[derive(Default)]
pub struct CsvExtension {}

impl Extension for CsvExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_csv".into()
    }
    fn parse_call_expr(
        &self,
        sess: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<OperatorData>, ScrError> {
        let expr = CallExpr::from_argument_mut(arg)?;
        if expr.op_name == "csv" {
            return parse_op_csv(sess, expr);
        }
        Ok(None)
    }
}
