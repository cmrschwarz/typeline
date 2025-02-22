use sel::parse_op_sel;
use sel_nav::parse_op_sel_nav;
use sel_xpath::parse_op_sel_xpath;
use typeline_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::operator::Operator,
    options::session_setup::SessionSetupData,
    typeline_error::TypelineError,
};

pub mod sel;
pub mod sel_nav;
pub mod sel_xpath;
pub mod selenium_data;

#[derive(Default)]
pub struct SeleniumExtension {}

impl Extension for SeleniumExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "typeline_ext_selenium".into()
    }
    fn parse_call_expr(
        &self,
        sess: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<Box<dyn Operator>>, TypelineError> {
        let expr = CallExpr::from_argument_mut(arg)?;
        if expr.op_name == "sel" {
            return Ok(Some(parse_op_sel(sess, expr)?));
        }
        if expr.op_name == "sel_nav" {
            return Ok(Some(parse_op_sel_nav(sess, expr)?));
        }
        if expr.op_name == "sel_xpath" {
            return Ok(Some(parse_op_sel_xpath(sess, expr)?));
        }
        Ok(None)
    }
}
