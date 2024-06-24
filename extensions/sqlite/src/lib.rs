use scr_core::{
    cli::call_expr::CallExpr,
    extension::Extension,
    operators::{errors::OperatorParsingError, operator::OperatorData},
    options::session_options::SessionOptions,
};

extern crate scr_core;

#[derive(Default)]
pub struct SqliteExtension {}

impl Extension for SqliteExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_sqlite".into()
    }
    fn parse_call_expr<'a>(
        &self,
        _ctx_opts: &mut SessionOptions,
        expr: CallExpr<'a>,
    ) -> Result<OperatorData, OperatorParsingError<'a>> {
        Err(OperatorParsingError::UnknownOperator(expr))
    }
}
