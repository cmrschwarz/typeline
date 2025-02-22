use typeline_core::{
    cli::call_expr::Argument, extension::Extension,
    operators::operator::Operator, options::session_setup::SessionSetupData,
    typeline_error::TypelineError,
};

#[derive(Default)]
pub struct SqliteExtension {}

impl Extension for SqliteExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "typeline_ext_sqlite".into()
    }
    fn parse_call_expr(
        &self,
        _ctx_opts: &mut SessionSetupData,
        _arg: &mut Argument,
    ) -> Result<Option<Box<dyn Operator>>, TypelineError> {
        Ok(None)
    }
}
