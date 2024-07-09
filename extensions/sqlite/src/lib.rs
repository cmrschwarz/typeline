use scr_core::{
    cli::call_expr::Argument,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_setup::SessionSetupData,
};

#[derive(Default)]
pub struct SqliteExtension {}

impl Extension for SqliteExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "scr_ext_sqlite".into()
    }
    fn parse_call_expr(
        &self,
        _ctx_opts: &mut SessionSetupData,
        _arg: &mut Argument,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        Ok(None)
    }
}
