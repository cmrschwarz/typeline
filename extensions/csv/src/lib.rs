use csv::create_op_csv_from_file;
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
        let expr = CallExpr::from_argument(arg)?;
        if expr.op_name == "csv" {
            let (flags, args) = expr.split_flags_arg(false);
            if args.len() != 1 {
                return Err(expr
                    .error_require_exact_positional_count(1)
                    .into());
            }
            let mut header = false;
            // TODO: this is non exhaustive.
            // add proper, generalized cli parsing code ala CLAP
            if let Some(flags) = flags {
                if flags.get("-h").is_some() {
                    header = true;
                }
            }
            return Ok(Some(create_op_csv_from_file(
                args[0].stringify_as_text(expr.op_name, sess)?.to_string(),
                header,
            )));
        }
        Ok(None)
    }
}
