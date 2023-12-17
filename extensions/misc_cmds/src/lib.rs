use scr_core::{
    cli::ParsedCliArgument,
    extension::Extension,
    operators::{
        errors::OperatorCreationError,
        operator::OperatorData,
        regex::{create_op_regex, create_op_regex_with_opts, RegexOptions},
    },
    options::session_options::SessionOptions,
    smallbox,
};
use sum::OpSum;

extern crate scr_core;

pub mod sum;

#[derive(Default)]
pub struct MiscCmdsExtension {}

impl Extension for MiscCmdsExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        arg: &ParsedCliArgument,
        _args: &[Vec<u8>],
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        if arg.argname == "sum" {
            arg.reject_value()?;
            return Ok(Some(OperatorData::Custom(
                smallbox![OpSum::default()],
            )));
        }
        if arg.argname == "lines" {
            arg.reject_value()?;

            return Ok(Some(
                create_op_regex_with_opts(
                    r"[^\n]+",
                    RegexOptions {
                        multimatch: true,
                        ..Default::default()
                    },
                )
                .unwrap(),
            ));
        }
        if arg.argname == "trim" {
            arg.reject_value()?;

            return Ok(Some(create_op_regex(r"\s+(?<>.*)\s+").unwrap()));
        }
        Ok(None)
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
