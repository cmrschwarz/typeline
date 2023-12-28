use primes::create_op_primes;
use scr_core::{
    cli::ParsedCliArgumentParts,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};
use string_utils::{create_op_chars, create_op_lines, create_op_trim};
use sum::create_op_sum;

extern crate scr_core;

pub mod primes;
pub mod string_utils;
pub mod sum;

#[derive(Default)]
pub struct MiscCmdsExtension {}

impl Extension for MiscCmdsExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        arg: &ParsedCliArgumentParts,
        _args: &[Vec<u8>],
        _next_arg_idx: &mut usize,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        let ctor_fn = match arg.argname {
            "sum" => create_op_sum,
            "primes" => create_op_primes,
            "lines" => create_op_lines,
            "chars" => create_op_chars,
            "trim" => create_op_trim,
            _ => return Ok(None),
        };
        arg.reject_value()?;
        Ok(Some(ctor_fn()))
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
