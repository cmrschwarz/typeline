#![allow(clippy::too_many_arguments)]

use explode::parse_op_explode;
use flatten::parse_op_flatten;
use from_tyson::create_op_from_tyson;
use head::parse_op_head;
use primes::create_op_primes;
use scr_core::{
    cli::ParsedCliArgumentParts,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};
use string_utils::{create_op_chars, create_op_lines, create_op_trim};
use sum::create_op_sum;
use tail::parse_op_tail;

extern crate scr_core;

pub mod explode;
pub mod flatten;
pub mod from_tyson;
pub mod head;
pub mod primes;
pub mod string_utils;
pub mod sum;
pub mod tail;

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
        let ctor_with_arg: Option<fn(_, _) -> _> = match arg.argname {
            "head" | "h" => Some(parse_op_head),
            "tail" | "t" => Some(parse_op_tail),
            "explode" => Some(parse_op_explode),
            "flatten" => Some(parse_op_flatten),
            _ => None,
        };
        if let Some(ctor) = ctor_with_arg {
            return Ok(Some(ctor(arg.value, Some(arg.cli_arg.idx))?));
        }

        let ctor_without_arg: Option<fn() -> _> = match arg.argname {
            "sum" => Some(create_op_sum),
            "primes" => Some(create_op_primes),
            "lines" => Some(create_op_lines),
            "chars" => Some(create_op_chars),
            "trim" => Some(create_op_trim),
            "from-tyson" => Some(create_op_from_tyson),
            _ => None,
        };
        if let Some(ctor) = ctor_without_arg {
            arg.reject_value()?;
            return Ok(Some(ctor()));
        }
        Ok(None)
    }

    fn setup(
        &mut self,
        _registry: &mut scr_core::extension::ExtensionRegistry,
    ) {
    }
}
