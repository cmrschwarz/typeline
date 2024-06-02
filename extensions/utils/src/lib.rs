#![allow(clippy::too_many_arguments)]

use crate::primes::create_op_primes;
use collect::create_op_collect;
use dup::{parse_op_drop, parse_op_dup};
use explode::parse_op_explode;
use flatten::parse_op_flatten;
use from_tyson::create_op_from_tyson;
use head::parse_op_head;
use scr_core::{
    cli::ParsedCliArgumentParts,
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};
use string_utils::{
    create_op_chars, create_op_lines, create_op_to_tyson, create_op_trim,
};
use sum::create_op_sum;
use tail::parse_op_tail;
use typename::create_op_typename;

extern crate scr_core;

pub mod collect;
pub mod dup;
pub mod explode;
pub mod flatten;
pub mod from_tyson;
pub mod head;
pub mod primes;
pub mod string_utils;
pub mod sum;
pub mod tail;
pub mod typename;

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
            "head" => Some(parse_op_head),
            "tail" => Some(parse_op_tail),
            "dup" => Some(parse_op_dup),
            "drop" => Some(parse_op_drop),
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
            "lines" | "l" => Some(create_op_lines),
            "chars" => Some(create_op_chars),
            "typename" => Some(create_op_typename),
            "trim" => Some(create_op_trim),
            "from_tyson" => Some(create_op_from_tyson),
            "to_tyson" => Some(create_op_to_tyson),
            "collect" => Some(create_op_collect),
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
