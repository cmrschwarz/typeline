#![allow(clippy::too_many_arguments)]

use avg::parse_op_avg;
use chunks::parse_op_chunks;
use collect::parse_op_collect;
use count::parse_op_count;
use dup::{parse_op_drop, parse_op_dup};
use eliminate_errors::parse_op_eliminate_errors;
use exec::parse_op_exec;
use explode::parse_op_explode;
use filter::parse_op_filter;
use flatten::parse_op_flatten;
use from_tyson::create_op_from_tyson;
use head::parse_op_head;
use join::parse_op_join;
use max::parse_op_max;
use primes::parse_op_primes;
use sequence::{parse_op_seq, SequenceMode};
use string_utils::{
    create_op_chars, create_op_lines, create_op_to_tyson, create_op_trim,
};
use sum::parse_op_sum;
use tail::parse_op_tail;
use typeline_core::{
    cli::call_expr::{Argument, CallExpr},
    extension::Extension,
    operators::{
        compute::{create_op_to_float, create_op_to_int},
        errors::OperatorCreationError,
        operator::Operator,
    },
    options::session_setup::SessionSetupData,
    typeline_error::TypelineError,
};
use typename::create_op_typename;

pub mod avg;
pub mod chunks;
pub mod collect;
pub mod count;
pub mod dup;
pub mod eliminate_errors;
pub mod exec;
pub mod explode;
pub mod filter;
pub mod flatten;
pub mod from_tyson;
pub mod head;
pub mod join;
pub mod lines;
pub mod max;
pub mod primes;
pub mod sequence;
pub mod string_utils;
pub mod sum;
pub mod tail;
pub mod typename;

#[derive(Default)]
pub struct UtilsExtension {}

impl Extension for UtilsExtension {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "typeline_ext_utils".into()
    }
    fn parse_call_expr(
        &self,
        sess: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<Box<dyn Operator>>, TypelineError> {
        let expr = CallExpr::from_argument_mut(arg)?;

        fn parse_op_reject_args(
            expr: &CallExpr,
            create_fn: fn() -> Box<dyn Operator>,
        ) -> Result<Box<dyn Operator>, OperatorCreationError> {
            expr.reject_args()?;
            Ok(create_fn())
        }

        Ok(Some(match expr.op_name {
            "tail" => parse_op_tail(sess, &expr)?,
            "head" => parse_op_head(&expr)?,
            "exec" => parse_op_exec(&expr)?,
            "dup" => parse_op_dup(&expr)?,
            "drop" => parse_op_drop(&expr)?,
            "explode" => parse_op_explode(&expr)?,
            "flatten" => parse_op_flatten(&expr)?,
            "sum" => parse_op_sum(&expr)?,
            "count" => parse_op_count(&expr)?,
            "avg" => parse_op_avg(&expr)?,
            "max" => parse_op_max(&expr)?,
            "chunks" => parse_op_chunks(sess, arg)?,
            "join" | "j" => parse_op_join(&expr)?,
            "seq" => parse_op_seq(sess, &expr, SequenceMode::Sequence, false)?,
            "seqn" => parse_op_seq(sess, &expr, SequenceMode::Sequence, true)?,
            "enum" => parse_op_seq(sess, &expr, SequenceMode::Enum, false)?,
            "enumn" => parse_op_seq(sess, &expr, SequenceMode::Enum, true)?,
            "enum-u" => {
                parse_op_seq(sess, &expr, SequenceMode::EnumUnbounded, false)?
            }
            "enumn-u" => {
                parse_op_seq(sess, &expr, SequenceMode::EnumUnbounded, true)?
            }
            "eliminate_errors" | "ee" => parse_op_eliminate_errors(&expr)?,
            "filter" => parse_op_filter(&expr)?,
            "primes" => parse_op_primes(&expr)?,
            "collect" => parse_op_collect(&expr)?,
            "lines" | "l" => parse_op_reject_args(&expr, create_op_lines)?,
            "chars" => parse_op_reject_args(&expr, create_op_chars)?,
            "typename" => parse_op_reject_args(&expr, create_op_typename)?,
            "trim" => parse_op_reject_args(&expr, create_op_trim)?,
            "from_tyson" => parse_op_reject_args(&expr, create_op_from_tyson)?,
            "to_tyson" => parse_op_reject_args(&expr, create_op_to_tyson)?,
            "to_int" => parse_op_reject_args(&expr, create_op_to_int)?,
            "to_float" => parse_op_reject_args(&expr, create_op_to_float)?,
            _ => return Ok(None),
        }))
    }

    fn setup(
        &mut self,
        _registry: &mut typeline_core::extension::ExtensionRegistry,
    ) {
    }
}
