use metamatch::metamatch;
use num::{BigInt, BigRational};
use std::{borrow::Cow, io::BufReader, sync::Arc};

use crate::{
    cli::call_expr::{Argument, CallExpr, ParsedArgValue, Span},
    job::JobData,
    options::{
        chain_settings::{ChainSetting, SettingUseFloatingPointMath},
        session_setup::SessionSetupData,
    },
    record_data::{
        array::Array,
        custom_data::CustomDataBox,
        field_data::FieldValueRepr,
        field_value::{FieldValue, FieldValueKind, Object},
        iter_hall::IterId,
        push_interface::PushInterface,
        stream_value::{StreamValue, StreamValueData},
    },
    scr_error::ScrError,
    tyson::{parse_tyson, TysonParseError},
    utils::{cow_to_small_str, cow_to_str},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{OperatorBase, OperatorData, OperatorName},
    transform::{TransformData, TransformId, TransformState},
    utils::maintain_single_value::{maintain_single_value, ExplicitCount},
};

#[derive(Clone)]
pub enum Literal {
    Bytes(Vec<u8>),
    StreamBytes(Arc<Vec<u8>>),
    Text(String),
    StreamString(Arc<String>),
    Object(Object),
    Array(Array),
    Int(i64),
    BigInt(BigInt),
    Float(f64),
    BigRational(BigRational),
    Null,
    Undefined,
    Error(String),
    StreamError(String),
    Custom(CustomDataBox),
    Argument(Argument),
}

#[derive(Clone)]
pub struct OpLiteral {
    pub data: Literal,
    pub insert_count: Option<usize>,
}

pub struct TfLiteral<'a> {
    data: &'a Literal,
    explicit_count: Option<ExplicitCount>,
    value_inserted: bool,
    iter_id: IterId,
}

impl OpLiteral {
    pub fn default_op_name(&self) -> OperatorName {
        match &self.data {
            Literal::Null => "null",
            Literal::Undefined => "undefined",
            Literal::Text(_) => "str",
            Literal::StreamString(_) => "~str",
            Literal::Bytes(_) => "bytes",
            Literal::StreamBytes(_) => "~bytes",
            Literal::Error(_) => "error",
            Literal::StreamError(_) => "~error",
            Literal::Int(_) => "int",
            Literal::BigInt(_) => "integer",
            Literal::Float(_) => "float",
            Literal::BigRational(_) => "rational",
            Literal::Object(_) => "object",
            Literal::Array(_) => "array",
            Literal::Argument(_) => "argument",
            Literal::Custom(v) => return cow_to_small_str(v.type_name()),
        }
        .into()
    }
}

pub fn build_tf_literal<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpLiteral,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let actor_id = jd.add_actor_for_tf_state(tf_state);
    let iter_id = jd.claim_iter_for_tf_state(tf_state);
    TransformData::Literal(TfLiteral {
        data: &op.data,
        explicit_count: op
            .insert_count
            .map(|count| ExplicitCount { count, actor_id }),
        value_inserted: false,
        iter_id,
    })
}

pub fn insert_value(
    jd: &mut JobData,
    tf_id: TransformId,
    lit: &mut TfLiteral,
) {
    let tf = &jd.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
    let of_id = jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let mut output_field = jd.field_mgr.fields[of_id].borrow_mut();
    metamatch!(match lit.data {
        #[expand(REP in [Null, Undefined])]
        Literal::REP => {
            output_field
                .iter_hall
                .push_zst(FieldValueRepr::REP, 1, true)
        }

        #[expand((REP, PUSH_FN, VAL) in [
            (Int, push_int, *v),
            (Float, push_float, *v),
            (Bytes, push_bytes, v),
            (Text, push_str, v),
            (Object, push_object, v.clone()),
            (Array, push_array, v.clone()),
            (BigInt, push_big_int, v.clone()),
            (BigRational, push_big_rational, v.clone()),
            (Custom, push_custom, v.clone()),
            (Argument, push_fixed_size_type, v.clone()),
        ])]
        Literal::REP(v) => {
            output_field.iter_hall.PUSH_FN(VAL, 1, true, true)
        }

        #[expand((LIT, DATA) in [(StreamString, Text), (StreamBytes, Bytes)])]
        Literal::LIT(ss) => {
            let sv_id = jd.sv_mgr.claim_stream_value(
                StreamValue::from_data_done(StreamValueData::DATA {
                    data: ss.clone(),
                    range: 0..ss.len(),
                }),
            );
            output_field
                .iter_hall
                .push_stream_value_id(sv_id, 1, true, false);
        }

        Literal::Error(e) => output_field.iter_hall.push_error(
            OperatorApplicationError::new_s(e.clone(), op_id),
            1,
            true,
            false,
        ),

        Literal::StreamError(ss) => {
            let sv_id = jd.sv_mgr.claim_stream_value(StreamValue {
                error: Some(Arc::new(OperatorApplicationError::new_s(
                    ss.clone(),
                    op_id,
                ))),
                done: true,
                ..Default::default()
            });
            output_field
                .iter_hall
                .push_stream_value_id(sv_id, 1, true, false);
        }
    })
}

pub fn handle_tf_literal(
    jd: &mut JobData,
    tf_id: TransformId,
    lit: &mut TfLiteral,
) {
    if !lit.value_inserted {
        lit.value_inserted = true;
        insert_value(jd, tf_id, lit);
    }
    let (batch_size, ps) = maintain_single_value(
        jd,
        tf_id,
        lit.explicit_count.as_ref(),
        lit.iter_id,
    );
    jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
}
pub fn parse_op_literal_zst(
    expr: &CallExpr,
    literal: Literal,
) -> Result<OperatorData, ScrError> {
    let insert_count = parse_insert_count_reject_value(expr)?;
    Ok(OperatorData::Literal(OpLiteral {
        data: literal,
        insert_count,
    }))
}
pub fn parse_op_str(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
    stream: bool,
) -> Result<OperatorData, ScrError> {
    let (insert_count, value, _value_span) =
        parse_insert_count_and_value_args_str(sess, expr)?;
    let value_owned = value.into_owned();
    Ok(OperatorData::Literal(OpLiteral {
        data: if stream {
            Literal::StreamString(Arc::new(value_owned))
        } else {
            Literal::Text(value_owned)
        },
        insert_count,
    }))
}

pub fn parse_op_error(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
    stream: bool,
) -> Result<OperatorData, ScrError> {
    let (insert_count, value, _value_span) =
        parse_insert_count_and_value_args_str(sess, expr)?;
    let value_owned = value.into_owned();
    Ok(OperatorData::Literal(OpLiteral {
        data: if stream {
            Literal::StreamError(value_owned)
        } else {
            Literal::Error(value_owned)
        },
        insert_count,
    }))
}

pub fn parse_insert_count_reject_value(
    expr: &CallExpr,
) -> Result<Option<usize>, ScrError> {
    let mut insert_count = None;
    for arg in expr.parsed_args_iter() {
        match arg.value {
            ParsedArgValue::NamedArg { key, value } => {
                if key == "i" || key == "insert_count" {
                    insert_count =
                        Some(value.try_cast_int(false).ok_or_else(|| {
                            expr.error_arg_invalid_int(key, arg.span)
                        })? as usize);
                    continue;
                }
                return Err(expr
                    .error_named_arg_unsupported(key, arg.span)
                    .into());
            }
            ParsedArgValue::Flag(flag) => {
                return Err(expr
                    .error_flag_unsupported(flag, arg.span)
                    .into());
            }
            ParsedArgValue::PositionalArg { .. } => {
                return Err(expr
                    .error_positional_args_unsupported(arg.span)
                    .into())
            }
        }
    }
    Ok(insert_count)
}

pub fn parse_insert_count_and_value_args<'a>(
    sess: &mut SessionSetupData,
    expr: &'a CallExpr<'a>,
) -> Result<(Option<usize>, Cow<'a, [u8]>, Span), ScrError> {
    let mut insert_count = None;
    let mut value = None;
    for arg in expr.parsed_args_iter_with_bounded_positionals(1, 1) {
        let arg = arg?;
        match arg.value {
            ParsedArgValue::Flag(flag) => {
                return Err(expr
                    .error_flag_unsupported(flag, arg.span)
                    .into());
            }
            ParsedArgValue::NamedArg { key, value } => {
                if key == "i" || key == "insert_count" {
                    // TODO: error on negative
                    insert_count =
                        Some(value.try_cast_int(false).ok_or_else(|| {
                            expr.error_arg_invalid_int(key, arg.span)
                        })? as usize);
                    continue;
                }
                return Err(expr
                    .error_named_arg_unsupported(key, arg.span)
                    .into());
            }
            ParsedArgValue::PositionalArg { arg, .. } => {
                // TODO: this is stupid
                value = Some((arg.stringify(sess).into_bytes_cow(), arg.span));
            }
        }
    }
    let (value, value_span) = value.unwrap();
    Ok((insert_count, value, value_span))
}

pub fn parse_insert_count_and_value_args_str<'a>(
    sess: &mut SessionSetupData,
    expr: &'a CallExpr<'a>,
) -> Result<(Option<usize>, Cow<'a, str>, Span), ScrError> {
    let (insert_count, value, value_span) =
        parse_insert_count_and_value_args(sess, expr)?;
    let value_str = cow_to_str(value)
        .map_err(|_| expr.error_positional_arg_invalid_utf8(value_span))?;

    Ok((insert_count, value_str, value_span))
}

pub fn parse_op_int(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
) -> Result<OperatorData, ScrError> {
    let (insert_count, value, value_span) =
        parse_insert_count_and_value_args_str(sess, expr)?;

    let data = if let Ok(i) = str::parse::<i64>(&value) {
        Literal::Int(i)
    } else {
        let Ok(big_int) = str::parse::<BigInt>(&value) else {
            return Err(expr
                .error_positional_arg_invalid_int(value_span)
                .into());
        };
        Literal::BigInt(big_int)
    };
    Ok(OperatorData::Literal(OpLiteral { data, insert_count }))
}
pub fn parse_op_bytes(
    sess: &mut SessionSetupData,
    arg: &mut Argument,
    stream: bool,
) -> Result<OperatorData, ScrError> {
    let call_expr = CallExpr::from_argument_mut(arg)?;
    let (insert_count, value, _value_span) =
        parse_insert_count_and_value_args(sess, &call_expr)?;
    Ok(OperatorData::Literal(OpLiteral {
        data: if stream {
            Literal::StreamBytes(Arc::new(value.into_owned()))
        } else {
            Literal::Bytes(value.into_owned())
        },
        insert_count,
    }))
}
pub fn field_value_to_literal(v: FieldValue) -> Literal {
    metamatch!(match v {
        #[expand(REP in [Null, Undefined])]
        FieldValue::REP => Literal::REP,

        #[expand(REP in [Int, Float, Bytes, Text, Array,  Custom])]
        FieldValue::REP(v) => Literal::REP(v),

        #[expand(REP in [BigInt, BigRational, Argument, Object])]
        FieldValue::REP(v) => Literal::REP(*v),

        FieldValue::Error(v) => Literal::Error(v.message().to_owned()),

        #[expand_pattern(REP in [Macro, StreamValueId, FieldReference, SlicedFieldReference])]
        FieldValue::REP(_) => {
            panic!("{} is not a valid literal", v.kind().to_str())
        }
    })
}
pub fn parse_op_tyson(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
    affinity: FieldValueKind,
) -> Result<OperatorData, ScrError> {
    let (insert_count, value, value_span) =
        parse_insert_count_and_value_args(sess, expr)?;
    let value = parse_tyson(
        BufReader::new(&*value),
        use_fpm(&mut Some(sess)),
        Some(&sess.extensions),
    )
    .map_err(|e| {
        OperatorCreationError::new_s(
            format!(
                "failed to parse value as {}: {}",
                affinity.to_str(),
                match e {
                    TysonParseError::Io(e) => e.to_string(),
                    TysonParseError::InvalidSyntax { kind, .. } =>
                        kind.to_string(),
                }
            ),
            value_span,
        )
    })?;
    let lit = field_value_to_literal(value);
    Ok(OperatorData::Literal(OpLiteral {
        data: lit,
        insert_count,
    }))
}

pub fn use_fpm(sess: &mut Option<&mut SessionSetupData>) -> bool {
    sess.as_deref_mut()
        .map(|sess| {
            sess.get_chain_setting::<SettingUseFloatingPointMath>(
                sess.curr_chain,
            )
        })
        .unwrap_or(SettingUseFloatingPointMath::DEFAULT)
}

pub fn build_op_tyson_value(
    mut sess: Option<&mut SessionSetupData>,
    value: &[u8],
    value_span: Span,
    insert_count: Option<usize>,
) -> Result<OperatorData, ScrError> {
    let value = parse_tyson(
        value,
        use_fpm(&mut sess),
        sess.as_ref().map(|sess| &*sess.extensions),
    )
    .map_err(|e| {
        OperatorCreationError::new_s(format!("invalid tyson: {e}"), value_span)
    })?;
    let lit = field_value_to_literal(value);
    Ok(OperatorData::Literal(OpLiteral {
        data: lit,
        insert_count,
    }))
}

pub fn parse_op_tyson_value(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
) -> Result<OperatorData, ScrError> {
    let (insert_count, value, value_span) =
        parse_insert_count_and_value_args(sess, expr)?;
    build_op_tyson_value(Some(sess), &value, value_span, insert_count)
}

pub fn create_op_literal_with_insert_count(
    data: Literal,
    insert_count: Option<usize>,
) -> OperatorData {
    OperatorData::Literal(OpLiteral { data, insert_count })
}

pub fn create_op_literal(data: Literal) -> OperatorData {
    create_op_literal_with_insert_count(data, None)
}
pub fn create_op_literal_n(
    data: Literal,
    insert_count: usize,
) -> OperatorData {
    create_op_literal_with_insert_count(data, Some(insert_count))
}

pub fn create_op_error(str: &str) -> OperatorData {
    create_op_literal(Literal::Error(str.to_owned()))
}
pub fn create_op_str(str: &str) -> OperatorData {
    create_op_literal(Literal::Text(str.to_owned()))
}
pub fn create_op_stream_bytes(v: &[u8]) -> OperatorData {
    create_op_literal(Literal::StreamBytes(Arc::new(v.to_owned())))
}
pub fn create_op_stream_str(v: &str) -> OperatorData {
    create_op_literal(Literal::StreamString(Arc::new(v.to_owned())))
}
pub fn create_op_bytes(v: &[u8]) -> OperatorData {
    create_op_literal(Literal::Bytes(v.to_owned()))
}
pub fn create_op_stream_error(str: &str) -> OperatorData {
    create_op_literal(Literal::StreamError(str.to_owned()))
}
pub fn create_op_int(v: i64) -> OperatorData {
    create_op_literal(Literal::Int(v))
}
pub fn create_op_int_big(v: BigInt) -> OperatorData {
    create_op_literal(Literal::BigInt(v))
}
pub fn create_op_null() -> OperatorData {
    create_op_literal(Literal::Null)
}
pub fn create_op_undefined() -> OperatorData {
    create_op_literal(Literal::Undefined)
}
pub fn create_op_v(str: &str) -> Result<OperatorData, ScrError> {
    build_op_tyson_value(None, str.as_bytes(), Span::Generated, None)
}

pub fn create_op_error_n(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Error(str.to_owned()), insert_count)
}
pub fn create_op_str_n(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Text(str.to_owned()), insert_count)
}
pub fn create_op_stream_bytes_n(
    v: &[u8],
    insert_count: usize,
) -> OperatorData {
    create_op_literal_n(
        Literal::StreamBytes(Arc::new(v.to_owned())),
        insert_count,
    )
}
pub fn create_op_stream_str_n(v: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(
        Literal::StreamString(Arc::new(v.to_owned())),
        insert_count,
    )
}
pub fn create_op_bytes_n(v: &[u8], insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Bytes(v.to_owned()), insert_count)
}
pub fn create_op_stream_error_n(
    str: &str,
    insert_count: usize,
) -> OperatorData {
    create_op_literal_n(Literal::StreamError(str.to_owned()), insert_count)
}
pub fn create_op_int_n(v: i64, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Int(v), insert_count)
}
pub fn create_op_null_n(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Null, insert_count)
}
pub fn create_op_success_n(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Undefined, insert_count)
}
pub fn create_op_v_n(
    str: &str,
    insert_count: usize,
) -> Result<OperatorData, ScrError> {
    build_op_tyson_value(
        None,
        str.as_bytes(),
        Span::Generated,
        Some(insert_count),
    )
}
