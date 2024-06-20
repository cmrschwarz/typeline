use std::sync::Arc;

use bstr::ByteSlice;
use num::{BigInt, BigRational};
use once_cell::sync::Lazy;
use regex::Regex;
use smallstr::SmallString;

use crate::{
    cli::{parse_args_as_single_str, require_single_operator_param},
    job::JobData,
    options::{
        argument::CliArgIdx, chain_options::DEFAULT_CHAIN_OPTIONS,
        session_options::SessionOptions,
    },
    record_data::{
        array::Array,
        custom_data::CustomDataBox,
        field_value::{FieldValue, FieldValueKind, Object},
        iter_hall::IterId,
        push_interface::PushInterface,
        stream_value::{StreamValue, StreamValueData},
    },
    tyson::{parse_tyson, TysonParseError},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
    utils::maintain_single_value::{maintain_single_value, ExplicitCount},
};

#[derive(Clone)]
pub enum Literal {
    Bytes(Vec<u8>),
    StreamBytes(Arc<Vec<u8>>),
    String(String),
    StreamString(Arc<String>),
    Object(Object),
    Array(Array),
    Int(i64),
    BigInt(BigInt),
    Float(f64),
    Rational(BigRational),
    Null,
    Undefined,
    Error(String),
    StreamError(String),
    Custom(CustomDataBox),
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
    pub fn default_op_name(&self) -> DefaultOperatorName {
        let mut res = SmallString::new();
        match &self.data {
            Literal::Null => res.push_str("null"),
            Literal::Undefined => res.push_str("undefined"),
            Literal::String(_) => res.push_str("str"),
            Literal::StreamString(_) => res.push_str("~str"),
            Literal::Bytes(_) => res.push_str("bytes"),
            Literal::StreamBytes(_) => res.push_str("~bytes"),
            Literal::Error(_) => res.push_str("error"),
            Literal::StreamError(_) => res.push_str("~error"),
            Literal::Int(_) => res.push_str("int"),
            Literal::BigInt(_) => res.push_str("integer"),
            Literal::Float(_) => res.push_str("float"),
            Literal::Rational(_) => res.push_str("rational"),
            Literal::Object(_) => res.push_str("object"),
            Literal::Array(_) => res.push_str("array"),
            Literal::Custom(v) => res.push_str(&v.type_name()),
        }
        res
    }
}

pub fn build_tf_literal<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpLiteral,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let iter_id = jd.add_iter_for_tf_state(tf_state);
    TransformData::Literal(TfLiteral {
        data: &op.data,
        explicit_count: op.insert_count.map(|count| ExplicitCount {
            count,
            actor_id: jd.add_actor_for_tf_state(tf_state),
        }),
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
    match lit.data {
        Literal::Bytes(b) => {
            output_field.iter_hall.push_bytes(b, 1, true, true)
        }
        Literal::String(s) => {
            output_field.iter_hall.push_str(s, 1, true, true)
        }
        Literal::Int(i) => output_field.iter_hall.push_int(*i, 1, true, true),
        Literal::Null => output_field.iter_hall.push_null(1, true),
        Literal::Undefined => output_field.iter_hall.push_undefined(1, true),
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
        Literal::StreamString(ss) => {
            let sv_id = jd.sv_mgr.claim_stream_value(
                StreamValue::from_data_done(StreamValueData::Text {
                    data: ss.clone(),
                    range: 0..ss.len(),
                }),
            );
            output_field
                .iter_hall
                .push_stream_value_id(sv_id, 1, true, false);
        }
        Literal::StreamBytes(sb) => {
            let sv_id = jd.sv_mgr.claim_stream_value(
                StreamValue::from_data_done(StreamValueData::Bytes {
                    data: sb.clone(),
                    range: 0..sb.len(),
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
        Literal::Object(o) => output_field.iter_hall.push_fixed_size_type(
            o.clone(),
            1,
            true,
            true,
        ),
        Literal::Array(v) => output_field.iter_hall.push_fixed_size_type(
            v.clone(),
            1,
            true,
            true,
        ),
        Literal::BigInt(v) => output_field.iter_hall.push_fixed_size_type(
            v.clone(),
            1,
            true,
            true,
        ),
        Literal::Float(v) => output_field
            .iter_hall
            .push_fixed_size_type(*v, 1, true, true),
        Literal::Rational(v) => output_field.iter_hall.push_fixed_size_type(
            v.clone(),
            1,
            true,
            true,
        ),
        Literal::Custom(v) => output_field.iter_hall.push_fixed_size_type(
            v.clone(),
            1,
            true,
            true,
        ),
    }
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
    arg: &str,
    literal: Literal,
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError {
            message: format!("{arg} takes no argument").into(),
            cli_arg_idx: arg_idx,
        });
    }
    Ok(OperatorData::Literal(OpLiteral {
        data: literal,
        insert_count,
    }))
}
pub fn parse_op_str(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
    stream_str: bool,
) -> Result<OperatorData, OperatorCreationError> {
    let tilde = if stream_str { "~" } else { "" };
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new_s(
                format!("missing value for {tilde}str"),
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new_s(
                format!("{tilde}str argument must be valid UTF-8, consider using {tilde}bytes=..."
            ),
                arg_idx,
            )
        })?;
    let value_owned = value_str.to_owned();
    Ok(OperatorData::Literal(OpLiteral {
        data: if stream_str {
            Literal::StreamString(Arc::new(value_owned))
        } else {
            Literal::String(value_owned)
        },
        insert_count,
    }))
}

pub fn parse_op_error(
    arg_str: &str,
    value: Option<&[u8]>,
    stream: bool,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new_s(
                format!("missing value for {arg_str}"),
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new_s(
                format!("{arg_str} argument must be valid UTF-8"),
                arg_idx,
            )
        })?;
    let value_owned = value_str.to_owned();
    Ok(OperatorData::Literal(OpLiteral {
        data: if stream {
            Literal::StreamError(value_owned)
        } else {
            Literal::Error(value_owned)
        },
        insert_count,
    }))
}

pub fn parse_op_int(
    params: &[&[u8]],
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value = require_single_operator_param("int", params, arg_idx)?;
    let value_str = parse_args_as_single_str("int", value, arg_idx)?;

    let data = if let Ok(i) = str::parse::<i64>(value_str) {
        Literal::Int(i)
    } else {
        let Ok(big_int) = str::parse::<BigInt>(value_str) else {
            return Err(OperatorCreationError::new(
                "failed to parse value as an integer",
                arg_idx,
            ));
        };
        Literal::BigInt(big_int)
    };
    Ok(OperatorData::Literal(OpLiteral { data, insert_count }))
}
pub fn parse_op_bytes(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
    stream_bytes: bool,
) -> Result<OperatorData, OperatorCreationError> {
    let parsed_value = if let Some(value) = value {
        value.to_owned()
    } else {
        return Err(OperatorCreationError::new(
            "missing value for bytes",
            arg_idx,
        ));
    };
    Ok(OperatorData::Literal(OpLiteral {
        data: if stream_bytes {
            Literal::StreamBytes(Arc::new(parsed_value))
        } else {
            Literal::Bytes(parsed_value)
        },
        insert_count,
    }))
}
pub fn field_value_to_literal(v: FieldValue) -> Literal {
    match v {
        FieldValue::Null => Literal::Null,
        FieldValue::Undefined => Literal::Undefined,
        FieldValue::Int(v) => Literal::Int(v),
        FieldValue::BigInt(v) => Literal::BigInt(*v),
        FieldValue::Float(v) => Literal::Float(v),
        FieldValue::Rational(v) => Literal::Rational(*v),
        FieldValue::Bytes(v) => Literal::Bytes(v),
        FieldValue::Text(v) => Literal::String(v),
        FieldValue::Error(v) => Literal::Error(v.message().to_owned()),
        FieldValue::Array(v) => Literal::Array(v),
        FieldValue::Object(v) => Literal::Object(v),
        FieldValue::Custom(v) => Literal::Custom(v),

        FieldValue::StreamValueId(_)
        | FieldValue::FieldReference(_)
        | FieldValue::SlicedFieldReference(_) => {
            panic!("{} is not a valid literal", v.kind().to_str())
        }
    }
}
pub fn parse_op_tyson(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
    affinity: FieldValueKind,
    sess: &SessionOptions,
) -> Result<OperatorData, OperatorCreationError> {
    let value = value.ok_or_else(|| {
        OperatorCreationError::new_s(
            format!("missing value for {}", affinity.to_str()),
            arg_idx,
        )
    })?;
    let value =
        parse_tyson(value, use_fpm(Some(sess)), Some(&sess.extensions))
            .map_err(|e| {
                OperatorCreationError::new_s(
                    format!(
                        "failed to parse value as {}: {}",
                        affinity.to_str(),
                        match e {
                            TysonParseError::Io(e) => e.to_string(),
                            TysonParseError::InvalidSyntax {
                                kind, ..
                            } => kind.to_string(),
                        }
                    ),
                    arg_idx,
                )
            })?;
    let lit = field_value_to_literal(value);
    Ok(OperatorData::Literal(OpLiteral {
        data: lit,
        insert_count,
    }))
}

pub fn use_fpm(sess: Option<&SessionOptions>) -> bool {
    let fpm_default = DEFAULT_CHAIN_OPTIONS.floating_point_math.get().unwrap();
    sess.map(|sess| {
        sess.chains[sess.curr_chain]
            .floating_point_math
            .get()
            .unwrap_or(fpm_default)
    })
    .unwrap_or(fpm_default)
}

pub fn parse_op_tyson_value(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
    sess: Option<&SessionOptions>,
) -> Result<OperatorData, OperatorCreationError> {
    let value = value
        .ok_or_else(|| OperatorCreationError::new("missing value", arg_idx))?;

    let value =
        parse_tyson(value, use_fpm(sess), sess.map(|sess| &*sess.extensions))
            .map_err(|e| {
                OperatorCreationError::new_s(
                    format!("invalid tyson: {e}"),
                    arg_idx,
                )
            })?;
    let lit = field_value_to_literal(value);
    Ok(OperatorData::Literal(OpLiteral {
        data: lit,
        insert_count,
    }))
}

static ARG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^(?<type>int|float|rational|~?bytes|~?str|~?error|null|undefined|object|array|v|tyson)(?<insert_count>[0-9]+)?$"
    ).unwrap()
});

pub fn argument_matches_op_literal(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_literal(
    argument: &str,
    value: &[&[u8]],
    arg_idx: Option<CliArgIdx>,
    sess: &SessionOptions,
) -> Result<OperatorData, OperatorCreationError> {
    // this should not happen in the cli parser because it checks using
    // `argument_matches_data_inserter`
    let args = ARG_REGEX.captures(argument).ok_or_else(|| {
        OperatorCreationError::new(
            "invalid argument syntax for literal",
            arg_idx,
        )
    })?;
    let insert_count = args
        .name("insert_count")
        .map(|ic| {
            ic.as_str().parse::<usize>().map_err(|_| {
                OperatorCreationError::new(
                    "failed to parse insertion count as an integer",
                    arg_idx,
                )
            })
        })
        .transpose()?;
    let arg_str = args.name("type").unwrap().as_str();
    match arg_str {
        "int" => parse_op_int(value, insert_count, arg_idx),
        "bytes" => parse_op_bytes(value, insert_count, arg_idx, false),
        "~bytes" => parse_op_bytes(value, insert_count, arg_idx, true),
        "str" => parse_op_str(value, insert_count, arg_idx, false),
        "~str" => parse_op_str(value, insert_count, arg_idx, true),
        "object" => parse_op_tyson(
            value,
            insert_count,
            arg_idx,
            FieldValueKind::Object,
            sess,
        ),
        "array" => parse_op_tyson(
            value,
            insert_count,
            arg_idx,
            FieldValueKind::Array,
            sess,
        ),
        "float" => parse_op_tyson(
            value,
            insert_count,
            arg_idx,
            FieldValueKind::Float,
            sess,
        ),
        "rational" => parse_op_tyson(
            value,
            insert_count,
            arg_idx,
            FieldValueKind::Rational,
            sess,
        ),
        "v" | "tyson" => {
            parse_op_tyson_value(value, insert_count, arg_idx, Some(sess))
        }
        "error" => {
            parse_op_error(arg_str, value, false, insert_count, arg_idx)
        }
        "~error" => {
            parse_op_error(arg_str, value, true, insert_count, arg_idx)
        }
        v @ "null" => parse_op_literal_zst(
            v,
            Literal::Null,
            value,
            insert_count,
            arg_idx,
        ),
        v @ "undefined" => parse_op_literal_zst(
            v,
            Literal::Undefined,
            value,
            insert_count,
            arg_idx,
        ),
        _ => unreachable!(),
    }
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
    create_op_literal(Literal::String(str.to_owned()))
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
pub fn create_op_v(str: &str) -> Result<OperatorData, OperatorCreationError> {
    parse_op_tyson_value(Some(str.as_bytes()), None, None, None)
}

pub fn create_op_error_n(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Error(str.to_owned()), insert_count)
}
pub fn create_op_str_n(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::String(str.to_owned()), insert_count)
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
) -> Result<OperatorData, OperatorCreationError> {
    parse_op_tyson_value(Some(str.as_bytes()), Some(insert_count), None, None)
}
