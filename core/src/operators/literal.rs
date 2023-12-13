use bstr::ByteSlice;
use num_bigint::BigInt;
use num_rational::BigRational;
use regex::Regex;
use smallstr::SmallString;

use crate::{
    extension::ExtensionRegistry,
    job_session::JobData,
    options::argument::CliArgIdx,
    record_data::{
        custom_data::CustomDataBox,
        field_value::{Array, FieldValue, FieldValueKind, Object},
        push_interface::PushInterface,
        stream_value::{StreamValue, StreamValueData},
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
    utils::tyson::{parse_tyson, TysonParseError},
};

#[derive(Clone)]
pub enum Literal {
    Bytes(Vec<u8>),
    StreamBytes(Vec<u8>),
    String(String),
    StreamString(String),
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
    insert_count: Option<usize>,
    value_inserted: bool,
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
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpLiteral,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Literal(TfLiteral {
        data: &op.data,
        insert_count: op.insert_count,
        value_inserted: false,
    })
}

pub fn handle_tf_literal(
    sess: &mut JobData,
    tf_id: TransformId,
    lit: &mut TfLiteral,
) {
    let tf = &sess.tf_mgr.transforms[tf_id];
    let initial_call = !lit.value_inserted;
    if !lit.value_inserted {
        lit.value_inserted = true;
        let op_id = tf.op_id.unwrap();
        let of_id = sess.tf_mgr.prepare_output_field(
            &mut sess.field_mgr,
            &mut sess.match_set_mgr,
            tf_id,
        );
        let mut output_field = sess.field_mgr.fields[of_id].borrow_mut();
        match lit.data {
            Literal::Bytes(b) => {
                output_field.iter_hall.push_bytes(b, 1, true, true)
            }
            Literal::String(s) => {
                output_field.iter_hall.push_str(s, 1, true, true)
            }
            Literal::Int(i) => {
                output_field.iter_hall.push_int(*i, 1, true, true)
            }
            Literal::Null => output_field.iter_hall.push_null(1, true),
            Literal::Undefined => {
                output_field.iter_hall.push_undefined(1, true)
            }
            Literal::StreamError(ss) => {
                let sv_id = sess.sv_mgr.stream_values.claim_with_value(
                    StreamValue::new(
                        StreamValueData::Error(
                            OperatorApplicationError::new_s(ss.clone(), op_id),
                        ),
                        true,
                        true,
                    ),
                );
                output_field
                    .iter_hall
                    .push_stream_value_id(sv_id, 1, true, false);
            }
            Literal::StreamString(ss) => {
                let sv_id = sess.sv_mgr.stream_values.claim_with_value(
                    StreamValue::new(
                        StreamValueData::Bytes(ss.clone().into_bytes()),
                        true,
                        true,
                    ),
                );
                output_field
                    .iter_hall
                    .push_stream_value_id(sv_id, 1, true, false);
            }
            Literal::StreamBytes(sb) => {
                let sv_id = sess.sv_mgr.stream_values.claim_with_value(
                    StreamValue::new(
                        StreamValueData::Bytes(sb.clone()),
                        true,
                        true,
                    ),
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
            Literal::Rational(v) => output_field
                .iter_hall
                .push_fixed_size_type(v.clone(), 1, true, true),
            Literal::Custom(v) => output_field.iter_hall.push_fixed_size_type(
                v.clone(),
                1,
                true,
                true,
            ),
        }
    }
    let (batch_size, input_done) = sess.tf_mgr.maintain_single_value(
        tf_id,
        &mut lit.insert_count,
        &sess.field_mgr,
        &mut sess.match_set_mgr,
        initial_call,
        true,
    );
    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr.push_tf_in_ready_stack(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
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
            Literal::StreamString(value_owned)
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
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new("missing value for int", arg_idx)
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "failed to parse value as integer (invalid utf-8)",
                arg_idx,
            )
        })?;
    let parsed_value = str::parse::<i64>(value_str).map_err(|_| {
        OperatorCreationError::new("failed to parse value as integer", arg_idx)
    })?;
    Ok(OperatorData::Literal(OpLiteral {
        data: Literal::Int(parsed_value),
        insert_count,
    }))
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
            Literal::StreamBytes(parsed_value)
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
        FieldValue::BigInt(v) => Literal::BigInt(v),
        FieldValue::Float(v) => Literal::Float(v),
        FieldValue::Rational(v) => Literal::Rational(*v),
        FieldValue::Bytes(v) => Literal::Bytes(v),
        FieldValue::Text(v) => Literal::String(v),
        FieldValue::Error(v) => Literal::Error(v.message().to_owned()),
        FieldValue::Array(v) => Literal::Array(v),
        FieldValue::Object(v) => Literal::Object(v),
        FieldValue::Custom(v) => Literal::Custom(v),
        FieldValue::FieldReference(_)
        | FieldValue::SlicedFieldReference(_) => {
            panic!("field reference is not a valid literal")
        }
    }
}
pub fn parse_op_tyson(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
    affinity: FieldValueKind,
    exts: &ExtensionRegistry,
) -> Result<OperatorData, OperatorCreationError> {
    let value = value.ok_or_else(|| {
        OperatorCreationError::new_s(
            format!("missing value for {}", affinity.to_str()),
            arg_idx,
        )
    })?;
    let value = parse_tyson(value, Some(exts)).map_err(|e| {
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
            arg_idx,
        )
    })?;
    let lit = field_value_to_literal(value);
    Ok(OperatorData::Literal(OpLiteral {
        data: lit,
        insert_count,
    }))
}

pub fn parse_op_tyson_value(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
    exts: Option<&ExtensionRegistry>,
) -> Result<OperatorData, OperatorCreationError> {
    let value = value
        .ok_or_else(|| OperatorCreationError::new("missing value", arg_idx))?;

    let value = parse_tyson(value, exts).map_err(|e| {
        OperatorCreationError::new_s(format!("invalid tyson: {e}"), arg_idx)
    })?;
    let lit = field_value_to_literal(value);
    Ok(OperatorData::Literal(OpLiteral {
        data: lit,
        insert_count,
    }))
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(
        r"^(?<type>int|integer|float|rational|~?bytes|~?str|~?error|null|undefined|object|array|v|tyson)(?<insert_count>[0-9]+)?$"
    ).unwrap();
}

pub fn argument_matches_op_literal(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_literal(
    argument: &str,
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
    ext: &ExtensionRegistry,
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
                    "failed to parse insertion count as integer",
                    arg_idx,
                )
            })
        })
        .transpose()?;
    let arg_str = args.name("type").unwrap().as_str();
    use FieldValueKind::*;
    match arg_str {
        "int" => parse_op_int(value, insert_count, arg_idx),
        "bytes" => parse_op_bytes(value, insert_count, arg_idx, false),
        "~bytes" => parse_op_bytes(value, insert_count, arg_idx, true),
        "str" => parse_op_str(value, insert_count, arg_idx, false),
        "~str" => parse_op_str(value, insert_count, arg_idx, true),
        "object" => parse_op_tyson(value, insert_count, arg_idx, Object, ext),
        "array" => parse_op_tyson(value, insert_count, arg_idx, Array, ext),
        "integer" => parse_op_tyson(value, insert_count, arg_idx, BigInt, ext),
        "float" => parse_op_tyson(value, insert_count, arg_idx, Float, ext),
        "rational" => {
            parse_op_tyson(value, insert_count, arg_idx, Rational, ext)
        }
        "v" | "tyson" => {
            parse_op_tyson_value(value, insert_count, arg_idx, Some(ext))
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

pub fn create_op_literal(
    data: Literal,
    insert_count: Option<usize>,
) -> OperatorData {
    OperatorData::Literal(OpLiteral { data, insert_count })
}
pub fn create_op_literal_n(
    data: Literal,
    insert_count: usize,
) -> OperatorData {
    OperatorData::Literal(OpLiteral {
        data,
        insert_count: if insert_count == 0 {
            None
        } else {
            Some(insert_count)
        },
    })
}

pub fn create_op_error(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Error(str.to_owned()), insert_count)
}
pub fn create_op_str(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::String(str.to_owned()), insert_count)
}
pub fn create_op_v(
    str: &str,
    insert_count: usize,
) -> Result<OperatorData, OperatorCreationError> {
    parse_op_tyson_value(
        Some(str.as_bytes()),
        if insert_count == 0 {
            None
        } else {
            Some(insert_count)
        },
        None,
        None,
    )
}
pub fn create_op_bytes(v: &[u8], insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Bytes(v.to_owned()), insert_count)
}
pub fn create_op_stream_error(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::StreamError(str.to_owned()), insert_count)
}
pub fn create_op_int(v: i64, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Int(v), insert_count)
}
pub fn create_op_null(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Null, insert_count)
}
pub fn create_op_success(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Undefined, insert_count)
}
