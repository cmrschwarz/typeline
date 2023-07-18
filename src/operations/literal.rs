use std::borrow::Cow;

use bstr::{BStr, BString, ByteSlice};
use regex::Regex;
use smallstr::SmallString;

use crate::{
    field_data::push_interface::PushInterface,
    options::argument::CliArgIdx,
    stream_value::{StreamValue, StreamValueData},
    worker_thread_session::JobSession,
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{OperatorBase, OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub enum Literal {
    Bytes(BString),
    String(String),
    Int(i64),
    Null,
    Unset,
    Success,
    Error(String),
    StreamError(String),
    StreamBytes(BString),
    StreamString(String),
}

#[derive(Clone)]
pub struct OpLiteral {
    pub data: Literal,
    pub insert_count: Option<usize>,
}

pub struct TfLiteral<'a> {
    data: &'a Literal,
    insert_count: Option<usize>,
}

impl OpLiteral {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        let mut res = SmallString::new();
        match self.data {
            Literal::Null => res.push_str("null"),
            Literal::Unset => res.push_str("unset"),
            Literal::Success => res.push_str("success"),
            Literal::String(_) => res.push_str("str"),
            Literal::Bytes(_) => res.push_str("bytes"),
            Literal::Error(_) => res.push_str("error"),
            Literal::StreamString(_) => res.push_str("~str"),
            Literal::StreamBytes(_) => res.push_str("~bytes"),
            Literal::StreamError(_) => res.push_str("~error"),
            Literal::Int(_) => res.push_str("int"),
        }
        res
    }
}

pub fn setup_tf_data_inserter<'a>(
    _sess: &mut JobSession,
    _op_base: &OperatorBase,
    op: &'a OpLiteral,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::DataInserter(TfLiteral {
        data: &op.data,
        insert_count: op.insert_count,
    })
}

pub fn handle_tf_literal(sess: &mut JobSession<'_>, tf_id: TransformId, di: &mut TfLiteral) {
    let (mut batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
    let output_field_id = tf.output_field;
    let mut unlink_after = false;
    if let Some(ic) = di.insert_count {
        if batch_size >= ic {
            sess.tf_mgr.unclaim_batch_size(tf_id, batch_size - ic);
            batch_size = ic;
            unlink_after = true;
        } else {
            let tf = &sess.tf_mgr.transforms[tf_id];
            if input_done {
                if batch_size < tf.desired_batch_size {
                    batch_size = ic.min(tf.desired_batch_size);
                }
                if batch_size == ic {
                    unlink_after = true;
                } else {
                    sess.tf_mgr.unclaim_batch_size(tf_id, ic - batch_size);
                }
            }
        }
        di.insert_count = Some(ic - batch_size);
    } else {
        let tf = &sess.tf_mgr.transforms[tf_id];
        let yield_to_cont = tf.continuation.is_some();
        if yield_to_cont {
            sess.tf_mgr
                .unclaim_batch_size(tf_id, batch_size.saturating_sub(1));
            batch_size = 1;
            unlink_after = true;
        } else {
            unlink_after = input_done;
            if batch_size == 0 {
                debug_assert!(input_done);
                batch_size = 1;
            }
        }
    }
    sess.prepare_for_output(tf_id, &[output_field_id]);
    let mut output_field = sess.field_mgr.fields[output_field_id].borrow_mut();
    match di.data {
        Literal::Bytes(b) => output_field
            .field_data
            .push_bytes(b, batch_size, true, true),
        Literal::String(s) => output_field.field_data.push_str(s, batch_size, true, true),
        Literal::Int(i) => output_field.field_data.push_int(*i, batch_size, true, true),
        Literal::Null => output_field.field_data.push_null(batch_size, true),
        Literal::Unset => output_field.field_data.push_unset(batch_size, true),
        Literal::Success => output_field.field_data.push_success(batch_size, true),
        Literal::StreamString(ss) => {
            output_field.field_data.push_stream_value_id(
                sess.sv_mgr.stream_values.claim_with_value(StreamValue::new(
                    StreamValueData::Bytes(ss.as_bytes().to_owned()),
                    true,
                    true,
                )),
                batch_size,
                true,
                false,
            );
        }
        Literal::StreamBytes(ss) => {
            output_field.field_data.push_stream_value_id(
                sess.sv_mgr.stream_values.claim_with_value(StreamValue::new(
                    StreamValueData::Bytes(ss.as_bytes().to_owned()),
                    false,
                    true,
                )),
                batch_size,
                true,
                false,
            );
        }
        Literal::StreamError(ss) => {
            output_field.field_data.push_stream_value_id(
                sess.sv_mgr.stream_values.claim_with_value(StreamValue::new(
                    StreamValueData::Error(OperatorApplicationError {
                        op_id,
                        message: ss.clone().into(),
                    }),
                    true,
                    true,
                )),
                batch_size,
                true,
                false,
            );
        }
        Literal::Error(e) => output_field.field_data.push_error(
            OperatorApplicationError {
                op_id,
                message: Cow::Owned(e.clone()),
            },
            batch_size,
            true,
            true,
        ),
    }
    drop(output_field);
    if unlink_after {
        sess.unlink_transform(tf_id, batch_size);
        return;
    }
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, batch_size);
    sess.tf_mgr.update_ready_state(tf_id);
}
pub fn parse_op_literal_zst(
    arg: &str,
    literal: Literal,
    value: Option<&BStr>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if !value.is_none() {
        return Err(OperatorCreationError {
            message: format!("{arg} takes no argument").into(),
            cli_arg_idx: arg_idx,
        });
    }
    Ok(OperatorData::DataInserter(OpLiteral {
        data: literal,
        insert_count,
    }))
}
pub fn parse_op_str(
    arg_str: &str,
    value: Option<&BStr>,
    stream: bool,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new_s(format!("missing value for {arg_str}"), arg_idx)
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new_s(
                format!("{arg_str} argument must be valid UTF-8, consider using bytes=..."),
                arg_idx,
            )
        })?;
    let value_owned = value_str.to_owned();
    Ok(OperatorData::DataInserter(OpLiteral {
        data: if stream {
            Literal::StreamString(value_owned)
        } else {
            Literal::String(value_owned)
        },
        insert_count,
    }))
}
pub fn parse_op_error(
    arg_str: &str,
    value: Option<&BStr>,
    stream: bool,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new_s(format!("missing value for {arg_str}"), arg_idx)
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new_s(format!("{arg_str} argument must be valid UTF-8"), arg_idx)
        })?;
    let value_owned = value_str.to_owned();
    Ok(OperatorData::DataInserter(OpLiteral {
        data: if stream {
            Literal::StreamError(value_owned)
        } else {
            Literal::Error(value_owned)
        },
        insert_count,
    }))
}

pub fn parse_op_int(
    value: Option<&BStr>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing value for int", arg_idx))?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new("failed to parse value as integer (invalid utf-8)", arg_idx)
        })?;
    let parsed_value = str::parse::<i64>(value_str)
        .map_err(|_| OperatorCreationError::new("failed to value as integer", arg_idx))?;
    Ok(OperatorData::DataInserter(OpLiteral {
        data: Literal::Int(parsed_value),
        insert_count,
    }))
}
pub fn parse_op_bytes(
    arg_str: &str,
    value: Option<&BStr>,
    stream: bool,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let parsed_value = if let Some(value) = value {
        value.to_owned()
    } else {
        return Err(OperatorCreationError::new_s(
            format!("missing value for {arg_str}"),
            arg_idx,
        ));
    };
    Ok(OperatorData::DataInserter(OpLiteral {
        data: if stream {
            Literal::StreamBytes(parsed_value)
        } else {
            Literal::Bytes(parsed_value)
        },
        insert_count,
    }))
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^(?<type>int|(?:~)bytes|(?:~)?str|(?:~)error|null|unset|success)(?<insert_count>[0-9]+)?$").unwrap();
}

pub fn argument_matches_op_literal(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_literal(
    argument: &str,
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    // this should not happen in the cli parser because it checks using `argument_matches_data_inserter`
    let args = ARG_REGEX.captures(&argument).ok_or_else(|| {
        OperatorCreationError::new("invalid argument syntax for data inserter", arg_idx)
    })?;
    let insert_count = args
        .name("insert_count")
        .map(|ic| {
            ic.as_str().parse::<usize>().map_err(|_| {
                OperatorCreationError::new("failed to parse insertion count as integer", arg_idx)
            })
        })
        .transpose()?;
    let arg_str = args.name("type").unwrap().as_str();
    match arg_str {
        "int" => parse_op_int(value, insert_count, arg_idx),
        "bytes" => parse_op_bytes(arg_str, value, false, insert_count, arg_idx),
        "~bytes" => parse_op_bytes(arg_str, value, true, insert_count, arg_idx),
        "str" => parse_op_str(arg_str, value, false, insert_count, arg_idx),
        "~str" => parse_op_str(arg_str, value, true, insert_count, arg_idx),
        "error" => parse_op_error(arg_str, value, false, insert_count, arg_idx),
        "~error" => parse_op_error(arg_str, value, true, insert_count, arg_idx),
        v @ "null" => parse_op_literal_zst(v, Literal::Null, value, insert_count, arg_idx),
        v @ "unset" => parse_op_literal_zst(v, Literal::Unset, value, insert_count, arg_idx),
        v @ "success" => parse_op_literal_zst(v, Literal::Success, value, insert_count, arg_idx),
        _ => unreachable!(),
    }
}

pub fn create_op_literal(data: Literal, insert_count: Option<usize>) -> OperatorData {
    OperatorData::DataInserter(OpLiteral { data, insert_count })
}
pub fn create_op_literal_n(data: Literal, insert_count: usize) -> OperatorData {
    OperatorData::DataInserter(OpLiteral {
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
pub fn create_op_bytes(v: &[u8], insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Bytes(v.as_bstr().to_owned()), insert_count)
}
pub fn create_op_stream_error(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::StreamError(str.to_owned()), insert_count)
}
pub fn create_op_stream_str(str: &str, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::StreamString(str.to_owned()), insert_count)
}
pub fn create_op_stream_bytes(v: &[u8], insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::StreamBytes(v.as_bstr().to_owned()), insert_count)
}
pub fn create_op_int(v: i64, insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Int(v), insert_count)
}
pub fn create_op_null(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Null, insert_count)
}
pub fn create_op_unset(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Unset, insert_count)
}
pub fn create_op_success(insert_count: usize) -> OperatorData {
    create_op_literal_n(Literal::Success, insert_count)
}
