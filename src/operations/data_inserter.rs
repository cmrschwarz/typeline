use bstr::{BStr, BString, ByteSlice};
use smallstr::SmallString;

use crate::{
    field_data::push_interface::PushInterface,
    options::argument::CliArgIdx,
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

pub enum AnyData {
    Bytes(BString),
    String(String),
    Int(i64),
}

impl AnyData {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self {
            AnyData::Bytes(_) => SmallString::from("bytes"),
            AnyData::String(_) => SmallString::from("str"),
            AnyData::Int(_) => SmallString::from("int"),
        }
    }
}

pub struct OpDataInserter {
    pub data: AnyData,
}

pub struct TfDataInserter<'a> {
    data: &'a AnyData,
}

pub fn setup_tf_data_inserter<'a>(
    _sess: &mut JobData,
    op: &'a OpDataInserter,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    // we will forward the whole input in one go and unlink us from the chain
    tf_state.desired_batch_size = usize::MAX;
    let data = TransformData::DataInserter(TfDataInserter { data: &op.data });
    (data, tf_state.input_field)
}

pub fn handle_tf_data_inserter(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    di: &mut TfDataInserter,
) {
    let mut input_field =
        sess.record_mgr.fields[sess.tf_mgr.transforms[tf_id].input_field].borrow_mut();
    match di.data {
        AnyData::Bytes(b) => input_field.field_data.push_bytes(b, 1, true, false),
        AnyData::String(s) => input_field.field_data.push_str(s, 1, true, false),
        AnyData::Int(i) => input_field.field_data.push_int(*i, 1, true, false),
    }
    sess.tf_mgr.unlink_transform(tf_id, 1);
}

pub fn parse_op_str(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing value for str", arg_idx))?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "str argument must be valid UTF-8, consider using bytes=...",
                arg_idx,
            )
        })?;
    Ok(OperatorData::DataInserter(OpDataInserter {
        data: AnyData::String(value_str.to_owned()),
    }))
}

pub fn parse_op_int(
    value: Option<&BStr>,
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
    Ok(OperatorData::DataInserter(OpDataInserter {
        data: AnyData::Int(parsed_value),
    }))
}
pub fn parse_op_bytes(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let parsed_value = if let Some(value) = value {
        value.to_owned()
    } else {
        return Err(OperatorCreationError::new(
            "missing value for bytes",
            arg_idx,
        ));
    };
    Ok(OperatorData::DataInserter(OpDataInserter {
        data: AnyData::Bytes(parsed_value),
    }))
}
