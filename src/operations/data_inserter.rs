use bstr::{BStr, BString, ByteSlice};
use smallstr::SmallString;

use crate::{
    field_data::fd_push_interface::FDPushInterface,
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
    tf_state.is_batch_producer = true;
    // we will forward the whole input in one go and unlink us from the chain
    tf_state.desired_batch_size = usize::MAX;
    let data = TransformData::DataInserter(TfDataInserter { data: &op.data });
    (data, tf_state.input_field)
}

pub fn handle_tf_data_inserter_batch_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    di: &mut TfDataInserter,
) {
    let (batch, field) = sess.claim_batch(tf_id, &[]);
    let tf_state = &mut sess.tf_mgr.transforms[tf_id];
    let mut input_field = sess.entry_data.fields[field].borrow_mut();
    match di.data {
        AnyData::Bytes(b) => input_field.field_data.push_bytes(b, 1, true, false),
        AnyData::String(s) => input_field.field_data.push_str(s, 1, true, false),
        AnyData::Int(i) => input_field.field_data.push_int(*i, 1, true, false),
    }
    let original_succ = tf_state.successor;
    let original_pred = tf_state.predecessor;
    if let Some(succ) = original_succ {
        sess.tf_mgr.transforms[succ].predecessor = original_pred;
        sess.tf_mgr.inform_transform_batch_available(succ, batch);
        //TODO: remove transform
    }
}

pub fn parse_op_str(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let parsed_value = if let Some(value) = value {
        value
            .as_bytes()
            .to_str()
            .map_err(|_| {
                OperatorCreationError::new(
                    "str argument must be valid UTF-8, consider using bytes=...",
                    arg_idx,
                )
            })?
            .to_owned()
    } else {
        return Err(OperatorCreationError::new(
            "missing value argument for str",
            arg_idx,
        ));
    };
    Ok(OperatorData::DataInserter(OpDataInserter {
        data: AnyData::String(parsed_value),
    }))
}

pub fn parse_op_int(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let parsed_value = if let Some(value) = value {
        let value = str::parse::<i64>(value.as_bytes().to_str().map_err(|_| {
            OperatorCreationError::new("failed to parse value as integer (invalid utf-8)", arg_idx)
        })?)
        .map_err(|_| OperatorCreationError::new("failed to value as integer", arg_idx))?;
        value
    } else {
        return Err(OperatorCreationError::new("missing value for int", arg_idx));
    };
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
        return Err(OperatorCreationError::new("missing value for int", arg_idx));
    };
    Ok(OperatorData::DataInserter(OpDataInserter {
        data: AnyData::Bytes(parsed_value),
    }))
}
