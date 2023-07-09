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

pub struct OpDataInserter {
    data: AnyData,
    append: bool,
}

pub struct TfDataInserter<'a> {
    data: &'a AnyData,
    output_field: FieldId,
}

impl OpDataInserter {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        let mut res = SmallString::new();
        if self.append {
            res.push('+');
        }
        match self.data {
            AnyData::Bytes(_) => res.push_str("bytes"),
            AnyData::String(_) => res.push_str("str"),
            AnyData::Int(_) => res.push_str("int"),
        }
        res
    }
}

pub fn setup_tf_data_inserter<'a>(
    sess: &mut JobData,
    op: &'a OpDataInserter,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    let output_field = if op.append {
        // we will forward the whole input in one go and unlink us from the chain
        tf_state.desired_batch_size = usize::MAX;
        tf_state.input_field
    } else {
        sess.record_mgr.add_field(tf_state.match_set_id, None)
    };
    let data = TransformData::DataInserter(TfDataInserter {
        data: &op.data,
        output_field,
    });
    (data, output_field)
}

pub fn handle_tf_data_inserter(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    di: &mut TfDataInserter,
) {
    let mut output_field = sess.record_mgr.fields[di.output_field].borrow_mut();
    match di.data {
        AnyData::Bytes(b) => output_field.field_data.push_bytes(b, 1, true, false),
        AnyData::String(s) => output_field.field_data.push_str(s, 1, true, false),
        AnyData::Int(i) => output_field.field_data.push_int(*i, 1, true, false),
    }
    sess.tf_mgr.unlink_transform(tf_id, 1);
}

pub fn parse_op_str(
    value: Option<&BStr>,
    append: bool,
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
        append,
    }))
}

pub fn parse_op_int(
    value: Option<&BStr>,
    append: bool,
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
        append,
    }))
}
pub fn parse_op_bytes(
    value: Option<&BStr>,
    append: bool,
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
        append,
    }))
}

pub fn create_op_data_inserter(data: AnyData, append: bool) -> OperatorData {
    OperatorData::DataInserter(OpDataInserter { data, append })
}
