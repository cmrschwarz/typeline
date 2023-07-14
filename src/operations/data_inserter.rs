use bstr::{BStr, BString, ByteSlice};
use regex::Regex;
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

#[derive(Clone)]
pub enum DataToInsert {
    Bytes(BString),
    String(String),
    Int(i64),
}

#[derive(Clone)]
pub struct OpDataInserter {
    data: DataToInsert,
    append: bool,
    insert_count: Option<usize>,
}

pub struct TfDataInserter<'a> {
    data: &'a DataToInsert,
    output_field: FieldId,
    insert_count: Option<usize>,
}

impl OpDataInserter {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        let mut res = SmallString::new();
        if self.append {
            res.push('+');
        }
        match self.data {
            DataToInsert::Bytes(_) => res.push_str("bytes"),
            DataToInsert::String(_) => res.push_str("str"),
            DataToInsert::Int(_) => res.push_str("int"),
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
        tf_state.is_appending = true;
        tf_state.input_field
    } else {
        sess.record_mgr.add_field(
            tf_state.match_set_id,
            sess.record_mgr.get_min_apf_idx(tf_state.input_field),
            None,
        )
    };
    let data = TransformData::DataInserter(TfDataInserter {
        data: &op.data,
        output_field,
        insert_count: op.insert_count,
    });
    (data, output_field)
}

fn insert(sess: &mut JobData<'_>, di: &mut TfDataInserter, run_length: usize) {
    let mut output_field = sess.record_mgr.fields[di.output_field].borrow_mut();
    match di.data {
        DataToInsert::Bytes(b) => output_field
            .field_data
            .push_bytes(b, run_length, true, true),
        DataToInsert::String(s) => output_field.field_data.push_str(s, run_length, true, true),
        DataToInsert::Int(i) => output_field.field_data.push_int(*i, run_length, true, true),
    }
}
pub fn handle_tf_data_inserter(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    di: &mut TfDataInserter,
) {
    sess.prepare_for_output(tf_id, &[di.output_field]);
    let (mut batch_size, input_done);
    let mut unlink_after = false;
    if let Some(ic) = di.insert_count {
        (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
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
                sess.tf_mgr.unclaim_batch_size(tf_id, ic - batch_size);
            }
        }
        di.insert_count = Some(ic - batch_size);
    } else {
        let tf = &sess.tf_mgr.transforms[tf_id];
        let amend_mode = di.output_field == tf.input_field;
        let yield_to_succ = tf.continuation.is_some();
        if amend_mode || yield_to_succ {
            batch_size = 1;
            unlink_after = true;
        } else {
            (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
            unlink_after = input_done;
            if batch_size == 0 {
                debug_assert!(input_done);
                batch_size = 1;
            }
        }
    }
    insert(sess, di, batch_size);
    if unlink_after {
        sess.unlink_transform(tf_id, batch_size);
        return;
    }
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, batch_size);
    sess.tf_mgr.update_ready_state(tf_id);
}

pub fn parse_op_str(
    value: Option<&BStr>,
    insert_count: Option<usize>,
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
        data: DataToInsert::String(value_str.to_owned()),
        insert_count,
        append,
    }))
}

pub fn parse_op_int(
    value: Option<&BStr>,
    insert_count: Option<usize>,
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
        data: DataToInsert::Int(parsed_value),
        insert_count,
        append,
    }))
}
pub fn parse_op_bytes(
    value: Option<&BStr>,
    insert_count: Option<usize>,
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
        data: DataToInsert::Bytes(parsed_value),
        insert_count,
        append,
    }))
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^(?<append>\+)?(?<type>int|bytes|str)(?<insert_count>[0-9]+)?$").unwrap();
}

pub fn argument_matches_data_inserter(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_data_inserter(
    argument: &str,
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    // this should not happen in the cli parser because it checks using `argument_matches_data_inserter`
    let args = ARG_REGEX.captures(&argument).ok_or_else(|| {
        OperatorCreationError::new("invalid argument syntax for data inserter", arg_idx)
    })?;
    let append = args.name("append").is_some();
    let insert_count = args
        .name("insert_count")
        .map(|ic| {
            ic.as_str().parse::<usize>().map_err(|_| {
                OperatorCreationError::new("failed to parse insertion count as integer", arg_idx)
            })
        })
        .transpose()?;
    match args.name("type").unwrap().as_str() {
        "int" => parse_op_int(value, insert_count, append, arg_idx),
        "bytes" => parse_op_bytes(value, insert_count, append, arg_idx),
        "str" => parse_op_str(value, insert_count, append, arg_idx),
        _ => unreachable!(),
    }
}

pub fn create_op_data_inserter(
    data: DataToInsert,
    insert_count: Option<usize>,
    append: bool,
) -> OperatorData {
    OperatorData::DataInserter(OpDataInserter {
        data,
        insert_count,
        append,
    })
}
