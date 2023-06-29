use std::sync::{Arc, Mutex, MutexGuard};

use crate::worker_thread_session::{FieldId, JobData, MatchSetId};

use super::{
    operator::OperatorData,
    transform::{TransformData, TransformId},
};

#[derive(Clone)]
pub struct StringSinkHandle {
    data: Arc<Mutex<Vec<String>>>,
}

impl StringSinkHandle {
    pub fn new() -> StringSinkHandle {
        StringSinkHandle {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn get(&self) -> MutexGuard<Vec<String>> {
        self.data.lock().unwrap()
    }
}

#[derive(Clone)]
pub struct OpStringSink {
    handle: StringSinkHandle,
}

pub fn create_op_string_sink(handle: StringSinkHandle) -> OperatorData {
    OperatorData::StringSink(OpStringSink { handle })
}

pub struct TfStringSink<'a> {
    handle: &'a Mutex<Vec<String>>,
}

pub fn setup_tf_string_sink<'a>(
    sess: &mut JobData,
    _ms_id: MatchSetId,
    input_field: FieldId,
    ss: &'a OpStringSink,
) -> (TransformData<'a>, FieldId) {
    let tf = TfStringSink {
        handle: &ss.handle.data,
    };
    (TransformData::StringSink(tf), input_field)
}

pub fn handle_tf_string_sink_batch_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfStringSink<'_>,
) {
    todo!();
}

pub fn handle_tf_string_sink_stream_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf_print: &mut TfStringSink,
) {
    todo!();
}
