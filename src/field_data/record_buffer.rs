use std::sync::{Condvar, Mutex};

use super::FieldData;

#[derive(Default)]
pub struct RecordBufferData {
    remaining_consumers: usize,
    available_batch_size: usize,
    fields: Vec<FieldData>,
}

pub type RecordBufferFieldId = u32;

#[derive(Default)]
pub struct RecordBuffer {
    pub updates: Condvar,
    pub fields: Mutex<RecordBufferData>,
}
