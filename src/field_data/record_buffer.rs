use std::sync::{Condvar, Mutex};

use nonmax::NonMaxU32;
use smallvec::SmallVec;

use crate::utils::{string_store::StringStoreEntry, universe::Universe};

use super::FieldData;

struct RecordBufferField {
    refcount: usize,
    names: SmallVec<[StringStoreEntry; 4]>,
    data: FieldData,
}

#[derive(Default)]
pub struct RecordBufferData {
    remaining_consumers: usize,
    available_batch_size: usize,
    fields: Universe<RecordBufferFieldId, RecordBufferField>,
}

pub type RecordBufferFieldId = NonMaxU32;

#[derive(Default)]
pub struct RecordBuffer {
    pub updates: Condvar,
    pub fields: Mutex<RecordBufferData>,
}
