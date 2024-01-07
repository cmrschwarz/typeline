use std::{
    cell::UnsafeCell,
    sync::{Condvar, Mutex},
};

use nonmax::NonMaxU32;
use smallvec::SmallVec;

use crate::utils::{string_store::StringStoreEntry, universe::Universe};

use super::field_value_repr::FieldData;

#[derive(Default)]
pub struct RecordBufferField {
    pub refcount: usize,
    pub name: Option<StringStoreEntry>,
    pub field_refs: SmallVec<[RecordBufferFieldId; 4]>,
    pub(super) data: UnsafeCell<FieldData>,
}

impl RecordBufferField {
    pub fn get_data_mut(&mut self) -> &mut FieldData {
        self.data.get_mut()
    }
}

#[derive(Default)]
pub struct RecordBufferData {
    pub remaining_consumers: usize,
    pub available_batch_size: usize,
    pub input_done: bool,
    pub fields: Universe<RecordBufferFieldId, RecordBufferField>,
}

pub type RecordBufferFieldId = NonMaxU32;

#[derive(Default)]
pub struct RecordBuffer {
    pub updates: Condvar,
    pub fields: Mutex<RecordBufferData>,
}
