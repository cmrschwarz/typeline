use std::{
    cell::UnsafeCell,
    sync::{Condvar, Mutex},
};

use smallvec::SmallVec;

use indexland::{nonmax::NonMaxU32, universe::Universe};

use crate::utils::string_store::StringStoreEntry;

use super::field_data::FieldData;

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
