use std::{
    cell::{Ref, RefCell, RefMut},
    collections::VecDeque,
    rc::Rc,
};

use crate::operations::errors::OperatorApplicationError;

pub enum StreamFieldValueData {
    Dropped,
    Error(OperatorApplicationError),
    BytesChunk(Vec<u8>),
    BytesBuffer(Vec<u8>),
    /* // TODO
    BytesChunk(Vec<u8>),
    BytesBuffer(Vec<u8>),
    BytesFile(File),
    TextFile(File),
    ArrayChunk(FieldData),
    ArrayBuffer(FieldData),
    ArrayFile(File, File),*/
}

pub struct StreamFieldValue {
    pub data: StreamFieldValueData,
    pub bytes_are_utf8: bool,
    pub done: bool,
}

pub type StreamValueId = usize;

#[derive(Default)]
pub struct StreamFieldData {
    pub id_offset: usize,
    pub values: VecDeque<Rc<RefCell<StreamFieldValue>>>,
    pub updates: Vec<StreamValueId>,
    pub values_dropped: usize,
    pub entries_dropped: usize,
    pub entries_added: usize,
}

impl StreamFieldData {
    pub fn get_value_mut<'a>(&'a self, id: StreamValueId) -> RefMut<'a, StreamFieldValue> {
        self.values[id - self.id_offset].borrow_mut()
    }
    pub fn get_value<'a>(&'a self, id: StreamValueId) -> Ref<'a, StreamFieldValue> {
        self.values[id - self.id_offset].borrow()
    }
    pub fn push_value(&mut self, value: StreamFieldValue) -> StreamValueId {
        let id = self.values.len() + self.id_offset;
        self.values.push_back(Rc::new(RefCell::new(value)));
        id
    }
}
