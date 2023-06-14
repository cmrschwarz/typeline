use std::collections::VecDeque;

use nonmax::NonMaxUsize;

pub enum StreamFieldValueData {
    TextChunk(String),
    TextBuffer(String),
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
    pub update_entry: Option<UpdateEntryId>,
    pub refcount: usize,
    pub done: bool,
}

pub type UpdateEntryId = NonMaxUsize;
pub type StreamValueId = usize;

#[derive(Default)]
pub struct StreamFieldData {
    pub id_offset: usize,
    pub values: VecDeque<StreamFieldValue>,
    pub updates: Vec<StreamValueId>,
    pub values_dropped: usize,
    pub entries_dropped: usize,
    pub entries_added: usize,
}
