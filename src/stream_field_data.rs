use std::{collections::VecDeque, rc::Rc};

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
    pub done: bool,
}

pub type StreamValueId = usize;

#[derive(Default)]
pub struct StreamFieldData {
    pub id_offset: usize,
    pub values: VecDeque<Rc<StreamFieldValue>>,
    pub updates: Vec<StreamValueId>,
    pub values_dropped: usize,
    pub entries_dropped: usize,
    pub entries_added: usize,
}
