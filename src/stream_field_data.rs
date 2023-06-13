use crate::field_data::RunLength;

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
    data: StreamFieldValueData,
    start: StreamEntryId,
    end: StreamEntryId,
    update_entry: Option<UpdateEntryId>,
}

pub type UpdateEntryId = u32;
pub type StreamValueId = u32;
pub type StreamEntryId = usize;

pub struct StreamFieldData {
    values: Vec<StreamFieldValue>,
    fields: Vec<StreamValueId>,
    updates: Vec<StreamValueId>,
}
