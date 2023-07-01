use smallvec::SmallVec;

use crate::operations::{errors::OperatorApplicationError, transform::TransformId};

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

pub struct StreamValue {
    pub data: StreamFieldValueData,
    pub bytes_are_utf8: bool,
    pub done: bool,
    // transforms that want to be readied as soon as this receives any data
    pub subscribers: SmallVec<[TransformId; 2]>,
}

impl StreamValue {
    pub fn promote_to_buffer(&mut self) {
        match &mut self.data {
            StreamFieldValueData::Dropped => (),
            StreamFieldValueData::Error(_) => (),
            StreamFieldValueData::BytesChunk(bc) => {
                self.data =
                    StreamFieldValueData::BytesBuffer(std::mem::replace(bc, Default::default()));
            }
            StreamFieldValueData::BytesBuffer(_) => (),
        }
    }
    pub fn subscribe(&mut self, tf_id: TransformId) {
        self.subscribers.push(tf_id);
    }
}

pub type StreamValueId = usize;
