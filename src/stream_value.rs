use smallvec::SmallVec;

use crate::operations::{errors::OperatorApplicationError, transform::TransformId};

pub enum StreamValueData {
    Dropped,
    Error(OperatorApplicationError),
    Bytes(Vec<u8>),
    /* // TODO
    BytesChunk(Vec<u8>),
    BytesBuffer(Vec<u8>),
    BytesFile(File),
    TextFile(File),
    ArrayChunk(FieldData),
    ArrayBuffer(FieldData),
    ArrayFile(File, File),*/
}

pub struct StreamValueSubscription {
    pub tf_id: TransformId,
    pub custom_data: usize,
    pub notify_only_once_done: bool,
}

pub struct StreamValue {
    pub data: StreamValueData,
    pub bytes_are_utf8: bool,
    pub bytes_are_chunk: bool,
    pub done: bool,
    // transforms that want to be readied as soon as this receives any data
    pub subscribers: SmallVec<[StreamValueSubscription; 1]>,
    pub ref_count: usize,
}

impl StreamValue {
    pub fn promote_to_buffer(&mut self) {
        if let StreamValueData::Bytes(_) = self.data {
            self.bytes_are_chunk = true;
        }
    }
    pub fn subscribe(
        &mut self,
        tf_id: TransformId,
        custom_data: usize,
        notify_only_once_done: bool,
    ) {
        self.subscribers.push(StreamValueSubscription {
            tf_id,
            custom_data,
            notify_only_once_done,
        });
        self.ref_count += 1;
    }
    pub fn is_buffered(&self) -> bool {
        match self.data {
            StreamValueData::Dropped => true,
            StreamValueData::Error(_) => true,
            StreamValueData::Bytes(_) => !self.bytes_are_chunk,
        }
    }
}

pub type StreamValueId = usize;
