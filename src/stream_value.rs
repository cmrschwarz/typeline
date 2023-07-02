use smallvec::SmallVec;

use crate::{
    field_data::RunLength,
    operations::{errors::OperatorApplicationError, transform::TransformId},
};

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

pub struct StreamValueSubscription {
    pub tfid: TransformId,
    pub custom_data: usize,
    pub notify_only_once_done: bool,
}

pub struct StreamValue {
    pub data: StreamFieldValueData,
    pub bytes_are_utf8: bool,
    pub done: bool,
    // transforms that want to be readied as soon as this receives any data
    pub subscribers: SmallVec<[StreamValueSubscription; 1]>,
    pub ref_count: usize,
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
    pub fn subscribe(
        &mut self,
        tfid: TransformId,
        custom_data: usize,
        notify_only_once_done: bool,
    ) {
        self.subscribers.push(StreamValueSubscription {
            tfid,
            custom_data,
            notify_only_once_done,
        });
        self.ref_count += 1;
    }
    pub fn drop_subscription(&mut self) {
        self.ref_count -= 1;
        //TODO
    }
    pub fn is_buffered(&self) -> bool {
        match self.data {
            StreamFieldValueData::Dropped => true,
            StreamFieldValueData::Error(_) => true,
            StreamFieldValueData::BytesBuffer(_) => true,
            StreamFieldValueData::BytesChunk(_) => false,
        }
    }
}

pub type StreamValueId = usize;
