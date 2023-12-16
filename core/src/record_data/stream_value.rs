use std::collections::VecDeque;

use smallvec::SmallVec;

use crate::{
    operators::{errors::OperatorApplicationError, transform::TransformId},
    utils::universe::Universe,
};

#[derive(Clone)]
pub enum StreamValueData {
    Dropped,
    Error(OperatorApplicationError),
    Bytes(Vec<u8>),
    // // TODO
    // BytesChunk(Vec<u8>),
    // BytesBuffer(Vec<u8>),
    // BytesFile(File),
    // TextFile(File),
    // ArrayChunk(FieldData),
    // ArrayBuffer(FieldData),
    // ArrayFile(File, File),
}

pub struct StreamValueUpdate {
    pub sv_id: StreamValueId,
    pub tf_id: TransformId,
    pub custom: usize,
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
            self.bytes_are_chunk = false;
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
    pub fn new(data: StreamValueData, utf8: bool, done: bool) -> StreamValue {
        StreamValue {
            data,
            bytes_are_utf8: utf8,
            bytes_are_chunk: !done,
            done,
            subscribers: Default::default(),
            ref_count: 1,
        }
    }
}

pub type StreamValueId = usize;

#[derive(Default)]
pub struct StreamValueManager {
    pub stream_values: Universe<StreamValueId, StreamValue>,
    pub updates: VecDeque<StreamValueUpdate>,
}

impl StreamValueManager {
    pub fn inform_stream_value_subscribers(&mut self, sv_id: StreamValueId) {
        let sv = &self.stream_values[sv_id];
        for sub in &sv.subscribers {
            if !sub.notify_only_once_done || sv.done {
                self.updates.push_back(StreamValueUpdate {
                    sv_id,
                    tf_id: sub.tf_id,
                    custom: sub.custom_data,
                });
            }
        }
    }
    pub fn drop_field_value_subscription(
        &mut self,
        sv_id: StreamValueId,
        tf_id_to_remove: Option<TransformId>,
    ) {
        let sv = &mut self.stream_values[sv_id];
        sv.ref_count -= 1;
        if sv.ref_count == 0 {
            sv.data = StreamValueData::Dropped;
            self.stream_values.release(sv_id);
        } else if let Some(tf_id) = tf_id_to_remove {
            sv.subscribers.swap_remove(
                sv.subscribers
                    .iter()
                    .position(|sub| sub.tf_id == tf_id)
                    .unwrap(),
            );
        }
    }
}
