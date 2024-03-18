use std::collections::VecDeque;

use smallvec::SmallVec;

use crate::{
    operators::{errors::OperatorApplicationError, transform::TransformId},
    utils::universe::Universe,
};

use super::{
    field_value::{FieldValue, FieldValueKind},
    field_value_ref::FieldValueRef,
};

#[derive(Clone, Copy)]
pub struct StreamValueUpdate {
    pub sv_id: StreamValueId,
    pub tf_id: TransformId,
    pub custom: usize,
}

#[derive(Clone, Copy)]
pub struct StreamValueSubscription {
    pub tf_id: TransformId,
    pub custom_data: usize,
    pub notify_only_once_done: bool,
}

#[derive(Clone)]
pub enum StreamValueData {
    Text(String),
    Bytes(Vec<u8>),
    Error(OperatorApplicationError),
}

pub struct StreamValue {
    pub data: StreamValueData,
    pub is_buffered: bool,
    pub done: bool,
    // transforms that want to be readied as soon as this receives any data
    pub subscribers: SmallVec<[StreamValueSubscription; 1]>,
    pub ref_count: usize,
}

impl StreamValue {
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
    pub fn from_data(data: StreamValueData) -> Self {
        Self {
            data,
            is_buffered: true,
            done: true,
            subscribers: SmallVec::new(),
            ref_count: 1,
        }
    }
    pub fn from_data_unfinished(
        value: StreamValueData,
        is_buffered: bool,
    ) -> Self {
        Self {
            data: value,
            is_buffered,
            done: false,
            subscribers: SmallVec::new(),
            ref_count: 1,
        }
    }
    pub fn clear_buffer(&mut self) {
        match &mut self.data {
            StreamValueData::Text(t) => t.clear(),
            StreamValueData::Bytes(bb) => bb.clear(),
            StreamValueData::Error(_) => (),
        }
    }
    pub fn clear_unless_buffered(&mut self) {
        if self.is_buffered {
            return;
        };
        self.clear_buffer();
    }
}

impl StreamValueData {
    pub fn as_field_value_ref<'a>(&'a self) -> FieldValueRef<'a> {
        match &self {
            StreamValueData::Text(v) => FieldValueRef::Text(v),
            StreamValueData::Bytes(v) => FieldValueRef::Bytes(v),
            StreamValueData::Error(v) => FieldValueRef::Error(v),
        }
    }
    pub fn to_field_value(&self) -> FieldValue {
        self.as_field_value_ref().to_field_value()
    }
    pub fn kind(&self) -> FieldValueKind {
        self.as_field_value_ref().repr().kind().unwrap()
    }
    pub fn is_error(&self) -> bool {
        matches!(self, StreamValueData::Error(_))
    }
    pub fn extend_with_bytes(&mut self, bytes: &[u8]) {
        match self {
            StreamValueData::Text(t) => {
                let mut b = std::mem::take(t).into_bytes();
                b.extend(bytes);
                *self = StreamValueData::Bytes(b);
            }
            StreamValueData::Bytes(b) => b.extend(bytes),
            StreamValueData::Error(_) => unimplemented!(),
        }
    }
    pub fn extend_with_text(&mut self, text: &str) {
        match self {
            StreamValueData::Text(t) => t.push_str(text),
            StreamValueData::Bytes(b) => b.extend(text.as_bytes()),
            StreamValueData::Error(_) => unimplemented!(),
        }
    }
}

impl Default for StreamValueData {
    fn default() -> Self {
        StreamValueData::Text(String::new())
    }
}
#[derive(Debug)]
pub struct UnsupportedStreamValueDataKindError(pub FieldValueKind);

impl TryFrom<FieldValue> for StreamValueData {
    type Error = UnsupportedStreamValueDataKindError;

    fn try_from(
        value: FieldValue,
    ) -> Result<Self, UnsupportedStreamValueDataKindError> {
        let v = match value {
            FieldValue::Text(t) => StreamValueData::Text(t),
            FieldValue::Bytes(t) => StreamValueData::Bytes(t),
            FieldValue::Error(t) => StreamValueData::Error(t),
            _ => {
                return Err(UnsupportedStreamValueDataKindError(value.kind()))
            }
        };
        Ok(v)
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
            std::mem::take(&mut sv.data);
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
    pub fn check_stream_value_ref_count(&mut self, sv_id: StreamValueId) {
        let sv = &mut self.stream_values[sv_id];
        if sv.ref_count == 0 {
            std::mem::take(&mut sv.data);
            self.stream_values.release(sv_id);
        }
    }
}
