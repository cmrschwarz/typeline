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

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone)]
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
        #[cfg_attr(not(feature = "debug_logging"), allow(unused))]
        sv_id: StreamValueId,
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
        #[cfg(feature = "debug_logging")]
        eprintln!(
            ":: tf {tf_id:02} subscribed to stream value {sv_id:02} (subs: {:?}, rc {}), [{:?}]",
            self.subscribers.iter().map(|svs|svs.tf_id).collect::<Vec<_>>(),
            self.ref_count,
            self.data
        );
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
    pub fn as_field_value_ref(&self) -> FieldValueRef {
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
    pub unsafe fn extend_with_bytes_raw(
        &mut self,
        bytes: &[u8],
        valid_utf8: bool,
    ) {
        match self {
            StreamValueData::Text(t) => {
                if valid_utf8 {
                    t.push_str(unsafe { std::str::from_utf8_unchecked(bytes) })
                } else {
                    let mut b = std::mem::take(t).into_bytes();
                    b.extend(bytes);
                    *self = StreamValueData::Bytes(b);
                }
            }
            StreamValueData::Bytes(b) => b.extend(bytes),
            StreamValueData::Error(_) => unimplemented!(),
        }
    }
    pub fn extend_with_bytes(&mut self, bytes: &[u8]) {
        unsafe { self.extend_with_bytes_raw(bytes, false) }
    }
    pub fn extend_with_text(&mut self, text: &str) {
        unsafe { self.extend_with_bytes_raw(text.as_bytes(), true) }
    }
    pub fn extend_from_sv_data(&mut self, data: &StreamValueData) {
        match data {
            StreamValueData::Text(t) => self.extend_with_text(t),
            StreamValueData::Bytes(b) => self.extend_with_bytes(b),
            StreamValueData::Error(e) => {
                if !self.is_error() {
                    *self = StreamValueData::Error(e.clone());
                }
            }
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
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                ":: queued updates to the {} subscriber(s) of stream value {sv_id:02} [{}done: {:?}]: {:?}",
                sv.subscribers.len(),
                if sv.done  {""} else { "not " },
                sv.data,
                self.updates
            );
        }
    }
    pub fn release_stream_value(&mut self, sv_id: StreamValueId) {
        #[cfg(feature = "debug_logging")]
        eprintln!(
            ":: releasing stream value {sv_id:02} [{:?}]",
            self.stream_values[sv_id].data
        );
        self.stream_values.release(sv_id);
    }
    pub fn drop_field_value_subscription(
        &mut self,
        sv_id: StreamValueId,
        tf_id_to_remove: Option<TransformId>,
    ) {
        let sv = &mut self.stream_values[sv_id];

        #[cfg(feature = "debug_logging")]
        eprintln!(
            ":: tf {:02} dropping stream value subscription to sv {sv_id:02} (subs: {:?}) [{}done, rc {}, {:?}]",
            tf_id_to_remove.map(|v|v.get() as i64).unwrap_or(-1),
            sv.subscribers.iter().map(|svs|svs.tf_id).collect::<Vec<_>>(),
            if sv.done {""} else {"not "},
            sv.ref_count,
            sv.data
        );
        sv.ref_count -= 1;
        if sv.ref_count == 0 {
            self.release_stream_value(sv_id);
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
            self.release_stream_value(sv_id);
        }
    }
    pub fn claim_stream_value(&mut self, sv: StreamValue) -> StreamValueId {
        let sv_id = self.stream_values.claim_with_value(sv);
        #[cfg(feature = "debug_logging")]
        eprintln!(
            ":: claimed stream value {sv_id:02} [{:?}]",
            self.stream_values[sv_id].data
        );
        sv_id
    }
    pub fn subscribe_to_stream_value(
        &mut self,
        sv_id: StreamValueId,
        tf_id: TransformId,
        custom_data: usize,
        notify_only_once_done: bool,
    ) {
        self.stream_values[sv_id].subscribe(
            sv_id,
            tf_id,
            custom_data,
            notify_only_once_done,
        )
    }
}
