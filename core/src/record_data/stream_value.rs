use std::collections::VecDeque;

use smallvec::SmallVec;

use crate::{operators::transform::TransformId, utils::universe::Universe};

use super::field_value::FieldValue;

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
    pub value: FieldValue,
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
    pub fn from_value(value: FieldValue) -> Self {
        Self {
            value,
            is_buffered: true,
            done: true,
            subscribers: Default::default(),
            ref_count: 1,
        }
    }
    pub fn from_value_unfinished(
        value: FieldValue,
        is_buffered: bool,
    ) -> Self {
        Self {
            value,
            is_buffered,
            done: false,
            subscribers: Default::default(),
            ref_count: 1,
        }
    }
}
impl Default for StreamValue {
    fn default() -> Self {
        Self::from_value_unfinished(FieldValue::Undefined, false)
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
            sv.value = FieldValue::Undefined;
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
            sv.value = FieldValue::Undefined;
            self.stream_values.release(sv_id);
        }
    }
}
