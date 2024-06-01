use std::{
    collections::VecDeque,
    io::{BufRead, Read, Write},
    ops::Range,
    slice::SliceIndex,
    string::FromUtf8Error,
    sync::Arc,
};

use smallvec::SmallVec;

use crate::{
    operators::{errors::OperatorApplicationError, transform::TransformId},
    utils::{
        escaped_writer::EscapedWriter,
        maybe_text::{MaybeText, MaybeTextCow, MaybeTextRef},
        retain_string_range, retain_vec_range, subrange,
        text_write::{TextWrite, TextWriteFormatAdapter, TextWriteIoAdapter},
        universe::Universe,
    },
};

use super::{
    field_value::{FieldValue, FieldValueKind},
    field_value_ref::FieldValueRef,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamValueDataOffset {
    pub values_consumed: usize,
    pub current_value_offset: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct StreamValueUpdate {
    // TODO: we can have multiple subscriptions from the same tf to the same
    // tf, e.g. if a stream is dup'ed. because of that, we need some
    // identifier to eventually remove those
    // pub stream_value_subscription_id: u32,
    pub sv_id: StreamValueId,
    pub tf_id: TransformId,
    pub data_offset: StreamValueDataOffset,
    pub custom: usize,
    pub may_consume_data: bool,
}

#[derive(Clone, Copy)]
pub struct StreamValueSubscription {
    pub tf_id: TransformId,
    pub data_offset: StreamValueDataOffset,
    pub custom_data: usize,
    pub notify_only_once_done: bool,
}

// TODO:
// 1. RC
// 2. Multiple Values, but no substreams
// 2. Offsets always inside Subscription
// 3. GetBytes / GetStr / ...
// 4. RingBuffer for Updates + Reenque self with partial updates
// 5. Simple mechanism for transform to say: fuck you i dont like trains

#[derive(Debug, Clone)]
pub enum StreamValueData<'a> {
    StaticText(&'a str),
    StaticBytes(&'a [u8]),
    Text {
        data: Arc<String>,
        range: Range<usize>,
    },
    Bytes {
        data: Arc<Vec<u8>>,
        range: Range<usize>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum StreamValueDataRef<'s, 'd> {
    StaticText(&'s str),
    StaticBytes(&'s [u8]),
    Text(&'d str),
    Bytes(&'d [u8]),
}

#[derive(Debug, Clone)]
pub enum StorageAgnosticStreamValueDataRef<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub enum StreamValueBufferMode {
    #[default]
    Stream,
    Buffered,
    Contiguous,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum StreamValueDataType {
    #[default]
    Text,
    // we try to use text, but might have to switch to bytes
    // example usecase: bs=1 sbs=1 str="foo" +bytes="bar" join=, p
    MaybeText,
    Bytes,
    VariableTypeArray,
    FixedTypeArray(FieldValueKind),
}

pub struct StreamValue<'a> {
    pub error: Option<Arc<OperatorApplicationError>>,
    pub data_type: Option<StreamValueDataType>,
    pub data: VecDeque<StreamValueData<'a>>,
    pub buffer_mode: StreamValueBufferMode,
    pub done: bool,
    // transforms that want to be readied as soon as this receives any data
    pub subscribers: SmallVec<[StreamValueSubscription; 1]>,
    pub ref_count: usize,
}

pub struct StreamValueDataCursor<'s, 'd> {
    stream_value: &'d mut StreamValue<'s>,
    initial_offset: StreamValueDataOffset,
    may_consume_data: bool,
    data_index: usize,
}

pub struct StreamValueDataInserter<'s, 'd> {
    #[cfg(feature = "stream_logging")]
    sv_id: StreamValueId,
    stream_value: &'d mut StreamValue<'s>,
    dead_elems_leading: usize,
    memory_budget: usize,
}

pub type StreamValueId = usize;

#[derive(Default)]
pub struct StreamValueManager<'a> {
    pub stream_values: Universe<StreamValueId, StreamValue<'a>>,
    pub updates: VecDeque<StreamValueUpdate>,
}

impl StreamValueBufferMode {
    pub fn require_buffered(&mut self) {
        if *self == StreamValueBufferMode::Stream {
            *self = StreamValueBufferMode::Buffered;
        }
    }
    pub fn require_contiguous(&mut self) {
        *self = StreamValueBufferMode::Contiguous;
    }
    pub fn is_streaming(&self) -> bool {
        *self == StreamValueBufferMode::Stream
    }
}

impl StreamValueDataType {
    pub fn kind(&self) -> FieldValueKind {
        match self {
            StreamValueDataType::Text => FieldValueKind::Text,
            StreamValueDataType::Bytes | StreamValueDataType::MaybeText => {
                FieldValueKind::Bytes
            }
            StreamValueDataType::VariableTypeArray
            | StreamValueDataType::FixedTypeArray(_) => FieldValueKind::Array,
        }
    }
}

impl<'s, 'd> StreamValueDataCursor<'s, 'd> {
    pub fn new(
        stream_value: &'d mut StreamValue<'s>,
        mut offset: StreamValueDataOffset,
        may_consume_data: bool,
    ) -> Self {
        // If the offset points to the end of an element
        // (that's how we store the offset inside updates to avoid missing
        // appends), skip the zero length remainder for this cursor.
        if offset.current_value_offset > 0 {
            if let Some(d) = stream_value.data.get(offset.values_consumed) {
                if d.len() == offset.current_value_offset {
                    offset.values_consumed += 1;
                    offset.current_value_offset = 0;
                }
            }
        }
        Self {
            stream_value,
            initial_offset: offset,
            may_consume_data,
            data_index: offset.values_consumed,
        }
    }
    pub fn peek_next(&self) -> Option<StreamValueDataRef<'s, '_>> {
        let data = self.stream_value.data.get(self.data_index)?.as_ref();
        if self.data_index > self.initial_offset.values_consumed {
            return Some(data);
        }
        Some(data.sliced(self.initial_offset.current_value_offset..))
    }
    pub fn peek_prev(&self) -> Option<StreamValueDataRef<'s, '_>> {
        if self.data_index == self.initial_offset.values_consumed {
            return None;
        }
        let idx = self.data_index - 1;
        let data = self.stream_value.data[idx].as_ref();
        if idx > self.initial_offset.values_consumed {
            return Some(data);
        }
        Some(data.sliced(self.initial_offset.current_value_offset..))
    }
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<StreamValueDataRef<'s, '_>> {
        let data = self.stream_value.data.get(self.data_index)?.as_ref();
        self.data_index += 1;
        if self.data_index > self.initial_offset.values_consumed + 1 {
            return Some(data);
        }
        Some(data.sliced(self.initial_offset.current_value_offset..))
    }
    pub fn prev(&mut self) -> Option<StreamValueDataRef<'s, '_>> {
        if self.data_index == self.initial_offset.values_consumed {
            return None;
        }
        self.data_index -= 1;
        let data = self.stream_value.data[self.data_index].as_ref();
        if self.data_index > self.initial_offset.values_consumed {
            return Some(data);
        }
        Some(data.sliced(self.initial_offset.current_value_offset..))
    }
    pub fn next_steal(
        &mut self,
        may_consume_data: bool,
    ) -> Option<StreamValueData<'s>> {
        let data = self.stream_value.data.get_mut(self.data_index)?;
        self.data_index += 1;
        if !self.may_consume_data || !may_consume_data {
            if self.data_index > self.initial_offset.values_consumed + 1 {
                return Some(data.clone());
            }
            return Some(data.sliced(
                self.initial_offset.current_value_offset..data.len(),
            ));
        }
        let mut data = std::mem::take(data);
        if self.data_index > self.initial_offset.values_consumed + 1 {
            return Some(data);
        }
        data.slice(self.initial_offset.current_value_offset..data.len());
        Some(data)
    }
    pub fn copy_prev(&self, target: &mut StreamValueDataInserter<'s, '_>) {
        if !self.may_consume_data {
            target.append_copy(self.peek_prev().unwrap());
            return;
        }
        let full = self.data_index > self.initial_offset.values_consumed
            || self.initial_offset.current_value_offset == 0;
        if full {
            target.append(self.stream_value.data[self.data_index - 1].clone());
        }
    }
    pub fn curr_offset(&self) -> StreamValueDataOffset {
        if self.data_index == self.initial_offset.values_consumed {
            return self.initial_offset;
        }
        StreamValueDataOffset {
            values_consumed: self.data_index,
            current_value_offset: 0,
        }
    }
    pub fn curr_offset_partial_prev(
        &self,
        consumed_of_last: usize,
    ) -> StreamValueDataOffset {
        let pos_delta = self.data_index - self.initial_offset.values_consumed;
        debug_assert!(pos_delta > 0);
        if pos_delta == 1 {
            return StreamValueDataOffset {
                values_consumed: self.initial_offset.values_consumed,
                current_value_offset: self.initial_offset.current_value_offset
                    + consumed_of_last,
            };
        }
        StreamValueDataOffset {
            values_consumed: self.data_index - 1,
            current_value_offset: consumed_of_last,
        }
    }
}

#[derive(Clone)]
pub struct StreamValueDataIter<'s, 'd> {
    iter: std::collections::vec_deque::Iter<'d, StreamValueData<'s>>,
    offset: usize,
}

impl<'s, 'd> StreamValueDataIter<'s, 'd> {
    pub fn new(
        sv: &'d StreamValue<'s>,
        offset: StreamValueDataOffset,
    ) -> Self {
        let mut iter = sv.data.iter();
        for _ in 0..offset.values_consumed {
            iter.next();
        }
        StreamValueDataIter {
            iter,
            offset: offset.current_value_offset,
        }
    }
    pub fn peek(&self) -> Option<StreamValueDataRef<'s, 'd>> {
        self.clone().next()
    }
    pub fn set_next_offset(&mut self, offset: usize) {
        self.offset = offset;
    }
}

impl<'s, 'd> Iterator for StreamValueDataIter<'s, 'd> {
    type Item = StreamValueDataRef<'s, 'd>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.offset;
        self.offset = 0;
        self.iter.next().map(|d| match d {
            StreamValueData::StaticText(t) => {
                StreamValueDataRef::StaticText(&t[offset..])
            }
            StreamValueData::StaticBytes(b) => {
                StreamValueDataRef::StaticBytes(&b[offset..])
            }
            StreamValueData::Text { data, range } => {
                StreamValueDataRef::Text(&data[range.clone()][offset..])
            }
            StreamValueData::Bytes { data, range } => {
                StreamValueDataRef::Bytes(&data[range.clone()][offset..])
            }
        })
    }
}

impl<'s, 'd> Read for StreamValueDataIter<'s, 'd> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut buf_offset = 0;
        while let Some(elem) = self.peek() {
            let elem_data = elem.as_bytes();
            let buf_len_rem = buf.len() - buf_offset;
            if elem_data.len() > buf_len_rem {
                buf[buf_offset..].copy_from_slice(&elem_data[0..buf_len_rem]);
                self.offset += buf_len_rem;
                return Ok(buf.len());
            }
            buf[buf_offset..buf_offset + elem_data.len()]
                .copy_from_slice(elem_data);
            buf_offset += elem_data.len();
            self.iter.next();
        }
        Ok(buf_offset)
    }
}

impl<'s, 'd> BufRead for StreamValueDataIter<'s, 'd> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        Ok(self.peek().map(|d| d.as_bytes()).unwrap_or(&[]))
    }
    fn consume(&mut self, mut amount: usize) {
        if let Some(elem) = self.peek() {
            let len_rem = elem.as_bytes().len() - self.offset;
            if len_rem > amount {
                self.offset += amount;
                return;
            }
            self.iter.next();
            amount -= len_rem;
            self.offset = 0;
        }
        while amount > 0 {
            let Some(next_len) = self.peek().map(|d| d.len()) else {
                return;
            };
            if next_len > amount {
                self.offset = amount;
                return;
            }
            amount -= next_len;
            self.iter.next();
        }
    }
}

const STREAM_VALUE_DATA_OVERHEAD: usize =
    std::mem::size_of::<StreamValueData>();

impl<'a, 'b> StreamValueDataInserter<'a, 'b> {
    pub fn new(
        #[cfg_attr(not(feature = "stream_logging"), allow(unused))]
        sv_id: StreamValueId,
        stream_value: &'b mut StreamValue<'a>,
        memory_budget: usize,
        clear_if_streaming: bool,
    ) -> Self {
        let mut dead_elems_leading = 0;
        if clear_if_streaming && !stream_value.is_buffered() {
            stream_value.data.retain(|svd| !svd.is_ref_type());
            dead_elems_leading = stream_value.data.len();
            stream_value.reset_subscriber_offsets();
        }
        let memory_budget = match stream_value.buffer_mode {
            StreamValueBufferMode::Stream => {
                let stream_buffers_cap = stream_value
                    .data
                    .iter()
                    .map(StreamValueData::capacity)
                    .sum::<usize>();
                let stream_buffers_overhead =
                    stream_value.data.len() * STREAM_VALUE_DATA_OVERHEAD;
                let stream_buffers_mem =
                    stream_buffers_cap + stream_buffers_overhead;
                stream_buffers_mem.max(memory_budget)
            }
            StreamValueBufferMode::Buffered
            | StreamValueBufferMode::Contiguous => {
                // we will eventually need the full thing in memory,
                // so no there is no point in limiting the memory consumption
                usize::MAX
            }
        };
        Self {
            #[cfg(feature = "stream_logging")]
            sv_id,
            stream_value,
            dead_elems_leading,
            memory_budget,
        }
    }
    pub fn stream_value(&mut self) -> &mut StreamValue<'a> {
        self.stream_value
    }
    pub fn consume_memory_budget(&mut self, mem: usize) {
        self.memory_budget = self.memory_budget.saturating_sub(mem);
    }
    pub fn consume_memory_budget_extra_buffered(&mut self, mem: usize) {
        self.consume_memory_budget(mem + STREAM_VALUE_DATA_OVERHEAD);
    }
    pub fn propagate_error(
        &mut self,
        error: &Option<Arc<OperatorApplicationError>>,
    ) -> bool {
        self.stream_value.propagate_error(error)
    }
    pub fn append_copy(&mut self, data: StreamValueDataRef<'a, '_>) {
        match data {
            StreamValueDataRef::StaticText(v) => {
                if self.stream_value.is_contiguous() {
                    self.append_text_copy(v)
                } else {
                    self.consume_memory_budget_extra_buffered(v.len());
                    self.stream_value
                        .data
                        .push_back(StreamValueData::StaticText(v));
                }
            }
            StreamValueDataRef::StaticBytes(v) => {
                if self.stream_value.is_contiguous() {
                    self.append_bytes_copy(v)
                } else {
                    self.consume_memory_budget_extra_buffered(v.len());
                    self.stream_value
                        .data
                        .push_back(StreamValueData::StaticBytes(v))
                }
            }
            StreamValueDataRef::Text(v) => self.append_text_copy(v),
            StreamValueDataRef::Bytes(v) => self.append_bytes_copy(v),
        }
    }
    pub fn with_bytes_buffer<R>(
        &mut self,
        f: impl FnOnce(&mut Vec<u8>) -> R,
    ) -> R {
        if self.dead_elems_leading > 0 {
            let mut d = self.stream_value.data.pop_front().unwrap();
            d.clear();
            let res = d.with_bytes_buffer(f);
            self.stream_value.data.push_back(d);
            self.dead_elems_leading -= 1;
            return res;
        }

        if self.stream_value.data.len() > self.dead_elems_leading {
            if let StreamValueData::Bytes { data, range } =
                self.stream_value.data.back_mut().unwrap()
            {
                if let Some(data) = Arc::get_mut(data) {
                    data.truncate(range.end);
                    let res = f(data);
                    range.end = data.len();
                    return res;
                }
            }
        }

        let mut v = Vec::new();
        let res = f(&mut v);
        self.stream_value
            .data
            .push_back(StreamValueData::from_bytes(v));
        res
    }
    pub fn with_text_buffer<R>(
        &mut self,
        f: impl FnOnce(&mut String) -> R,
    ) -> Result<R, FromUtf8Error> {
        if self.dead_elems_leading > 0 {
            self.dead_elems_leading -= 1;
            let mut d = self.stream_value.data.pop_front().unwrap();
            d.clear();
            let res = d.with_text_buffer(f)?;
            self.consume_memory_budget_extra_buffered(d.len());
            self.stream_value.data.push_back(d);
            return Ok(res);
        }

        if !self.stream_value.data.is_empty() {
            let last_data = self.stream_value.data.back_mut().unwrap();
            if let StreamValueData::Text { data, range } = last_data {
                if let Some(data) = Arc::get_mut(data) {
                    data.truncate(range.end);
                    let mut w = TextWriteFormatAdapter(data);
                    let res = f(&mut w);
                    let len_new = w.0.len();
                    let budet_used = len_new.saturating_sub(range.end);
                    range.end = len_new;
                    self.consume_memory_budget(budet_used);
                    return Ok(res);
                }
            }
            if self.stream_value.buffer_mode
                == StreamValueBufferMode::Contiguous
            {
                let len_before = last_data.len();
                let res = last_data.with_text_buffer(f);
                let used_budget = last_data.len().saturating_sub(len_before);
                self.consume_memory_budget(used_budget);
                return res;
            }
        }

        let mut s = String::new();
        let res = f(&mut s);
        self.consume_memory_budget_extra_buffered(s.len());
        self.stream_value
            .data
            .push_back(StreamValueData::from_string(s));
        Ok(res)
    }
    pub fn with_text_write<R>(
        &mut self,
        f: impl FnOnce(&mut dyn TextWrite) -> R,
    ) -> R {
        if self.dead_elems_leading > 0 {
            let mut d = self.stream_value.data.pop_front().unwrap();
            d.clear();
            let res = d.with_text_write(f);
            self.consume_memory_budget_extra_buffered(d.len());
            self.stream_value.data.push_back(d);
            self.dead_elems_leading -= 1;
            return res;
        }

        if !self.stream_value.data.is_empty() {
            let last_data = self.stream_value.data.back_mut().unwrap();
            if let StreamValueData::Text { data, range } = last_data {
                if let Some(data) = Arc::get_mut(data) {
                    data.truncate(range.end);
                    let mut w = TextWriteFormatAdapter(data);
                    let res = f(&mut w);
                    let len_new = w.0.len();
                    let used_budget = len_new - range.end;
                    range.end = len_new;
                    self.consume_memory_budget(used_budget);
                    return res;
                }
            }
            if self.stream_value.buffer_mode
                == StreamValueBufferMode::Contiguous
            {
                let len_before = last_data.len();
                let res = last_data.with_text_write(f);
                let budget = last_data.len().saturating_sub(len_before);
                self.consume_memory_budget(budget);
                return res;
            }
        }

        let mut s = String::new();
        let res = f(&mut TextWriteFormatAdapter(&mut s));
        self.consume_memory_budget_extra_buffered(s.len());
        self.stream_value
            .data
            .push_back(StreamValueData::from_string(s));
        res
    }
    pub fn append_bytes_copy(&mut self, data: &[u8]) {
        self.with_bytes_buffer(|b| b.extend_from_slice(data));
    }
    pub fn append_text_copy(&mut self, data: &str) {
        self.with_text_write(|s| s.write_all_text(data)).unwrap()
    }
    pub fn append(&mut self, data: StreamValueData<'a>) {
        if self.may_append_buffer() {
            // we only appended a clone of an Arc,
            // so we don't count the actual data size
            self.consume_memory_budget(STREAM_VALUE_DATA_OVERHEAD);
            self.stream_value.data.push_back(data);
            return;
        }
        match data {
            StreamValueData::StaticText(t) => self.append_text_copy(t),
            StreamValueData::StaticBytes(b) => self.append_bytes_copy(b),
            StreamValueData::Text { data, range } => {
                self.append_text_copy(&data[range])
            }
            StreamValueData::Bytes { data, range } => {
                self.append_bytes_copy(&data[range])
            }
        }
    }
    pub fn may_append_buffer(&self) -> bool {
        self.stream_value.data.is_empty()
            || self.stream_value.buffer_mode
                != StreamValueBufferMode::Contiguous
    }
    pub fn extend_from_cursor<'c>(
        &mut self,
        cursor: &mut StreamValueDataCursor<'a, 'c>,
    ) {
        while let Some(data) = cursor.next_steal(self.may_append_buffer()) {
            self.append(data);
        }
    }
    pub fn memory_budget_remaining(&self) -> usize {
        self.memory_budget
    }
    pub fn memory_budget_reached(&self) -> bool {
        self.memory_budget == 0
    }
}

impl<'a, 'b> Drop for StreamValueDataInserter<'a, 'b> {
    fn drop(&mut self) {
        self.stream_value.data.drain(0..self.dead_elems_leading);
        #[cfg(feature = "stream_logging")]
        eprintln!(
            ":: inserted into stream value {:02} {:?}",
            self.sv_id, self.stream_value.data
        );
    }
}

impl<'a> StreamValue<'a> {
    pub fn subscribe(
        &mut self,
        #[cfg_attr(not(feature = "stream_logging"), allow(unused))]
        sv_id: StreamValueId,
        tf_id: TransformId,
        custom_data: usize,
        notify_only_once_done: bool,
        treat_current_data_as_consumed: bool,
    ) {
        let data_offset =
            if treat_current_data_as_consumed && self.is_buffered() {
                self.curr_data_offset()
            } else {
                StreamValueDataOffset::default()
            };
        self.subscribers.push(StreamValueSubscription {
            tf_id,
            data_offset,
            custom_data,
            notify_only_once_done,
        });
        self.ref_count += 1;
        #[cfg(feature = "stream_logging")]
        eprintln!(
            ":: tf {tf_id:02} subscribed to stream value {sv_id:02} (subs: {:?}, rc {}), [{:?}]",
            self.subscribers.iter().map(|svs|svs.tf_id).collect::<Vec<_>>(),
            self.ref_count,
            self.data
        );
    }
    pub fn curr_data_offset(&self) -> StreamValueDataOffset {
        // we use the end of the last element, not the start of the next
        // in case this one gets appended
        StreamValueDataOffset {
            values_consumed: self.data.len().saturating_sub(1),
            current_value_offset: self
                .data
                .back()
                .map(StreamValueData::len)
                .unwrap_or(0),
        }
    }
    pub fn make_contiguous(&mut self) {
        if self.buffer_mode == StreamValueBufferMode::Contiguous {
            return;
        }
        self.buffer_mode = StreamValueBufferMode::Contiguous;
        if self.data.is_empty() {
            return;
        }
        if self.data.len() == 1 {
            self.data[0].make_modifiable();
            return;
        }
        match self.data_type.unwrap() {
            StreamValueDataType::Text | StreamValueDataType::MaybeText => {
                let mut res = String::new();
                let (s1, s2) = self.data.as_mut_slices();
                let (first, s1) = s1.split_first_mut().unwrap();
                if let StreamValueData::Text { data, range } = first {
                    if let Some(data) = Arc::get_mut(data) {
                        data.truncate(range.end);
                        for d in s1.iter().chain(s2.iter()) {
                            data.push_str(d.as_str().unwrap());
                        }
                        range.end = data.len();
                        self.data.truncate(1);
                        return;
                    }
                };
                for d in &self.data {
                    res.push_str(d.as_str().unwrap());
                }
                self.data.clear();
                self.data.push_back(StreamValueData::from_string(res));
            }
            StreamValueDataType::Bytes => {
                let mut res = Vec::new();
                let (s1, s2) = self.data.as_mut_slices();
                let (first, s1) = s1.split_first_mut().unwrap();
                if let StreamValueData::Bytes { data, range } = first {
                    if let Some(data) = Arc::get_mut(data) {
                        data.truncate(range.end);
                        for d in s1.iter().chain(s2.iter()) {
                            data.extend_from_slice(d.as_bytes());
                        }
                        range.end = data.len();
                        self.data.truncate(1);
                        return;
                    }
                };
                for d in &self.data {
                    res.extend_from_slice(d.as_bytes());
                }
                self.data.clear();
                self.data.push_back(StreamValueData::from_bytes(res));
            }
            StreamValueDataType::VariableTypeArray => todo!(),

            StreamValueDataType::FixedTypeArray(_) => todo!(),
        }
    }
    pub fn make_buffered(&mut self) {
        if self.buffer_mode == StreamValueBufferMode::Stream {
            self.buffer_mode = StreamValueBufferMode::Buffered;
        }
    }
    pub fn data_len_present(&self) -> usize {
        self.data.iter().map(StreamValueData::len).sum::<usize>()
    }
    pub fn to_field_value(&self) -> FieldValue {
        debug_assert!(self.done);
        if let Some(e) = &self.error {
            return FieldValue::Error((**e).clone());
        }
        let mut ms = MaybeText::default();
        for data in &self.data {
            match data.as_ref().storage_agnostic() {
                StorageAgnosticStreamValueDataRef::Text(t) => {
                    ms.write_all_text(t).unwrap()
                }
                StorageAgnosticStreamValueDataRef::Bytes(b) => {
                    ms.write_all(b).unwrap()
                }
            }
        }
        match ms {
            MaybeText::Text(t) => FieldValue::Text(t),
            MaybeText::Bytes(b) => FieldValue::Bytes(b),
        }
    }
    pub fn set_error(&mut self, error: Arc<OperatorApplicationError>) {
        debug_assert!(self.error.is_none());
        self.error = Some(error);
        self.done = true;
        self.data.clear();
    }
    pub fn new_empty(
        data_type: Option<StreamValueDataType>,
        buffer_mode: StreamValueBufferMode,
    ) -> Self {
        Self {
            error: None,
            data_type,
            data: VecDeque::new(),
            buffer_mode,
            done: false,
            subscribers: SmallVec::new(),
            ref_count: 1,
        }
    }
    pub fn from_data(
        data_type: Option<StreamValueDataType>,
        data: StreamValueData<'a>,
        buffer_mode: StreamValueBufferMode,
        done: bool,
    ) -> Self {
        Self {
            error: None,
            data_type,
            data: std::iter::once(data).collect::<VecDeque<_>>(),
            buffer_mode,
            done,
            subscribers: SmallVec::new(),
            ref_count: 1,
        }
    }
    pub fn from_data_done(data: StreamValueData<'a>) -> Self {
        let data_type = match &data {
            StreamValueData::StaticText(_) | StreamValueData::Text { .. } => {
                StreamValueDataType::Text
            }
            StreamValueData::StaticBytes(_)
            | StreamValueData::Bytes { .. } => StreamValueDataType::Bytes,
        };
        Self::from_data(
            Some(data_type),
            data,
            StreamValueBufferMode::Contiguous,
            true,
        )
    }
    pub fn reset_subscriber_offsets(&mut self) {
        for sub in &mut self.subscribers {
            sub.data_offset = StreamValueDataOffset::default();
        }
    }
    pub fn clear_buffer(&mut self) {
        self.data.clear();
        self.reset_subscriber_offsets();
    }
    pub fn clear_if_streaming(&mut self) {
        if !self.buffer_mode.is_streaming() {
            return;
        };
        self.clear_buffer();
    }
    pub fn propagate_error(
        &mut self,
        error: &Option<Arc<OperatorApplicationError>>,
    ) -> bool {
        if self.error.is_some() {
            return true;
        }
        if let Some(e) = error {
            self.done = true;
            self.error = Some(e.clone());
            return true;
        }
        false
    }
    pub fn data_cursor(
        &mut self,
        offset: StreamValueDataOffset,
        may_consume_data: bool,
    ) -> StreamValueDataCursor<'a, '_> {
        StreamValueDataCursor::new(self, offset, may_consume_data)
    }
    pub fn data_cursor_from_update(
        &mut self,
        update: &StreamValueUpdate,
    ) -> StreamValueDataCursor<'a, '_> {
        StreamValueDataCursor::new(
            self,
            update.data_offset,
            update.may_consume_data,
        )
    }
    pub fn data_iter<'b>(
        &'b self,
        offset: StreamValueDataOffset,
    ) -> StreamValueDataIter<'a, 'b> {
        StreamValueDataIter::new(self, offset)
    }
    pub fn data_inserter(
        &mut self,
        sv_id: StreamValueId,
        default_batch_size: usize,
        clear_if_streaming: bool,
    ) -> StreamValueDataInserter<'a, '_> {
        StreamValueDataInserter::new(
            sv_id,
            self,
            default_batch_size,
            clear_if_streaming,
        )
    }
    pub fn is_buffered(&self) -> bool {
        self.buffer_mode != StreamValueBufferMode::Stream
    }
    pub fn is_contiguous(&self) -> bool {
        self.buffer_mode == StreamValueBufferMode::Contiguous
    }
    pub fn single_data(&self) -> &StreamValueData<'a> {
        debug_assert!(self.data.len() == 1);
        &self.data[0]
    }
    pub fn single_data_mut(&mut self) -> &mut StreamValueData<'a> {
        debug_assert!(self.data.len() == 1);
        &mut self.data[0]
    }

    pub fn mark_done(&mut self) {
        self.done = true;
        if self.data_type == Some(StreamValueDataType::MaybeText) {
            self.data_type = Some(StreamValueDataType::Text)
        }
    }
}

impl<'a> StreamValueData<'a> {
    pub fn from_string(data: String) -> Self {
        StreamValueData::Text {
            range: 0..data.len(),
            data: Arc::new(data),
        }
    }
    pub fn from_bytes(data: Vec<u8>) -> Self {
        StreamValueData::Bytes {
            range: 0..data.len(),
            data: Arc::new(data),
        }
    }
    pub fn from_maybe_text(data: MaybeText) -> Self {
        match data {
            MaybeText::Text(s) => Self::from_string(s),
            MaybeText::Bytes(b) => Self::from_bytes(b),
        }
    }
    pub fn from_maybe_text_cow(data: MaybeTextCow<'a>) -> Self {
        match data {
            MaybeTextCow::Text(s) => Self::from_string(s),
            MaybeTextCow::Bytes(b) => Self::from_bytes(b),
            MaybeTextCow::TextRef(t) => Self::StaticText(t),
            MaybeTextCow::BytesRef(b) => Self::StaticBytes(b),
        }
    }
    pub fn from_maybe_text_ref(data: MaybeTextRef<'a>) -> Self {
        match data {
            MaybeTextRef::Text(t) => StreamValueData::StaticText(t),
            MaybeTextRef::Bytes(b) => StreamValueData::StaticBytes(b),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            StreamValueData::StaticText(v) => Some(v),
            StreamValueData::Text { data, range } => {
                Some(&data[range.clone()])
            }
            StreamValueData::StaticBytes(_)
            | StreamValueData::Bytes { .. } => None,
        }
    }
    pub fn as_ref(&self) -> StreamValueDataRef<'a, '_> {
        match self {
            StreamValueData::StaticText(v) => {
                StreamValueDataRef::StaticText(v)
            }
            StreamValueData::StaticBytes(v) => {
                StreamValueDataRef::StaticBytes(v)
            }
            StreamValueData::Text { data, range } => {
                StreamValueDataRef::Text(&data[range.clone()])
            }
            StreamValueData::Bytes { data, range } => {
                StreamValueDataRef::Bytes(&data[range.clone()])
            }
        }
    }
    pub fn as_field_value_ref(&self) -> FieldValueRef {
        match &self {
            StreamValueData::StaticBytes(v) => FieldValueRef::Bytes(v),
            StreamValueData::StaticText(v) => FieldValueRef::Text(v),
            StreamValueData::Text { data, range } => {
                FieldValueRef::Text(&data[range.clone()])
            }
            StreamValueData::Bytes { data, range } => {
                FieldValueRef::Bytes(&data[range.clone()])
            }
        }
    }
    pub fn to_field_value(&self) -> FieldValue {
        self.as_field_value_ref().to_field_value()
    }
    pub fn kind(&self) -> FieldValueKind {
        self.as_field_value_ref().repr().kind()
    }
    pub fn len(&self) -> usize {
        match self {
            StreamValueData::StaticText(t) => t.len(),
            StreamValueData::StaticBytes(b) => b.len(),
            StreamValueData::Text { range, .. }
            | StreamValueData::Bytes { range, .. } => range.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn capacity(&self) -> usize {
        match self {
            StreamValueData::StaticText(t) => t.len(),
            StreamValueData::StaticBytes(b) => b.len(),
            StreamValueData::Text { data, .. } => data.capacity(),
            StreamValueData::Bytes { data, .. } => data.capacity(),
        }
    }
    pub fn clear(&mut self) {
        match self {
            StreamValueData::StaticText(t) => *t = "",
            StreamValueData::StaticBytes(b) => *b = b"",
            StreamValueData::Text { data, range } => {
                if let Some(data) = Arc::get_mut(data) {
                    data.clear();
                } else {
                    *data = Arc::new(String::new());
                }
                *range = 0..0;
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(data) = Arc::get_mut(data) {
                    data.clear();
                } else {
                    *data = Arc::new(Vec::new());
                }
                *range = 0..0;
            }
        }
    }
    pub fn is_ref_type(&self) -> bool {
        match self {
            StreamValueData::StaticText(_)
            | StreamValueData::StaticBytes(_) => true,
            StreamValueData::Text { .. } | StreamValueData::Bytes { .. } => {
                false
            }
        }
    }
    pub unsafe fn extend_with_bytes_raw(
        &mut self,
        bytes: &[u8],
        valid_utf8: bool,
    ) {
        if !valid_utf8 {
            self.extend_with_bytes(bytes);
            return;
        }
        self.extend_with_text(unsafe { std::str::from_utf8_unchecked(bytes) })
    }
    pub fn extend_with_bytes(&mut self, bytes: &[u8]) {
        match self {
            StreamValueData::Text { .. } => {
                let StreamValueData::Text { data, range } =
                    std::mem::take(self)
                else {
                    unreachable!()
                };
                match Arc::try_unwrap(data) {
                    Ok(t) => {
                        let mut v = t.into_bytes();
                        retain_vec_range(&mut v, range);
                        v.extend_from_slice(bytes);
                        *self = StreamValueData::from_bytes(v);
                    }
                    Err(t) => {
                        let mut s =
                            Vec::with_capacity(range.len() + bytes.len());
                        s.extend_from_slice(&t.as_bytes()[range]);
                        s.extend_from_slice(bytes);
                        *self = StreamValueData::from_bytes(s);
                    }
                };
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(t) = Arc::get_mut(data) {
                    retain_vec_range(t, range.clone());
                    t.extend_from_slice(bytes);
                    *range = 0..t.len();
                } else {
                    let mut v = Vec::with_capacity(range.len() + bytes.len());
                    v.extend_from_slice(&data[range.clone()]);
                    v.extend_from_slice(bytes);
                    *self = StreamValueData::from_bytes(v);
                }
            }
            StreamValueData::StaticText(t) => {
                let mut v = Vec::with_capacity(t.len() + bytes.len());
                v.extend_from_slice(t.as_bytes());
                v.extend_from_slice(bytes);
                *self = StreamValueData::from_bytes(v);
            }
            StreamValueData::StaticBytes(t) => {
                let mut v = Vec::with_capacity(t.len() + bytes.len());
                v.extend_from_slice(t);
                v.extend_from_slice(bytes);
                *self = StreamValueData::from_bytes(v);
            }
        }
    }
    pub fn extend_with_text(&mut self, text: &str) {
        match self {
            StreamValueData::Text { data, range } => {
                if let Some(t) = Arc::get_mut(data) {
                    retain_string_range(t, range.clone());
                    t.push_str(text);
                    *range = 0..t.len();
                } else {
                    let mut s =
                        String::with_capacity(range.len() + text.len());
                    s.push_str(&data[range.clone()]);
                    s.push_str(text);
                    *self = StreamValueData::from_string(s)
                }
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(t) = Arc::get_mut(data) {
                    retain_vec_range(t, range.clone());
                    t.extend_from_slice(text.as_bytes());
                    *range = 0..t.len();
                } else {
                    let mut v = Vec::with_capacity(range.len() + text.len());
                    v.extend_from_slice(&data[range.clone()]);
                    v.extend_from_slice(text.as_bytes());
                    *self = StreamValueData::from_bytes(v)
                }
            }
            StreamValueData::StaticText(t) => {
                let mut s = String::with_capacity(t.len() + text.len());
                s.push_str(t);
                s.push_str(text);
                *self = StreamValueData::from_string(s);
            }
            StreamValueData::StaticBytes(t) => {
                let mut v = Vec::with_capacity(t.len() + text.len());
                v.extend_from_slice(t);
                v.extend_from_slice(text.as_bytes());
                *self = StreamValueData::from_bytes(v);
            }
        }
    }
    pub fn extend_from_sv_data(&mut self, data: &StreamValueData) {
        match data.as_ref() {
            StreamValueDataRef::StaticText(t)
            | StreamValueDataRef::Text(t) => self.extend_with_text(t),
            StreamValueDataRef::StaticBytes(b)
            | StreamValueDataRef::Bytes(b) => self.extend_with_bytes(b),
        }
    }
    pub fn slice(&mut self, range: Range<usize>) {
        match self {
            StreamValueData::StaticText(t) => {
                *self = StreamValueData::StaticText(&t[range]);
            }
            StreamValueData::StaticBytes(t) => {
                *self = StreamValueData::StaticBytes(&t[range]);
            }
            StreamValueData::Text {
                range: range_prev, ..
            }
            | StreamValueData::Bytes {
                range: range_prev, ..
            } => {
                *range_prev = subrange(range_prev, &range);
            }
        }
    }
    pub fn sliced(&self, range: Range<usize>) -> StreamValueData<'a> {
        let mut res = self.clone();
        res.slice(range);
        res
    }
    pub fn with_bytes_buffer<R>(
        &mut self,
        f: impl FnOnce(&mut Vec<u8>) -> R,
    ) -> R {
        match self {
            StreamValueData::StaticText(text) => {
                let mut v = Vec::new();
                v.extend_from_slice(text.as_bytes());
                let res = f(&mut v);
                *self = Self::from_bytes(v);
                res
            }
            StreamValueData::StaticBytes(bytes) => {
                let mut v = Vec::new();
                v.extend_from_slice(bytes);
                let res = f(&mut v);
                *self = Self::from_bytes(v);
                res
            }
            StreamValueData::Text { data, range } => {
                let mut v;
                let res;
                let range_new: Range<usize>;
                if let Some(data) = Arc::get_mut(data) {
                    v = std::mem::take(data).into_bytes();
                    v.truncate(range.len());
                    res = f(&mut v);
                    range_new = 0..v.len();
                } else {
                    v = Vec::new();
                    v.extend_from_slice(&data.as_bytes()[range.clone()]);
                    res = f(&mut v);
                    range_new = 0..v.len();
                };
                *self = StreamValueData::Bytes {
                    data: Arc::new(v),
                    range: range_new,
                };
                res
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(v) = Arc::get_mut(data) {
                    v.truncate(range.len());
                    let res = f(v);
                    range.end = v.len();
                    return res;
                };
                let mut v = Vec::new();
                v.extend_from_slice(&data[range.clone()]);
                let res = f(&mut v);
                *self = Self::from_bytes(v);
                res
            }
        }
    }
    // fails if the data is currently a non empty byte range
    pub fn with_text_write<R>(
        &mut self,
        f: impl FnOnce(&mut dyn TextWrite) -> R,
    ) -> R {
        match self {
            StreamValueData::StaticText(text) => {
                let mut s = String::new();
                s.push_str(text);
                let res = f(&mut TextWriteFormatAdapter(&mut s));
                *self = Self::from_string(s);
                res
            }
            StreamValueData::Text { data, range } => {
                if let Some(s) = Arc::get_mut(data) {
                    s.truncate(range.len());
                    let mut w = TextWriteFormatAdapter(s);
                    let res = f(&mut w);
                    *range = range.start..w.0.len();
                    return res;
                };
                let mut s = String::new();
                s.push_str(&data[range.clone()]);
                let res = f(&mut TextWriteFormatAdapter(&mut s));
                *self = Self::from_string(s);
                res
            }
            StreamValueData::StaticBytes(b) => {
                let mut v = Vec::new();
                v.extend_from_slice(b);
                let res = f(&mut TextWriteIoAdapter(&mut v));
                *self = Self::from_bytes(v);
                res
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(v) = Arc::get_mut(data) {
                    v.truncate(range.len());
                    let res = f(&mut TextWriteIoAdapter(v));
                    range.end = data.len();
                    return res;
                };
                let mut v = Vec::new();
                v.extend_from_slice(&data[range.clone()]);
                let res = f(&mut TextWriteIoAdapter(&mut v));
                *self = Self::from_bytes(v);
                res
            }
        }
    }
    pub fn make_modifiable(&mut self) {
        self.extend_with_text(""); // PERF: we could optimize this
    }
    // fails if this is a non empty bytes buffer containing invalid utf-8
    pub fn with_text_buffer<R>(
        &mut self,
        f: impl FnOnce(&mut String) -> R,
    ) -> Result<R, FromUtf8Error> {
        match self {
            StreamValueData::StaticText(text) => {
                let mut s = String::new();
                s.push_str(text);
                let res = f(&mut s);
                *self = Self::from_string(s);
                Ok(res)
            }
            StreamValueData::Text { data, range } => {
                if let Some(s) = Arc::get_mut(data) {
                    s.truncate(range.len());
                    let mut w = TextWriteFormatAdapter(s);
                    let res = f(&mut w);
                    *range = range.start..w.0.len();
                    return Ok(res);
                };
                let mut s = String::new();
                s.push_str(&data[range.clone()]);
                let res = f(&mut s);
                *self = Self::from_string(s);
                Ok(res)
            }
            StreamValueData::StaticBytes(b) => {
                let mut s = String::from_utf8(b.to_owned())?;
                let res = f(&mut s);
                *self = Self::from_string(s);
                Ok(res)
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(v) = Arc::get_mut(data) {
                    retain_vec_range(v, range.clone());
                    let mut s = String::from_utf8(std::mem::take(v))?;
                    let res = f(&mut s);
                    *self = Self::from_string(s);
                    return Ok(res);
                };
                let mut s = String::from_utf8(data[range.clone()].to_owned())?;
                let res = f(&mut s);
                *self = Self::from_string(s);
                Ok(res)
            }
        }
    }
    pub fn as_escaped_text(&self, quote_to_escape: u8) -> Self {
        // PERF: might be able to optimize this
        let mut w = EscapedWriter::new(String::new(), quote_to_escape);
        w.write_all(self.as_bytes()).unwrap();
        Self::from_string(w.into_inner().unwrap())
    }
}

impl<'a, 'b> StreamValueDataRef<'a, 'b> {
    pub fn sliced<
        R: SliceIndex<[u8], Output = [u8]> + SliceIndex<str, Output = str>,
    >(
        &self,
        range: R,
    ) -> Self {
        match self {
            StreamValueDataRef::StaticText(v) => {
                StreamValueDataRef::StaticText(&v[range])
            }
            StreamValueDataRef::StaticBytes(v) => {
                StreamValueDataRef::StaticBytes(&v[range])
            }
            StreamValueDataRef::Bytes(v) => {
                StreamValueDataRef::Bytes(&v[range])
            }
            StreamValueDataRef::Text(v) => StreamValueDataRef::Text(&v[range]),
        }
    }
    pub fn to_owned(&self) -> StreamValueData {
        match self {
            StreamValueDataRef::StaticText(t) => {
                StreamValueData::StaticText(t)
            }
            StreamValueDataRef::StaticBytes(b) => {
                StreamValueData::StaticBytes(b)
            }
            StreamValueDataRef::Text(t) => StreamValueData::Text {
                data: Arc::new((*t).to_string()),
                range: 0..t.len(),
            },
            StreamValueDataRef::Bytes(b) => StreamValueData::Bytes {
                data: Arc::new(b.to_vec()),
                range: 0..b.len(),
            },
        }
    }
    pub fn len(&self) -> usize {
        match *self {
            StreamValueDataRef::StaticBytes(b)
            | StreamValueDataRef::Bytes(b) => b.len(),
            StreamValueDataRef::Text(t)
            | StreamValueDataRef::StaticText(t) => t.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl<'a> StreamValueDataRef<'a, 'a> {
    pub fn storage_agnostic(self) -> StorageAgnosticStreamValueDataRef<'a> {
        match self {
            StreamValueDataRef::Text(v)
            | StreamValueDataRef::StaticText(v) => {
                StorageAgnosticStreamValueDataRef::Text(v)
            }
            StreamValueDataRef::Bytes(v)
            | StreamValueDataRef::StaticBytes(v) => {
                StorageAgnosticStreamValueDataRef::Bytes(v)
            }
        }
    }
    pub fn as_bytes(&self) -> &'a [u8] {
        match self {
            StreamValueDataRef::Text(v)
            | StreamValueDataRef::StaticText(v) => v.as_bytes(),
            StreamValueDataRef::StaticBytes(v)
            | StreamValueDataRef::Bytes(v) => v,
        }
    }
}

impl<'d> StreamValueDataRef<'static, 'd> {
    pub fn from_maybe_text(mt: impl Into<MaybeTextRef<'d>>) -> Self {
        match mt.into() {
            MaybeTextRef::Bytes(b) => StreamValueDataRef::Bytes(b),
            MaybeTextRef::Text(t) => StreamValueDataRef::Text(t),
        }
    }
}
impl<'s> StreamValueDataRef<'s, 'static> {
    pub fn from_maybe_text_static(mt: impl Into<MaybeTextRef<'s>>) -> Self {
        match mt.into() {
            MaybeTextRef::Bytes(b) => StreamValueDataRef::StaticBytes(b),
            MaybeTextRef::Text(t) => StreamValueDataRef::StaticText(t),
        }
    }
}

impl<'a> Default for StreamValue<'a> {
    fn default() -> Self {
        Self {
            error: None,
            data_type: None,
            data: VecDeque::new(),
            buffer_mode: StreamValueBufferMode::Stream,
            done: false,
            subscribers: SmallVec::new(),
            ref_count: 1,
        }
    }
}

impl<'a> Default for StreamValueData<'a> {
    fn default() -> Self {
        StreamValueData::StaticText("")
    }
}

impl<'a> StorageAgnosticStreamValueDataRef<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            StorageAgnosticStreamValueDataRef::Text(v) => v.as_bytes(),
            StorageAgnosticStreamValueDataRef::Bytes(v) => v,
        }
    }
}

#[derive(Debug)]
pub struct UnsupportedStreamValueDataKindError(pub FieldValueKind);

impl TryFrom<FieldValue> for StreamValueData<'static> {
    type Error = UnsupportedStreamValueDataKindError;

    fn try_from(
        value: FieldValue,
    ) -> Result<Self, UnsupportedStreamValueDataKindError> {
        let v = match value {
            FieldValue::Text(t) => StreamValueData::from_string(t),
            FieldValue::Bytes(b) => StreamValueData::from_bytes(b),
            _ => {
                return Err(UnsupportedStreamValueDataKindError(value.kind()))
            }
        };
        Ok(v)
    }
}

impl<'a> StreamValueManager<'a> {
    pub fn inform_stream_value_subscribers(&mut self, sv_id: StreamValueId) {
        let sv = &mut self.stream_values[sv_id];
        let last_sub = sv.subscribers.len().saturating_sub(1);
        let new_data_offset = sv.curr_data_offset();
        for (i, sub) in sv.subscribers.iter_mut().enumerate() {
            if !sub.notify_only_once_done || sv.done {
                self.updates.push_back(StreamValueUpdate {
                    sv_id,
                    tf_id: sub.tf_id,
                    custom: sub.custom_data,
                    data_offset: sub.data_offset,
                    may_consume_data: i == last_sub,
                });
                sub.data_offset = new_data_offset;
            }
        }
        #[cfg(feature = "stream_logging")]
        {
            eprintln!(
                ":: queued updates to the {} subscriber(s) of stream value {sv_id:02} (data: {:?}, done: {})",
                sv.subscribers.len(),
                sv.data,
                sv.done,
            );
            eprint!("   pending updates: ");
            self.log_pending_updates(4);
            eprintln!();
        }
    }
    pub fn log_pending_updates(&self, indent_level: usize) {
        if self.updates.is_empty() {
            eprint!("[]");
            return;
        }
        eprintln!("[");
        for u in &self.updates {
            eprintln!("{:padding$}{u:?},", "", padding = indent_level + 4);
        }
        eprint!("{:padding$}]", "", padding = indent_level.saturating_sub(1));
    }
    pub fn release_stream_value(&mut self, sv_id: StreamValueId) {
        #[cfg(feature = "stream_logging")]
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

        #[cfg(feature = "stream_logging")]
        {
            use crate::utils::indexing_type::IndexingType;
            eprintln!(
                ":: tf {:02} dropping stream value subscription to sv {sv_id:02} (subs: {:?}) [{}done, rc {}, {:?}]",
                tf_id_to_remove.map(|v|v.into_usize() as i64).unwrap_or(-1),
                sv.subscribers.iter().map(|svs|svs.tf_id).collect::<Vec<_>>(),
                if sv.done {""} else {"not "},
                sv.ref_count,
                sv.data
            );
        }
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
    pub fn claim_stream_value(
        &mut self,
        sv: StreamValue<'a>,
    ) -> StreamValueId {
        let sv_id = self.stream_values.claim_with_value(sv);
        #[cfg(feature = "stream_logging")]
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
        treat_current_data_as_consumed: bool,
    ) {
        self.stream_values[sv_id].subscribe(
            sv_id,
            tf_id,
            custom_data,
            notify_only_once_done,
            treat_current_data_as_consumed,
        )
    }
}
