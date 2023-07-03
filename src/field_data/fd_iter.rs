use std::{marker::PhantomData, ptr::NonNull};

use crate::{
    field_data::{
        field_value_flags, FieldData, FieldReference, FieldValueFlags, FieldValueFormat,
        FieldValueHeader, FieldValueKind, Html, Object, RunLength,
    },
    operations::errors::OperatorApplicationError,
    stream_value::StreamValueId,
};

#[derive(Clone, Copy)]
pub enum FDTypedSlice<'a> {
    Unset(&'a [()]),
    Null(&'a [()]),
    Integer(&'a [i64]),
    StreamValueId(&'a [StreamValueId]),
    Reference(&'a [FieldReference]),
    Error(&'a [OperatorApplicationError]),
    Html(&'a [Html]),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    BytesBuffer(&'a [Vec<u8>]),
    Object(&'a [Object]),
}

#[derive(Clone)]
pub struct TypedSliceIter<'a, T> {
    values: NonNull<T>,
    header: *const FieldValueHeader,
    header_end: *const FieldValueHeader,
    header_rl_rem: RunLength,
    last_oversize: RunLength,
    _phantom_data: PhantomData<&'a FieldValueHeader>,
}

impl<'a, T> TypedSliceIter<'a, T> {
    pub fn new(
        values: &'a [T],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        let mut header_rl_rem = 0;
        if !headers.is_empty() {
            header_rl_rem = headers[0].run_length - first_oversize
        };
        if headers.len() == 1 {
            header_rl_rem -= last_oversize;
        }
        let headers_range = headers.as_ptr_range();
        let values = if values.is_empty() {
            NonNull::dangling()
        } else {
            NonNull::from(&values[0])
        };
        Self {
            values,
            header: headers_range.start,
            header_end: headers_range.end,
            header_rl_rem,
            last_oversize,
            _phantom_data: PhantomData::default(),
        }
    }
    pub fn from_typed_range(range: &'a FDTypedRange<'a>, values: &'a [T]) -> Self {
        Self::new(
            values,
            range.headers,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        )
    }
    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let rl = if (*self.header).shared_value() {
                self.header_rl_rem
            } else {
                1
            };
            return Some((self.values.as_ref(), rl));
        }
    }
    pub fn next_no_sv(&mut self) -> Option<&'a T> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let value = self.values.as_ref();
            self.header_rl_rem -= 1;
            if self.header_rl_rem == 0 {
                self.next_header();
                if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
            } else if !(*self.header).shared_value() {
                self.next_value();
            }
            return Some(value);
        }
    }
    pub fn next_n_fields(&mut self, mut n: usize) {
        if self.header == self.header_end {
            return;
        }
        loop {
            if self.header_rl_rem as usize > n {
                self.header_rl_rem -= n as RunLength;
                unsafe {
                    if !(*self.header).shared_value() {
                        self.advance_value(n);
                    }
                }
                return;
            }
            n -= self.header_rl_rem as usize;
            unsafe {
                if !(*self.header).shared_value() {
                    self.advance_value(self.header_rl_rem as usize);
                } else if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
                self.next_header();
            }
        }
    }
    unsafe fn next_header(&mut self) {
        self.header = self.header.add(1);
        if self.header == self.header_end {
            return;
        }
        let h = *self.header;
        self.header_rl_rem = h.run_length;
        if self.header.add(1) == self.header_end {
            self.header_rl_rem -= self.last_oversize;
        }
    }
    unsafe fn advance_value(&mut self, n: usize) {
        self.values = NonNull::new_unchecked(self.values.as_ptr().add(n));
    }
    unsafe fn next_value(&mut self) {
        self.advance_value(1);
    }
    pub fn has_next(&mut self) -> bool {
        return self.header_rl_rem > 0 || self.header != self.header_end;
    }
    pub fn clear(&mut self) {
        self.header_rl_rem = 0;
        self.header = self.header_end;
    }
}

impl<'a, T: 'a> Iterator for TypedSliceIter<'a, T> {
    type Item = (&'a T, RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let value = self.values.as_ref();
            if (*self.header).shared_value() {
                let rl = self.header_rl_rem;
                self.next_header();
                if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
                return Some((value, rl));
            }
            self.header_rl_rem -= 1;
            if self.header_rl_rem == 0 {
                self.next_header();
                if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
            } else {
                self.next_value();
            }
            return Some((value, 1));
        }
    }
}

#[derive(Clone)]
pub struct InlineBytesIter<'a> {
    //TODO: rework this similarly to typed slice
    data: &'a [u8],
    headers: &'a [FieldValueHeader],
    headers_idx: usize,
    first_oversize: RunLength,
    last_oversize: RunLength,
    header_rl_offset: RunLength,
    header_rl_total: RunLength,
    header_value_size: u16,
    data_offset: usize,
}

impl<'a> InlineBytesIter<'a> {
    pub fn new(
        data: &'a [u8],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        Self {
            data,
            headers: headers,
            headers_idx: 0,
            first_oversize,
            last_oversize,
            header_rl_offset: 0,
            header_rl_total: 0,
            header_value_size: 0,
            data_offset: 0,
        }
    }
    pub fn from_typed_range(range: &'a FDTypedRange<'a>, data: &'a [u8]) -> Self {
        Self::new(
            data,
            range.headers,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        )
    }
}

impl<'a> Iterator for InlineBytesIter<'a> {
    type Item = (&'a [u8], RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header_rl_offset != self.header_rl_total {
            self.header_rl_offset += 1;
            let data_offset_prev = self.data_offset;
            self.data_offset += self.header_value_size as usize;
            return Some((&self.data[data_offset_prev..self.data_offset], 1));
        }
        if self.headers_idx == self.headers.len() {
            return None;
        }
        let h = self.headers[self.headers_idx];
        self.headers_idx += 1;
        if h.shared_value() {
            let data_offset_prev = self.data_offset;
            if !h.same_value_as_previous() {
                self.data_offset += h.size as usize;
            }
            let mut rl = h.run_length;
            if self.headers_idx == 1 {
                rl -= self.first_oversize;
            }
            if self.headers_idx == self.headers.len() {
                rl -= self.last_oversize;
            }
            return Some((
                &self.data[data_offset_prev..data_offset_prev + h.size as usize],
                rl,
            ));
        }
        self.header_rl_offset = 0;
        self.header_rl_total = h.run_length;
        if self.headers_idx == 1 {
            self.header_rl_offset += self.first_oversize;
        }
        if self.headers_idx == self.headers.len() {
            self.data_offset += self.last_oversize as usize * h.size as usize;
            self.header_rl_total -= self.last_oversize;
        }
        self.header_value_size = h.size;
        self.header_rl_offset += 1;
        let data_offset_prev = self.data_offset;
        self.data_offset += h.size as usize;
        return Some((&self.data[data_offset_prev..self.data_offset], 1));
    }
}

pub struct InlineTextIter<'a> {
    iter: InlineBytesIter<'a>,
}

impl<'a> InlineTextIter<'a> {
    pub fn new(
        data: &'a str,
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        Self {
            iter: InlineBytesIter::new(data.as_bytes(), headers, first_oversize, last_oversize),
        }
    }
    pub fn from_typed_range(range: &'a FDTypedRange<'a>, data: &'a str) -> Self {
        Self {
            iter: InlineBytesIter::from_typed_range(range, data.as_bytes()),
        }
    }
}

impl<'a> Iterator for InlineTextIter<'a> {
    type Item = (&'a str, RunLength);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(v, rl)| (unsafe { std::str::from_utf8_unchecked(v) }, rl))
    }
}

pub enum FDTypedValue<'a> {
    Unset(()),
    Null(()),
    Integer(i64),
    StreamValueId(StreamValueId),
    Reference(&'a FieldReference),
    Error(&'a OperatorApplicationError),
    Html(&'a Html),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    BytesBuffer(&'a Vec<u8>),
    Object(&'a Object),
}

pub struct FDTypedRange<'a> {
    pub headers: &'a [FieldValueHeader],
    pub data: FDTypedSlice<'a>,
    pub field_count: usize,
    pub first_header_run_length_oversize: RunLength,
    pub last_header_run_length_oversize: RunLength,
}

pub struct FDTypedField<'a> {
    pub header: FieldValueHeader,
    pub value: FDTypedValue<'a>,
}

impl<'a> Default for FDTypedRange<'a> {
    fn default() -> Self {
        Self {
            headers: &[],
            data: FDTypedSlice::Unset(&[]),
            field_count: 0,
            first_header_run_length_oversize: 0,
            last_header_run_length_oversize: 0,
        }
    }
}
unsafe fn to_zst_slice<T: Sized>(len: usize) -> &'static [T] {
    std::slice::from_raw_parts(NonNull::dangling().as_ptr() as *const T, len)
}

unsafe fn to_slice<T: Sized>(fd: &FieldData, data_begin: usize, len: usize) -> &[T] {
    std::slice::from_raw_parts(
        std::mem::transmute::<&u8, &T>(&fd.data[data_begin]) as *const T,
        len,
    )
}

unsafe fn to_ref<T: Sized>(fd: &FieldData, data_begin: usize) -> &T {
    std::mem::transmute::<&u8, &T>(&fd.data[data_begin])
}

unsafe fn to_typed_range<'a>(
    fd: &'a FieldData,
    flag_mask: FieldValueFlags,
    fmt: FieldValueFormat,
    data_begin: usize,
    data_end: usize,
    field_count: usize,
    header_begin: usize,
    header_end: usize,
    first_header_run_length_oversize: RunLength,
    last_header_run_length_oversize: RunLength,
) -> FDTypedRange<'a> {
    let headers = &fd.header[header_begin..header_end];
    let data = match fmt.kind {
        FieldValueKind::Unset => FDTypedSlice::Unset(to_zst_slice(field_count)),
        FieldValueKind::Null => FDTypedSlice::Null(to_zst_slice(field_count)),
        FieldValueKind::BytesInline => {
            if fmt.flags & flag_mask & field_value_flags::BYTES_ARE_UTF8 != 0 {
                FDTypedSlice::TextInline(std::str::from_utf8_unchecked(to_slice(
                    fd,
                    data_begin,
                    data_end - data_begin,
                )))
            } else {
                FDTypedSlice::BytesInline(to_slice(fd, data_begin, data_end - data_begin))
            }
        }
        FieldValueKind::EntryId => todo!(),
        FieldValueKind::Integer => FDTypedSlice::Integer(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Reference => FDTypedSlice::Reference(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Error => FDTypedSlice::Error(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Html => FDTypedSlice::Html(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Object => FDTypedSlice::Object(to_slice(fd, data_begin, field_count)),
        FieldValueKind::StreamValueId => {
            FDTypedSlice::StreamValueId(to_slice(fd, data_begin, field_count))
        }
        FieldValueKind::BytesBuffer => {
            FDTypedSlice::BytesBuffer(to_slice(fd, data_begin, field_count))
        }
        FieldValueKind::BytesFile => todo!(),
    };
    FDTypedRange {
        headers,
        data,
        field_count,
        first_header_run_length_oversize,
        last_header_run_length_oversize,
    }
}
unsafe fn to_typed_field<'a>(
    fd: &'a FieldData,
    fmt: FieldValueFormat,
    data_begin: usize,
    run_len: RunLength,
) -> FDTypedField<'a> {
    let value = match fmt.kind {
        FieldValueKind::Unset => FDTypedValue::Unset(()),
        FieldValueKind::Null => FDTypedValue::Null(()),
        FieldValueKind::BytesInline => {
            if fmt.flags & field_value_flags::BYTES_ARE_UTF8 != 0 {
                FDTypedValue::TextInline(std::str::from_utf8_unchecked(to_slice(
                    fd,
                    data_begin,
                    fmt.size as usize,
                )))
            } else {
                FDTypedValue::BytesInline(to_slice(fd, data_begin, fmt.size as usize))
            }
        }
        FieldValueKind::EntryId => todo!(),
        FieldValueKind::Integer => FDTypedValue::Integer(*to_ref(fd, data_begin)),
        FieldValueKind::StreamValueId => FDTypedValue::StreamValueId(*to_ref(fd, data_begin)),
        FieldValueKind::Reference => FDTypedValue::Reference(to_ref(fd, data_begin)),
        FieldValueKind::Error => FDTypedValue::Error(to_ref(fd, data_begin)),
        FieldValueKind::Html => FDTypedValue::Html(to_ref(fd, data_begin)),
        FieldValueKind::Object => FDTypedValue::Object(to_ref(fd, data_begin)),
        FieldValueKind::BytesBuffer => FDTypedValue::BytesBuffer(to_ref(fd, data_begin)),
        FieldValueKind::BytesFile => todo!(),
    };
    FDTypedField {
        header: FieldValueHeader {
            fmt: fmt,
            run_length: run_len,
        },
        value,
    }
}

pub trait FDIterator<'a>: Sized {
    fn get_next_field_pos(&self) -> usize;
    fn is_next_valid(&self) -> bool;
    fn is_prev_valid(&self) -> bool;
    fn get_next_field_format(&self) -> FieldValueFormat;
    fn get_next_field_data(&self) -> usize;
    fn get_prev_field_data_end(&self) -> usize;
    // if the cursor is in the middle of a header, *that* header will be
    // returned, not the one after it
    fn get_next_header(&self) -> FieldValueHeader;
    fn get_next_header_data(&self) -> usize;
    fn get_next_header_index(&self) -> usize;
    fn get_prev_header_index(&self) -> usize;
    fn get_next_typed_field(&mut self) -> FDTypedField<'a>;
    fn field_run_length_fwd(&mut self) -> RunLength;
    fn field_run_length_bwd(&mut self) -> RunLength;
    fn next_header(&mut self) -> RunLength;
    fn prev_header(&mut self) -> RunLength;
    fn next_field(&mut self) -> RunLength;
    fn prev_field(&mut self) -> RunLength;
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize;
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize;
    fn move_to_field_pos(&mut self, field_pos: usize) {
        let curr = self.get_next_field_pos();
        if curr > field_pos {
            self.prev_n_fields(curr - field_pos);
        } else if curr < field_pos {
            self.next_n_fields(field_pos - curr);
        }
    }
    fn next_n_fields(&mut self, n: usize) -> usize {
        self.next_n_fields_with_fmt(n, [], 0, 0)
    }
    fn prev_n_fields(&mut self, n: usize) -> usize {
        self.prev_n_fields_with_fmt(n, [], 0, 0)
    }
    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>>;
    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>>;
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>>;
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>>;
    fn bounded(self, backwards: usize, forwards: usize) -> BoundedFDIter<'a, Self> {
        BoundedFDIter::new_relative(self, backwards, forwards)
    }
    fn as_base_iter(self) -> FDIter<'a>;
}

#[repr(C)]
#[derive(Clone)]
pub struct FDIter<'a> {
    pub(super) fd: &'a FieldData,
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    pub(super) header_rl_total: RunLength,
    pub(super) header_fmt: FieldValueFormat,
}

#[repr(C)]
pub struct FDIterMut<'a> {
    pub(super) fd: &'a mut FieldData,
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    pub(super) header_rl_total: RunLength,
    pub(super) header_fmt: FieldValueFormat,
}

impl<'a> FDIter<'a> {
    pub fn from_start(fd: &'a FieldData, initial_field_offset: usize) -> Self {
        let first_header = fd.header.first();
        Self {
            fd,
            field_pos: initial_field_offset,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
            header_rl_total: first_header.map_or(0, |h| h.run_length),
            header_fmt: first_header.map(|h| h.fmt).unwrap_or_default(),
        }
    }
    pub fn from_end(fd: &'a FieldData, initial_field_offset: usize) -> Self {
        Self {
            fd,
            field_pos: initial_field_offset + fd.field_count,
            data: fd.data.len(),
            header_idx: fd.header.len(),
            header_rl_offset: 0,
            header_rl_total: 0,
            header_fmt: Default::default(),
        }
    }
}
impl<'a> FDIterator<'a> for FDIter<'a> {
    fn get_next_field_pos(&self) -> usize {
        self.field_pos
    }
    fn is_next_valid(&self) -> bool {
        self.header_rl_total != 0
    }
    fn is_prev_valid(&self) -> bool {
        !(self.header_idx == 0 && self.header_rl_offset == 0)
    }
    fn get_next_field_format(&self) -> FieldValueFormat {
        debug_assert!(self.is_next_valid());
        self.header_fmt
    }
    fn get_next_field_data(&self) -> usize {
        debug_assert!(self.is_next_valid());
        if self.header_fmt.shared_value() {
            self.data
        } else {
            self.data + self.header_rl_offset as usize * self.header_fmt.size as usize
        }
    }
    fn get_prev_field_data_end(&self) -> usize {
        debug_assert!(self.is_prev_valid());
        if self.header_rl_offset > 0 {
            if self.header_fmt.shared_value() {
                return self.data + self.header_fmt.size as usize;
            }
            return self.data + (self.header_rl_offset as usize) * self.header_fmt.size as usize;
        }
        self.data - self.header_fmt.leading_padding()
    }
    fn get_next_header(&self) -> FieldValueHeader {
        debug_assert!(self.is_next_valid());
        FieldValueHeader {
            fmt: self.header_fmt,
            run_length: self.header_rl_total,
        }
    }
    fn get_next_header_data(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.data
    }
    fn get_next_header_index(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.header_idx
    }
    fn get_prev_header_index(&self) -> usize {
        debug_assert!(self.is_prev_valid());
        if self.header_rl_offset == 0 {
            self.header_idx - 1
        } else {
            self.header_idx
        }
    }
    fn get_next_typed_field(&mut self) -> FDTypedField<'a> {
        // SAFETY: debug assert is not enough here because we use unsafe below
        assert!(self.is_next_valid());
        let data = self.get_next_field_data();
        let run_len = if self.header_fmt.shared_value() {
            self.field_run_length_fwd()
        } else {
            1
        };
        unsafe { to_typed_field(self.fd, self.header_fmt, data, run_len) }
    }
    fn field_run_length_fwd(&mut self) -> RunLength {
        self.header_rl_total - self.header_rl_offset
    }
    fn field_run_length_bwd(&mut self) -> RunLength {
        self.header_rl_offset
    }
    fn next_header(&mut self) -> RunLength {
        let stride = self.header_rl_total - self.header_rl_offset;
        if stride == 0 {
            return 0;
        }
        self.header_rl_offset = 0;
        self.data += self.fd.header[self.header_idx].data_size();
        self.field_pos += stride as usize;
        loop {
            self.header_idx += 1;
            if self.header_idx == self.fd.header.len() {
                self.header_rl_total = 0;
                return stride;
            }

            let h = self.fd.header[self.header_idx];
            if h.deleted() {
                self.data += h.total_size();
                continue;
            }
            self.data += h.leading_padding();
            self.header_fmt = h.fmt;
            self.header_rl_total = h.run_length;
            return stride;
        }
    }
    fn prev_header(&mut self) -> RunLength {
        let mut data_offset = self.header_fmt.leading_padding();
        loop {
            if self.header_idx == 0 {
                return 0;
            }
            self.header_idx -= 1;
            let h = self.fd.header[self.header_idx];

            data_offset += h.total_size();
            if h.deleted() {
                continue;
            }

            let stride = self.header_rl_offset + 1;
            self.data -= data_offset;
            self.header_fmt = h.fmt;
            self.header_rl_total = h.run_length;
            self.header_rl_offset = self.header_rl_total - 1;
            self.field_pos += stride as usize;
            return stride;
        }
    }
    fn next_field(&mut self) -> RunLength {
        if self.header_rl_offset + 1 < self.header_rl_total {
            self.header_rl_offset += 1;
            self.field_pos += 1;
            return 1;
        }
        return self.next_header();
    }
    fn prev_field(&mut self) -> RunLength {
        if self.header_rl_offset > 0 {
            self.header_rl_offset -= 1;
            self.field_pos -= 1;
            return 1;
        }
        return self.prev_header();
    }
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        let mut stride_rem = n;
        let curr_header_rem = (self.header_rl_total - self.header_rl_offset) as usize;
        if curr_header_rem == 0
            || (self.header_fmt.flags & flag_mask) != flags
            || (!kinds.is_empty() && !kinds.contains(&self.header_fmt.kind))
        {
            return 0;
        }
        if curr_header_rem > stride_rem {
            self.header_rl_offset += stride_rem as RunLength;
            self.field_pos += stride_rem;
            return stride_rem;
        }
        loop {
            if flag_mask & field_value_flags::DELETED != 0
                && self.fd.header.len() != self.header_idx + 1
                && self.fd.header[self.header_idx + 1].deleted()
            {
                stride_rem -= self.next_header() as usize;
                return n - stride_rem;
            }
            stride_rem -= self.next_header() as usize;
            if !self.is_next_valid()
                || (self.header_fmt.flags & flag_mask) != flags
                || (!kinds.is_empty() && !kinds.contains(&self.header_fmt.kind))
            {
                return n - stride_rem;
            }
            if self.header_rl_total as usize > stride_rem {
                self.header_rl_offset = stride_rem as RunLength;
                return n;
            }
        }
    }
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        if n == 0
            || self.prev_field() == 0
            || (self.header_fmt.flags & flag_mask) != flags
            || (!kinds.is_empty() && !kinds.contains(&self.header_fmt.kind))
        {
            return 0;
        }
        let mut stride_rem = n - 1;
        if self.header_rl_offset as usize > stride_rem {
            self.header_rl_offset -= stride_rem as RunLength;
            self.field_pos -= stride_rem;
            return stride_rem;
        }
        loop {
            if flag_mask & field_value_flags::DELETED != 0
                && self.header_idx != 0
                && self.fd.header[self.header_idx - 1].deleted()
            {
                stride_rem -= self.prev_header() as usize;
                return n - stride_rem;
            }
            stride_rem -= self.prev_header() as usize;
            if !self.is_next_valid()
                || (self.header_fmt.flags & flag_mask) != flags
                || (!kinds.is_empty() && !kinds.contains(&self.header_fmt.kind))
            {
                return n - stride_rem;
            }
            if self.header_rl_total as usize > stride_rem {
                self.header_rl_offset -= stride_rem as RunLength;
                return n;
            }
        }
    }
    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>> {
        if limit == 0 || !self.is_next_valid() {
            None
        } else {
            let data = self.get_next_field_data();
            let fmt = self.header_fmt;
            let run_len = if self.header_fmt.shared_value() {
                let rl = self.field_run_length_fwd();
                if rl <= limit {
                    self.next_header();
                    rl
                } else {
                    self.header_rl_offset += limit;
                    self.field_pos += limit as usize;
                    limit
                }
            } else {
                self.next_field();
                1
            };
            Some(unsafe { to_typed_field(self.fd, fmt, data, run_len) })
        }
    }
    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>> {
        if limit == 0 || self.prev_field() == 0 {
            None
        } else {
            let data = self.get_next_field_data();
            let fmt = self.header_fmt;
            let run_len = if self.header_fmt.shared_value() {
                let rl = self.field_run_length_bwd() + 1;
                if rl <= limit {
                    self.prev_header();
                    rl
                } else {
                    self.header_rl_offset -= limit - 1;
                    self.field_pos -= (limit - 1) as usize;
                    limit
                }
            } else {
                1
            };
            Some(unsafe { to_typed_field(self.fd, fmt, data, run_len) })
        }
    }
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        if limit == 0 || !self.is_next_valid() {
            return None;
        }
        let flag_mask = flag_mask | field_value_flags::DELETED;
        let mut data_begin = self.data;
        let fmt = self.header_fmt;
        let oversize_start = self.header_rl_offset;
        if !fmt.shared_value() {
            data_begin += oversize_start as usize * fmt.size as usize;
        }
        let header_start = self.header_idx;
        let field_count =
            self.next_n_fields_with_fmt(limit, [fmt.kind], flag_mask, fmt.flags & flag_mask);
        let mut data_end = self.get_prev_field_data_end();
        let mut oversize_end = 0;
        let mut header_end = self.header_idx;
        if self.is_next_valid() {
            if self.field_run_length_bwd() != 0 {
                header_end += 1;
                oversize_end = self.header_rl_total - self.header_rl_offset;
            } else {
                while header_end > 0 && self.fd.header[header_end - 1].deleted() {
                    header_end -= 1;
                    data_end -= self.fd.header[header_end].data_size();
                }
            }
        }

        unsafe {
            to_typed_range(
                self.fd,
                flag_mask,
                fmt,
                data_begin,
                data_end,
                field_count,
                header_start,
                header_end,
                oversize_start,
                oversize_end,
            )
            .into()
        }
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        if limit == 0 || !self.is_prev_valid() {
            return None;
        }
        let flag_mask = flag_mask | field_value_flags::DELETED;
        let oversize_end = if self.is_next_valid() {
            self.header_rl_total - self.header_rl_offset - 1
        } else {
            0
        };
        self.prev_field();
        let fmt = self.header_fmt;
        let data_end = self.get_next_field_data() + fmt.size as usize;
        let header_end = self.header_idx + 1;
        let field_count =
            self.prev_n_fields_with_fmt(limit - 1, [fmt.kind], flag_mask, fmt.flags & flag_mask)
                + 1;
        let header_start = self.header_idx;
        let data_start = self.get_next_field_data();
        unsafe {
            to_typed_range(
                self.fd,
                flag_mask,
                fmt,
                data_start,
                data_end,
                field_count,
                header_start,
                header_end,
                self.header_rl_offset,
                oversize_end,
            )
        }
        .into()
    }

    fn as_base_iter(self) -> FDIter<'a> {
        self
    }
}

#[derive(Clone)]
pub struct BoundedFDIter<'a, I>
where
    I: FDIterator<'a>,
{
    pub(super) iter: I,
    pub(super) min: usize,
    pub(super) max: usize,
    _phantom_data: PhantomData<&'a FieldData>,
}
impl<'a, I> BoundedFDIter<'a, I>
where
    I: FDIterator<'a>,
{
    pub fn new(
        iter: I,
        min: usize, // inclusive
        max: usize, // exclusive
    ) -> Self {
        let pos = iter.get_next_field_pos();
        assert!(pos >= min && pos < max);
        Self {
            iter,
            min,
            max,
            _phantom_data: PhantomData::default(),
        }
    }
    pub fn new_relative(
        iter: I,
        backwards: usize, // inclusive
        forward: usize,   // inclusive
    ) -> Self {
        let pos = iter.get_next_field_pos();
        Self {
            iter,
            min: pos.saturating_sub(backwards),
            max: pos.saturating_add(forward).saturating_add(1),
            _phantom_data: PhantomData::default(),
        }
    }
    pub fn range_fwd(&self) -> usize {
        self.max - self.get_next_field_pos() - 1
    }
    pub fn range_bwd(&self) -> usize {
        self.get_next_field_pos() - self.min
    }
}
impl<'a, I> FDIterator<'a> for BoundedFDIter<'a, I>
where
    I: FDIterator<'a>,
{
    fn get_next_field_pos(&self) -> usize {
        self.iter.get_next_field_pos()
    }
    fn is_next_valid(&self) -> bool {
        if self.get_next_field_pos() == self.max {
            return false;
        }
        self.iter.is_next_valid()
    }
    fn is_prev_valid(&self) -> bool {
        if self.get_next_field_pos() == self.min {
            return false;
        }
        self.iter.is_prev_valid()
    }
    fn get_next_field_format(&self) -> FieldValueFormat {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_format()
    }
    fn get_next_field_data(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_data()
    }
    fn get_prev_field_data_end(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.iter.get_prev_field_data_end()
    }
    fn get_next_header(&self) -> FieldValueHeader {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_header()
    }
    fn get_next_header_data(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_header_data()
    }
    fn get_next_header_index(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_header_index()
    }
    fn get_prev_header_index(&self) -> usize {
        debug_assert!(self.is_prev_valid());
        self.iter.get_prev_header_index()
    }
    fn get_next_typed_field(&mut self) -> FDTypedField<'a> {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_typed_field()
    }
    fn field_run_length_fwd(&mut self) -> RunLength {
        self.range_fwd()
            .min(self.iter.field_run_length_fwd() as usize) as RunLength
    }
    fn field_run_length_bwd(&mut self) -> RunLength {
        self.range_bwd()
            .min(self.iter.field_run_length_bwd() as usize) as RunLength
    }
    fn next_header(&mut self) -> RunLength {
        let range = self.range_fwd();
        let rl_rem = self.iter.field_run_length_fwd() as usize;
        if range < rl_rem {
            self.iter.next_n_fields(range) as RunLength
        } else {
            self.iter.next_header()
        }
    }
    fn prev_header(&mut self) -> RunLength {
        let range = self.range_fwd();
        let rl_rem = self.iter.field_run_length_bwd() as usize;
        if range < rl_rem {
            self.iter.prev_n_fields(range) as RunLength
        } else {
            self.iter.prev_header()
        }
    }
    fn next_field(&mut self) -> RunLength {
        if self.get_next_field_pos() == self.max {
            0
        } else {
            let stride = self.iter.next_field();
            stride
        }
    }
    fn prev_field(&mut self) -> RunLength {
        if self.get_next_field_pos() == self.min {
            0
        } else {
            let stride = self.iter.prev_field();
            stride
        }
    }
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        let n = n.min(self.range_fwd());
        let stride = self.iter.next_n_fields_with_fmt(n, kinds, flag_mask, flags);
        stride
    }
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        let n = n.min(self.range_bwd());
        let stride = self.iter.prev_n_fields_with_fmt(n, kinds, flag_mask, flags);
        stride
    }
    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>> {
        self.iter
            .typed_field_fwd((limit as usize).min(self.range_fwd()) as RunLength)
    }
    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>> {
        self.iter
            .typed_field_bwd((limit as usize).min(self.range_bwd()) as RunLength)
    }
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        self.iter
            .typed_range_fwd(limit.min(self.range_fwd()), flag_mask)
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        self.iter
            .typed_range_bwd(limit.min(self.range_bwd()), flag_mask)
    }

    fn as_base_iter(self) -> FDIter<'a> {
        self.iter.as_base_iter()
    }
}

impl<'a> FDIterMut<'a> {
    pub fn from_start(fd: &'a mut FieldData, initial_field_offset: usize) -> Self {
        let first_header = fd.header.first().cloned();
        Self {
            fd,
            field_pos: initial_field_offset,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
            header_rl_total: first_header.map_or(0, |h| h.run_length),
            header_fmt: first_header.map(|h| h.fmt).unwrap_or_default(),
        }
    }
    pub fn from_end(fd: &'a mut FieldData, initial_field_offset: usize) -> Self {
        let header_len = fd.header.len();
        let data_len = fd.data.len();
        let field_count = fd.field_count;
        Self {
            fd,
            field_pos: field_count + initial_field_offset,
            data: data_len,
            header_idx: header_len,
            header_rl_offset: 0,
            header_rl_total: 0,
            header_fmt: Default::default(),
        }
    }
    pub fn into_fd_iter(self) -> FDIter<'a> {
        unsafe { std::mem::transmute(self) }
    }
    pub fn as_fd_iter(&self) -> &FDIter<'a> {
        unsafe { std::mem::transmute(self) }
    }
    pub fn as_fd_iter_mut(&mut self) -> &mut FDIter<'a> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a> FDIterator<'a> for FDIterMut<'a> {
    fn get_next_field_pos(&self) -> usize {
        self.as_fd_iter().get_next_field_pos()
    }
    fn is_next_valid(&self) -> bool {
        self.as_fd_iter().is_next_valid()
    }
    fn is_prev_valid(&self) -> bool {
        self.as_fd_iter().is_prev_valid()
    }

    fn get_next_field_format(&self) -> FieldValueFormat {
        self.as_fd_iter().get_next_field_format()
    }

    fn get_next_field_data(&self) -> usize {
        self.as_fd_iter().get_next_field_data()
    }
    fn get_prev_field_data_end(&self) -> usize {
        self.as_fd_iter().get_prev_field_data_end()
    }
    fn get_next_header(&self) -> FieldValueHeader {
        self.as_fd_iter().get_next_header()
    }

    fn get_next_header_data(&self) -> usize {
        self.as_fd_iter().get_next_header_data()
    }

    fn get_next_header_index(&self) -> usize {
        self.as_fd_iter().get_next_header_index()
    }
    fn get_prev_header_index(&self) -> usize {
        self.as_fd_iter().get_prev_header_index()
    }

    fn get_next_typed_field(&mut self) -> FDTypedField<'a> {
        self.as_fd_iter_mut().get_next_typed_field()
    }

    fn field_run_length_fwd(&mut self) -> RunLength {
        self.as_fd_iter_mut().field_run_length_fwd()
    }

    fn field_run_length_bwd(&mut self) -> RunLength {
        self.as_fd_iter_mut().field_run_length_bwd()
    }

    fn next_header(&mut self) -> RunLength {
        self.as_fd_iter_mut().next_header()
    }

    fn prev_header(&mut self) -> RunLength {
        self.as_fd_iter_mut().prev_header()
    }

    fn next_field(&mut self) -> RunLength {
        self.as_fd_iter_mut().next_field()
    }

    fn prev_field(&mut self) -> RunLength {
        self.as_fd_iter_mut().prev_field()
    }

    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        self.as_fd_iter_mut()
            .next_n_fields_with_fmt(n, kinds, flag_mask, flags)
    }

    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        self.as_fd_iter_mut()
            .prev_n_fields_with_fmt(n, kinds, flag_mask, flags)
    }

    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>> {
        self.as_fd_iter_mut().typed_field_fwd(limit)
    }

    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<FDTypedField<'a>> {
        self.as_fd_iter_mut().typed_field_bwd(limit)
    }

    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        self.as_fd_iter_mut().typed_range_fwd(limit, flag_mask)
    }

    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        self.as_fd_iter_mut().typed_range_bwd(limit, flag_mask)
    }

    fn as_base_iter(self) -> FDIter<'a> {
        self.into_fd_iter()
    }
}
