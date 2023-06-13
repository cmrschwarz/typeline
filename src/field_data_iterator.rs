use std::{marker::PhantomData, ptr::NonNull};

use crate::{
    field_data::{
        field_value_flags, FieldData, FieldReference, FieldValueFlags, FieldValueFormat,
        FieldValueHeader, FieldValueKind, Html, Object, RunLength,
    },
    operations::OperatorApplicationError,
};

pub enum FDTypedSlice<'a> {
    Unset(&'a [()]),
    Null(&'a [()]),
    Integer(&'a [i64]),
    Reference(&'a [FieldReference]),
    Error(&'a [OperatorApplicationError]),
    Html(&'a [Html]),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    Object(&'a [Object]),
}

pub enum FDTypedValue<'a> {
    Unset(()),
    Null(()),
    Integer(i64),
    Reference(&'a FieldReference),
    Error(&'a OperatorApplicationError),
    Html(&'a Html),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    Object(&'a Object),
}

pub struct FDTypedRange<'a> {
    pub headers: &'a [FieldValueHeader],
    pub data: FDTypedSlice<'a>,
    pub field_count: usize,
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
        _ => todo!(),
    };
    FDTypedRange {
        headers,
        data,
        field_count,
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
                    fmt.size as usize - data_begin,
                )))
            } else {
                FDTypedValue::BytesInline(to_slice(fd, data_begin, fmt.size as usize - data_begin))
            }
        }
        FieldValueKind::EntryId => todo!(),
        FieldValueKind::Integer => FDTypedValue::Integer(*to_ref(fd, data_begin)),
        FieldValueKind::Reference => FDTypedValue::Reference(to_ref(fd, data_begin)),
        FieldValueKind::Error => FDTypedValue::Error(to_ref(fd, data_begin)),
        FieldValueKind::Html => FDTypedValue::Html(to_ref(fd, data_begin)),
        FieldValueKind::Object => FDTypedValue::Object(to_ref(fd, data_begin)),
        _ => todo!(),
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
    fn is_next_valid(&self) -> bool;
    fn get_next_field_format(&self) -> FieldValueFormat;
    fn get_next_field_data(&self) -> usize;
    // if the cursor is in the middle of a header, *that* header will be
    // returned, not the one after it
    fn get_next_header(&self) -> FieldValueHeader;
    fn get_next_header_data(&self) -> usize;
    fn get_next_header_index(&self) -> usize;
    fn get_next_typed_field(&mut self) -> FDTypedField<'a>;
    fn field_run_length_fwd(&mut self) -> RunLength;
    fn field_run_length_bwd(&mut self) -> RunLength;
    fn next_header(&mut self) -> RunLength;
    fn prev_header(&mut self) -> RunLength;
    fn next_field(&mut self) -> RunLength;
    fn prev_field(&mut self) -> RunLength;
    fn next_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize;
    fn prev_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize;
    fn next_n_fields(&mut self, n: usize) -> usize {
        self.next_n_fields_with_fmt(n, None, 0, 0)
    }
    fn prev_n_fields(&mut self, n: usize) -> usize {
        self.prev_n_fields_with_fmt(n, None, 0, 0)
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
    fn bounded(self, before: usize, after: usize) -> BoundedFDIter<'a, Self> {
        BoundedFDIter::new(self, 0, before, before + after + 1)
    }
}

pub struct FDIter<'a> {
    fd: &'a FieldData,
    data: usize,
    header_idx: usize,
    header_rl_offset: RunLength,
    header_rl_total: RunLength,
    header_fmt: FieldValueFormat,
}
impl<'a> FDIter<'a> {
    pub fn from_start(fd: &'a FieldData) -> Self {
        Self {
            fd,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
            header_rl_total: fd.header.first().map_or(0, |h| h.run_length),
            header_fmt: Default::default(),
        }
    }
    pub fn from_end(fd: &'a FieldData) -> Self {
        Self {
            fd,
            data: fd.data.len(),
            header_idx: fd.header.len(),
            header_rl_offset: 0,
            header_rl_total: 0,
            header_fmt: Default::default(),
        }
    }
}
impl<'a> FDIterator<'a> for FDIter<'a> {
    fn is_next_valid(&self) -> bool {
        self.header_rl_total != 0
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
        self.header_idx
    }
    fn get_next_typed_field(&mut self) -> FDTypedField<'a> {
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
        self.data += self.fd.header[self.header_idx].total_size();
        self.header_rl_offset = 0;
        self.header_idx += 1;
        if self.header_idx == self.fd.header.len() {
            self.header_rl_total = 0;
            return stride;
        }
        let h = self.fd.header[self.header_idx];
        self.header_fmt = h.fmt;
        self.header_rl_total = h.run_length;
        return stride;
    }
    fn prev_header(&mut self) -> RunLength {
        if self.header_idx == 0 {
            return 0;
        }
        self.header_idx -= 1;
        let stride = self.header_rl_offset + 1;

        let h = self.fd.header[self.header_idx];

        self.data -= h.total_size();

        self.header_fmt = h.fmt;
        self.header_rl_total = h.run_length;
        self.header_rl_offset = self.header_rl_total - 1;
        return stride;
    }
    fn next_field(&mut self) -> RunLength {
        if self.header_rl_offset + 1 < self.header_rl_total {
            self.header_rl_offset += 1;
            return 1;
        }
        return self.next_header();
    }
    fn prev_field(&mut self) -> RunLength {
        if self.header_rl_offset > 0 {
            self.header_rl_offset -= 1;
            return 1;
        }
        return self.prev_header();
    }
    fn next_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        let mut stride_rem = n;
        let curr_header_rem = (self.header_rl_total - self.header_rl_offset) as usize;
        if curr_header_rem == 0 || (self.header_fmt.flags & flag_mask) != flags {
            return 0;
        }
        if curr_header_rem > stride_rem {
            self.header_rl_offset += stride_rem as RunLength;
            return stride_rem;
        }
        loop {
            stride_rem -= self.next_header() as usize;
            if !self.is_next_valid()
                || (self.header_fmt.flags & flag_mask) != flags
                || kind.map_or(false, |k| k != self.header_fmt.kind)
            {
                return n - stride_rem;
            }
            if self.header_rl_total as usize > stride_rem {
                self.header_rl_offset = stride_rem as RunLength;
                return n;
            }
        }
    }
    fn prev_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        if n == 0 || self.prev_field() == 0 || (self.header_fmt.flags & flag_mask) != flags {
            return 0;
        }
        let mut stride_rem = n - 1;
        if self.header_rl_offset as usize > stride_rem {
            self.header_rl_offset -= stride_rem as RunLength;
            return stride_rem;
        }
        loop {
            stride_rem -= self.prev_header() as usize;
            if !self.is_next_valid()
                || (self.header_fmt.flags & flag_mask) != flags
                || kind.map_or(false, |k| k != self.header_fmt.kind)
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
                if rl >= limit {
                    self.next_header();
                    rl
                } else {
                    self.header_rl_offset += limit;
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
                if rl >= limit {
                    self.prev_header();
                    rl
                } else {
                    self.header_rl_offset -= limit - 1;
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
        let data_begin = self.data;
        let fmt = self.header_fmt;
        let header_start = self.header_idx;
        let field_count =
            self.next_n_fields_with_fmt(limit, Some(fmt.kind), flag_mask, fmt.flags & flag_mask);
        let mut data_end = if self.is_next_valid() {
            self.get_next_field_data()
        } else {
            self.data
        };
        if self.header_rl_offset == 0 && self.header_idx > 0 {
            data_end -= self.fd.header[self.header_idx - 1].alignment_after();
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
                self.header_idx,
            )
            .into()
        }
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        if limit == 0 || self.prev_field() == 0 {
            return None;
        };
        let fmt = self.header_fmt;
        let mut data_end = self.get_next_field_data();
        data_end += fmt.size as usize;
        let header_end = self.header_idx + 1;
        let field_count = self.prev_n_fields_with_fmt(
            limit - 1,
            Some(fmt.kind),
            flag_mask,
            fmt.flags & flag_mask,
        ) + 1;
        let header_start = self.header_idx;
        unsafe {
            to_typed_range(
                self.fd,
                flag_mask,
                fmt,
                self.data,
                data_end,
                field_count,
                header_start,
                header_end,
            )
        }
        .into()
    }
}

pub struct BoundedFDIter<'a, I>
where
    I: FDIterator<'a>,
{
    iter: I,
    pos: usize,
    min: usize,
    max: usize,
    _phantom_data: PhantomData<&'a FieldData>,
}
impl<'a, I> BoundedFDIter<'a, I>
where
    I: FDIterator<'a>,
{
    pub fn new(
        iter: I,
        min: usize, // inclusive
        pos: usize,
        max: usize, // exclusive
    ) -> Self {
        assert!(pos >= min && pos < max);
        Self {
            iter,
            pos,
            min,
            max,
            _phantom_data: PhantomData::default(),
        }
    }
    pub fn range_fwd(&self) -> usize {
        self.max - self.pos
    }
    pub fn range_bwd(&self) -> usize {
        self.pos - self.min
    }
}
impl<'a, I> FDIterator<'a> for BoundedFDIter<'a, I>
where
    I: FDIterator<'a>,
{
    fn is_next_valid(&self) -> bool {
        if self.pos == self.max {
            return false;
        }
        self.iter.is_next_valid()
    }
    fn get_next_field_format(&self) -> FieldValueFormat {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_format()
    }
    fn get_next_field_data(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_data()
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
            self.pos = self.max;
            self.iter.next_n_fields(range) as RunLength
        } else {
            self.pos += rl_rem as usize;
            self.iter.next_header()
        }
    }
    fn prev_header(&mut self) -> RunLength {
        let range = self.range_fwd();
        let rl_rem = self.iter.field_run_length_bwd() as usize;
        if range < rl_rem {
            self.pos = self.min;
            self.iter.prev_n_fields(range) as RunLength
        } else {
            self.pos += rl_rem as usize;
            self.iter.prev_header()
        }
    }
    fn next_field(&mut self) -> RunLength {
        if self.pos == self.max {
            0
        } else {
            self.iter.next_field()
        }
    }
    fn prev_field(&mut self) -> RunLength {
        if self.pos == self.min {
            0
        } else {
            self.iter.prev_field()
        }
    }
    fn next_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        let n = n.min(self.range_fwd());
        self.iter.next_n_fields_with_fmt(n, kind, flag_mask, flags)
    }
    fn prev_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        let n = n.min(self.range_bwd());
        self.iter.prev_n_fields_with_fmt(n, kind, flag_mask, flags)
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
        let limit = limit.min(self.range_fwd());
        self.iter.typed_range_fwd(limit, flag_mask)
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<FDTypedRange<'a>> {
        let limit = limit.min(self.range_bwd());
        self.iter.typed_range_bwd(limit, flag_mask)
    }
}
