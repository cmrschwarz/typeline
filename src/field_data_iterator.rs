use std::{marker::PhantomData, mem::transmute, os::fd, slice::from_raw_parts};

use crate::{
    field_data::{
        FieldData, FieldReference, FieldValueFlags, FieldValueFormat, FieldValueHeader,
        FieldValueKind, Html, Object, RunLength,
    },
    operations::OperatorApplicationError,
};

use FieldValueFlags::Type as Flags;

pub enum FDTypedData<'a> {
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

pub struct FDTypedRange<'a> {
    pub headers: &'a [FieldValueHeader],
    pub data: FDTypedData<'a>,
    pub field_count: usize,
}
impl<'a> Default for FDTypedRange<'a> {
    fn default() -> Self {
        Self {
            headers: &[],
            data: FDTypedData::Unset(&[]),
            field_count: 0,
        }
    }
}

unsafe fn to_slice<T: Sized>(fd: &FieldData, data_begin: usize, len: usize) -> &[T] {
    std::slice::from_raw_parts(
        std::mem::transmute::<&u8, &T>(&fd.data[data_begin]) as *const T,
        len,
    )
}

unsafe fn to_typed_range<'a>(
    fd: &'a FieldData,
    flag_mask: Flags,
    fmt: FieldValueFormat,
    data_begin: usize,
    data_end: usize,
    field_count: usize,
    header_begin: usize,
    header_end: usize,
) -> Option<FDTypedRange<'a>> {
    let headers = &fd.header[header_begin..header_end];
    let data = match fmt.kind {
        FieldValueKind::Unset => FDTypedData::Unset(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Null => FDTypedData::Null(to_slice(fd, data_begin, field_count)),
        FieldValueKind::BytesInline => {
            if fmt.flags & flag_mask & FieldValueFlags::BYTES_ARE_UTF8 != 0 {
                FDTypedData::TextInline(std::str::from_utf8_unchecked(to_slice(
                    fd,
                    data_begin,
                    data_end - data_begin,
                )))
            } else {
                FDTypedData::BytesInline(to_slice(fd, data_begin, data_end - data_begin))
            }
        }
        FieldValueKind::EntryId => todo!(),
        FieldValueKind::Integer => FDTypedData::Integer(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Reference => FDTypedData::Reference(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Error => FDTypedData::Error(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Html => FDTypedData::Html(to_slice(fd, data_begin, field_count)),
        FieldValueKind::Object => FDTypedData::Object(to_slice(fd, data_begin, field_count)),
        _ => todo!(),
    };
    Some(FDTypedRange {
        headers,
        data,
        field_count,
    })
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
        flag_mask: Flags,
        flags: Flags,
    ) -> usize;
    fn prev_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: Flags,
        flags: Flags,
    ) -> usize;
    fn next_n_fields(&mut self, n: usize) -> usize {
        self.next_n_fields_with_fmt(n, None, 0, 0)
    }
    fn prev_n_fields(&mut self, n: usize) -> usize {
        self.prev_n_fields_with_fmt(n, None, 0, 0)
    }
    fn consume_typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: Flags,
    ) -> Option<FDTypedRange<'a>>;
    fn consume_typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: Flags,
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
    fn field_run_length_fwd(&mut self) -> RunLength {
        self.header_rl_total - self.header_rl_offset
    }
    fn field_run_length_bwd(&mut self) -> RunLength {
        self.header_rl_offset
    }
    fn next_header(&mut self) -> RunLength {
        let idx = self.header_idx.wrapping_add(1);
        if idx >= self.fd.header.len() {
            self.header_idx = self.fd.header.len();
            self.header_rl_offset = 0;
            self.header_rl_total = 0;
            return 0;
        }
        self.header_idx = idx;
        let h = self.fd.header[idx];
        let stride = self.header_rl_total - self.header_rl_offset;
        self.data += h.total_size();
        self.header_fmt = h.fmt;
        self.header_idx += 1;
        self.header_rl_offset = 0;
        self.header_rl_total = h.run_length;
        return stride;
    }
    fn prev_header(&mut self) -> RunLength {
        if self.header_idx == 0 || self.header_idx == usize::MAX {
            self.header_idx = usize::MAX;
            self.header_rl_offset = 0;
            self.header_rl_total = 0;
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
        flag_mask: Flags,
        flags: Flags,
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
        flag_mask: Flags,
        flags: Flags,
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
    fn consume_typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: Flags,
    ) -> Option<FDTypedRange<'a>> {
        if limit == 0 || !self.is_next_valid() {
            return None;
        }
        let data_begin = self.data;
        let fmt = self.header_fmt;
        let header_start = self.header_idx;
        let field_count =
            self.next_n_fields_with_fmt(limit, Some(fmt.kind), flag_mask, fmt.flags & flag_mask);
        let mut data_end = self.get_next_field_data();
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
        }
    }
    fn consume_typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: Flags,
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
        self.iter.get_next_header_index()
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
        flag_mask: Flags,
        flags: Flags,
    ) -> usize {
        let n = n.min(self.range_fwd());
        self.iter.next_n_fields_with_fmt(n, kind, flag_mask, flags)
    }
    fn prev_n_fields_with_fmt(
        &mut self,
        n: usize,
        kind: Option<FieldValueKind>,
        flag_mask: Flags,
        flags: Flags,
    ) -> usize {
        let n = n.min(self.range_bwd());
        self.iter.prev_n_fields_with_fmt(n, kind, flag_mask, flags)
    }
    fn consume_typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: Flags,
    ) -> Option<FDTypedRange<'a>> {
        let limit = limit.min(self.range_fwd());
        self.iter.consume_typed_range_fwd(limit, flag_mask)
    }
    fn consume_typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: Flags,
    ) -> Option<FDTypedRange<'a>> {
        let limit = limit.min(self.range_bwd());
        self.iter.consume_typed_range_bwd(limit, flag_mask)
    }
}
