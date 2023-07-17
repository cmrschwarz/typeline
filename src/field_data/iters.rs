use std::marker::PhantomData;

use crate::field_data::{
    field_value_flags, FieldData, FieldValueFlags, FieldValueFormat, FieldValueHeader,
    FieldValueKind, RunLength,
};

use super::typed::{TypedField, TypedRange, TypedSlice, ValidTypedRange};

impl<'a> Default for TypedRange<'a> {
    fn default() -> Self {
        Self {
            headers: &[],
            data: TypedSlice::Unset(&[]),
            field_count: 0,
            first_header_run_length_oversize: 0,
            last_header_run_length_oversize: 0,
        }
    }
}

pub trait FieldIterator<'a>: Sized {
    fn field_data_ref(&self) -> &'a FieldData;
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
    fn get_next_header_ref(&self) -> &'a FieldValueHeader {
        &self.field_data_ref().header[self.get_next_header_index()]
    }
    fn get_next_header_index(&self) -> usize;
    fn get_prev_header_index(&self) -> usize;
    fn get_next_typed_field(&mut self) -> TypedField<'a>;
    fn field_run_length_fwd(&self) -> RunLength;
    fn field_run_length_bwd(&self) -> RunLength;
    fn field_run_length_fwd_oversize(&self) -> RunLength {
        if self.field_run_length_bwd() != 0 {
            return self.field_run_length_fwd();
        }
        0
    }
    fn next_header(&mut self) -> RunLength;
    fn prev_header(&mut self) -> RunLength;
    fn next_field(&mut self) -> RunLength;
    fn prev_field(&mut self) -> RunLength;
    fn next_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize;
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        self.next_n_fields_with_fmt_and_data_check(
            n,
            kinds,
            invert_kinds_check,
            flag_mask,
            flags,
            |_, _| true,
        )
    }
    fn prev_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize;
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
    ) -> usize {
        self.prev_n_fields_with_fmt_and_data_check(
            n,
            kinds,
            invert_kinds_check,
            flag_mask,
            flags,
            |_, _| true,
        )
    }
    fn move_to_field_pos(&mut self, field_pos: usize) {
        let curr = self.get_next_field_pos();
        if curr > field_pos {
            self.prev_n_fields(curr - field_pos);
        } else if curr < field_pos {
            self.next_n_fields(field_pos - curr);
        }
    }
    fn next_n_fields(&mut self, n: usize) -> usize {
        self.next_n_fields_with_fmt(n, [], true, 0, 0)
    }
    fn prev_n_fields(&mut self, n: usize) -> usize {
        self.prev_n_fields_with_fmt(n, [], true, 0, 0)
    }
    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<TypedField<'a>>;
    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<TypedField<'a>>;
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>>;
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>>;
    fn bounded(self, backwards: usize, forwards: usize) -> BoundedIter<'a, Self> {
        BoundedIter::new_relative(self, backwards, forwards)
    }
    fn into_base_iter(self) -> Iter<'a>;
}

#[repr(C)]
#[derive(Clone)]
pub struct Iter<'a> {
    pub(super) fd: &'a FieldData,
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    pub(super) header_rl_total: RunLength,
    pub(super) header_fmt: FieldValueFormat,
}

#[repr(C)]
pub struct IterMut<'a> {
    pub(super) fd: &'a mut FieldData,
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    pub(super) header_rl_total: RunLength,
    pub(super) header_fmt: FieldValueFormat,
}

impl<'a> Iter<'a> {
    pub fn from_start(fd: &'a FieldData, initial_field_offset: usize) -> Self {
        let first_header = fd.header.first();
        let mut res = Self {
            fd,
            field_pos: initial_field_offset,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
            header_rl_total: first_header.map_or(0, |h| h.run_length),
            header_fmt: first_header.map(|h| h.fmt).unwrap_or_default(),
        };
        res.skip_dead_fields();
        res
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
    pub(super) fn skip_dead_fields(&mut self) {
        while self.header_fmt.deleted() && self.is_next_valid() {
            self.next_header();
        }
    }
}
impl<'a> FieldIterator<'a> for Iter<'a> {
    fn field_data_ref(&self) -> &'a FieldData {
        self.fd
    }
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
        if self.header_fmt.shared_value() {
            self.data
        } else {
            self.data + self.header_rl_offset as usize * self.header_fmt.size as usize
        }
    }
    fn get_prev_field_data_end(&self) -> usize {
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
    fn get_next_typed_field(&mut self) -> TypedField<'a> {
        // SAFETY: debug assert is not enough here because we use unsafe below
        assert!(self.is_next_valid());
        let data = self.get_next_field_data();
        let run_len = if self.header_fmt.shared_value() {
            self.field_run_length_fwd()
        } else {
            1
        };
        unsafe { TypedField::new(self.fd, self.header_fmt, data, run_len) }
    }
    fn field_run_length_fwd(&self) -> RunLength {
        self.header_rl_total - self.header_rl_offset
    }
    fn field_run_length_bwd(&self) -> RunLength {
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
                // to make sure there's no padding
                self.header_fmt = Default::default();
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
        let mut i = self.header_idx;
        loop {
            if i == 0 {
                return 0;
            }
            i -= 1;
            let h = self.fd.header[i];

            data_offset += h.total_size();
            if h.deleted() {
                if i == 0 {
                    return 0;
                }
                continue;
            }

            let stride = self.header_rl_offset + 1;
            self.header_idx = i;
            self.data -= data_offset;
            self.header_fmt = h.fmt;
            self.header_rl_total = h.run_length;
            self.header_rl_offset = self.header_rl_total - 1;
            self.field_pos -= stride as usize;
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
    fn next_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        mut flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize {
        flags &= flag_mask;
        let mut stride_rem = n;
        let curr_header_rem = (self.header_rl_total - self.header_rl_offset) as usize;
        if curr_header_rem == 0
            || (self.header_fmt.flags & flag_mask) != flags
            || (kinds.contains(&self.header_fmt.kind) == invert_kinds_check)
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
                || (kinds.contains(&self.header_fmt.kind) == invert_kinds_check)
                || !data_check(&self.header_fmt, unsafe {
                    self.fd.data.as_ptr().add(self.get_next_field_data())
                })
            {
                return n - stride_rem;
            }
            if self.header_rl_total as usize > stride_rem {
                self.field_pos += stride_rem;
                self.header_rl_offset += stride_rem as RunLength;
                return n;
            }
        }
    }
    fn prev_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        mut flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize {
        flags &= flag_mask;
        if n == 0
            || self.prev_field() == 0
            || (self.header_fmt.flags & flag_mask) != flags
            || (kinds.contains(&self.header_fmt.kind) == invert_kinds_check)
        {
            return 0;
        }
        let mut stride_rem = n - 1;
        if stride_rem == 0 {
            return 1;
        }
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
            if !self.is_prev_valid()
                || (self.header_fmt.flags & flag_mask) != flags
                || (kinds.contains(&self.header_fmt.kind) == invert_kinds_check)
                || !data_check(&self.header_fmt, unsafe {
                    self.fd.data.as_ptr().add(self.get_next_field_data())
                })
            {
                return n - stride_rem;
            }
            if self.header_rl_total as usize > stride_rem {
                self.header_rl_offset -= stride_rem as RunLength;
                self.field_pos -= stride_rem;
                return n;
            }
        }
    }
    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<TypedField<'a>> {
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
            Some(unsafe { TypedField::new(self.fd, fmt, data, run_len) })
        }
    }
    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<TypedField<'a>> {
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
            Some(unsafe { TypedField::new(self.fd, fmt, data, run_len) })
        }
    }
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>> {
        if limit == 0 || !self.is_next_valid() {
            return None;
        }
        let mut data_begin = self.data;
        let fmt = self.header_fmt;
        let oversize_start = self.header_rl_offset;
        if !fmt.shared_value() {
            data_begin += oversize_start as usize * fmt.size as usize;
        }
        let header_start = self.header_idx;
        let field_count =
            self.next_n_fields_with_fmt(limit, [fmt.kind], false, flag_mask, fmt.flags & flag_mask);
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
            ValidTypedRange(TypedRange::new(
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
            ))
            .into()
        }
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>> {
        if limit == 0 || !self.is_prev_valid() {
            return None;
        }
        let oversize_end = if self.is_next_valid() {
            self.header_rl_total - self.header_rl_offset - 1
        } else {
            0
        };
        self.prev_field();
        let fmt = self.header_fmt;
        let data_end = self.get_next_field_data() + fmt.size as usize;
        let header_end = self.header_idx + 1;
        let field_count = self.prev_n_fields_with_fmt(
            limit - 1,
            [fmt.kind],
            false,
            flag_mask,
            fmt.flags & flag_mask,
        ) + 1;
        let header_start = self.header_idx;
        let data_start = self.get_next_field_data();
        unsafe {
            ValidTypedRange(TypedRange::new(
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
            ))
        }
        .into()
    }

    fn into_base_iter(self) -> Iter<'a> {
        self
    }
}

#[derive(Clone)]
pub struct BoundedIter<'a, I>
where
    I: FieldIterator<'a>,
{
    pub(super) iter: I,
    pub(super) min: usize,
    pub(super) max: usize,
    _phantom_data: PhantomData<&'a FieldData>,
}
impl<'a, I> BoundedIter<'a, I>
where
    I: FieldIterator<'a>,
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
impl<'a, I> FieldIterator<'a> for BoundedIter<'a, I>
where
    I: FieldIterator<'a>,
{
    fn field_data_ref(&self) -> &'a FieldData {
        self.iter.field_data_ref()
    }
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
        self.iter.get_next_field_data()
    }
    fn get_prev_field_data_end(&self) -> usize {
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
        self.iter.get_next_header_index()
    }
    fn get_prev_header_index(&self) -> usize {
        debug_assert!(self.is_prev_valid());
        self.iter.get_prev_header_index()
    }
    fn get_next_typed_field(&mut self) -> TypedField<'a> {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_typed_field()
    }
    fn field_run_length_fwd(&self) -> RunLength {
        self.range_fwd()
            .min(self.iter.field_run_length_fwd() as usize) as RunLength
    }
    fn field_run_length_bwd(&self) -> RunLength {
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
    fn next_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize {
        let n = n.min(self.range_fwd());
        let stride = self.iter.next_n_fields_with_fmt_and_data_check(
            n,
            kinds,
            invert_kinds_check,
            flag_mask,
            flags,
            data_check,
        );
        stride
    }
    fn prev_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize {
        let n = n.min(self.range_bwd());
        let stride = self.iter.prev_n_fields_with_fmt_and_data_check(
            n,
            kinds,
            invert_kinds_check,
            flag_mask,
            flags,
            data_check,
        );
        stride
    }
    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<TypedField<'a>> {
        self.iter
            .typed_field_fwd((limit as usize).min(self.range_fwd()) as RunLength)
    }
    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<TypedField<'a>> {
        self.iter
            .typed_field_bwd((limit as usize).min(self.range_bwd()) as RunLength)
    }
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>> {
        self.iter
            .typed_range_fwd(limit.min(self.range_fwd()), flag_mask)
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>> {
        self.iter
            .typed_range_bwd(limit.min(self.range_bwd()), flag_mask)
    }

    fn into_base_iter(self) -> Iter<'a> {
        self.iter.into_base_iter()
    }
}

impl<'a> IterMut<'a> {
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
    pub fn into_base_iter(self) -> Iter<'a> {
        unsafe { std::mem::transmute(self) }
    }
    pub fn as_base_iter(&self) -> &Iter<'a> {
        unsafe { std::mem::transmute(self) }
    }
    pub fn as_base_iter_mut(&mut self) -> &mut Iter<'a> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a> FieldIterator<'a> for IterMut<'a> {
    fn field_data_ref(&self) -> &'a FieldData {
        self.as_base_iter().field_data_ref()
    }
    fn get_next_field_pos(&self) -> usize {
        self.as_base_iter().get_next_field_pos()
    }
    fn is_next_valid(&self) -> bool {
        self.as_base_iter().is_next_valid()
    }
    fn is_prev_valid(&self) -> bool {
        self.as_base_iter().is_prev_valid()
    }

    fn get_next_field_format(&self) -> FieldValueFormat {
        self.as_base_iter().get_next_field_format()
    }

    fn get_next_field_data(&self) -> usize {
        self.as_base_iter().get_next_field_data()
    }
    fn get_prev_field_data_end(&self) -> usize {
        self.as_base_iter().get_prev_field_data_end()
    }
    fn get_next_header(&self) -> FieldValueHeader {
        self.as_base_iter().get_next_header()
    }

    fn get_next_header_data(&self) -> usize {
        self.as_base_iter().get_next_header_data()
    }

    fn get_next_header_index(&self) -> usize {
        self.as_base_iter().get_next_header_index()
    }
    fn get_prev_header_index(&self) -> usize {
        self.as_base_iter().get_prev_header_index()
    }

    fn get_next_typed_field(&mut self) -> TypedField<'a> {
        self.as_base_iter_mut().get_next_typed_field()
    }

    fn field_run_length_fwd(&self) -> RunLength {
        self.as_base_iter().field_run_length_fwd()
    }

    fn field_run_length_bwd(&self) -> RunLength {
        self.as_base_iter().field_run_length_bwd()
    }

    fn next_header(&mut self) -> RunLength {
        self.as_base_iter_mut().next_header()
    }

    fn prev_header(&mut self) -> RunLength {
        self.as_base_iter_mut().prev_header()
    }

    fn next_field(&mut self) -> RunLength {
        self.as_base_iter_mut().next_field()
    }

    fn prev_field(&mut self) -> RunLength {
        self.as_base_iter_mut().prev_field()
    }

    fn next_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize {
        self.as_base_iter_mut()
            .next_n_fields_with_fmt_and_data_check(
                n,
                kinds,
                invert_kinds_check,
                flag_mask,
                flags,
                data_check,
            )
    }

    fn prev_n_fields_with_fmt_and_data_check<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueKind; N],
        invert_kinds_check: bool,
        flag_mask: FieldValueFlags,
        flags: FieldValueFlags,
        data_check: impl Fn(&FieldValueFormat, *const u8) -> bool,
    ) -> usize {
        self.as_base_iter_mut()
            .prev_n_fields_with_fmt_and_data_check(
                n,
                kinds,
                invert_kinds_check,
                flag_mask,
                flags,
                data_check,
            )
    }

    fn typed_field_fwd(&mut self, limit: RunLength) -> Option<TypedField<'a>> {
        self.as_base_iter_mut().typed_field_fwd(limit)
    }

    fn typed_field_bwd(&mut self, limit: RunLength) -> Option<TypedField<'a>> {
        self.as_base_iter_mut().typed_field_bwd(limit)
    }

    fn typed_range_fwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>> {
        self.as_base_iter_mut().typed_range_fwd(limit, flag_mask)
    }

    fn typed_range_bwd(
        &mut self,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<ValidTypedRange<'a>> {
        self.as_base_iter_mut().typed_range_bwd(limit, flag_mask)
    }

    fn into_base_iter(self) -> Iter<'a> {
        self.into_base_iter()
    }
}

pub struct UnfoldRunLength<I, T> {
    iter: I,
    last: Option<T>,
    remaining_run_len: RunLength,
}

impl<I: Iterator<Item = (T, RunLength)>, T: Clone> UnfoldRunLength<I, T> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            last: None,
            remaining_run_len: 0,
        }
    }
}

pub trait UnfoldIterRunLength<T>: Sized {
    fn unfold_rl(self) -> UnfoldRunLength<Self, T>;
}

impl<T: Clone, I: Iterator<Item = (T, RunLength)>> UnfoldIterRunLength<T> for I {
    fn unfold_rl(self) -> UnfoldRunLength<Self, T> {
        UnfoldRunLength::new(self)
    }
}

impl<I: Iterator<Item = (T, RunLength)>, T: Clone> Iterator for UnfoldRunLength<I, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_run_len > 0 {
            self.remaining_run_len -= 1;
            return self.last.clone();
        } else if let Some((v, rl)) = self.iter.next() {
            self.remaining_run_len = rl - 1;
            self.last = Some(v);
        } else {
            self.last = None;
        }
        self.last.clone()
    }
}
