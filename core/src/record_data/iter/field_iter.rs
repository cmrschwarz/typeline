use crate::record_data::{
    action_buffer::ActorId,
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, RunLength,
    },
    iter_hall::{FieldLocation, IterKind, IterLocation, IterState},
};
use std::marker::PhantomData;

use super::{
    super::{
        field_data_ref::FieldDataRef,
        field_value_ref::{TypedField, TypedRange, ValidTypedRange},
    },
    field_iterator::{FieldIterOpts, FieldIterator},
};

#[repr(C)]
pub struct FieldIter<R: FieldDataRef> {
    pub(super) fdr: R,
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    pub(super) header_rl_total: RunLength,
    pub(super) header_fmt: FieldValueFormat,
    pub(super) _phantom_data: PhantomData<&'static ()>,
}

impl<R: FieldDataRef> Clone for FieldIter<R> {
    fn clone(&self) -> Self {
        Self {
            // This is the reason we cannot derive this.
            fdr: self.fdr.clone_ref(),
            field_pos: self.field_pos,
            data: self.data,
            header_idx: self.header_idx,
            header_rl_offset: self.header_rl_offset,
            header_rl_total: self.header_rl_total,
            header_fmt: self.header_fmt,
            _phantom_data: self._phantom_data,
        }
    }
}

impl<R: FieldDataRef> FieldIter<R> {
    pub unsafe fn from_iter_location(fdr: R, loc: IterLocation) -> Self {
        let headers = fdr.headers();
        debug_assert!(loc.header_idx <= headers.len());
        if headers.len() == loc.header_idx {
            return FieldIter::from_start(fdr);
        }
        let h = headers[loc.header_idx];
        let mut res = FieldIter {
            fdr,
            field_pos: loc.field_pos,
            data: loc.header_start_data_pos_post_padding,
            header_idx: loc.header_idx,
            header_rl_offset: loc.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
            _phantom_data: PhantomData,
        };
        res.skip_dead_fields();
        res
    }
    pub unsafe fn from_field_location(fdr: R, loc: FieldLocation) -> Self {
        let headers = fdr.headers();
        debug_assert!(loc.header_idx <= headers.len());
        if headers.len() == loc.header_idx {
            return FieldIter::from_start(fdr);
        }
        let h = headers[loc.header_idx];
        let data_pos = {
            let mut h_dummy = h;
            h_dummy.run_length = loc.header_rl_offset;
            loc.data_pos - h_dummy.data_size_unique()
        };
        let mut res = FieldIter {
            fdr,
            field_pos: loc.field_pos,
            data: data_pos,
            header_idx: loc.header_idx,
            header_rl_offset: loc.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
            _phantom_data: PhantomData,
        };
        res.skip_dead_fields();
        res
    }
    pub fn into_iter_state(
        mut self,
        first_left_leaning_actor_id: ActorId,
        #[cfg_attr(not(feature = "debug_state"), allow(unused_variables))]
        kind: IterKind,
    ) -> IterState {
        let mut loc = self.get_iter_location();
        let mut state = IterState {
            field_pos: loc.field_pos,
            header_start_data_pos_pre_padding: loc
                .header_start_data_pos_post_padding,
            header_idx: loc.header_idx,
            header_rl_offset: loc.header_rl_offset,
            first_right_leaning_actor_id: first_left_leaning_actor_id,
            #[cfg(feature = "debug_state")]
            kind,
        };
        // we use the field count from the iter becase the field might be
        // cow
        if loc.header_rl_offset == 0
            && loc.field_pos == self.field_data_ref().field_count()
        {
            // Uphold the 'no `IterState` on the last header except 0'
            // invariant.
            if loc.field_pos == 0 {
                // When our header index is already 0, resetting is a noop.
                // The other case (header_idx > 0) happens if all fields
                // before are deleted. Calling
                // `prev_field`, like in
                // the other branch, would fail here, but
                // having the iterator sit at 0/0 works out.
                state.header_start_data_pos_pre_padding = 0;
                state.header_idx = 0;
                return state;
            }
            self.prev_field();
            state.header_rl_offset = self.field_run_length_bwd() + 1;
            loc = self.get_iter_location();
        }
        state.header_idx = loc.header_idx;

        state.header_start_data_pos_pre_padding = loc
            .header_start_data_pos_post_padding
            - self.field_data_ref().headers()[loc.header_idx]
                .leading_padding();
        state
    }
    pub fn from_start_allow_dead(fdr: R) -> Self {
        let first_header = fdr.headers().front();
        Self {
            field_pos: 0,
            data: first_header.map(|h| h.leading_padding()).unwrap_or(0),
            header_idx: 0,
            header_rl_offset: 0,
            header_rl_total: first_header.map_or(0, |h| h.run_length),
            header_fmt: first_header.map(|h| h.fmt).unwrap_or_default(),
            fdr,
            _phantom_data: PhantomData,
        }
    }
    pub fn from_start(fdr: R) -> Self {
        let mut res = Self::from_start_allow_dead(fdr);
        res.skip_dead_fields();
        res
    }
    pub fn from_end(fdr: R) -> Self {
        Self {
            field_pos: fdr.field_count(),
            data: fdr.data().len(),
            header_idx: fdr.headers().len(),
            header_rl_offset: 0,
            header_rl_total: 0,
            header_fmt: FieldValueFormat::default(),
            fdr,
            _phantom_data: PhantomData,
        }
    }
    pub fn skip_dead_fields(&mut self) -> usize {
        if !self.header_fmt.deleted() {
            return 0;
        }
        let mut skip_count =
            (self.header_rl_total - self.header_rl_offset) as usize;
        let headers = self.fdr.headers();
        let mut prev_header_size = headers[self.header_idx].data_size();
        loop {
            self.header_idx += 1;
            if self.header_idx == headers.len() {
                self.header_rl_total = 0;
                // to make sure there's no padding
                self.header_fmt = FieldValueFormat::default();
                self.data += prev_header_size;
                break;
            }
            let h = headers[self.header_idx];
            if !h.same_value_as_previous() {
                self.data += prev_header_size;
            }
            if h.deleted() {
                skip_count += h.run_length as usize;
                prev_header_size = h.total_size();
                continue;
            }

            self.data += h.leading_padding();
            self.header_fmt = h.fmt;
            self.header_rl_total = h.run_length;
            break;
        }
        self.header_rl_offset = 0;
        skip_count
    }
    pub fn next_field_allow_dead(&mut self) {
        assert!(self.is_next_valid());
        if !self.header_fmt.deleted() {
            self.field_pos += 1;
        }
        if self.header_rl_offset + 1 < self.header_rl_total {
            self.header_rl_offset += 1;
            return;
        }
        let headers = self.fdr.headers();
        let prev_header_size = headers[self.header_idx].data_size();
        self.header_idx += 1;
        self.header_rl_offset = 0;
        if self.header_idx == headers.len() {
            self.header_rl_total = 0;
            // to make sure there's no padding
            self.header_fmt = FieldValueFormat::default();
            self.data += prev_header_size;
            return;
        }
        let h = headers[self.header_idx];
        if !h.same_value_as_previous() {
            self.data += prev_header_size;
        }
        self.data += h.leading_padding();
        self.header_fmt = h.fmt;
        self.header_rl_total = h.run_length;
    }
}
impl<R: FieldDataRef> FieldIterator for FieldIter<R> {
    type FieldDataRefType = R;
    fn field_data_ref(&self) -> &Self::FieldDataRefType {
        &self.fdr
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
            self.data
                + self.header_rl_offset as usize
                    * self.header_fmt.size as usize
        }
    }
    fn get_prev_field_data_end(&self) -> usize {
        if self.header_fmt.same_value_as_previous() {
            debug_assert!(self.header_fmt.shared_value());
            return self.data + self.header_fmt.size as usize;
        }
        if self.header_rl_offset > 0 {
            if self.header_fmt.shared_value() {
                return self.data + self.header_fmt.size as usize;
            }
            return self.data
                + (self.header_rl_offset as usize)
                    * self.header_fmt.size as usize;
        }
        self.data - self.header_fmt.leading_padding()
    }
    fn get_next_field_header(&self) -> FieldValueHeader {
        debug_assert!(self.is_next_valid());
        FieldValueHeader {
            fmt: self.header_fmt,
            run_length: self.header_rl_total,
        }
    }
    fn get_next_field_header_data_start(&self) -> usize {
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
    fn get_next_typed_field(&mut self) -> TypedField {
        // SAFETY: debug assert is not enough here because we use unsafe below
        assert!(self.is_next_valid());
        let data = self.get_next_field_data();
        let run_len = if self.header_fmt.shared_value() {
            self.field_run_length_fwd()
        } else {
            1
        };
        unsafe { TypedField::new(&self.fdr, self.header_fmt, data, run_len) }
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
        if !self.header_fmt.deleted() {
            self.field_pos += stride as usize;
        }
        let headers = self.fdr.headers();
        let mut prev_header_size = headers[self.header_idx].data_size();
        loop {
            self.header_idx += 1;
            if self.header_idx == headers.len() {
                self.header_rl_total = 0;
                // to make sure there's no padding
                self.header_fmt = FieldValueFormat::default();
                self.data += prev_header_size;
                return stride;
            }
            let h = headers[self.header_idx];
            if !h.same_value_as_previous() {
                self.data += prev_header_size;
            }
            if h.deleted() {
                prev_header_size = h.total_size();
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
        let mut same_as_prev = self.header_fmt.same_value_as_previous();
        debug_assert!(!same_as_prev || data_offset == 0);
        loop {
            if i == 0 {
                return 0;
            }
            i -= 1;
            let h = self.fdr.headers()[i];
            if !same_as_prev {
                data_offset += h.data_size();
            }
            if h.deleted() {
                if i == 0 {
                    return 0;
                }
                same_as_prev = h.same_value_as_previous();
                data_offset += h.leading_padding();
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
        self.next_header()
    }
    fn prev_field(&mut self) -> RunLength {
        if self.header_rl_offset > 0 {
            self.header_rl_offset -= 1;
            self.field_pos -= 1;
            return 1;
        }
        self.prev_header()
    }
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueRepr; N],
        opts: FieldIterOpts,
    ) -> usize {
        if n == 0 {
            // edge case: allow advancing by zero even after the end
            return 0;
        }
        let mut stride_rem = n;
        let mut curr_header_rem =
            (self.header_rl_total - self.header_rl_offset) as usize;
        if curr_header_rem == 0
            || (self.header_fmt.deleted() && opts.stop_on_dead())
            || (kinds.contains(&self.header_fmt.repr)
                == opts.invert_kinds_check())
        {
            return 0;
        }

        let mut data_pos = self.data;
        if !self.header_fmt.shared_value() {
            data_pos +=
                self.header_rl_offset as usize * self.header_fmt.size as usize;
        }

        let mut data_wrap_pos = if opts.allow_data_ring_wrap() {
            usize::MAX
        } else {
            let slice_0_len = self.fdr.data().as_slices().0.len();
            if data_pos < slice_0_len {
                slice_0_len
            } else {
                usize::MAX
            }
        };

        let header_wrap_idx = if opts.allow_header_ring_wrap() {
            usize::MAX
        } else {
            let slice_0_len = self.fdr.headers().as_slices().0.len();
            if self.header_idx < slice_0_len {
                slice_0_len
            } else {
                usize::MAX
            }
        };

        loop {
            let stride_size = if self.header_fmt.shared_value() {
                self.header_fmt.size as usize
            } else {
                self.header_fmt.size as usize * curr_header_rem
            };

            if data_pos + stride_size > data_wrap_pos {
                // this is to not be div by zero because otherwise
                // we woultn't overflow our data pos
                let rem =
                    (data_wrap_pos - data_pos) / self.header_fmt.size as usize;
                self.header_rl_offset += rem as RunLength;
                self.field_pos += rem;
                stride_rem -= rem;
                return n - stride_rem;
            }
            if curr_header_rem > stride_rem {
                self.header_rl_offset += stride_rem as RunLength;
                self.field_pos += stride_rem;
                return n;
            }
            if self.fdr.headers().len() != self.header_idx + 1 {
                let h = self.fdr.headers()[self.header_idx + 1];

                let mut done = h.deleted() && opts.stop_on_dead();
                done |= h.deleted()
                    && !opts.allow_different_kind_if_dead()
                    && kinds.contains(&h.repr) == opts.invert_kinds_check();
                if done {
                    stride_rem -= self.next_header() as usize;
                    return n - stride_rem;
                }
            }

            stride_rem -= self.next_header() as usize;
            curr_header_rem = self.header_rl_total as usize;
            data_pos = self.data;
            if data_pos >= data_wrap_pos {
                if stride_rem == n {
                    data_wrap_pos = usize::MAX;
                } else {
                    return n - stride_rem;
                }
            }
            if !self.is_next_valid()
                || (self.header_fmt.deleted() && opts.stop_on_dead())
                || (kinds.contains(&self.header_fmt.repr)
                    == opts.invert_kinds_check())
                || self.header_idx == header_wrap_idx
            {
                return n - stride_rem;
            }
        }
    }
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueRepr; N],
        opts: FieldIterOpts,
    ) -> usize {
        // HACK // SAFETY
        // TODO: this currently does **not** respect data_ring_wrap
        // which might lead to invalid memory if somebody decides to
        // create slices from this
        if n == 0
            || self.prev_field() == 0
            || (self.header_fmt.deleted() && opts.stop_on_dead())
            || (kinds.contains(&self.header_fmt.repr)
                == opts.invert_kinds_check())
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
        let wrap_idx = if opts.allow_header_ring_wrap() {
            usize::MAX
        } else {
            self.fdr.headers().as_slices().0.len().saturating_sub(1)
        };
        loop {
            if self.header_idx != 0 {
                let h = self.fdr.headers()[self.header_idx + 1];

                let mut done = h.deleted() && opts.stop_on_dead();
                done |= h.deleted()
                    && !opts.allow_different_kind_if_dead()
                    && kinds.contains(&h.repr) == opts.invert_kinds_check();
                if done {
                    stride_rem -= self.prev_header() as usize;
                    return n - stride_rem;
                }
            }

            stride_rem -= self.prev_header() as usize;
            if !self.is_prev_valid()
                || (self.header_fmt.deleted() && opts.stop_on_dead())
                || (kinds.contains(&self.header_fmt.repr)
                    == opts.invert_kinds_check())
                || wrap_idx == self.header_idx
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
    fn typed_field_fwd(&mut self, limit: usize) -> Option<TypedField> {
        if limit == 0 || !self.is_next_valid() {
            None
        } else {
            let data = self.get_next_field_data();
            let fmt = self.header_fmt;
            let run_len = if self.header_fmt.shared_value() {
                let rl = self.field_run_length_fwd();
                if rl as usize <= limit {
                    self.next_header();
                    rl
                } else {
                    self.header_rl_offset += limit as RunLength;
                    self.field_pos += limit;
                    limit as RunLength
                }
            } else {
                self.next_field();
                1
            };
            Some(unsafe { TypedField::new(&self.fdr, fmt, data, run_len) })
        }
    }
    fn typed_field_bwd(&mut self, limit: usize) -> Option<TypedField> {
        if limit == 0 || self.prev_field() == 0 {
            None
        } else {
            let data = self.get_next_field_data();
            let fmt = self.header_fmt;
            let run_len = if self.header_fmt.shared_value() {
                let rl = self.field_run_length_bwd() + 1;
                if rl as usize <= limit {
                    self.prev_header();
                    rl
                } else {
                    self.header_rl_offset -= limit as RunLength - 1;
                    self.field_pos -= limit - 1;
                    limit as RunLength
                }
            } else {
                1
            };
            Some(unsafe { TypedField::new(&self.fdr, fmt, data, run_len) })
        }
    }
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<ValidTypedRange> {
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
        let field_count = self.next_n_fields_with_fmt(
            limit,
            [fmt.repr],
            opts.with_invert_kinds_check(false)
                .with_allow_header_ring_wrap(false)
                .with_allow_data_ring_wrap(false),
        );
        let mut data_end = self.get_prev_field_data_end();
        let mut oversize_end = 0;
        let mut header_end = self.header_idx;
        if self.is_next_valid() {
            if self.field_run_length_bwd() != 0 {
                header_end += 1;
                oversize_end = self.header_rl_total - self.header_rl_offset;
            } else {
                while header_end > 0
                    && self.fdr.headers()[header_end - 1].deleted()
                {
                    header_end -= 1;
                    data_end -= self.fdr.headers()[header_end].data_size();
                }
            }
        }
        let range = TypedRange::new(
            &self.fdr,
            fmt,
            data_begin,
            data_end,
            field_count,
            header_start,
            header_end,
            oversize_start,
            oversize_end,
        );
        unsafe { ValidTypedRange::new_unchecked(range).into() }
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<ValidTypedRange> {
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
            [fmt.repr],
            opts.with_invert_kinds_check(false)
                .with_allow_data_ring_wrap(false)
                .with_allow_header_ring_wrap(false),
        ) + 1;
        let header_start = self.header_idx;
        let data_start = self.get_next_field_data();
        let range = TypedRange::new(
            &self.fdr,
            fmt,
            data_start,
            data_end,
            field_count,
            header_start,
            header_end,
            self.header_rl_offset,
            oversize_end,
        );
        unsafe { ValidTypedRange::new_unchecked(range) }.into()
    }

    fn into_base_iter(self) -> FieldIter<R> {
        self
    }
}
