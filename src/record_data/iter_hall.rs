use std::cell::Cell;

use nonmax::NonMaxU32;

use crate::utils::universe::Universe;

use super::{
    field_data::{
        FieldData, FieldDataInternals, FieldValueFlags, FieldValueHeader,
        FieldValueKind, RunLength,
    },
    iters::{FieldDataRef, FieldIterator, Iter},
    match_set_manager::MatchSetManager,
    push_interface::{
        FieldReferenceInserter, FixedSizeTypeInserter, InlineBytesInserter,
        InlineStringInserter, IntegerInserter, RawPushInterface,
        VariableSizeTypeInserter, VaryingTypeInserter,
    },
    ref_iter::AutoDerefIter,
};

pub type IterId = NonMaxU32;

#[derive(Default)]
pub struct IterHall {
    pub(super) fd: FieldData,
    pub(super) iters: Universe<IterId, Cell<IterState>>,
}

#[derive(Default, Clone, Copy)]
pub struct IterState {
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
}

impl IterState {
    pub fn is_valid(&self) -> bool {
        self.field_pos != usize::MAX
    }
    pub fn invalidate(&mut self) {
        self.field_pos = usize::MAX
    }
}
impl IterHall {
    pub fn claim_iter(&mut self) -> IterId {
        let iter_id = self.iters.claim();
        self.iters[iter_id].set(IterState {
            field_pos: 0,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
        });
        iter_id
    }
    pub fn reserve_iter_id(&mut self, iter_id: IterId) {
        self.iters.reserve_id(iter_id);
    }
    pub fn release_iter(&mut self, iter_id: IterId) {
        self.iters[iter_id].get_mut().invalidate();
        self.iters.release(iter_id)
    }
    pub fn iter(&self) -> Iter<'_, &'_ FieldData> {
        self.fd.iter()
    }
    pub fn get_iter_state(&self, iter_id: IterId) -> IterState {
        self.iters[iter_id].get()
    }
    fn calculate_start_header(
        &self,
        state: &mut IterState,
    ) -> FieldValueHeader {
        if state.header_idx == self.fd.header.len() {
            let diff = self.fd.field_count - state.field_pos;
            if diff == 0 {
                return Default::default();
            }
            state.header_idx -= 1;
            let h = self.fd.header[state.header_idx];
            if !h.same_value_as_previous() {
                state.data -= if h.shared_value() {
                    h.size as usize
                } else {
                    h.size as usize * (h.run_length as usize - diff)
                };
            }
            state.header_rl_offset = h.run_length - diff as RunLength;
            return h;
        }
        let mut h = self.fd.header[state.header_idx];
        if h.run_length == state.header_rl_offset
            && state.header_idx < self.fd.header.len()
        {
            state.header_idx += 1;
            state.header_rl_offset = 0;
            state.data += h.total_size();
            if state.header_idx == self.fd.header.len() {
                h = Default::default();
            } else {
                h = self.fd.header[state.header_idx];
            }
        }
        h
    }
    pub fn get_iter(&self, iter_id: IterId) -> Iter<'_, &'_ FieldData> {
        unsafe { self.get_iter_from_state(self.iters[iter_id].get()) }
    }
    pub unsafe fn get_iter_from_state(
        &self,
        mut state: IterState,
    ) -> Iter<'_, &'_ FieldData> {
        let h = self.calculate_start_header(&mut state);
        let mut res = Iter {
            fdr: &self.fd,
            field_pos: state.field_pos,
            data: state.data,
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
            _phantom_data: Default::default(),
        };
        res.skip_dead_fields();
        res
    }
    pub fn iter_is_from_iter_hall<'a, R: FieldDataRef<'a>>(
        &self,
        iter: &Iter<'a, R>,
    ) -> bool {
        std::ptr::eq(iter.fdr.data().as_ptr(), self.fd.data.as_ptr())
    }
    pub fn store_iter<'a>(
        &self,
        iter_id: IterId,
        iter: impl FieldIterator<'a>,
    ) {
        let iter = iter.into_base_iter();
        assert!(self.iter_is_from_iter_hall(&iter));
        unsafe { self.store_iter_unchecked(iter_id, iter) };
    }
    // the point of this is not to save the runtime of one assert, but
    // to actually bypass that check if we store an iter that comes from our
    // cow_source
    pub unsafe fn store_iter_unchecked<'a, R: FieldDataRef<'a>>(
        &self,
        iter_id: IterId,
        mut iter: Iter<'a, R>,
    ) {
        let mut state = self.iters[iter_id].get();
        state.field_pos = iter.field_pos;
        state.header_rl_offset = iter.header_rl_offset;
        if iter.header_idx == self.fd.header.len() && iter.header_idx > 0 {
            iter.prev_field();
            state.header_rl_offset = iter.field_run_length_bwd() + 1;
        }
        state.header_idx = iter.header_idx;
        state.data = iter.data;
        self.iters[iter_id].set(state);
    }

    /// returns a tuple of (FieldData, initial_field_offset, field_count)
    pub unsafe fn internals(&mut self) -> FieldDataInternals {
        unsafe { self.fd.internals() }
    }
    pub unsafe fn raw(&mut self) -> &mut FieldData {
        &mut self.fd
    }

    pub fn copy<'a>(
        iter: impl FieldIterator<'a> + Clone,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut IterHall)),
    ) -> usize {
        let adapted_target_applicator =
            &mut |f: &mut dyn FnMut(&mut FieldData)| {
                let g = &mut |fdih: &mut IterHall| f(&mut fdih.fd);
                targets_applicator(g);
            };
        FieldData::copy(iter, adapted_target_applicator)
    }
    pub fn copy_resolve_refs<'a, I: FieldIterator<'a>>(
        match_set_mgr: &mut MatchSetManager,
        iter: &mut AutoDerefIter<'a, I>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut IterHall)),
    ) -> usize {
        let adapted_target_applicator =
            &mut |f: &mut dyn FnMut(&mut FieldData)| {
                let g = &mut |fdih: &mut IterHall| f(&mut fdih.fd);
                targets_applicator(g);
            };
        FieldData::copy_resolve_refs(
            match_set_mgr,
            iter,
            adapted_target_applicator,
        )
    }
    pub fn field_count(&self) -> usize {
        self.fd.field_count
    }
    pub fn reset_iterators(&mut self) {
        for it in self.iters.iter_mut() {
            let it = it.get_mut();
            it.data = 0;
            it.header_rl_offset = 0;
            it.header_idx = 0;
            it.field_pos = 0;
        }
    }
    pub fn clear(&mut self) {
        self.reset_iterators();
        self.fd.clear();
    }
    pub fn reset(&mut self) {
        self.clear();
    }
    pub fn reset_with_data(&mut self, fd: FieldData) {
        self.reset_iterators();
        self.fd = fd;
    }
    pub fn new_with_data(fd: FieldData) -> Self {
        Self {
            fd,
            iters: Default::default(),
        }
    }
    pub fn int_inserter(&mut self) -> IntegerInserter {
        IntegerInserter::new(&mut self.fd)
    }
    pub fn field_reference_inserter(&mut self) -> FieldReferenceInserter {
        FieldReferenceInserter::new(&mut self.fd)
    }
    pub fn inline_bytes_inserter(&mut self) -> InlineBytesInserter {
        InlineBytesInserter::new(&mut self.fd)
    }
    pub fn inline_str_inserter(&mut self) -> InlineStringInserter {
        InlineStringInserter::new(&mut self.fd)
    }
    pub fn varying_type_inserter(
        &mut self,
        re_reserve_count: RunLength,
    ) -> VaryingTypeInserter {
        VaryingTypeInserter::new(&mut self.fd, re_reserve_count)
    }
}

unsafe impl RawPushInterface for IterHall {
    unsafe fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.fd.push_variable_sized_type(
                kind,
                flags,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }

    unsafe fn push_fixed_size_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.fd.push_fixed_size_type(
                kind,
                flags,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }

    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        unsafe {
            self.fd.push_zst_unchecked(
                kind,
                flags,
                run_length,
                try_header_rle,
            );
        }
    }
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
    ) -> *mut u8 {
        unsafe {
            self.fd.push_variable_sized_type_uninit(
                kind, flags, data_len, run_length,
            )
        }
    }
}
impl IterHall {
    pub fn dup_last_value(&mut self, run_length: usize) {
        self.fd.dup_last_value(run_length);
    }
    pub fn drop_last_value(&mut self, run_length: usize) {
        self.fd.drop_last_value(run_length);
    }
}
