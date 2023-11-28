use core::panic;
use std::cell::{Cell, UnsafeCell};

use nonmax::NonMaxU32;
use thin_vec::ThinVec;

use crate::utils::universe::Universe;

use super::{
    field::{FieldId, FieldManager},
    field_data::{
        FieldData, FieldDataBuffer, FieldDataInternals, FieldValueFlags,
        FieldValueHeader, FieldValueKind, RunLength,
    },
    iters::{FieldDataRef, FieldIterator, Iter},
    match_set::MatchSetManager,
    push_interface::{
        FieldReferenceInserter, FixedSizeTypeInserter, InlineBytesInserter,
        InlineStringInserter, IntegerInserter, RawPushInterface,
        VariableSizeTypeInserter, VaryingTypeInserter,
    },
    ref_iter::AutoDerefIter,
};

pub type IterId = NonMaxU32;

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum FieldDataSource {
    #[default]
    Owned,
    Cow(FieldId),
    DataCow {
        src_field: FieldId,
        header_iter: IterId,
    },
    RecordBufferCow(*const UnsafeCell<FieldData>),
    RecordBufferDataCow(*const UnsafeCell<FieldData>),
}

#[derive(Default)]
pub struct IterHall {
    pub(super) data_source: FieldDataSource,
    pub(super) field_data: FieldData,
    pub(super) iters: Universe<IterId, Cell<IterState>>,
    pub(super) cow_targets: ThinVec<FieldId>,
}
unsafe impl Send for IterHall {}
unsafe impl Sync for IterHall {}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
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
    pub fn get_iter_state_at_end(&self, fm: &FieldManager) -> IterState {
        if self.field_data.field_count == 0 {
            return IterState {
                field_pos: 0,
                data: 0,
                header_idx: 0,
                header_rl_offset: 0,
            };
        }
        IterState {
            field_pos: self.field_data.field_count,
            data: self.get_field_data_len(fm)
                - self
                    .field_data
                    .headers
                    .last()
                    .map(|h| h.total_size_unique())
                    .unwrap_or(0),
            // TODO: respect cow
            header_idx: self.field_data.headers.len() - 1,
            header_rl_offset: self
                .field_data
                .headers
                .last()
                .unwrap()
                .run_length,
        }
    }
    pub fn get_field_data_len(&self, fm: &FieldManager) -> usize {
        match self.data_source {
            FieldDataSource::Owned => self.field_data.data.len(),
            FieldDataSource::Cow(src_field)
            | FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => fm.fields[src_field]
                .borrow()
                .iter_hall
                .get_field_data_len(fm),
            FieldDataSource::RecordBufferCow(data)
            | FieldDataSource::RecordBufferDataCow(data) => {
                unsafe { &*(*data).get() }.data.len()
            }
        }
    }
    pub fn claim_iter_at_end(&mut self, fm: &FieldManager) -> IterId {
        let iter_id = self.iters.claim();
        self.iters[iter_id].set(self.get_iter_state_at_end(fm));
        iter_id
    }
    pub fn reserve_iter_id(&mut self, iter_id: IterId) {
        self.iters.reserve_id(iter_id);
    }
    pub fn release_iter(&mut self, iter_id: IterId) {
        self.iters[iter_id].get_mut().invalidate();
        self.iters.release(iter_id)
    }
    pub fn get_iter_state(&self, iter_id: IterId) -> IterState {
        self.iters[iter_id].get()
    }
    fn calculate_start_header<'a, R: FieldDataRef<'a>>(
        &self,
        fr: &R,
        state: &mut IterState,
    ) -> FieldValueHeader {
        if state.header_idx == fr.headers().len() {
            let diff = fr.field_count() - state.field_pos;
            if diff == 0 {
                return Default::default();
            }
            state.header_idx -= 1;
            let h = fr.headers()[state.header_idx];
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
        let mut h = fr.headers()[state.header_idx];
        if h.run_length == state.header_rl_offset
            && state.header_idx < fr.headers().len()
        {
            state.header_idx += 1;
            state.header_rl_offset = 0;
            state.data += h.total_size();
            if state.header_idx == fr.headers().len() {
                h = Default::default();
            } else {
                h = fr.headers()[state.header_idx];
            }
        }
        h
    }
    // SAFETY: caller must ensure that the state comes from this data source
    pub unsafe fn get_iter_from_state_unchecked<'a, R: FieldDataRef<'a>>(
        &self,
        fr: R,
        mut state: IterState,
    ) -> Iter<'a, R> {
        let h = self.calculate_start_header(&fr, &mut state);
        let mut res = Iter {
            fdr: fr,
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
    // SAFETY: caller must ensure that the iter uses the correct data source
    pub unsafe fn store_iter_unchecked<'a, R: FieldDataRef<'a>>(
        &self,
        iter_id: IterId,
        mut iter: Iter<'a, R>,
    ) {
        let mut state = self.iters[iter_id].get();
        state.field_pos = iter.field_pos;
        state.header_rl_offset = iter.header_rl_offset;
        if iter.header_idx == iter.field_data_ref().headers().len()
            && iter.header_idx > 0
        {
            if iter.field_pos == 0 {
                // this happens if all fields before are deleted
                // calling prev_field would fail here
                self.reset_iter(iter_id);
                return;
            } else {
                iter.prev_field();
                state.header_rl_offset = iter.field_run_length_bwd() + 1;
            }
        }
        state.header_idx = iter.header_idx;
        state.data = iter.data;
        unsafe {
            self.store_iter_state_unchecked(iter_id, state);
        }
    }
    pub unsafe fn store_iter_state_unchecked(
        &self,
        iter_id: IterId,
        iter_state: IterState,
    ) {
        self.iters[iter_id].set(iter_state);
    }

    /// returns a tuple of (FieldData, initial_field_offset, field_count)
    pub unsafe fn internals(&mut self) -> FieldDataInternals {
        unsafe { self.get_owned_data().internals() }
    }
    pub unsafe fn raw(&mut self) -> &mut FieldData {
        self.get_owned_data()
    }

    pub fn copy<'a>(
        iter: &mut impl FieldIterator<'a>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut IterHall)),
    ) -> usize {
        let adapted_target_applicator =
            &mut |f: &mut dyn FnMut(&mut FieldData)| {
                let g = &mut |fdih: &mut IterHall| f(fdih.get_owned_data());
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
                let g = &mut |fdih: &mut IterHall| f(fdih.get_owned_data());
                targets_applicator(g);
            };
        FieldData::copy_resolve_refs(
            match_set_mgr,
            iter,
            adapted_target_applicator,
        )
    }
    pub fn cow_field(&self) -> Option<FieldId> {
        match self.data_source {
            FieldDataSource::Owned => None,
            FieldDataSource::Cow(src) => Some(src),
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => Some(src_field),
            FieldDataSource::RecordBufferCow(_) => None,
            FieldDataSource::RecordBufferDataCow(_) => None,
        }
    }
    pub fn is_cow(&self) -> bool {
        match self.data_source {
            FieldDataSource::Owned => true,
            FieldDataSource::Cow(_) => false,
            FieldDataSource::DataCow { .. } => false,
            FieldDataSource::RecordBufferCow(_) => false,
            FieldDataSource::RecordBufferDataCow(_) => false,
        }
    }
    pub fn are_headers_owned(&self) -> bool {
        match self.data_source {
            FieldDataSource::Owned => true,
            FieldDataSource::DataCow { .. } => true,
            FieldDataSource::RecordBufferDataCow(_) => true,
            FieldDataSource::Cow(_) => false,
            FieldDataSource::RecordBufferCow(_) => false,
        }
    }
    // source field of cow, data cow only
    pub fn cow_source_field(&self) -> (Option<FieldId>, Option<bool>) {
        match self.data_source {
            FieldDataSource::Owned => (None, None),
            FieldDataSource::Cow(src) => (Some(src), Some(false)),
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => (Some(src_field), Some(true)),
            FieldDataSource::RecordBufferCow(_) => (None, Some(false)),
            FieldDataSource::RecordBufferDataCow(_) => (None, Some(true)),
        }
    }
    pub fn reset_iter(&self, iter_id: IterId) {
        self.iters[iter_id].set(IterState::default());
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
    pub fn reset_cow_headers(&mut self) {
        match &mut self.data_source {
            FieldDataSource::Owned => (),
            FieldDataSource::Cow(_) => (),
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => self.data_source = FieldDataSource::Cow(*src_field),
            FieldDataSource::RecordBufferCow(_) => todo!(),
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                self.data_source = FieldDataSource::RecordBufferCow(*data_ref)
            }
        }
    }
    pub fn reset(&mut self) {
        self.reset_iterators();
        self.data_source = FieldDataSource::Owned;
        self.field_data.clear();
    }
    pub fn reset_with_data(&mut self, fd: FieldData) {
        self.reset_iterators();
        self.data_source = FieldDataSource::Owned;
        self.field_data = fd;
    }
    pub fn new_with_data(fd: FieldData) -> Self {
        Self {
            data_source: FieldDataSource::Owned,
            field_data: fd,
            iters: Default::default(),
            cow_targets: Default::default(),
        }
    }
    pub fn int_inserter(&mut self) -> IntegerInserter {
        IntegerInserter::new(self.get_owned_data())
    }
    pub fn field_reference_inserter(&mut self) -> FieldReferenceInserter {
        FieldReferenceInserter::new(self.get_owned_data())
    }
    pub fn inline_bytes_inserter(&mut self) -> InlineBytesInserter {
        InlineBytesInserter::new(self.get_owned_data())
    }
    pub fn inline_str_inserter(&mut self) -> InlineStringInserter {
        InlineStringInserter::new(self.get_owned_data())
    }
    pub fn varying_type_inserter(
        &mut self,
        re_reserve_count: RunLength,
    ) -> VaryingTypeInserter {
        VaryingTypeInserter::new(self.get_owned_data(), re_reserve_count)
    }
    fn get_owned_data(&mut self) -> &mut FieldData {
        match &mut self.data_source {
            FieldDataSource::Owned => &mut self.field_data,
            _ => panic!("IterHall uses COW!"),
        }
    }
    pub fn append_headers_to(
        &self,
        fm: &FieldManager,
        header_tgt: &mut Vec<FieldValueHeader>,
    ) -> usize {
        match self.data_source {
            FieldDataSource::Owned
            | FieldDataSource::DataCow { .. }
            | FieldDataSource::RecordBufferDataCow(_) => {
                header_tgt.extend_from_slice(&self.field_data.headers);
                self.field_data.field_count
            }
            FieldDataSource::Cow(src) => fm.fields[src]
                .borrow()
                .iter_hall
                .append_headers_to(fm, header_tgt),
            FieldDataSource::RecordBufferCow(rb) => {
                let fd = unsafe { &*(*rb).get() };
                header_tgt.extend_from_slice(&fd.headers);
                fd.field_count
            }
        }
    }
    pub fn append_data_to(
        &self,
        fm: &FieldManager,
        target: &mut FieldDataBuffer,
    ) {
        match self.data_source {
            FieldDataSource::Owned => {
                self.field_data.append_data_to(target);
            }
            FieldDataSource::Cow(src) => {
                fm.fields[src].borrow().iter_hall.append_data_to(fm, target);
            }
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => fm.fields[src_field]
                .borrow()
                .iter_hall
                .append_data_to(fm, target),
            FieldDataSource::RecordBufferDataCow(data_ref)
            | FieldDataSource::RecordBufferCow(data_ref) => {
                unsafe { &*(*data_ref).get() }.append_data_to(target);
            }
        }
    }
    pub fn append_to(&self, fm: &FieldManager, target: &mut FieldData) {
        match self.data_source {
            FieldDataSource::Owned => self.field_data.clone_into(target),
            FieldDataSource::Cow(src) => {
                fm.fields[src].borrow().iter_hall.append_to(fm, target)
            }
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => {
                fm.fields[src_field]
                    .borrow()
                    .iter_hall
                    .append_to(fm, target);
            }
            FieldDataSource::RecordBufferCow(data_ref) => {
                target.append_from_other(unsafe { &*(*data_ref).get() });
            }
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                target.headers.extend_from_slice(&self.field_data.headers);
                target.field_count += self.field_data.field_count;
                unsafe { &*(*data_ref).get() }
                    .append_data_to(&mut target.data);
            }
        }
    }
    pub fn get_field_count(&self, fm: &FieldManager) -> usize {
        match self.data_source {
            FieldDataSource::Owned
            | FieldDataSource::DataCow { .. }
            | FieldDataSource::RecordBufferDataCow(_) => {
                self.field_data.field_count
            }
            FieldDataSource::Cow(src) => {
                fm.fields[src].borrow().iter_hall.get_field_count(fm)
            }
            FieldDataSource::RecordBufferCow(data_ref) => {
                let fd = &unsafe { &*(*data_ref).get() };
                fd.field_count
            }
        }
    }
    // returns the FieldId that was COWd and now needs a ref count drop
    // sadly we can't drop ourselves because we currently borrow a field
    pub fn uncow_get_field_with_rc(
        &mut self,
        fm: &FieldManager,
    ) -> Option<FieldId> {
        match self.data_source {
            FieldDataSource::Owned => None,
            FieldDataSource::Cow(src_id) => {
                debug_assert!(self.field_data.is_empty());
                let src = fm.fields[src_id].borrow();
                self.field_data.append_from_other(&src.iter_hall.field_data);
                src.iter_hall.append_to(fm, &mut self.field_data);
                self.data_source = FieldDataSource::Owned;
                Some(src_id)
            }
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => {
                debug_assert!(self.field_data.is_empty());
                let src = fm.fields[src_field].borrow();
                src.iter_hall.append_data_to(fm, &mut self.field_data.data);
                Some(src_field) // TODO: fix up header_iter
            }
            FieldDataSource::RecordBufferCow(data_ref) => {
                debug_assert!(self.field_data.is_empty());
                self.field_data = unsafe { &*(*data_ref).get() }.clone();
                None
            }
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                debug_assert!(self.field_data.is_empty());
                unsafe { &*(*data_ref).get() }
                    .append_data_to(&mut self.field_data.data);
                None
            }
        }
    }
    pub fn uncow_headers(&mut self, fm: &FieldManager) {
        if self.are_headers_owned() {
            return;
        }
        match self.data_source {
            FieldDataSource::Owned => unreachable!(),
            FieldDataSource::DataCow { .. } => unreachable!(),
            FieldDataSource::RecordBufferDataCow(_) => unreachable!(),
            FieldDataSource::Cow(src_field_id) => {
                debug_assert!(self.field_data.is_empty());
                let mut src_field = fm.fields[src_field_id].borrow_mut();
                self.field_data.field_count = src_field
                    .iter_hall
                    .append_headers_to(fm, &mut self.field_data.headers);
                let header_iter = src_field.iter_hall.claim_iter_at_end(fm);
                self.data_source = FieldDataSource::DataCow {
                    src_field: src_field_id,
                    header_iter,
                };
            }
            FieldDataSource::RecordBufferCow(rb) => {
                let fd = unsafe { &*(*rb).get() };
                debug_assert!(self.field_data.is_empty());
                self.field_data.field_count = fd.field_count;
                self.field_data.headers.extend_from_slice(&fd.headers);
                self.data_source = FieldDataSource::RecordBufferDataCow(rb);
            }
        };
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
            self.get_owned_data().push_variable_sized_type(
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
            self.get_owned_data().push_fixed_size_type(
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
            self.get_owned_data().push_zst_unchecked(
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
            self.get_owned_data().push_variable_sized_type_uninit(
                kind, flags, data_len, run_length,
            )
        }
    }
}
impl IterHall {
    pub fn dup_last_value(&mut self, run_length: usize) {
        self.get_owned_data().dup_last_value(run_length);
    }
    pub fn drop_last_value(&mut self, run_length: usize) {
        self.get_owned_data().drop_last_value(run_length);
    }
}
