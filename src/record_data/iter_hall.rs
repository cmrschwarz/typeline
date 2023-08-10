use core::panic;
use std::cell::{Cell, UnsafeCell};

use nonmax::NonMaxU32;
use thin_vec::ThinVec;

use crate::utils::{aligned_buf::AlignedBuf, universe::Universe};

use super::{
    field::{FieldId, FieldManager},
    field_data::{
        FieldData, FieldDataInternals, FieldValueFlags, FieldValueHeader,
        FieldValueKind, RunLength, MAX_FIELD_ALIGN,
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

pub(super) enum FieldDataSource {
    Owned(FieldData),
    Cow(FieldId),
    DataCow {
        headers: Vec<FieldValueHeader>,
        field_count: usize,
        data_ref: FieldId,
    },
    #[allow(dead_code)] // TODO
    RecordBufferCow(*const UnsafeCell<FieldData>),
    RecordBufferDataCow {
        headers: Vec<FieldValueHeader>,
        field_count: usize,
        data_ref: *const UnsafeCell<FieldData>,
    },
}

impl Default for FieldDataSource {
    fn default() -> Self {
        FieldDataSource::Owned(FieldData::default())
    }
}
impl FieldDataSource {
    pub fn is_data_owned(&self) -> bool {
        match self {
            FieldDataSource::Owned(_) => true,
            FieldDataSource::Cow(_) => false,
            FieldDataSource::DataCow { .. } => false,
            FieldDataSource::RecordBufferCow(_) => false,
            FieldDataSource::RecordBufferDataCow { .. } => false,
        }
    }
    pub fn are_headers_owned(&self) -> bool {
        match self {
            FieldDataSource::Owned(_) => true,
            FieldDataSource::DataCow { .. } => true,
            FieldDataSource::RecordBufferDataCow { .. } => true,
            FieldDataSource::Cow(_) => false,
            FieldDataSource::RecordBufferCow(_) => false,
        }
    }
    pub fn get_headers_cloned(
        &self,
        fm: &FieldManager,
    ) -> (Vec<FieldValueHeader>, usize) {
        match self {
            FieldDataSource::Owned(fd) => (fd.headers.clone(), fd.field_count),
            FieldDataSource::Cow(src) => fm.fields[*src]
                .borrow()
                .field_data
                .data_source
                .get_headers_cloned(fm),
            FieldDataSource::DataCow {
                headers,
                field_count,
                ..
            } => (headers.clone(), *field_count),
            FieldDataSource::RecordBufferDataCow {
                headers,
                field_count,
                ..
            } => (headers.clone(), *field_count),
            FieldDataSource::RecordBufferCow(rb) => {
                let fd = unsafe { &*(**rb).get() };
                (fd.headers.clone(), fd.field_count)
            }
        }
    }
    pub fn get_data_cloned(
        &self,
        fm: &FieldManager,
    ) -> AlignedBuf<MAX_FIELD_ALIGN> {
        match self {
            FieldDataSource::Owned(fd) => fd.clone_data(),
            FieldDataSource::Cow(src) => fm.fields[*src]
                .borrow()
                .field_data
                .data_source
                .get_data_cloned(fm),
            FieldDataSource::DataCow { data_ref, .. } => fm.fields[*data_ref]
                .borrow()
                .field_data
                .data_source
                .get_data_cloned(fm),
            FieldDataSource::RecordBufferDataCow { data_ref: rb, .. }
            | FieldDataSource::RecordBufferCow(rb) => {
                unsafe { &*(**rb).get() }.clone_data()
            }
        }
    }
    pub fn get_cloned(&self, fm: &FieldManager) -> FieldData {
        match self {
            FieldDataSource::Owned(fd) => fd.clone(),
            FieldDataSource::Cow(src) => fm.fields[*src]
                .borrow()
                .field_data
                .data_source
                .get_cloned(fm),
            FieldDataSource::DataCow {
                headers,
                field_count,
                data_ref,
            } => FieldData {
                headers: (*headers).clone(),
                field_count: *field_count,
                data: fm.fields[*data_ref]
                    .borrow()
                    .field_data
                    .data_source
                    .get_data_cloned(fm),
            },
            FieldDataSource::RecordBufferCow(rb) => {
                unsafe { &*(**rb).get() }.clone()
            }
            FieldDataSource::RecordBufferDataCow {
                headers,
                field_count,
                data_ref: data,
            } => FieldData {
                headers: headers.clone(),
                field_count: *field_count,
                data: unsafe { &*(**data).get() }.data.clone(),
            },
        }
    }
    pub fn get_field_count(&self, fm: &FieldManager) -> usize {
        match &self {
            FieldDataSource::Owned(fd) => fd.field_count,
            FieldDataSource::Cow(src) => fm.fields[*src]
                .borrow()
                .field_data
                .data_source
                .get_field_count(fm),
            FieldDataSource::DataCow { field_count, .. } => *field_count,
            FieldDataSource::RecordBufferCow(rb) => {
                let fd = &unsafe { &*(**rb).get() };
                fd.field_count
            }
            FieldDataSource::RecordBufferDataCow { field_count, .. } => {
                *field_count
            }
        }
    }
    // returns the FieldId that was COWd and now needs a ref count drop
    // sadly we can't drop ourselves because we currently borrow a field
    pub fn uncow_get_field_with_rc(
        &mut self,
        fm: &FieldManager,
    ) -> Option<FieldId> {
        if self.is_data_owned() {
            return None;
        }
        let temp = std::mem::replace(
            self,
            FieldDataSource::Cow(NonMaxU32::default()),
        );
        let res;
        (res, *self) = match temp {
            FieldDataSource::Owned(_) => unreachable!(),
            FieldDataSource::Cow(src_id) => {
                let src = fm.fields[src_id].borrow();
                (
                    Some(src_id),
                    FieldDataSource::Owned(
                        src.field_data.data_source.get_cloned(fm),
                    ),
                )
            }
            FieldDataSource::DataCow {
                headers,
                field_count,
                data_ref,
            } => {
                let src = fm.fields[data_ref].borrow();
                (
                    Some(data_ref),
                    FieldDataSource::Owned(FieldData {
                        headers,
                        field_count,
                        data: src.field_data.data_source.get_data_cloned(fm),
                    }),
                )
            }
            FieldDataSource::RecordBufferCow(rb) => (
                None,
                FieldDataSource::Owned(unsafe { &*(*rb).get() }.clone()),
            ),
            FieldDataSource::RecordBufferDataCow {
                headers,
                field_count,
                data_ref: data,
            } => (
                None,
                FieldDataSource::Owned(FieldData {
                    headers,
                    field_count,
                    data: unsafe { &*(*data).get() }.data.clone(),
                }),
            ),
        };
        res
    }
    pub fn uncow_headers(&mut self, fm: &FieldManager) {
        if self.are_headers_owned() {
            return;
        }
        let temp = std::mem::replace(
            self,
            FieldDataSource::Cow(NonMaxU32::default()),
        );
        *self = match temp {
            FieldDataSource::Owned(_) => unreachable!(),
            FieldDataSource::DataCow { .. } => unreachable!(),
            FieldDataSource::RecordBufferDataCow { .. } => unreachable!(),
            FieldDataSource::Cow(src) => {
                let (headers, field_count) = fm.fields[src]
                    .borrow()
                    .field_data
                    .data_source
                    .get_headers_cloned(fm);
                FieldDataSource::DataCow {
                    headers,
                    field_count,
                    data_ref: src,
                }
            }
            FieldDataSource::RecordBufferCow(rb) => {
                let fd = unsafe { &*(*rb).get() };
                FieldDataSource::RecordBufferDataCow {
                    headers: fd.headers.clone(),
                    field_count: fd.field_count,
                    data_ref: rb,
                }
            }
        };
    }
}

#[derive(Default)]
pub struct IterHall {
    pub(super) data_source: FieldDataSource,
    pub(super) iters: Universe<IterId, Cell<IterState>>,
    pub(super) cow_targets: ThinVec<FieldId>,
}
unsafe impl Send for IterHall {}

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
        self.iters[iter_id].set(state);
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
    pub fn is_data_owned(&self) -> bool {
        self.data_source.is_data_owned()
    }
    pub fn are_headers_owned(&self) -> bool {
        self.data_source.are_headers_owned()
    }
    // source field of cow, data cow only
    pub fn cow_source_field(&self) -> (Option<FieldId>, Option<bool>) {
        match self.data_source {
            FieldDataSource::Owned(_) => (None, None),
            FieldDataSource::Cow(src) => (Some(src), Some(false)),
            FieldDataSource::DataCow { data_ref, .. } => {
                (Some(data_ref), Some(true))
            }
            FieldDataSource::RecordBufferCow(_) => (None, Some(false)),
            FieldDataSource::RecordBufferDataCow { .. } => (None, Some(true)),
        }
    }
    pub fn field_count(&self, fm: &FieldManager) -> usize {
        // TOOD: maybe handle the data cow cases here?
        self.data_source.get_field_count(fm)
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
            FieldDataSource::Owned(_) => (),
            FieldDataSource::Cow(_) => (),
            FieldDataSource::DataCow { data_ref, .. } => {
                self.data_source = FieldDataSource::Cow(*data_ref)
            }
            FieldDataSource::RecordBufferCow(_) => todo!(),
            FieldDataSource::RecordBufferDataCow {
                data_ref: data, ..
            } => self.data_source = FieldDataSource::RecordBufferCow(*data),
        }
    }
    pub fn reset_with_data(&mut self, fd: FieldData) {
        self.reset_iterators();
        self.data_source = FieldDataSource::Owned(fd);
    }
    pub fn new_with_data(fd: FieldData) -> Self {
        Self {
            data_source: FieldDataSource::Owned(fd),
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
            FieldDataSource::Owned(fd) => fd,
            _ => panic!("IterHall uses COW!"),
        }
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
