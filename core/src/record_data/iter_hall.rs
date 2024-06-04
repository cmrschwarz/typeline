use core::panic;
use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    marker::PhantomData,
};

use thin_vec::ThinVec;

use crate::{
    operators::transform::TransformId,
    utils::{debuggable_nonmax::DebuggableNonMaxU32, universe::Universe},
};

use super::{
    bytes_insertion_stream::{
        BytesInsertionStream, MaybeTextInsertionStream, TextInsertionStream,
    },
    field::{FieldId, FieldManager},
    field_data::{
        FieldData, FieldDataBuffer, FieldDataInternals, FieldDataInternalsMut,
        FieldValueFlags, FieldValueHeader, FieldValueRepr, FieldValueType,
        RunLength,
    },
    fixed_sized_type_inserter::FixedSizeTypeInserter,
    iters::{FieldDataRef, FieldIter, FieldIterator},
    match_set::MatchSetManager,
    push_interface::PushInterface,
    ref_iter::AutoDerefIter,
    variable_sized_type_inserter::{
        InlineBytesInserter, InlineStringInserter, VariableSizeTypeInserter,
    },
    varying_type_inserter::VaryingTypeInserter,
};

pub type IterId = DebuggableNonMaxU32;

/// A COW Field reflects the state of another field **at a certain point
/// in time**. This means that when source or target change, the other side
/// remains unaffected (semantically). In practice, we have to change from
/// `FullCow` to `DataCow` in the target whenever one side changes, unless
/// all fields from the source have already been deleted in the target.
/// Source and target can be re-synced by calling the
/// `MatchSet::update_cow_targets` method.
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct CowDataSource {
    pub src_field_id: FieldId,
    // When the data source gets appended, we need to know up to which point
    // we already copied when appending.
    // We also need this in case the headers are cow'ed because the source
    // could be amended
    pub header_iter_id: IterId,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum FieldDataSource {
    #[default]
    Owned,
    Alias(FieldId),
    FullCow(CowDataSource),
    DataCow(CowDataSource),
    RecordBufferFullCow(*const UnsafeCell<FieldData>),
    RecordBufferDataCow(*const UnsafeCell<FieldData>),
}
// SAFETY: We make sure that the referenced RecordBufferField (and the
// UnsafeCell<FieldData> inside it) does not get modified while any fields are
// actively using it
unsafe impl Send for FieldDataSource {}

#[derive(Default)]
pub struct IterHall {
    pub(super) data_source: FieldDataSource,
    pub(super) field_data: FieldData,
    pub(super) iters: Universe<IterId, Cell<IterState>>,
    pub(super) cow_targets: ThinVec<FieldId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterKind {
    Undefined, // used in release mode
    Transform(TransformId),
    CowField(FieldId),
    RefLookup,
}

/// Position of an iterator inside of `FieldData` to be stored inside of an
/// `IterHall`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IterState {
    pub(super) field_pos: usize,
    // Will **not** include leading padding introduced by the current header.
    pub(super) data: usize,
    // The `header_idx` will never be greater than or equal to the field's
    // header count, unless that count is 0. This means that we have to
    // push an iterator that reached the end of the field slightly
    // backwards to sit after the last `header_rl_offset` on the previous
    // header. We do this to avoid appends from having to adjust iters
    // that are 'after' them.
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    #[cfg(feature = "debug_logging")]
    pub(super) kind: IterKind,
}

impl IterState {
    pub fn is_valid(&self) -> bool {
        self.field_pos != usize::MAX
    }
    pub fn invalidate(&mut self) {
        self.field_pos = usize::MAX
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CowVariant {
    FullCow,
    DataCow,
    RecordBufferDataCow,
    RecordBufferFullCow,
}

impl FieldDataSource {
    pub fn cow_variant(&self) -> Option<CowVariant> {
        match self {
            FieldDataSource::Owned | FieldDataSource::Alias(_) => None,
            FieldDataSource::FullCow(_) => Some(CowVariant::FullCow),
            FieldDataSource::DataCow(_) => Some(CowVariant::DataCow),
            FieldDataSource::RecordBufferFullCow(_) => {
                Some(CowVariant::RecordBufferFullCow)
            }
            FieldDataSource::RecordBufferDataCow(_) => {
                Some(CowVariant::RecordBufferDataCow)
            }
        }
    }
}

impl IterHall {
    pub fn get_iter_state_at_begin(
        &self,
        #[allow(unused)] kind: IterKind,
    ) -> IterState {
        IterState {
            field_pos: 0,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
            #[cfg(feature = "debug_logging")]
            kind,
        }
    }
    pub fn get_iter_state_at_end(
        &self,
        fm: &FieldManager,
        kind: IterKind,
    ) -> IterState {
        match self.data_source {
            FieldDataSource::Owned | FieldDataSource::DataCow(_) => (),
            FieldDataSource::Alias(src_field_id) => {
                return fm.fields[src_field_id]
                    .borrow()
                    .iter_hall
                    .get_iter_state_at_end(fm, kind);
            }
            FieldDataSource::FullCow(CowDataSource {
                src_field_id,
                header_iter_id,
            }) => {
                let iter = fm.fields[src_field_id].borrow().iter_hall.iters
                    [header_iter_id]
                    .get();
                #[cfg(feature = "debug_logging")]
                let iter = IterState { kind, ..iter };
                return iter;
            }
            FieldDataSource::RecordBufferFullCow(_) => todo!(),
            FieldDataSource::RecordBufferDataCow(_) => todo!(),
        }

        if self.field_data.field_count == 0 {
            return self.get_iter_state_at_begin(kind);
        }
        IterState {
            field_pos: self.field_data.field_count,
            data: self.get_field_data_len(fm)
                - self
                    .field_data
                    .headers
                    .back()
                    .map(FieldValueHeader::total_size_unique)
                    .unwrap_or(0),
            // TODO: respect cow
            header_idx: self.field_data.headers.len() - 1,
            header_rl_offset: self
                .field_data
                .headers
                .back()
                .unwrap()
                .run_length,
            #[cfg(feature = "debug_logging")]
            kind,
        }
    }

    pub fn claim_iter(&mut self, kind: IterKind) -> IterId {
        self.iters
            .claim_with_value(Cell::new(self.get_iter_state_at_begin(kind)))
    }

    pub fn get_field_data_len(&self, fm: &FieldManager) -> usize {
        match self.data_source {
            FieldDataSource::Owned => self.field_data.data.len(),
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::DataCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::Alias(src_field_id) => fm.fields[src_field_id]
                .borrow()
                .iter_hall
                .get_field_data_len(fm),
            FieldDataSource::RecordBufferFullCow(data)
            | FieldDataSource::RecordBufferDataCow(data) => {
                unsafe { &*(*data).get() }.data.len()
            }
        }
    }
    pub fn claim_iter_at_end(
        &mut self,
        fm: &FieldManager,
        kind: IterKind,
    ) -> IterId {
        self.iters
            .claim_with_value(Cell::new(self.get_iter_state_at_end(fm, kind)))
    }
    pub fn reserve_iter_id(&mut self, iter_id: IterId, kind: IterKind) {
        let v = Cell::new(self.get_iter_state_at_begin(kind));
        self.iters.reserve_id_with(iter_id, || v);
    }
    pub fn release_iter(&mut self, iter_id: IterId) {
        self.iters[iter_id].get_mut().invalidate();
        self.iters.release(iter_id)
    }
    pub fn get_iter_state(&self, iter_id: IterId) -> IterState {
        self.iters[iter_id].get()
    }
    fn calculate_start_header<'a, R: FieldDataRef<'a>>(
        fr: &R,
        state: &mut IterState,
    ) -> FieldValueHeader {
        if state.header_idx == fr.headers().len() {
            debug_assert!(state.header_idx == 0);
            return FieldValueHeader::default();
        }
        let mut h = fr.headers()[state.header_idx];
        if h.run_length != state.header_rl_offset {
            return h;
        }
        state.header_idx += 1;
        state.header_rl_offset = 0;
        state.data += h.total_size_unique();
        if state.header_idx == fr.headers().len() {
            return FieldValueHeader::default();
        }
        h = fr.headers()[state.header_idx];
        h
    }
    // SAFETY: caller must ensure that the state comes from this data source
    pub unsafe fn get_iter_from_state_unchecked<'a, R: FieldDataRef<'a>>(
        &self,
        fr: R,
        mut state: IterState,
    ) -> FieldIter<'a, R> {
        let h = Self::calculate_start_header(&fr, &mut state);
        let mut res = FieldIter {
            fdr: fr,
            field_pos: state.field_pos,
            data: state.data + h.leading_padding(),
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
            _phantom_data: PhantomData,
        };
        res.skip_dead_fields();
        res
    }
    // SAFETY: caller must ensure that the iter uses the correct data source
    pub unsafe fn store_iter_unchecked<'a, R: FieldDataRef<'a>>(
        &self,
        #[allow(unused)] field_id: FieldId,
        iter_id: IterId,
        mut iter: FieldIter<'a, R>,
    ) {
        let mut state = self.iters[iter_id].get();
        state.field_pos = iter.field_pos;
        state.header_rl_offset = iter.header_rl_offset;
        // we use the field count from the iter becase the field might be cow
        if iter.header_rl_offset == 0
            && iter.field_pos == iter.fdr.field_count()
        {
            // Uphold the 'no `IterState` on the last header except 0'
            // invariant.
            if iter.field_pos == 0 {
                // When our header index is already 0, resetting is a noop.
                // The other case (header_idx > 0) happens if all fields before
                // are deleted. Calling `prev_field`, like in
                // the other branch, would fail here, but
                // having the iterator sit at 0/0 works out.
                self.reset_iter(iter_id);
                return;
            }
            iter.prev_field();
            state.header_rl_offset = iter.field_run_length_bwd() + 1;
        }
        state.header_idx = iter.header_idx;
        state.data = iter.data - iter.header_fmt.leading_padding();
        // #[cfg(feature = "iter_state_logging")]
        // eprintln!("storing iter for field {field_id:02}: {state:?}");
        self.iters[iter_id].set(state);
    }
    pub unsafe fn store_iter_state_unchecked(
        &self,
        iter_id: IterId,
        iter_state: IterState,
    ) {
        self.iters[iter_id].set(iter_state);
    }

    /// returns a tuple of `(field_data, initial_field_offset, field_count)`
    pub fn internals(&self) -> FieldDataInternals {
        self.field_data.internals()
    }
    /// returns a tuple of `(field_data, initial_field_offset, field_count)`
    pub unsafe fn internals_mut(&mut self) -> FieldDataInternalsMut {
        unsafe { self.get_owned_data_mut().internals_mut() }
    }
    pub unsafe fn raw(&mut self) -> &mut FieldData {
        self.get_owned_data_mut()
    }

    pub fn copy<'a>(
        iter: &mut impl FieldIterator<'a>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut IterHall)),
    ) -> usize {
        let adapted_target_applicator =
            &mut |f: &mut dyn FnMut(&mut FieldData)| {
                let g =
                    &mut |fdih: &mut IterHall| f(fdih.get_owned_data_mut());
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
                let g =
                    &mut |fdih: &mut IterHall| f(fdih.get_owned_data_mut());
                targets_applicator(g);
            };
        FieldData::copy_resolve_refs(
            match_set_mgr,
            iter,
            adapted_target_applicator,
        )
    }
    pub fn alias_source(&self) -> Option<FieldId> {
        match self.data_source {
            FieldDataSource::Alias(src) => Some(src),
            _ => None,
        }
    }
    // source field of cow, data cow only
    pub fn cow_source_field(
        &self,
        fm: &FieldManager,
    ) -> (Option<FieldId>, Option<bool>) {
        match self.data_source {
            FieldDataSource::Owned => (None, None),
            FieldDataSource::Alias(src) => {
                fm.fields[src].borrow().iter_hall.cow_source_field(fm)
            }
            FieldDataSource::FullCow(cds) => {
                (Some(cds.src_field_id), Some(false))
            }
            FieldDataSource::DataCow(cds) => {
                (Some(cds.src_field_id), Some(true))
            }
            FieldDataSource::RecordBufferFullCow(_) => (None, Some(false)),
            FieldDataSource::RecordBufferDataCow(_) => (None, Some(true)),
        }
    }
    pub fn get_iter_kind(&self, #[allow(unused)] iter_id: IterId) -> IterKind {
        #[cfg(not(feature = "debug_logging"))]
        return IterKind::Undefined;
        #[cfg(feature = "debug_logging")]
        return self.iters[iter_id].get().kind;
    }
    pub fn reset_iter(&self, iter_id: IterId) {
        self.iters[iter_id]
            .set(self.get_iter_state_at_begin(self.get_iter_kind(iter_id)));
    }
    pub fn reset_iterators(&mut self) {
        for (i, _) in &mut self.iters.iter_enumerated() {
            self.reset_iter(i)
        }
    }
    pub fn reset_cow_headers(&mut self, fm: &FieldManager) {
        match &mut self.data_source {
            FieldDataSource::Owned | FieldDataSource::FullCow(_) => (),
            FieldDataSource::Alias(src) => {
                fm.fields[*src].borrow_mut().iter_hall.reset_cow_headers(fm)
            }
            FieldDataSource::DataCow(cds) => {
                self.data_source = FieldDataSource::FullCow(*cds)
            }
            FieldDataSource::RecordBufferFullCow(_) => todo!(),
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                self.data_source =
                    FieldDataSource::RecordBufferFullCow(*data_ref)
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
            iters: Universe::default(),
            cow_targets: ThinVec::new(),
        }
    }
    pub fn fixed_size_type_inserter<T: FieldValueType + PartialEq + Clone>(
        &mut self,
    ) -> FixedSizeTypeInserter<T> {
        FixedSizeTypeInserter::<T>::new(self.get_owned_data_mut())
    }
    pub fn inline_bytes_inserter(&mut self) -> InlineBytesInserter {
        InlineBytesInserter::new(self.get_owned_data_mut())
    }
    pub fn inline_str_inserter(&mut self) -> InlineStringInserter {
        InlineStringInserter::new(self.get_owned_data_mut())
    }
    pub fn varying_type_inserter(
        &mut self,
    ) -> VaryingTypeInserter<&mut FieldData> {
        VaryingTypeInserter::new(self.get_owned_data_mut())
    }
    fn get_owned_data_mut(&mut self) -> &mut FieldData {
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
                header_tgt.extend(&self.field_data.headers);
                self.field_data.field_count
            }
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::Alias(src_field_id) => fm.fields[src_field_id]
                .borrow()
                .iter_hall
                .append_headers_to(fm, header_tgt),
            FieldDataSource::RecordBufferFullCow(rb) => {
                let fd = unsafe { &*(*rb).get() };
                header_tgt.extend(&fd.headers);
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
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::Alias(src_field_id) => {
                fm.fields[src_field_id]
                    .borrow()
                    .iter_hall
                    .append_data_to(fm, target);
            }
            FieldDataSource::DataCow(cds) => fm.fields[cds.src_field_id]
                .borrow()
                .iter_hall
                .append_data_to(fm, target),
            FieldDataSource::RecordBufferDataCow(data_ref)
            | FieldDataSource::RecordBufferFullCow(data_ref) => {
                unsafe { &*(*data_ref).get() }.append_data_to(target);
            }
        }
    }
    pub fn append_to(&self, fm: &FieldManager, target: &mut FieldData) {
        match self.data_source {
            FieldDataSource::Owned => self.field_data.clone_into(target),
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::Alias(src_field_id) => fm.fields[src_field_id]
                .borrow()
                .iter_hall
                .append_to(fm, target),
            FieldDataSource::DataCow(cds) => {
                fm.fields[cds.src_field_id]
                    .borrow()
                    .iter_hall
                    .append_to(fm, target);
            }
            FieldDataSource::RecordBufferFullCow(data_ref) => {
                target.append_from_other(unsafe { &*(*data_ref).get() });
            }
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                target.headers.extend(&self.field_data.headers);
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
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::Alias(src_field_id) => fm.fields[src_field_id]
                .borrow()
                .iter_hall
                .get_field_count(fm),
            FieldDataSource::RecordBufferFullCow(data_ref) => {
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
            FieldDataSource::Alias(src) => fm.fields[src]
                .borrow_mut()
                .iter_hall
                .uncow_get_field_with_rc(fm),
            FieldDataSource::FullCow(cds) => {
                debug_assert!(self.field_data.is_empty());
                let src = fm.fields[cds.src_field_id].borrow();
                self.field_data.append_from_other(&src.iter_hall.field_data);
                src.iter_hall.append_to(fm, &mut self.field_data);
                self.data_source = FieldDataSource::Owned;
                Some(cds.src_field_id)
            }
            FieldDataSource::DataCow(cds) => {
                debug_assert!(self.field_data.is_empty());
                let src = fm.fields[cds.src_field_id].borrow();
                src.iter_hall.append_data_to(fm, &mut self.field_data.data);
                Some(cds.src_field_id) // TODO: fix up header_iter
            }
            FieldDataSource::RecordBufferFullCow(data_ref) => {
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
    pub(super) fn get_cow_data_source_mut(
        &mut self,
    ) -> Option<&mut CowDataSource> {
        match &mut self.data_source {
            FieldDataSource::FullCow(cds) | FieldDataSource::DataCow(cds) => {
                Some(cds)
            }
            FieldDataSource::Owned
            | FieldDataSource::Alias(_)
            | FieldDataSource::RecordBufferFullCow(_)
            | FieldDataSource::RecordBufferDataCow(_) => None,
        }
    }
    pub(crate) fn copy_headers_from_cow_src(
        &mut self,
        src_headers: &VecDeque<FieldValueHeader>,
        cow_end: IterState,
    ) {
        self.field_data
            .headers
            .extend(src_headers.range(..cow_end.header_idx));
        if cow_end.header_rl_offset != 0 {
            let mut last_header = src_headers[cow_end.header_idx];
            last_header.run_length =
                cow_end.header_rl_offset.min(last_header.run_length);
            self.field_data.headers.push_back(last_header);
        }
        self.field_data.field_count += cow_end.field_pos;
    }
    pub(crate) fn uncow_headers(&mut self, fm: &FieldManager) {
        match self.data_source {
            FieldDataSource::Owned
            | FieldDataSource::DataCow { .. }
            | FieldDataSource::RecordBufferDataCow(_) => (),
            FieldDataSource::Alias(src_field_id) => {
                fm.fields[src_field_id]
                    .borrow_mut()
                    .iter_hall
                    .uncow_headers(fm);
            }
            FieldDataSource::FullCow(cds) => {
                debug_assert!(self.field_data.is_empty());
                let cow_end =
                    fm.fields[cds.src_field_id].borrow().iter_hall.iters
                        [cds.header_iter_id]
                        .get();
                let src_field = fm.get_cow_field_ref_raw(cds.src_field_id);
                let src_headers = src_field.headers();
                self.copy_headers_from_cow_src(src_headers, cow_end);
                self.data_source = FieldDataSource::DataCow(cds);
            }
            FieldDataSource::RecordBufferFullCow(rb) => {
                // TODO: add an iterator to these aswell
                let fd = unsafe { &*(*rb).get() };
                debug_assert!(self.field_data.is_empty());
                self.field_data.field_count = fd.field_count;
                self.field_data.headers.extend(&fd.headers);
                self.data_source = FieldDataSource::RecordBufferDataCow(rb);
            }
        };
    }
}

unsafe impl PushInterface for IterHall {
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: FieldValueRepr,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.get_owned_data_mut()
                .push_variable_sized_type_unchecked(
                    kind,
                    data,
                    run_length,
                    try_header_rle,
                    try_data_rle,
                );
        }
    }

    unsafe fn push_fixed_size_type_unchecked<T: PartialEq + FieldValueType>(
        &mut self,
        kind: FieldValueRepr,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.get_owned_data_mut().push_fixed_size_type_unchecked(
                kind,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        unsafe {
            self.get_owned_data_mut().push_zst_unchecked(
                kind,
                flags,
                run_length,
                try_header_rle,
            );
        }
    }
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueRepr,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        unsafe {
            self.get_owned_data_mut().push_variable_sized_type_uninit(
                kind,
                data_len,
                run_length,
                try_header_rle,
            )
        }
    }

    fn bytes_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> BytesInsertionStream {
        self.field_data.bytes_insertion_stream(run_length)
    }

    fn text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> TextInsertionStream {
        self.field_data.text_insertion_stream(run_length)
    }
    fn maybe_text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> MaybeTextInsertionStream {
        self.field_data.maybe_text_insertion_stream(run_length)
    }
}
impl IterHall {
    pub fn dup_last_value(&mut self, run_length: usize) {
        self.get_owned_data_mut().dup_last_value(run_length);
    }
    pub fn drop_last_value(&mut self, run_length: usize) {
        self.get_owned_data_mut().drop_last_value(run_length);
    }
}

impl Drop for IterHall {
    fn drop(&mut self) {
        if !matches!(self.data_source, FieldDataSource::Owned) {
            // we don't want the destructor for
            // `FieldData` to assume that headers and data match
            // and drop the data contents, so we clear them beforehand
            self.field_data.headers.clear();
            self.field_data.data.clear();
        }
    }
}
