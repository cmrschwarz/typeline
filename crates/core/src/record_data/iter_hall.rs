use core::panic;
use std::{
    cell::{Cell, UnsafeCell},
    cmp::Ordering,
    collections::VecDeque,
};

use thin_vec::ThinVec;

use crate::operators::transform::TransformId;
use indexland::{debuggable_nonmax::DebuggableNonMaxU32, universe::Universe};

use super::{
    action_buffer::ActorId,
    bytes_insertion_stream::{
        BytesInsertionStream, MaybeTextInsertionStream, TextInsertionStream,
    },
    field::{FieldId, FieldManager},
    field_data::{
        FieldData, FieldDataBuffer, FieldDataInternals, FieldDataInternalsMut,
        FieldValueFlags, FieldValueHeader, FieldValueRepr, FieldValueType,
        RunLength,
    },
    field_data_ref::FieldDataRef,
    fixed_sized_type_inserter::FixedSizeTypeInserter,
    iter::{
        field_iter::FieldIter, field_iterator::FieldIterator,
        ref_iter::AutoDerefIter,
    },
    match_set::MatchSetManager,
    push_interface::PushInterface,
    variable_sized_type_inserter::{
        InlineBytesInserter, InlineStringInserter, VariableSizeTypeInserter,
    },
    varying_type_inserter::VaryingTypeInserter,
};

pub type FieldIterId = DebuggableNonMaxU32;

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
    pub header_iter_id: FieldIterId,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum FieldDataSource {
    #[default]
    Owned,
    Alias(FieldId),
    FullCow(CowDataSource),
    SameMsCow(FieldId),
    DataCow {
        source: CowDataSource,
        observed_data_size: usize,
    },
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
    pub(super) iters: Universe<FieldIterId, Cell<IterState>>,
    pub(super) cow_targets: ThinVec<FieldId>,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterKind {
    #[default]
    Undefined, // used in release mode
    Transform(TransformId),
    CowField(FieldId),
    RefLookup,
}

/// Position of an iterator inside of `FieldData` to be stored inside of an
/// `IterHall`.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct IterState {
    pub field_pos: usize,
    // Will **not** include leading padding introduced by the current header.
    // We have to take out the leading padding because that could
    // potentially be lowered by some forkcat data drop shenanegans
    // so in order for our iter state to be stable we have to store
    // it this way
    pub header_start_data_pos_pre_padding: usize,
    // The `header_idx` will never be greater than or equal to the field's
    // header count, unless that count is 0. This means that we have to
    // push an iterator that reached the end of the field slightly
    // backwards to sit after the last `header_rl_offset` on the previous
    // header. We do this to avoid appends from having to adjust iters
    // that are 'after' them.
    pub header_idx: usize,
    pub header_rl_offset: RunLength,

    /// In case of dups, iterators always lean left
    /// (-> stay before the dup) because we assume that affected transforms
    /// are *after* the emitting actor, as transforms before it should
    /// have advanced their iterators beyond any passed records.
    /// When the transform observing an iterator is *before* the actor
    /// that does an insert, we want the iterator to be advanced,
    /// because the transform should not observe inserts that happen after it.
    /// If the transform observing the iterator is *after* the
    /// actor doing the insert, we want it to *not* be advanced because it
    /// should observe the inserted elements.
    /// This happens e.g. for cow position markers or just for
    /// named field refs like format happening e.g. after a count/sum that
    /// may insert zeroes for empty groups
    /// we use "first right leaning" instead of "last left leaning"
    /// so we can represent 'always left leaning' as `ActorId::MAX`
    /// instead of having to reserve actor id zero
    pub first_right_leaning_actor_id: ActorId,

    #[cfg(feature = "debug_state")]
    pub kind: IterKind,
}

// The main difference from `IterLocation` is that this struct
// stores the data position **of the current field position**
// instead of the data position at the start of the current header
#[derive(Clone, Copy, Default, Debug)]
pub struct FieldLocation {
    pub field_pos: usize,
    pub header_idx: usize,
    pub header_rl_offset: RunLength,
    // actual position of the data, unlike in `IterLocation` or `IterState`
    pub data_pos: usize,
}

// Unlike `FieldLocation`, stores the data position that a FieldIter
// would hold at this point instead of where the data of this field positon
// starts. This is the data position at the start of the current header,
// but after any leading padding.
// TODO: maybe refactor this and make the iter store its actual data position
// to simplify this mess.
#[derive(Clone, Copy, Default, Debug)]
pub struct IterLocation {
    pub field_pos: usize,
    pub header_idx: usize,
    pub header_rl_offset: RunLength,
    // data pos at the start of the current header after but leading padding
    // just in the way that `FieldIter` would store it
    pub header_start_data_pos_post_padding: usize,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct IterStateRaw {
    pub field_pos: usize,
    pub header_start_data_pos_pre_padding: usize,
    pub header_idx: usize,
    pub header_rl_offset: RunLength,
    pub first_right_leaning_actor_id: ActorId,
}

impl From<IterState> for IterStateRaw {
    fn from(is: IterState) -> Self {
        IterStateRaw {
            field_pos: is.field_pos,
            header_start_data_pos_pre_padding: is
                .header_start_data_pos_pre_padding,
            header_idx: is.header_idx,
            header_rl_offset: is.header_rl_offset,
            first_right_leaning_actor_id: is.first_right_leaning_actor_id,
        }
    }
}

impl PartialOrd for IterState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IterState {
    fn cmp(&self, rhs: &Self) -> Ordering {
        let mut ord = self.field_pos.cmp(&rhs.field_pos);
        if ord != Ordering::Equal {
            return ord;
        }
        // this can differ if one iter sits at the end of a header
        // and another at the start of the next
        ord = self.header_idx.cmp(&rhs.header_idx);
        if ord != Ordering::Equal {
            return ord;
        }

        // TODO: // HACK we shouldn't really need this
        // unless iterators sit on random positionw within dead
        // headers which they shouldn't. Fix those issues and get rid of this.
        ord = self.header_rl_offset.cmp(&rhs.header_rl_offset);
        if ord != Ordering::Equal {
            return ord;
        }
        // the smaller the min right leaning id is, the more right
        // leaning the iterator is, leading to a
        // potentially larger final position
        self.first_right_leaning_actor_id
            .cmp(&rhs.first_right_leaning_actor_id)
            .reverse()
    }
}

impl IterState {
    pub fn is_valid(&self) -> bool {
        self.field_pos != usize::MAX
    }
    pub fn invalidate(&mut self) {
        self.field_pos = usize::MAX
    }
    pub fn from_raw_with_dummy_kind(is: IterStateRaw) -> Self {
        IterState {
            field_pos: is.field_pos,
            header_start_data_pos_pre_padding: is
                .header_start_data_pos_pre_padding,
            header_idx: is.header_idx,
            header_rl_offset: is.header_rl_offset,
            first_right_leaning_actor_id: is.first_right_leaning_actor_id,
            #[cfg(feature = "debug_state")]
            kind: IterKind::Undefined,
        }
    }
    pub fn as_field_location(&self) -> FieldLocation {
        FieldLocation {
            field_pos: self.field_pos,
            header_idx: self.header_idx,
            header_rl_offset: self.header_rl_offset,
            data_pos: self.header_start_data_pos_pre_padding,
        }
    }
    #[cfg_attr(not(feature = "debug_state"), allow(clippy::unused_self))]
    fn iter_kind(&self) -> IterKind {
        #[cfg(feature = "debug_state")]
        return self.kind;

        #[cfg(not(feature = "debug_state"))]
        return IterKind::Undefined;
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CowVariant {
    FullCow,
    DataCow,
    SameMsCow,
    RecordBufferDataCow,
    RecordBufferFullCow,
}

impl FieldDataSource {
    pub fn cow_variant(&self) -> Option<CowVariant> {
        match self {
            FieldDataSource::Owned | FieldDataSource::Alias(_) => None,
            FieldDataSource::FullCow(_) => Some(CowVariant::FullCow),
            FieldDataSource::DataCow { .. } => Some(CowVariant::DataCow),
            FieldDataSource::SameMsCow(_) => Some(CowVariant::SameMsCow),
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
        first_right_leaning_actor_id: ActorId,
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        kind: IterKind,
    ) -> IterState {
        IterState {
            field_pos: 0,
            header_start_data_pos_pre_padding: 0,
            header_idx: 0,
            header_rl_offset: 0,
            first_right_leaning_actor_id,
            #[cfg(feature = "debug_state")]
            kind,
        }
    }
    pub fn get_iter_state_at_end(
        &self,
        fm: &FieldManager,
        first_right_leaning_actor_id: ActorId,
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        kind: IterKind,
    ) -> IterState {
        match self.data_source {
            FieldDataSource::Owned | FieldDataSource::DataCow { .. } => (),
            FieldDataSource::Alias(src_field_id)
            | FieldDataSource::SameMsCow(src_field_id) => {
                return fm.fields[src_field_id]
                    .borrow()
                    .iter_hall
                    .get_iter_state_at_end(
                        fm,
                        first_right_leaning_actor_id,
                        kind,
                    );
            }
            FieldDataSource::FullCow(CowDataSource {
                src_field_id,
                header_iter_id,
            }) => {
                let iter = fm.fields[src_field_id].borrow().iter_hall.iters
                    [header_iter_id]
                    .get();
                #[cfg(feature = "debug_state")]
                let iter = IterState { kind, ..iter };
                return iter;
            }
            FieldDataSource::RecordBufferFullCow(_) => todo!(),
            FieldDataSource::RecordBufferDataCow(_) => todo!(),
        }

        if self.field_data.field_count == 0 {
            return self
                .get_iter_state_at_begin(first_right_leaning_actor_id, kind);
        }
        IterState {
            field_pos: self.field_data.field_count,
            header_start_data_pos_pre_padding: self.get_field_data_len(fm)
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
            first_right_leaning_actor_id,
            #[cfg(feature = "debug_state")]
            kind,
        }
    }

    pub fn claim_iter(
        &mut self,
        first_left_leaning_actor_id: ActorId,
        kind: IterKind,
    ) -> FieldIterId {
        let iter_state =
            self.get_iter_state_at_begin(first_left_leaning_actor_id, kind);
        self.iters.claim_with_value(Cell::new(iter_state))
    }

    pub fn get_field_data_len(&self, fm: &FieldManager) -> usize {
        match self.data_source {
            FieldDataSource::Owned => self.field_data.data.len(),
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::SameMsCow(src_field_id)
            | FieldDataSource::DataCow {
                source: CowDataSource { src_field_id, .. },
                ..
            }
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
        first_left_leaning_actor_id: ActorId,
        kind: IterKind,
    ) -> FieldIterId {
        self.iters
            .claim_with_value(Cell::new(self.get_iter_state_at_end(
                fm,
                first_left_leaning_actor_id,
                kind,
            )))
    }
    pub fn reserve_iter_id(
        &mut self,
        iter_id: FieldIterId,
        first_right_leaning_actor_id: ActorId,
        kind: IterKind,
    ) {
        let v = Cell::new(
            self.get_iter_state_at_begin(first_right_leaning_actor_id, kind),
        );
        self.iters.reserve_id_with(iter_id, || v);
    }
    pub fn release_iter(&mut self, iter_id: FieldIterId) {
        self.iters[iter_id].get_mut().invalidate();
        self.iters.release(iter_id)
    }
    pub fn get_iter_state(&self, iter_id: FieldIterId) -> IterState {
        self.iters[iter_id].get()
    }
    pub fn iter_states(&self) -> impl Iterator<Item = IterState> + '_ {
        self.iters.iter().map(Cell::get)
    }
    fn calculate_start_header<R: FieldDataRef>(
        fr: &R,
        state: &mut IterState,
    ) -> FieldValueHeader {
        if fr.headers().is_empty() {
            return FieldValueHeader::default();
        }
        let mut h = fr.headers()[state.header_idx];

        // we store the state after the end of the last header so it sits
        // correctly in case data is appended or copied in by forkcats.
        // the iterator expects it at the beginning of the next header
        // though so we adjust it here
        if h.run_length != state.header_rl_offset
            || state.header_idx + 1 == fr.headers().len()
        {
            return h;
        }
        state.header_idx += 1;
        state.header_rl_offset = 0;
        state.header_start_data_pos_pre_padding += h.total_size_unique();
        h = fr.headers()[state.header_idx];
        h
    }
    // SAFETY: caller must ensure that the state comes from this data source
    pub unsafe fn get_iter_from_state_unchecked<R: FieldDataRef>(
        fdr: R,
        mut state: IterState,
    ) -> FieldIter<R> {
        let h = Self::calculate_start_header(&fdr, &mut state);
        unsafe {
            FieldIter::from_iter_location(
                fdr,
                IterLocation {
                    field_pos: state.field_pos,
                    header_idx: state.header_idx,
                    header_rl_offset: state.header_rl_offset,
                    // TODO: investigate. seems sus that we have to add the
                    // padding here
                    header_start_data_pos_post_padding: state
                        .header_start_data_pos_pre_padding
                        + h.leading_padding(),
                },
                true,
            )
        }
    }

    // SAFETY: caller must ensure that the iter uses the correct data source
    pub unsafe fn store_iter_unchecked<R: FieldDataRef>(
        &self,
        #[allow(unused)] field_id: FieldId,
        iter_id: FieldIterId,
        iter: FieldIter<R>,
    ) {
        let mut state = self.iters[iter_id].get();
        state = iter.into_iter_state(
            state.first_right_leaning_actor_id,
            state.iter_kind(),
        );
        // #[cfg(feature = "debug_logging_iter_states")]
        // eprintln!("storing iter for field {field_id:02}: {state:?}");
        self.iters[iter_id].set(state);
    }
    pub unsafe fn store_iter_state_unchecked(
        &self,
        iter_id: FieldIterId,
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

    pub fn copy(
        iter: &mut impl FieldIterator,
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
    pub fn copy_resolve_refs<I: FieldIterator>(
        match_set_mgr: &mut MatchSetManager,
        iter: &mut AutoDerefIter<I>,
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
            FieldDataSource::SameMsCow(src_field_id) => {
                (Some(src_field_id), Some(false))
            }
            FieldDataSource::FullCow(cds) => {
                (Some(cds.src_field_id), Some(false))
            }
            FieldDataSource::DataCow { source: cds, .. } => {
                (Some(cds.src_field_id), Some(true))
            }
            FieldDataSource::RecordBufferFullCow(_) => (None, Some(false)),
            FieldDataSource::RecordBufferDataCow(_) => (None, Some(true)),
        }
    }
    pub fn get_cow_iter_state(&self, fm: &FieldManager) -> Option<IterState> {
        match self.data_source {
            FieldDataSource::Owned | FieldDataSource::SameMsCow(_) => None,
            FieldDataSource::Alias(src) => {
                fm.fields[src].borrow().iter_hall.get_cow_iter_state(fm)
            }
            FieldDataSource::FullCow(cds)
            | FieldDataSource::DataCow { source: cds, .. } => Some(
                fm.fields[cds.src_field_id].borrow().iter_hall.iters
                    [cds.header_iter_id]
                    .get(),
            ),
            FieldDataSource::RecordBufferFullCow(_)
            | FieldDataSource::RecordBufferDataCow(_) => todo!(),
        }
    }
    pub fn cow_variant(&self) -> Option<CowVariant> {
        self.data_source.cow_variant()
    }
    pub fn get_iter_kind(
        &self,
        #[allow(unused)] iter_id: FieldIterId,
    ) -> IterKind {
        #[cfg(not(feature = "debug_state"))]
        return IterKind::Undefined;
        #[cfg(feature = "debug_state")]
        return self.iters[iter_id].get().kind;
    }
    pub fn reset_iter(&self, iter_id: FieldIterId) {
        self.iters[iter_id].set(self.get_iter_state_at_begin(
            self.iters[iter_id].get().first_right_leaning_actor_id,
            self.get_iter_kind(iter_id),
        ));
    }
    pub fn reset_iterators(&mut self) {
        for (i, _) in &mut self.iters.iter_enumerated() {
            self.reset_iter(i)
        }
    }
    pub fn reset_cow_headers(&mut self, fm: &FieldManager) {
        match &mut self.data_source {
            FieldDataSource::Owned
            | FieldDataSource::FullCow(_)
            | FieldDataSource::SameMsCow(_) => (),
            FieldDataSource::Alias(src) => {
                fm.fields[*src].borrow_mut().iter_hall.reset_cow_headers(fm)
            }
            FieldDataSource::DataCow { source: cds, .. } => {
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
    pub fn get_owned_data(&self) -> &FieldData {
        match &self.data_source {
            FieldDataSource::Owned => &self.field_data,
            _ => panic!("IterHall uses COW!"),
        }
    }
    pub fn get_owned_data_mut(&mut self) -> &mut FieldData {
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
            | FieldDataSource::SameMsCow(src_field_id)
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
            | FieldDataSource::SameMsCow(src_field_id)
            | FieldDataSource::Alias(src_field_id) => {
                fm.fields[src_field_id]
                    .borrow()
                    .iter_hall
                    .append_data_to(fm, target);
            }
            FieldDataSource::DataCow { source: cds, .. } => fm.fields
                [cds.src_field_id]
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
            | FieldDataSource::SameMsCow(src_field_id)
            | FieldDataSource::Alias(src_field_id) => fm.fields[src_field_id]
                .borrow()
                .iter_hall
                .append_to(fm, target),
            FieldDataSource::DataCow { source: cds, .. } => {
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
            | FieldDataSource::SameMsCow(src_field_id)
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
            FieldDataSource::SameMsCow(src_field_id)
            | FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            }) => {
                debug_assert!(self.field_data.is_empty());
                let src = fm.fields[src_field_id].borrow();
                self.field_data.append_from_other(&src.iter_hall.field_data);
                src.iter_hall.append_to(fm, &mut self.field_data);
                self.data_source = FieldDataSource::Owned;
                Some(src_field_id)
            }
            FieldDataSource::DataCow { source: cds, .. } => {
                debug_assert!(self.field_data.is_empty());
                let src = fm.fields[cds.src_field_id].borrow();
                src.iter_hall.append_data_to(fm, &mut self.field_data.data);
                self.data_source = FieldDataSource::Owned;
                Some(cds.src_field_id)
            }
            FieldDataSource::RecordBufferFullCow(data_ref) => {
                debug_assert!(self.field_data.is_empty());
                self.field_data = unsafe { &*(*data_ref).get() }.clone();
                self.data_source = FieldDataSource::Owned;
                None
            }
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                debug_assert!(self.field_data.is_empty());
                unsafe { &*(*data_ref).get() }
                    .append_data_to(&mut self.field_data.data);
                self.data_source = FieldDataSource::Owned;
                None
            }
        }
    }
    pub(super) fn get_cow_data_source(&self) -> Option<&CowDataSource> {
        match & self.data_source {
            FieldDataSource::FullCow(cds) | FieldDataSource::DataCow{source: cds, ..} => {
                Some(cds)
            }
            FieldDataSource::Owned
            | FieldDataSource::SameMsCow(_) //TODO: is this correct?
            | FieldDataSource::Alias(_)
            | FieldDataSource::RecordBufferFullCow(_)
            | FieldDataSource::RecordBufferDataCow(_) => None,
        }
    }
    pub(super) fn get_cow_data_source_mut(
        &mut self,
    ) -> Option<&mut CowDataSource> {
        match &mut self.data_source {
            FieldDataSource::FullCow(cds) | FieldDataSource::DataCow{source: cds, ..} => {
                Some(cds)
            }
            FieldDataSource::Owned
            | FieldDataSource::SameMsCow(_) //TODO: is this correct?
            | FieldDataSource::Alias(_)
            | FieldDataSource::RecordBufferFullCow(_)
            | FieldDataSource::RecordBufferDataCow(_) => None,
        }
    }

    /// returns the observed data size after the copy, as needed by
    /// `FieldDataSource::DataCow::observed_data_size`
    pub(crate) fn copy_headers_from_cow_src(
        &mut self,
        src_headers: &VecDeque<FieldValueHeader>,
        cow_end: IterState,
    ) -> usize {
        self.field_data
            .headers
            .extend(src_headers.range(..cow_end.header_idx));
        self.field_data.field_count += cow_end.field_pos;
        if cow_end.header_rl_offset == 0 {
            return cow_end.header_start_data_pos_pre_padding;
        }
        // must exist, otherwise offset would have been zero
        let mut last_header = src_headers[cow_end.header_idx];
        last_header.run_length =
            cow_end.header_rl_offset.min(last_header.run_length);
        self.field_data.headers.push_back(last_header);
        let mut data_end = cow_end.header_start_data_pos_pre_padding;
        if !last_header.same_value_as_previous() {
            data_end += last_header.leading_padding();
            data_end += if last_header.shared_value() {
                last_header.size as usize
            } else {
                last_header.size as usize * cow_end.header_rl_offset as usize
            };
        }
        data_end
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
                debug_assert!(
                    self.field_data.data.is_empty()
                        && self.field_data.headers.is_empty()
                        && self.field_data.field_count == 0
                );
                let cow_end =
                    fm.fields[cds.src_field_id].borrow().iter_hall.iters
                        [cds.header_iter_id]
                        .get();
                let src_field = fm.get_cow_field_ref_raw(cds.src_field_id);
                let src_headers = src_field.headers();
                let observed_data_size =
                    self.copy_headers_from_cow_src(src_headers, cow_end);
                self.data_source = FieldDataSource::DataCow {
                    source: cds,
                    observed_data_size,
                };
            }
            FieldDataSource::RecordBufferFullCow(rb) => {
                // TODO: add an iterator to these aswell
                let fd = unsafe { &*(*rb).get() };
                debug_assert!(self.field_data.is_empty());
                self.field_data.field_count = fd.field_count;
                self.field_data.headers.extend(&fd.headers);
                self.data_source = FieldDataSource::RecordBufferDataCow(rb);
            }
            FieldDataSource::SameMsCow(_) => {
                panic!("cannot uncow headers for same ms cow")
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
