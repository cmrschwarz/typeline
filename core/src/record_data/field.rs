use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    collections::VecDeque,
    marker::PhantomData,
};

use smallvec::SmallVec;

use crate::utils::universe::Universe;

use super::{
    action_buffer::{ActionBuffer, ActorId, ActorRef, SnapshotRef},
    field_data::{
        field_value_flags::SAME_VALUE_AS_PREVIOUS, FieldData, FieldDataBuffer,
        FieldValueFormat, FieldValueHeader,
    },
    iter_hall::{CowDataSource, FieldDataSource, IterHall, IterId, IterKind},
    iters::{
        BoundedIter, DestructuredFieldDataRef, FieldDataRef, FieldIter,
        FieldIterator,
    },
    match_set::{MatchSetId, MatchSetManager},
    record_buffer::RecordBufferField,
    ref_iter::AutoDerefIter,
    varying_type_inserter::VaryingTypeInserter,
};

pub const FIELD_REF_LOOKUP_ITER_ID: IterId = IterId::MIN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldIterRef {
    pub field_id: FieldId,
    pub iter_id: IterId,
}

#[derive(Default)]
pub struct Field {
    pub shadowed_since: ActorId,
    pub shadowed_by: Option<FieldId>,

    pub first_actor: Cell<ActorRef>,
    pub snapshot: Cell<SnapshotRef>,

    // fields potentially referenced by this field.
    // keeps them alive until this field is dropped
    // for cow'ed fields, initialization of this is *not* done by default
    // and requires calling setup_field_refs
    pub field_refs: SmallVec<[FieldId; 4]>,
    pub iter_hall: IterHall,

    pub match_set: MatchSetId,

    pub ref_count: usize,

    #[cfg(feature = "debug_logging")]
    pub producing_transform_id:
        Option<crate::operators::transform::TransformId>,
    #[cfg(feature = "debug_logging")]
    pub producing_transform_arg: String,
}

pub type FieldId = u32;

// Field references don't contain the `FieldId` of their target field directly,
// but an index into the `field_references` Vec of the Field they reside in.
// This is necessary so that when we COW the field we can just supply a
// different `field_references` array for the COW field without having
// to modify the original field data
pub type FieldRefOffset = u16;

impl Field {
    pub fn has_cow_targets(&self) -> bool {
        !self.iter_hall.cow_targets.is_empty()
    }
}

#[derive(Default)]
pub struct FieldManager {
    pub fields: Universe<FieldId, RefCell<Field>>,
}

pub struct CowFieldDataRef<'a> {
    field_count: usize,
    headers_ref: Ref<'a, VecDeque<FieldValueHeader>>,
    data_ref: Ref<'a, FieldDataBuffer>,
    _phantom: PhantomData<&'a FieldData>,
}

impl<'a> CowFieldDataRef<'a> {
    pub fn new(
        field_count: usize,
        headers_ref: Ref<'a, VecDeque<FieldValueHeader>>,
        data_ref: Ref<'a, FieldDataBuffer>,
    ) -> Self {
        Self {
            field_count,
            headers_ref,
            data_ref,
            _phantom: PhantomData,
        }
    }
    pub fn destructured_field_ref(&'a self) -> DestructuredFieldDataRef<'a> {
        DestructuredFieldDataRef {
            headers: &self.headers_ref,
            data: &self.data_ref,
            field_count: self.field_count,
        }
    }
    #[allow(clippy::iter_not_returning_iterator)]
    pub fn iter(&'a self) -> FieldIter<'a, &'a CowFieldDataRef<'a>> {
        FieldIter::from_start(self)
    }
    pub fn iter_from_end(&'a self) -> FieldIter<'a, &'a CowFieldDataRef<'a>> {
        FieldIter::from_end(self)
    }
    pub fn headers(&'a self) -> &'a VecDeque<FieldValueHeader> {
        &self.headers_ref
    }
    pub fn data(&'a self) -> &'a FieldDataBuffer {
        &self.data_ref
    }
}

impl<'a> FieldDataRef<'a> for &'a CowFieldDataRef<'a> {
    fn headers(&self) -> &'a VecDeque<FieldValueHeader> {
        &self.headers_ref
    }

    fn data(&self) -> &'a FieldDataBuffer {
        &self.data_ref
    }

    fn field_count(&self) -> usize {
        self.field_count
    }
}

impl<'a> Clone for CowFieldDataRef<'a> {
    fn clone(&self) -> Self {
        Self {
            field_count: self.field_count,
            headers_ref: Ref::clone(&self.headers_ref),
            data_ref: Ref::clone(&self.data_ref),
            _phantom: PhantomData,
        }
    }
}

impl FieldManager {
    pub fn claim_iter_ref(
        &self,
        field_id: FieldId,
        kind: IterKind,
    ) -> FieldIterRef {
        FieldIterRef {
            field_id,
            iter_id: self.claim_iter_non_cow(field_id, kind),
        }
    }
    pub fn claim_iter_non_cow(
        &self,
        field_id: FieldId,
        kind: IterKind,
    ) -> IterId {
        self.borrow_field_dealiased_mut(field_id)
            .iter_hall
            .claim_iter_non_cow(kind)
    }
    pub fn dealias_field_id(&self, mut field_id: FieldId) -> FieldId {
        loop {
            let field = self.fields[field_id].borrow();
            let Some(alias_src) = field.iter_hall.alias_source() else {
                return field_id;
            };
            field_id = alias_src;
        }
    }
    // convenience wrapper for getting and modifying field id
    pub fn get_dealiased_field_id(&self, field_id: &mut FieldId) -> FieldId {
        *field_id = self.dealias_field_id(*field_id);
        *field_id
    }
    pub fn borrow_field_dealiased(
        &self,
        field_id: &mut FieldId,
    ) -> Ref<'_, Field> {
        loop {
            let field = self.fields[*field_id].borrow();
            let Some(alias_src) = field.iter_hall.alias_source() else {
                return field;
            };
            *field_id = alias_src;
        }
    }
    pub fn borrow_field_dealiased_mut(
        &self,
        mut field_id: FieldId,
    ) -> RefMut<'_, Field> {
        loop {
            let field = self.fields[field_id].borrow_mut();
            let Some(alias_src) = field.iter_hall.alias_source() else {
                return field;
            };
            field_id = alias_src;
        }
    }
    fn remove_from_cow_tgt_list(
        &self,
        field_id: FieldId,
        cow_data_source: FieldId,
    ) {
        let mut cow_src = self.fields[cow_data_source].borrow_mut();
        let cow_tgt_pos = cow_src
            .iter_hall
            .cow_targets
            .iter()
            .position(|t| *t == field_id)
            .unwrap();
        cow_src.iter_hall.cow_targets.swap_remove(cow_tgt_pos);
    }
    pub fn uncow(&mut self, msm: &mut MatchSetManager, field_id: FieldId) {
        let mut field = self.fields[field_id].borrow_mut();
        let cow_source = field.iter_hall.uncow_get_field_with_rc(self);
        if let Some(cow_source) = cow_source {
            drop(field);
            self.remove_from_cow_tgt_list(field_id, cow_source);
            self.drop_field_refcount(cow_source, msm);
        }
    }

    pub fn get_field_headers<'a>(
        &'a self,
        fr: Ref<'a, Field>,
    ) -> (Ref<'a, VecDeque<FieldValueHeader>>, usize) {
        match fr.iter_hall.data_source {
            FieldDataSource::FullCow(CowDataSource {
                src_field_id: src_field,
                ..
            })
            | FieldDataSource::SameMsCow(src_field)
            | FieldDataSource::Alias(src_field) => {
                self.get_field_headers(self.fields[src_field].borrow())
            }
            FieldDataSource::RecordBufferFullCow(src) => {
                let f = &unsafe { &*(*src).get() };
                (Ref::map(fr, |_| &f.headers), f.field_count)
            }
            FieldDataSource::Owned
            | FieldDataSource::RecordBufferDataCow(_)
            | FieldDataSource::DataCow { .. } => {
                let field_count = fr.iter_hall.field_data.field_count;
                (
                    Ref::map(fr, |f| &f.iter_hall.field_data.headers),
                    field_count,
                )
            }
        }
    }
    pub fn get_field_data<'a>(
        &'a self,
        fr: Ref<'a, Field>,
    ) -> Ref<'a, FieldDataBuffer> {
        match &fr.iter_hall.data_source {
            FieldDataSource::FullCow(CowDataSource {
                src_field_id: src_field,
                ..
            })
            | FieldDataSource::SameMsCow(src_field)
            | FieldDataSource::DataCow(CowDataSource {
                src_field_id: src_field,
                ..
            })
            | FieldDataSource::Alias(src_field) => {
                self.get_field_data(self.fields[*src_field].borrow())
            }
            FieldDataSource::RecordBufferFullCow(_)
            | FieldDataSource::RecordBufferDataCow(_)
            | FieldDataSource::Owned => {
                Ref::map(fr, |f| match f.iter_hall.data_source {
                    FieldDataSource::RecordBufferFullCow(data_ref)
                    | FieldDataSource::RecordBufferDataCow(data_ref) => {
                        &unsafe { &*(*data_ref).get() }.data
                    }
                    FieldDataSource::Owned => &f.iter_hall.field_data.data,
                    _ => unreachable!(),
                })
            }
        }
    }
    pub fn get_varying_type_inserter(
        &self,
        field_id: FieldId,
    ) -> VaryingTypeInserter<RefMut<FieldData>> {
        VaryingTypeInserter::new(RefMut::map(
            self.fields[field_id].borrow_mut(),
            |f| unsafe { f.iter_hall.raw() },
        ))
    }
    pub fn get_first_actor(&self, field_id: FieldId) -> ActorRef {
        let field = self.fields[field_id].borrow();
        field.first_actor.get()
    }
    // TODO: take the field name out here completely.
    // only use scopes to give fields names
    pub fn add_field(
        &mut self,
        ms_id: MatchSetId,
        first_actor: ActorRef,
    ) -> FieldId {
        self.add_field_with_data(ms_id, first_actor, FieldData::default())
    }
    pub fn add_field_with_data(
        &mut self,
        ms_id: MatchSetId,
        first_actor: ActorRef,
        data: FieldData,
    ) -> FieldId {
        let mut field = Field {
            ref_count: 1,
            shadowed_since: ActionBuffer::MAX_ACTOR_ID,
            shadowed_by: None,
            match_set: ms_id,
            first_actor: Cell::new(first_actor),
            snapshot: Cell::new(SnapshotRef::default()),
            iter_hall: IterHall::new_with_data(data),
            field_refs: SmallVec::new(),
            #[cfg(feature = "debug_logging")]
            producing_transform_id: None,
            #[cfg(feature = "debug_logging")]
            producing_transform_arg: String::default(),
        };
        field.iter_hall.reserve_iter_id(
            FIELD_REF_LOOKUP_ITER_ID,
            false,
            IterKind::RefLookup,
        );
        self.fields.claim_with_value(RefCell::new(field))
    }
    pub fn update_data_cow(&self, field_id: FieldId) {
        let mut field = self.fields[field_id].borrow_mut();
        let FieldDataSource::DataCow(cds) = field.iter_hall.data_source else {
            return;
        };

        let src = self.fields[cds.src_field_id].borrow();
        let iter = src.iter_hall.get_iter_state(cds.header_iter_id);
        let (headers, count) = self.get_field_headers(src);
        let mut copy_headers_from = iter.header_idx;
        if iter.header_rl_offset != 0 {
            let h = headers[iter.header_idx];
            let additional_run_len = h.run_length - iter.header_rl_offset;
            if additional_run_len > 0 {
                field.iter_hall.field_data.headers.push_back(
                    FieldValueHeader {
                        fmt: FieldValueFormat {
                            flags: h.flags
                                | if h.shared_value() {
                                    SAME_VALUE_AS_PREVIOUS
                                } else {
                                    0
                                },
                            ..h.fmt
                        },
                        run_length: additional_run_len,
                    },
                )
            }
            copy_headers_from += 1;
        }
        let additional_len = count - iter.field_pos;
        field
            .iter_hall
            .field_data
            .headers
            .extend(headers.range(copy_headers_from..));
        field.iter_hall.field_data.field_count += additional_len;
        let src = self.fields[cds.src_field_id].borrow();
        unsafe {
            src.iter_hall.store_iter_state_unchecked(
                cds.header_iter_id,
                src.iter_hall.get_iter_state_at_end(
                    self,
                    true,
                    src.iter_hall.get_iter_kind(cds.header_iter_id),
                ),
            );
        }
    }

    pub fn apply_field_actions(
        &self,
        msm: &MatchSetManager,
        mut field_id: FieldId,
        update_cow_source: bool,
    ) {
        let field = self.borrow_field_dealiased(&mut field_id);
        let (cow_src_id, _) = field.iter_hall.cow_source_field(self);

        if update_cow_source {
            if let Some(cow_src) = cow_src_id {
                drop(field);
                // the cow source will update us by itself because we are
                // it's cow target
                self.apply_field_actions(msm, cow_src, true);
                return;
            }
        }

        let field_ms_id = field.match_set;

        for &f in &field.field_refs {
            self.apply_field_actions(msm, f, true);
        }

        // For full cow, it is critically important to always apply
        // any updates to the cow target **before** changing the source.
        // Otherwise the state of the source that the full cow
        // sees will become incorrect.
        // For data cow, it is nice to do this too because it might
        // increase the amount of dead data that the source can then drop.
        for &ct in &field.iter_hall.cow_targets {
            self.apply_field_actions(msm, ct, false);
        }

        drop(field);

        let mut ab = msm.match_sets[field_ms_id].action_buffer.borrow_mut();
        ab.update_field(self, msm, field_id, None);
        drop(ab);
    }

    pub fn drop_field_actions(
        &self,
        msm: &MatchSetManager,
        mut field_id: FieldId,
    ) {
        let field = self.borrow_field_dealiased(&mut field_id);
        let match_set = field.match_set;
        let ab = &mut msm.match_sets[match_set].action_buffer.borrow_mut();
        ab.drop_field_actions(field_id, &field);
    }
    // bumps the refcount of the field by one
    pub fn get_cross_ms_cow_field(
        &mut self,
        msm: &mut MatchSetManager,
        tgt_match_set: MatchSetId,
        src_field_id: FieldId,
    ) -> FieldId {
        if let Some(&tgt_field_id) = msm.match_sets[tgt_match_set]
            .fields_cow_map
            .get(&src_field_id)
        {
            self.bump_field_refcount(tgt_field_id);
            return tgt_field_id;
        }
        let tgt_field_id = self.add_field(tgt_match_set, ActorRef::default());
        self.setup_cow_between_fields(msm, src_field_id, tgt_field_id);
        tgt_field_id
    }

    pub fn setup_cow_between_fields(
        &mut self,
        msm: &mut MatchSetManager,
        src_field_id: FieldId,
        tgt_field_id: FieldId,
    ) {
        let mut src_field = self.fields[src_field_id].borrow_mut();
        src_field.ref_count += 1;
        src_field.iter_hall.cow_targets.push(tgt_field_id);
        let src_field_ms = src_field.match_set;
        let header_iter = src_field
            .iter_hall
            .claim_iter(true, IterKind::CowField(tgt_field_id));
        let mut tgt_field = self.fields[tgt_field_id].borrow_mut();
        debug_assert!(matches!(
            tgt_field.iter_hall.data_source,
            FieldDataSource::Owned
        ));

        let tgt_field_ms = tgt_field.match_set;
        if tgt_field_ms == src_field_ms {
            tgt_field
                .field_refs
                .extend_from_slice(&src_field.field_refs);
            drop(src_field);
            tgt_field.iter_hall.data_source =
                FieldDataSource::SameMsCow(src_field_id);
            for &field_id in &tgt_field.field_refs {
                self.bump_field_refcount(field_id);
            }
            return;
        }
        msm.match_sets[tgt_field_ms]
            .fields_cow_map
            .insert(src_field_id, tgt_field_id);
        tgt_field.iter_hall.data_source =
            FieldDataSource::FullCow(CowDataSource {
                src_field_id,
                header_iter_id: header_iter,
            });
        for i in 0..src_field.field_refs.len() {
            let ref_field_id = src_field.field_refs[i];
            drop(src_field);
            drop(tgt_field);
            let cow_field_id =
                self.get_cross_ms_cow_field(msm, tgt_field_ms, ref_field_id);
            tgt_field = self.fields[tgt_field_id].borrow_mut();
            src_field = self.fields[src_field_id].borrow_mut();
            tgt_field.field_refs.push(cow_field_id);
        }
    }

    pub fn create_same_ms_cow_field(
        &mut self,
        msm: &mut MatchSetManager,
        src_field_id: FieldId,
    ) -> FieldId {
        let src_field = self.fields[src_field_id].borrow();
        let ms_id = src_field.match_set;
        drop(src_field);
        let tgt_field_id = self.add_field(ms_id, ActorRef::default());
        self.setup_cow_between_fields(msm, src_field_id, tgt_field_id);
        tgt_field_id
    }
    pub fn append_to_buffer<'a>(
        &self,
        iter: &mut impl FieldIterator<'a>,
        tgt: &RecordBufferField,
    ) {
        let fd = unsafe { &mut (*tgt.data.get()) };
        FieldData::copy(iter, &mut |f| f(fd));
    }
    pub fn swap_into_buffer(
        &self,
        field_id: FieldId,
        tgt: &mut RecordBufferField,
    ) {
        let tgt = tgt.data.get_mut();
        tgt.clear();
        let mut src = self.fields[field_id].borrow_mut();
        match src.iter_hall.data_source {
            FieldDataSource::Owned => {
                std::mem::swap(tgt, &mut src.iter_hall.field_data);
            }
            FieldDataSource::FullCow(CowDataSource {
                src_field_id, ..
            })
            | FieldDataSource::SameMsCow(src_field_id)
            | FieldDataSource::Alias(src_field_id) => {
                let fr = self.get_cow_field_ref_raw(src_field_id);
                let mut iter =
                    FieldIter::from_start(fr.destructured_field_ref());
                FieldData::copy(&mut iter, &mut |f| f(tgt));
            }
            FieldDataSource::DataCow(cds) => {
                std::mem::swap(
                    &mut tgt.field_count,
                    &mut src.iter_hall.field_data.field_count,
                );
                std::mem::swap(
                    &mut tgt.headers,
                    &mut src.iter_hall.field_data.headers,
                );
                let fr = self.get_cow_field_ref_raw(cds.src_field_id);
                let iter = FieldIter::from_start(fr.destructured_field_ref());
                unsafe {
                    FieldData::copy_data(iter, &mut |f| f(tgt));
                }
            }
            FieldDataSource::RecordBufferFullCow(rb) => {
                let fd = unsafe { &mut *(*rb).get() };
                let mut iter = fd.iter();
                FieldData::copy(&mut iter, &mut |f| f(tgt));
            }
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                std::mem::swap(
                    &mut tgt.field_count,
                    &mut src.iter_hall.field_data.field_count,
                );
                std::mem::swap(
                    &mut tgt.headers,
                    &mut src.iter_hall.field_data.headers,
                );
                unsafe {
                    let fd = &mut *(*data_ref).get();
                    FieldData::copy_data(fd.iter(), &mut |f| f(tgt));
                }
            }
        }
    }
    // used to also cow in the field refs of a cow field once it is actually
    // accessed.
    // sometimes we have to cow a lot of fields (e.g. in case of dyn access)
    // so it's nice to be a bit lazy about this
    // TODO: evaluate this? seems like it's no longer needed?
    pub fn setup_field_refs(
        &mut self,
        msm: &mut MatchSetManager,
        field_id: FieldId,
    ) {
        let field = self.fields[field_id].borrow();
        if !field.field_refs.is_empty() {
            return;
        }
        let ms_id = field.match_set;
        let (Some(cow_src_id), _data_cow) =
            field.iter_hall.cow_source_field(self)
        else {
            return;
        };
        drop(field);
        self.setup_field_refs(msm, cow_src_id);
        let mut field = self.fields[field_id].borrow_mut();
        let cow_src = self.fields[cow_src_id].borrow();
        let field_ref_len = cow_src.field_refs.len();
        field.field_refs.extend_from_slice(&cow_src.field_refs);
        drop(cow_src);
        drop(field);
        for i in 0..field_ref_len {
            let fr_src = self.fields[field_id].borrow().field_refs[i];
            let fr = if self.fields[fr_src].borrow().match_set == ms_id {
                fr_src
            } else if let Some(&id) =
                msm.match_sets[ms_id].fields_cow_map.get(&fr_src)
            {
                id
            } else {
                let id = self.get_cross_ms_cow_field(msm, ms_id, fr_src);
                self.setup_field_refs(msm, id);
                id
            };
            self.fields[field_id].borrow_mut().field_refs[i] = fr;
        }
    }
    pub(crate) fn get_cow_field_ref_raw(
        &self,
        field_id: FieldId,
    ) -> CowFieldDataRef {
        let field = self.fields[field_id].borrow();
        let (headers_ref, field_count) =
            self.get_field_headers(Ref::clone(&field));
        let data_ref = self.get_field_data(field);
        CowFieldDataRef {
            headers_ref,
            field_count,
            data_ref,
            _phantom: PhantomData,
        }
    }
    pub fn get_cow_field_ref(
        &self,
        msm: &MatchSetManager,
        field_id: FieldId,
    ) -> CowFieldDataRef {
        let field_id = self.dealias_field_id(field_id);
        self.apply_field_actions(msm, field_id, true);
        self.get_cow_field_ref_raw(field_id)
    }
    pub fn move_iter(
        &self,
        msm: &mut MatchSetManager,
        field_id: FieldId,
        iter_id: IterId,
        delta: isize,
    ) {
        let field_id = self.dealias_field_id(field_id);
        let fr = self.get_cow_field_ref(msm, field_id);
        let mut iter = self.lookup_iter(field_id, &fr, iter_id);
        iter.move_n_fields(delta, true);
        unsafe {
            self.fields[field_id]
                .borrow()
                .iter_hall
                .store_iter_unchecked(field_id, iter_id, iter)
        }
    }
    pub fn get_auto_deref_iter<'a>(
        &'a self,
        input_field_id: FieldId,
        input_field: &'a CowFieldDataRef<'a>,
        input_iter_id: IterId,
    ) -> AutoDerefIter<'a, FieldIter<DestructuredFieldDataRef<'a>>> {
        AutoDerefIter::new(
            self,
            input_field_id,
            self.lookup_iter(input_field_id, input_field, input_iter_id),
        )
    }
    pub fn get_bounded_auto_deref_iter<'a>(
        &'a self,
        input_field_id: FieldId,
        input_field: &'a CowFieldDataRef<'a>,
        input_iter_id: IterId,
        batch_size: usize,
    ) -> AutoDerefIter<'a, BoundedIter<FieldIter<DestructuredFieldDataRef<'a>>>>
    {
        AutoDerefIter::new(
            self,
            input_field_id,
            self.lookup_iter(input_field_id, input_field, input_iter_id)
                .bounded(0, batch_size),
        )
    }
    pub fn lookup_iter<'a>(
        &self,
        mut field_id: FieldId,
        cfdr: &'a CowFieldDataRef<'a>,
        iter_id: IterId,
    ) -> FieldIter<'a, DestructuredFieldDataRef<'a>> {
        // PERF: maybe write a custom compare instead of doing this traversal?
        assert!(cfdr.destructured_field_ref().equals(
            &self
                .get_cow_field_ref_raw(field_id)
                .destructured_field_ref()
        ));
        let field = self.borrow_field_dealiased(&mut field_id);
        let state = field.iter_hall.get_iter_state(iter_id);
        unsafe {
            IterHall::get_iter_from_state_unchecked(
                cfdr.destructured_field_ref(),
                state,
            )
        }
    }
    pub fn lookup_iter_from_ref<'a>(
        &self,
        iter_ref: FieldIterRef,
        cfdr: &'a CowFieldDataRef<'a>,
    ) -> FieldIter<'a, DestructuredFieldDataRef<'a>> {
        self.lookup_iter(iter_ref.field_id, cfdr, iter_ref.iter_id)
    }
    pub fn store_iter<'a, R: FieldDataRef<'a>>(
        &self,
        mut field_id: FieldId,
        iter_id: IterId,
        iter: impl Into<FieldIter<'a, R>>,
    ) {
        let iter_base = iter.into();
        let field = self.borrow_field_dealiased(&mut field_id);
        assert!(iter_base.field_data_ref().equals(
            &self
                .get_cow_field_ref_raw(field_id)
                .destructured_field_ref()
        ));
        unsafe {
            field
                .iter_hall
                .store_iter_unchecked(field_id, iter_id, iter_base)
        };
    }
    pub fn store_iter_from_ref<'a, R: FieldDataRef<'a>>(
        &self,
        iter_ref: FieldIterRef,
        iter: impl Into<FieldIter<'a, R>>,
    ) {
        self.store_iter(iter_ref.field_id, iter_ref.iter_id, iter)
    }
    pub fn store_iter_drop_action_lists<'a, R: FieldDataRef<'a>>(
        &self,
        msm: &MatchSetManager,
        mut field_id: FieldId,
        iter_id: IterId,
        iter: impl Into<FieldIter<'a, R>>,
    ) {
        let iter_base = iter.into();
        let field = self.borrow_field_dealiased(&mut field_id);
        assert!(iter_base.field_data_ref().equals(
            &self
                .get_cow_field_ref_raw(field_id)
                .destructured_field_ref()
        ));
        unsafe {
            field
                .iter_hall
                .store_iter_unchecked(field_id, iter_id, iter_base)
        };
        self.drop_field_actions(msm, field_id)
    }

    pub fn bump_field_refcount(&self, field_id: FieldId) {
        self.fields[field_id].borrow_mut().ref_count += 1;
    }
    pub fn inc_field_refcount(&self, field_id: FieldId, n: usize) {
        self.fields[field_id].borrow_mut().ref_count += n;
    }
    pub fn register_field_reference(
        &self,
        refs_field: FieldId,
        refs_target: FieldId,
    ) -> FieldRefOffset {
        let mut src = self.fields[refs_field].borrow_mut();
        let mut tgt = self.fields[refs_target].borrow_mut();
        tgt.ref_count += 1;
        for fr in &tgt.field_refs {
            self.fields[*fr].borrow_mut().ref_count += 1;
        }
        src.field_refs.extend_from_slice(&tgt.field_refs);
        // we put the ref target itself last so that fields with only one
        // ref (commonly to their input field), have the same offsets as their
        // ref target and can be copy field refs (and objects containing them)
        // straight to the output. This property is depended on by the
        // `extend_from_ref_aware_range_smart_ref` function of the
        // push interface, that is used e.g. by the flatten / explode operators
        let id = FieldRefOffset::try_from(src.field_refs.len()).unwrap();
        src.field_refs.push(refs_target);
        id
    }
    pub fn remove_field(&mut self, id: FieldId, msm: &mut MatchSetManager) {
        #[cfg(feature = "debug_logging")]
        {
            eprint!("removing field {id}");
            eprintln!();
        }
        let mut field = self.fields[id].borrow_mut();

        // there is no need to take the field out of the
        // field name map, because nobody will observe this
        // (otherwise we wouldn't delete the field)
        // // if let Some(name) = field.name {
        // // msm.match_sets[field.match_set].field_name_map.remove(&name);
        // // }
        let (cow_src, _) = field.iter_hall.cow_source_field(self);
        let frs = std::mem::take(&mut field.field_refs);
        let shadowed_by = field.shadowed_by;
        drop(field);
        self.fields.release(id);
        if let Some(cow_src) = cow_src {
            self.remove_from_cow_tgt_list(id, cow_src);
            self.drop_field_refcount(cow_src, msm);
        }
        for fr in &frs {
            self.drop_field_refcount(*fr, msm);
        }
        if let Some(shadowed_by) = shadowed_by {
            self.drop_field_refcount(shadowed_by, msm);
        }
    }
    pub fn drop_field_refcount(
        &mut self,
        field_id: FieldId,
        msm: &mut MatchSetManager,
    ) {
        let mut field = self.fields[field_id].borrow_mut();
        field.ref_count -= 1;
        let rc = field.ref_count;
        drop(field);
        if rc == 0 {
            self.remove_field(field_id, msm);
        } else if cfg!(feature = "debug_logging") {
            eprint!("dropped ref to field {field_id} (rc {rc})");
            eprintln!();
        }
    }
    pub fn print_field_stats_for_ref(&self, field: &Field, id: FieldId) {
        eprint!("field id {id:02}");
        // if let Some(name) = field.name {
        //    eprint!(" '@{}'",
        // self.session_data.string_store.lookup(name));
        //}
        eprint!(", ms {}", field.match_set);
        eprint!(", rc {:>2}", field.ref_count);
        eprint!(", actor {:?}", field.first_actor);
        eprint!(", fc {}", field.iter_hall.field_data.field_count);
        eprint!(", hc {}", field.iter_hall.field_data.headers.len());
        eprint!(", ds {:>2}", field.iter_hall.field_data.data.len());
        #[cfg(feature = "debug_logging")]
        {
            if let Some(prod_id) = field.producing_transform_id {
                eprint!(
                    " (output of tf {prod_id} `{}`)",
                    field.producing_transform_arg
                );
            } else if !field.producing_transform_arg.is_empty() {
                eprint!(" (`{}`)", field.producing_transform_arg);
            }
        }
        if let Some(shadowed_by) = field.shadowed_by {
            eprint!(
                " (aliased by field id {} since actor id `{}`)",
                shadowed_by, field.shadowed_since
            );
        }
        if let (cow_src_field, Some(data_cow)) =
            field.iter_hall.cow_source_field(self)
        {
            eprint!(
                " [{} cow{}]",
                if data_cow { "data" } else { "full" },
                if let Some(src) = cow_src_field {
                    format!(" src: {src}")
                } else {
                    String::default()
                }
            );
        }
        if !field.field_refs.is_empty() {
            eprint!(" ( field refs:");
            for fr in &field.field_refs {
                eprint!(" {fr}");
            }
            eprint!(" )");
        }
    }
    pub fn print_field_stats(&self, id: FieldId) {
        self.print_field_stats_for_ref(&self.fields[id].borrow(), id)
    }
    pub fn print_field_header_data_for_ref(
        &self,
        field: &Field,
        indent_level: usize,
    ) {
        let fd = &field.iter_hall.field_data;
        if fd.headers.is_empty() {
            eprint!("[]");
            return;
        }
        eprintln!("[");
        for &h in &fd.headers {
            eprintln!("{:indent_level$}    {h:?},", "");
        }
        eprint!("{:indent_level$}]", "");
    }
    pub fn print_field_header_data(&self, id: FieldId, indent_level: usize) {
        self.print_field_header_data_for_ref(
            &self.fields[id].borrow(),
            indent_level,
        );
    }
    pub fn print_field_iter_data_for_ref(
        &self,
        field: &Field,
        indent_level: usize,
    ) {
        let iter =
            field.iter_hall.iters.iter().filter(|#[allow(unused)] v| {
                #[cfg(feature = "debug_logging")]
                let res = true; // v.get().kind != IterKind::RefLookup;
                #[cfg(not(feature = "debug_logging"))]
                let res = false;
                res
            });
        if iter.clone().next().is_none() {
            eprint!("[]");
            return;
        }
        eprintln!("[");
        for is in iter {
            eprintln!("{:indent_level$}    {:?},", "", is.get());
        }
        eprint!("{:indent_level$}]", "");
    }
    pub fn print_field_iter_data(&self, id: FieldId, indent_level: usize) {
        let field = self.fields[id].borrow();
        self.print_field_iter_data_for_ref(&field, indent_level);
    }
    pub fn print_fields_with_header_data(&self) {
        for (id, _) in self.fields.iter_enumerated() {
            self.print_field_stats(id);
            eprint!(" ");
            self.print_field_header_data(id, 0);
            eprintln!();
        }
    }
    pub fn print_fields_with_iter_data(&self) {
        for (id, _) in self.fields.iter_enumerated() {
            self.print_field_stats(id);
            eprint!(" ");
            self.print_field_iter_data(id, 0);
            eprintln!();
        }
    }
    pub fn print_field_report_for_ref(&self, field: &Field, id: FieldId) {
        self.print_field_stats_for_ref(field, id);
        eprint!("\n    ");
        self.print_field_header_data_for_ref(field, 4);
        eprint!("\n    ");
        self.print_field_iter_data_for_ref(field, 4);
        eprintln!();
    }
    pub fn print_field_report(&self, id: FieldId) {
        self.print_field_report_for_ref(&self.fields[id].borrow(), id);
    }
}

impl Drop for FieldManager {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if !std::thread::panicking() {
            // TODO: this does not work yet, because e.g. callcc
            // does not properly clean up it's cow targets yet
            // reenable this once it works
            // debug_assert!(self.fields.any_used().is_none());
        }
    }
}
