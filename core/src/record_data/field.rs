use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    collections::hash_map::Entry,
    marker::PhantomData,
    ops::DerefMut,
};

use nonmax::NonMaxU16;
use smallvec::SmallVec;

use crate::utils::{
    nonzero_ext::NonMaxU32Ext, string_store::StringStoreEntry,
    universe::Universe,
};

use super::{
    action_buffer::{ActionBuffer, ActorId, ActorRef, SnapshotRef},
    field_data::{
        field_value_flags::SAME_VALUE_AS_PREVIOUS, FieldData, FieldDataBuffer,
        FieldValueFormat, FieldValueHeader,
    },
    iter_hall::{FieldDataSource, IterHall, IterId},
    iters::{
        BoundedIter, DestructuredFieldDataRef, FieldDataRef, FieldIterator,
        Iter,
    },
    match_set::{MatchSetId, MatchSetManager},
    push_interface::VaryingTypeInserter,
    record_buffer::RecordBufferField,
    ref_iter::AutoDerefIter,
};

#[cfg(feature = "debug_logging")]
use crate::operators::transform::TransformId;

pub const FIELD_REF_LOOKUP_ITER_ID: IterId = IterId::MIN;

#[derive(Default)]
pub struct Field {
    pub shadowed_since: ActorId,
    pub shadowed_by: FieldId,

    pub first_actor: ActorRef,
    pub snapshot: SnapshotRef,

    // fields potentially referenced by this field.
    // keeps them alive until this field is dropped
    // for cow'ed fields, initialization of this is *not* done by default
    // and requires calling setup_field_refs
    pub field_refs: SmallVec<[FieldId; 4]>,
    pub iter_hall: IterHall,

    pub match_set: MatchSetId,

    // typically called on input fields which we don't want to borrow mut
    pub clear_delay_request_count: Cell<u16>,

    pub name: Option<StringStoreEntry>,
    pub ref_count: usize,

    #[cfg(feature = "debug_logging")]
    pub producing_transform_id: Option<TransformId>,
    #[cfg(feature = "debug_logging")]
    pub producing_transform_arg: String,
}

pub type FieldId = u32;

// Field references don't contain the `FieldId` of their target field directly,
// but an index into the `field_references` Vec of the Field they reside in.
// This is necessary so that when we COW the field we can just supply a
// different `field_references` array for the COW field without having
// to modify the original field data
pub type FieldRefOffset = NonMaxU16;
pub const VOID_FIELD_ID: FieldId = FieldId::MIN;

impl Field {
    pub fn get_clear_delay_request_count(&self) -> u16 {
        self.clear_delay_request_count.get()
    }
    pub fn has_cow_targets(&self) -> bool {
        !self.iter_hall.cow_targets.is_empty()
    }
}

pub struct FieldManager {
    pub fields: Universe<FieldId, RefCell<Field>>,
}

pub struct CowFieldDataRef<'a> {
    field_count: usize,
    headers_ref: Ref<'a, Vec<FieldValueHeader>>,
    data_ref: Ref<'a, FieldDataBuffer>,
    _phantom: PhantomData<&'a FieldData>,
}

impl<'a> CowFieldDataRef<'a> {
    pub fn new(
        field_count: usize,
        headers_ref: Ref<'a, Vec<FieldValueHeader>>,
        data_ref: Ref<'a, FieldDataBuffer>,
    ) -> Self {
        Self {
            field_count,
            headers_ref,
            data_ref,
            _phantom: PhantomData,
        }
    }
}

impl<'a> FieldDataRef<'a> for &'a CowFieldDataRef<'a> {
    fn headers(&self) -> &'a [FieldValueHeader] {
        &self.headers_ref
    }

    fn data(&self) -> &'a [u8] {
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

impl<'a> CowFieldDataRef<'a> {
    pub fn destructured_field_ref(&'a self) -> DestructuredFieldDataRef<'a> {
        DestructuredFieldDataRef {
            headers: &self.headers_ref,
            data: &self.data_ref,
            field_count: self.field_count,
        }
    }
}

impl FieldManager {
    pub fn claim_iter(&self, field_id: FieldId) -> IterId {
        self.borrow_field_dealiased_mut(field_id)
            .iter_hall
            .claim_iter()
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
    ) -> (Ref<'a, Vec<FieldValueHeader>>, usize) {
        match fr.iter_hall.data_source {
            FieldDataSource::Cow(src) | FieldDataSource::Alias(src) => {
                self.get_field_headers(self.fields[src].borrow())
            }
            FieldDataSource::RecordBufferCow(src) => {
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
            FieldDataSource::Cow(src_field)
            | FieldDataSource::Alias(src_field)
            | FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => self.get_field_data(self.fields[*src_field].borrow()),
            FieldDataSource::RecordBufferCow(_)
            | FieldDataSource::RecordBufferDataCow(_)
            | FieldDataSource::Owned => {
                Ref::map(fr, |f| match f.iter_hall.data_source {
                    FieldDataSource::RecordBufferCow(data_ref)
                    | FieldDataSource::RecordBufferDataCow(data_ref) => {
                        &unsafe { &*(*data_ref).get() }.data
                    }
                    FieldDataSource::Owned => &f.iter_hall.field_data.data,
                    _ => unreachable!(),
                })
            }
        }
    }
    pub fn request_clear_delay(&self, field_id: FieldId) {
        let field = self.fields[field_id].borrow();
        field
            .clear_delay_request_count
            .set(field.clear_delay_request_count.get() + 1);
        for &fr in &field.field_refs {
            self.request_clear_delay(fr);
        }
    }
    pub fn relinquish_clear_delay(&self, field_id: FieldId) {
        let field = self.fields[field_id].borrow();
        field
            .clear_delay_request_count
            .set(field.clear_delay_request_count.get() - 1);
        for &fr in &field.field_refs {
            self.relinquish_clear_delay(fr);
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
    // returns false if it was uncow'ed
    fn propagate_clear(
        &mut self,
        msm: &mut MatchSetManager,
        source_field_id: FieldId,
        field_id: FieldId,
    ) -> bool {
        let cow_source = self.fields[source_field_id].borrow_mut();
        let mut field = self.fields[field_id].borrow_mut();
        match &field.iter_hall.data_source {
            FieldDataSource::Owned => {
                panic!(
                    "propagate_clear called for {:?}",
                    field.iter_hall.data_source
                )
            }
            FieldDataSource::Alias(_) | FieldDataSource::Cow(_) => (),
            FieldDataSource::RecordBufferCow(_) => (),
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => {
                if field.get_clear_delay_request_count() > 0 {
                    return false;
                } else {
                    field.iter_hall.data_source =
                        FieldDataSource::Cow(*src_field);
                }
            }
            FieldDataSource::RecordBufferDataCow(data_ref) => {
                if field.get_clear_delay_request_count() > 0 {
                    return false;
                } else {
                    field.iter_hall.data_source =
                        FieldDataSource::RecordBufferCow(*data_ref);
                }
            }
        }
        field.iter_hall.reset_iterators();
        let fr = field.deref_mut();
        msm.match_sets[fr.match_set]
            .action_buffer
            .drop_field_commands(
                field_id,
                &mut fr.first_actor,
                &mut fr.snapshot,
            );
        drop(cow_source);
        let mut i = 0;
        while field.iter_hall.cow_targets.len() > i {
            let cow_tgt_id = field.iter_hall.cow_targets[i];
            drop(field);
            let still_cowed = self.propagate_clear(msm, field_id, cow_tgt_id);
            field = self.fields[field_id].borrow_mut();
            if still_cowed {
                i += 1;
            } else {
                let mut cow_tgt = self.fields[cow_tgt_id].borrow_mut();
                field.iter_hall.cow_targets.swap_remove(i);
                cow_tgt.iter_hall.uncow_get_field_with_rc(self);
            }
        }
        true
    }
    pub fn clear_if_owned(
        &mut self,
        msm: &mut MatchSetManager,
        field_id: FieldId,
    ) {
        let mut field = self.fields[field_id].borrow_mut();
        let FieldDataSource::Owned = &field.iter_hall.data_source else {
            return;
        };
        let shadowed_by = field.shadowed_by;
        if shadowed_by != VOID_FIELD_ID {
            drop(field);
            self.propagate_clear(msm, field_id, shadowed_by);
            field = self.fields[field_id].borrow_mut();
        }

        let mut i = 0;
        let mut first_uncow_field_id = None;

        while field.iter_hall.cow_targets.len() > i {
            let cow_tgt_id = field.iter_hall.cow_targets[i];
            drop(field);
            let still_cowed = self.propagate_clear(msm, field_id, cow_tgt_id);
            field = self.fields[field_id].borrow_mut();
            if still_cowed {
                i += 1;
            } else {
                field.iter_hall.cow_targets.swap_remove(i);
                if first_uncow_field_id.is_none() {
                    first_uncow_field_id = Some(cow_tgt_id);
                } else {
                    let mut cow_tgt = self.fields[cow_tgt_id].borrow_mut();
                    cow_tgt.iter_hall.uncow_get_field_with_rc(self);
                }
            }
        }
        if let Some(first_uncow_field_id) = first_uncow_field_id {
            let mut cow_tgt = self.fields[first_uncow_field_id].borrow_mut();
            std::mem::swap(
                &mut cow_tgt.iter_hall.field_data,
                &mut field.iter_hall.field_data,
            );
            debug_assert!(field.iter_hall.field_data.is_empty());
            cow_tgt.iter_hall.data_source = FieldDataSource::Owned;
        } else {
            field.iter_hall.field_data.clear();
        }
        field.iter_hall.reset_iterators();
        let fr = field.deref_mut();
        msm.match_sets[fr.match_set]
            .action_buffer
            .drop_field_commands(
                field_id,
                &mut fr.first_actor,
                &mut fr.snapshot,
            );
    }
    pub fn get_first_actor(&self, field_id: FieldId) -> ActorRef {
        let field = self.fields[field_id].borrow();
        field.first_actor
    }
    pub fn add_field(
        &mut self,
        msm: &mut MatchSetManager,
        ms_id: MatchSetId,
        name: Option<StringStoreEntry>,
        first_actor: ActorRef,
    ) -> FieldId {
        self.add_field_with_data(
            msm,
            ms_id,
            name,
            first_actor,
            FieldData::default(),
        )
    }
    pub fn add_field_with_data(
        &mut self,
        msm: &mut MatchSetManager,
        ms_id: MatchSetId,
        name: Option<StringStoreEntry>,
        first_actor: ActorRef,
        data: FieldData,
    ) -> FieldId {
        let mut field = Field {
            name,
            ref_count: 1,
            shadowed_since: ActionBuffer::MAX_ACTOR_ID,
            shadowed_by: VOID_FIELD_ID,
            clear_delay_request_count: Cell::new(0),
            match_set: ms_id,
            first_actor,
            snapshot: Default::default(),
            iter_hall: IterHall::new_with_data(data),
            field_refs: Default::default(),
            #[cfg(feature = "debug_logging")]
            producing_transform_id: None,
            #[cfg(feature = "debug_logging")]
            producing_transform_arg: "".to_string(),
        };
        self.bump_field_refcount(field.shadowed_by);
        field.iter_hall.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        let field_id = self.fields.claim_with_value(RefCell::new(field));
        if let Some(name) = name {
            msm.match_sets[ms_id].field_name_map.insert(name, field_id);
        }
        field_id
    }
    pub fn update_data_cow_headers(&self, field_id: FieldId) {
        let mut field = self.fields[field_id].borrow_mut();
        let FieldDataSource::DataCow {
            src_field,
            header_iter,
        } = field.iter_hall.data_source
        else {
            return;
        };

        let src = self.fields[src_field].borrow();
        let iter = src.iter_hall.get_iter_state(header_iter);
        let (headers, count) = self.get_field_headers(src);
        let mut copy_headers_from = iter.header_idx;
        if iter.header_rl_offset != 0 {
            let h = headers[iter.header_idx];
            let additional_run_len = h.run_length - iter.header_rl_offset;
            if additional_run_len > 0 {
                field.iter_hall.field_data.headers.push(FieldValueHeader {
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
                })
            }
            copy_headers_from += 1;
        }
        let additional_len = count - iter.field_pos;
        field
            .iter_hall
            .field_data
            .headers
            .extend_from_slice(&headers[copy_headers_from..]);
        field.iter_hall.field_data.field_count += additional_len;
        let src = self.fields[src_field].borrow();
        unsafe {
            src.iter_hall.store_iter_state_unchecked(
                header_iter,
                src.iter_hall.get_iter_state_at_end(self),
            );
        }
    }

    pub fn apply_field_actions(
        &self,
        msm: &mut MatchSetManager,
        mut field_id: FieldId,
    ) {
        let mut field = self.borrow_field_dealiased(&mut field_id);
        if let (Some(cow_src_id), _) = field.iter_hall.cow_source_field(self) {
            self.apply_field_actions(msm, cow_src_id);
            drop(field);
            self.update_data_cow_headers(field_id);
            field = self.fields[field_id].borrow();
        }
        for &f in &field.field_refs {
            self.apply_field_actions(msm, f);
        }
        let match_set = field.match_set;
        let cb = &mut msm.match_sets[match_set].action_buffer;
        drop(field);
        cb.execute(self, field_id);
    }
    // bumps the refcount of the field by one
    pub fn get_cross_ms_cow_field(
        &mut self,
        msm: &mut MatchSetManager,
        tgt_match_set: MatchSetId,
        src_field_id: FieldId,
    ) -> FieldId {
        let name = self.fields[src_field_id].borrow().name;
        let field_id =
            self.add_field(msm, tgt_match_set, name, ActorRef::default());
        let vacant_entry =
            match msm.match_sets[tgt_match_set].cow_map.entry(src_field_id) {
                Entry::Occupied(e) => {
                    let field_id = *e.get();
                    self.bump_field_refcount(field_id);
                    return field_id;
                }
                Entry::Vacant(e) => e,
            };
        let mut src_field = self.fields[src_field_id].borrow_mut();
        src_field.ref_count += 1;
        src_field.iter_hall.cow_targets.push(field_id);
        vacant_entry.insert(field_id);
        let mut field = self.fields[field_id].borrow_mut();
        field.iter_hall.data_source = FieldDataSource::Cow(src_field_id);
        for i in 0..src_field.field_refs.len() {
            let ref_field_id = src_field.field_refs[i];
            drop(src_field);
            drop(field);
            let cow_field_id =
                self.get_cross_ms_cow_field(msm, tgt_match_set, ref_field_id);
            field = self.fields[field_id].borrow_mut();
            src_field = self.fields[src_field_id].borrow_mut();
            field.field_refs.push(cow_field_id);
        }
        field_id
    }
    pub fn setup_cross_ms_cow_fields(
        &mut self,
        msm: &mut MatchSetManager,
        #[allow(unused)] // only used for debug_assert
        src_ms_id: MatchSetId,
        tgt_ms_id: MatchSetId,
        src_field_ids: &[FieldId],
        tgt_field_ids: &[FieldId],
    ) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(src_field_ids.len() == tgt_field_ids.len());
            src_field_ids
                .iter()
                .all(|id| self.fields[*id].borrow().match_set == src_ms_id);
            tgt_field_ids
                .iter()
                .all(|id| self.fields[*id].borrow().match_set == tgt_ms_id);
        }
        let tgt_ms = &mut msm.match_sets[tgt_ms_id];
        tgt_ms
            .cow_map
            .extend(src_field_ids.iter().zip(tgt_field_ids));
        for &tgt_field_id in tgt_field_ids {
            let mut f = self.fields[tgt_field_id].borrow_mut();
            f.iter_hall.field_data.clear();
            for i in 0..f.field_refs.len() {
                let fr = f.field_refs[i];
                drop(f);
                self.drop_field_refcount(fr, msm);
                f = self.fields[tgt_field_id].borrow_mut();
            }
            f.field_refs.clear();
            let (cow_src_field_id, _) = f.iter_hall.cow_source_field(self);
            if let Some(id) = cow_src_field_id {
                self.remove_from_cow_tgt_list(tgt_field_id, id);
            }
        }
        for (&src_id, &tgt_id) in src_field_ids.iter().zip(tgt_field_ids) {
            let mut src_field = self.fields[src_id].borrow_mut();
            let mut tgt_field = self.fields[tgt_id].borrow_mut();
            src_field.ref_count += 1;
            src_field.iter_hall.cow_targets.push(tgt_id);
            tgt_field.iter_hall.data_source = FieldDataSource::Cow(src_id);
            for i in 0..src_field.field_refs.len() {
                let ref_field_id = src_field.field_refs[i];
                drop(src_field);
                drop(tgt_field);
                let cow_field_id =
                    self.get_cross_ms_cow_field(msm, tgt_ms_id, ref_field_id);
                src_field = self.fields[src_id].borrow_mut();
                tgt_field = self.fields[tgt_id].borrow_mut();
                tgt_field.field_refs.push(cow_field_id);
            }
        }
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
            FieldDataSource::Cow(src) | FieldDataSource::Alias(src) => {
                let fr = self.get_cow_field_ref_raw(src);
                let mut iter = Iter::from_start(fr.destructured_field_ref());
                FieldData::copy(&mut iter, &mut |f| f(tgt));
            }
            FieldDataSource::DataCow {
                src_field,
                header_iter: _,
            } => {
                std::mem::swap(
                    &mut tgt.field_count,
                    &mut src.iter_hall.field_data.field_count,
                );
                std::mem::swap(
                    &mut tgt.headers,
                    &mut src.iter_hall.field_data.headers,
                );
                let fr = self.get_cow_field_ref_raw(src_field);
                let iter = Iter::from_start(fr.destructured_field_ref());
                unsafe {
                    FieldData::copy_data(iter, &mut |f| f(tgt));
                }
            }
            FieldDataSource::RecordBufferCow(rb) => {
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
        if let (Some(cow_src_id), _data_cow) =
            field.iter_hall.cow_source_field(self)
        {
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
                let fr = if self.fields[fr_src].borrow().match_set != ms_id {
                    if let Some(&id) =
                        msm.match_sets[ms_id].cow_map.get(&fr_src)
                    {
                        id
                    } else {
                        let id =
                            self.get_cross_ms_cow_field(msm, ms_id, fr_src);
                        self.setup_field_refs(msm, id);
                        id
                    }
                } else {
                    fr_src
                };
                self.fields[field_id].borrow_mut().field_refs[i] = fr;
            }
        }
    }
    pub(crate) fn get_cow_field_ref_raw(
        &self,
        field_id: FieldId,
    ) -> CowFieldDataRef<'_> {
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
        msm: &mut MatchSetManager,
        field_id: FieldId,
    ) -> CowFieldDataRef<'_> {
        let field_id = self.dealias_field_id(field_id);
        self.apply_field_actions(msm, field_id);
        return self.get_cow_field_ref_raw(field_id);
    }
    pub fn get_auto_deref_iter<'a>(
        &'a self,
        input_field_id: FieldId,
        input_field: &'a CowFieldDataRef<'a>,
        input_iter_id: IterId,
        batch_size: usize,
    ) -> AutoDerefIter<'a, BoundedIter<Iter<DestructuredFieldDataRef<'a>>>>
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
    ) -> Iter<'a, DestructuredFieldDataRef<'a>> {
        let field = self.borrow_field_dealiased(&mut field_id);
        // PERF: maybe write a custom compare instead of doing this traversal?
        assert!(cfdr.destructured_field_ref().equals(
            &self
                .get_cow_field_ref_raw(field_id)
                .destructured_field_ref()
        ));
        let state = field.iter_hall.get_iter_state(iter_id);
        unsafe {
            field.iter_hall.get_iter_from_state_unchecked(
                cfdr.destructured_field_ref(),
                state,
            )
        }
    }
    pub fn store_iter<'a, R: FieldDataRef<'a>>(
        &self,
        mut field_id: FieldId,
        iter_id: IterId,
        iter: impl Into<Iter<'a, R>>,
    ) {
        let iter_base = iter.into();
        let field = self.borrow_field_dealiased(&mut field_id);
        assert!(iter_base.field_data_ref().equals(
            &self
                .get_cow_field_ref_raw(field_id)
                .destructured_field_ref()
        ));
        unsafe { field.iter_hall.store_iter_unchecked(iter_id, iter_base) };
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
        let id = FieldRefOffset::new(src.field_refs.len() as u16).unwrap();
        src.field_refs.push(refs_target);
        id
    }
    pub fn remove_field(&mut self, id: FieldId, msm: &mut MatchSetManager) {
        #[cfg(feature = "debug_logging")]
        {
            print!("removing field {id}");
            println!();
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
        let alias = field.shadowed_by;
        drop(field);
        self.fields.release(id);
        if let Some(cow_src) = cow_src {
            self.remove_from_cow_tgt_list(id, cow_src);
            self.drop_field_refcount(cow_src, msm);
        }
        for fr in &frs {
            self.drop_field_refcount(*fr, msm);
        }
        self.drop_field_refcount(alias, msm);
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
            print!("dropped ref to field {field_id} (rc {})", rc);
            println!();
        }
    }
}

impl Drop for FieldManager {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if !std::thread::panicking() {
            self.fields.release(VOID_FIELD_ID);
            // TODO: this does not work yet, because e.g. callcc
            // does not properly clean up it's cow targets yet
            // reenable this once it works
            // debug_assert!(self.fields.any_used().is_none());
        }
    }
}

impl Default for FieldManager {
    fn default() -> Self {
        let mut res = Self {
            fields: Default::default(),
        };
        let id = res.fields.claim_with_value(RefCell::new(Field {
            ref_count: 1,
            #[cfg(feature = "debug_logging")]
            producing_transform_arg: "<Dummy Input Field>".to_string(),
            ..Default::default()
        }));
        debug_assert!(VOID_FIELD_ID == id);
        res
    }
}
