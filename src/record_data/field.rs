use std::{
    cell::{Cell, Ref, RefCell},
    ops::DerefMut,
};

use nonmax::{NonMaxU16, NonMaxU32};
use smallvec::SmallVec;

use crate::utils::{
    aligned_buf::AlignedBuf, nonzero_ext::NonMaxU32Ext,
    string_store::StringStoreEntry, universe::Universe,
};

use super::{
    command_buffer::{ActionProducingFieldIndex, FieldActionIndices},
    field_data::{FieldData, FieldValueHeader, MAX_FIELD_ALIGN},
    iter_hall::{FieldDataSource, IterHall, IterId},
    iters::{DestructuredFieldDataRef, FieldDataRef, FieldIterator, Iter},
    match_set::{MatchSetId, MatchSetManager},
    record_buffer::RecordBufferField,
};

#[cfg(feature = "debug_logging")]
use crate::operators::transform::TransformId;

pub const FIELD_REF_LOOKUP_ITER_ID: IterId = IterId::MIN;

#[derive(Default)]
pub struct Field {
    pub ref_count: usize,

    pub action_indices: FieldActionIndices,

    pub names: SmallVec<[StringStoreEntry; 4]>,
    // fields potentially referenced by this field.
    // keeps them alive until this field is dropped
    pub field_refs: SmallVec<[FieldId; 4]>,
    pub field_data: IterHall,

    pub match_set: MatchSetId,

    #[cfg(feature = "debug_logging")]
    pub producing_transform_id: Option<TransformId>,
    #[cfg(feature = "debug_logging")]
    pub producing_transform_arg: String,

    // typically called on input fields which we don't want to borrow mut
    pub clear_delay_request_count: Cell<usize>,
    pub has_unconsumed_input: Cell<bool>,
}

pub type FieldId = NonMaxU32;
pub type FieldIdOffset = NonMaxU16; // NonMaxU32;
pub const DUMMY_INPUT_FIELD_ID: FieldId = FieldId::MIN;

impl Field {
    pub fn get_clear_delay_request_count(&self) -> usize {
        self.clear_delay_request_count.get()
    }
    pub fn inform_of_unconsumed_input(&self, value: bool) {
        self.has_unconsumed_input
            .set(self.has_unconsumed_input.get() || value);
    }
    pub fn request_clear_delay(&self) {
        self.has_unconsumed_input.set(true);
        self.clear_delay_request_count
            .set(self.clear_delay_request_count.get() + 1);
    }
    pub fn drop_clear_delay_request(&self) {
        self.clear_delay_request_count
            .set(self.clear_delay_request_count.get() - 1);
    }
    pub(super) fn uncow(&mut self, own_field_id: FieldId, fm: &FieldManager) {
        self.field_data.data_source.uncow(own_field_id, fm);
    }
}

pub struct FieldManager {
    pub fields: Universe<FieldId, RefCell<Field>>,
}

pub struct CowFieldDataRef<'a> {
    pub(super) field_count: usize,
    pub(super) headers_ref: Ref<'a, Vec<FieldValueHeader>>,
    pub(super) data_ref: Ref<'a, AlignedBuf<MAX_FIELD_ALIGN>>,
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
    pub fn uncow(&self, field: FieldId) {
        self.fields[field].borrow_mut().uncow(field, self);
    }
    pub fn get_field_headers_for_iter_hall<'a>(
        &'a self,
        fr: Ref<'a, IterHall>,
    ) -> (Ref<'a, Vec<FieldValueHeader>>, usize) {
        match fr.data_source {
            FieldDataSource::Cow(src) => self.get_field_headers_for_iter_hall(
                Ref::map(self.fields[src].borrow(), |f| &f.field_data),
            ),
            FieldDataSource::Owned(_)
            | FieldDataSource::RecordBufferDataCow { .. }
            | FieldDataSource::DataCow { .. }
            | FieldDataSource::RecordBufferCow(_) => {
                let mut fc = 0;
                let headers_ref = Ref::map(fr, |f| match &f.data_source {
                    FieldDataSource::Owned(fd) => {
                        fc = fd.field_count;
                        &fd.headers
                    }
                    FieldDataSource::DataCow {
                        headers,
                        field_count,
                        ..
                    } => {
                        fc = *field_count;
                        headers
                    }
                    FieldDataSource::RecordBufferDataCow {
                        headers,
                        field_count,
                        ..
                    } => {
                        fc = *field_count;
                        headers
                    }
                    FieldDataSource::Cow(_) => unreachable!(),
                    FieldDataSource::RecordBufferCow(rb) => {
                        let fd = unsafe { &*(**rb).get() };
                        fc = fd.field_count;
                        &fd.headers
                    }
                });
                (headers_ref, fc)
            }
        }
    }
    pub fn get_field_data_for_iter_hall<'a>(
        &'a self,
        fr: Ref<'a, IterHall>,
    ) -> Ref<'a, AlignedBuf<MAX_FIELD_ALIGN>> {
        match &fr.data_source {
            FieldDataSource::Cow(data_ref)
            | FieldDataSource::DataCow { data_ref, .. } => self
                .get_field_data_for_iter_hall(Ref::map(
                    self.fields[*data_ref].borrow(),
                    |f| &f.field_data,
                )),
            FieldDataSource::RecordBufferCow(_)
            | FieldDataSource::RecordBufferDataCow { .. } => {
                Ref::map(fr, |f| {
                    if let FieldDataSource::RecordBufferDataCow {
                        data, ..
                    } = &f.data_source
                    {
                        &unsafe { &*(**data).get() }.data
                    } else {
                        unreachable!()
                    }
                })
            }
            FieldDataSource::Owned(_) => Ref::map(fr, |f| {
                if let FieldDataSource::Owned(fd) = &f.data_source {
                    &fd.data
                } else {
                    unreachable!()
                }
            }),
        }
    }
    pub fn get_min_apf_idx(
        &self,
        field_id: FieldId,
    ) -> Option<ActionProducingFieldIndex> {
        let field = self.fields[field_id].borrow();
        field.action_indices.min_apf_idx
    }
    pub fn add_field(
        &mut self,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
    ) -> FieldId {
        self.add_field_with_data(ms_id, min_apf, FieldData::default())
    }
    pub fn add_field_with_data(
        &mut self,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
        data: FieldData,
    ) -> FieldId {
        let mut field = Field {
            ref_count: 1,
            clear_delay_request_count: Cell::new(0),
            has_unconsumed_input: Cell::new(false),
            match_set: ms_id,
            action_indices: FieldActionIndices {
                min_apf_idx: min_apf,
                curr_apf_idx: None,
                first_unapplied_al_idx: 0,
            },
            names: Default::default(),
            field_data: IterHall::new_with_data(data),
            #[cfg(feature = "debug_logging")]
            producing_transform_id: None,
            #[cfg(feature = "debug_logging")]
            producing_transform_arg: "".to_string(),
            field_refs: Default::default(),
        };
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        self.fields.claim_with_value(RefCell::new(field))
    }

    pub fn apply_field_actions(
        &self,
        msm: &mut MatchSetManager,
        field: FieldId,
    ) {
        let mut field_ref = self.fields[field].borrow_mut();
        field_ref.field_data.data_source.uncow_headers(self);
        let f = field_ref.deref_mut();
        let match_set = f.match_set;
        let cb = &mut msm.match_sets[match_set].command_buffer;
        cb.execute_for_iter_hall(
            field.get() as usize,
            &mut f.field_data,
            &mut f.action_indices,
        );
    }

    pub fn setup_cow(&self, field_id: FieldId, data_source_id: FieldId) {
        let mut field = self.fields[field_id].borrow_mut();
        let mut data_source = self.fields[data_source_id].borrow_mut();
        data_source.ref_count += 1;
        data_source.field_data.cow_targets.push(field_id);
        field.field_refs.push(data_source_id);
        assert!(field.field_data.data_source.get_field_count(self) == 0);
        field.field_data.data_source = FieldDataSource::Cow(data_source_id);
    }
    pub fn append_to_buffer<'a>(
        &self,
        iter: impl FieldIterator<'a>,
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
        match &mut src.field_data.data_source {
            FieldDataSource::Owned(fd) => std::mem::swap(tgt, fd),
            FieldDataSource::Cow(_) => {
                let fr = self.get_cow_field_ref(field_id, false);
                let iter = Iter::from_start(fr.destructured_field_ref());
                FieldData::copy(iter, &mut |f| f(tgt));
            }
            FieldDataSource::DataCow {
                headers,
                field_count,
                data_ref,
            } => {
                *field_count = 0;
                let fr = self.get_cow_field_ref(*data_ref, false);
                let iter = Iter::from_start(fr.destructured_field_ref());
                std::mem::swap(&mut tgt.headers, headers);
                unsafe {
                    FieldData::copy_data(iter, &mut |f| f(tgt));
                }
            }
            FieldDataSource::RecordBufferCow(rb) => {
                let fd = unsafe { &mut *(**rb).get() };
                FieldData::copy(fd.iter(), &mut |f| f(tgt));
            }
            FieldDataSource::RecordBufferDataCow {
                headers,
                field_count,
                data,
            } => {
                *field_count = 0;
                std::mem::swap(&mut tgt.headers, headers);
                unsafe {
                    let fd = &mut *(**data).get();
                    FieldData::copy_data(fd.iter(), &mut |f| f(tgt));
                }
            }
        }
    }

    pub fn get_cow_field_ref_for_iter_hall<'a>(
        &'a self,
        ih: Ref<'a, IterHall>,
    ) -> CowFieldDataRef<'a> {
        let (headers_ref, field_count) =
            self.get_field_headers_for_iter_hall(Ref::clone(&ih));
        let data_ref = self.get_field_data_for_iter_hall(ih);
        CowFieldDataRef {
            headers_ref,
            field_count,
            data_ref,
        }
    }
    pub fn get_cow_field_ref(
        &self,
        field_id: FieldId,
        inform_of_unconsumed_input: bool,
    ) -> CowFieldDataRef<'_> {
        let field = self.fields[field_id].borrow();
        field.inform_of_unconsumed_input(inform_of_unconsumed_input);
        let fd = Ref::map(field, |f| &f.field_data);
        self.get_cow_field_ref_for_iter_hall(fd)
    }
    pub fn lookup_iter<'a>(
        &self,
        field_id: FieldId,
        cfdr: &'a CowFieldDataRef<'a>,
        iter_id: IterId,
    ) -> Iter<'a, DestructuredFieldDataRef<'a>> {
        let field = self.fields[field_id].borrow();
        // PERF: maybe write a custom compare instead of doing this traversal?
        assert!(cfdr.destructured_field_ref().equals(
            &self
                .get_cow_field_ref(field_id, false)
                .destructured_field_ref()
        ));
        let state = field.field_data.get_iter_state(iter_id);
        unsafe {
            field.field_data.get_iter_from_state_unchecked(
                cfdr.destructured_field_ref(),
                state,
            )
        }
    }
    pub fn store_iter<'a, R: FieldDataRef<'a>>(
        &self,
        field_id: FieldId,
        iter_id: IterId,
        iter: impl Into<Iter<'a, R>>,
    ) {
        let iter_base = iter.into();
        let field = self.fields[field_id].borrow();
        assert!(iter_base.field_data_ref().equals(
            &self
                .get_cow_field_ref(field_id, false)
                .destructured_field_ref()
        ));
        unsafe { field.field_data.store_iter_unchecked(iter_id, iter_base) };
    }

    pub fn bump_field_refcount(&self, field_id: FieldId) {
        self.fields[field_id].borrow_mut().ref_count += 1;
    }
    pub fn inc_field_refcount(&self, field_id: FieldId, n: usize) {
        self.fields[field_id].borrow_mut().ref_count += n;
    }
    pub fn register_field_reference(&self, source: FieldId, target: FieldId) {
        let mut src = self.fields[source].borrow_mut();
        let mut tgt = self.fields[target].borrow_mut();
        src.field_refs.push(target);
        tgt.ref_count += 1;
        for fr in &tgt.field_refs {
            self.fields[*fr].borrow_mut().ref_count += 1;
        }
        src.field_refs.extend_from_slice(&tgt.field_refs);
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
        debug_assert!(DUMMY_INPUT_FIELD_ID == id);
        res
    }
}
