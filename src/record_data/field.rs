use std::cell::{Cell, Ref, RefCell};

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

    pub name: Option<StringStoreEntry>,
    // fields potentially referenced by this field.
    // keeps them alive until this field is dropped
    pub field_refs: SmallVec<[FieldId; 4]>,
    pub iter_hall: IterHall,

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
    pub fn has_cow_targets(&self) -> bool {
        !self.iter_hall.cow_targets.is_empty()
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
    pub fn untangle_cow_field_refs(
        &mut self,
        msm: &mut MatchSetManager,
        field_id: FieldId,
        cow_source: FieldId,
    ) {
        let field = self.fields[field_id].borrow();
        for &fr in &field.field_refs {
            self.bump_field_refcount(fr);
        }
        drop(field);
        self.drop_field_refcount(cow_source, msm);
    }
    pub fn uncow(&mut self, msm: &mut MatchSetManager, field_id: FieldId) {
        let mut field = self.fields[field_id].borrow_mut();
        let cow_source = field.iter_hall.uncow_get_field_with_rc(self);
        if let Some(cow_source) = cow_source {
            drop(field);
            let mut cow_src = self.fields[cow_source].borrow_mut();
            let cow_tgt_pos = cow_src
                .iter_hall
                .cow_targets
                .iter()
                .position(|t| *t == field_id)
                .unwrap();
            cow_src.iter_hall.cow_targets.swap_remove(cow_tgt_pos);
            drop(cow_src);
            self.untangle_cow_field_refs(msm, field_id, cow_source);
        }
    }

    pub fn get_field_headers_for_iter_hall<'a>(
        &'a self,
        fr: Ref<'a, IterHall>,
    ) -> (Ref<'a, Vec<FieldValueHeader>>, usize) {
        match fr.data_source {
            FieldDataSource::Cow(src) => self.get_field_headers_for_iter_hall(
                Ref::map(self.fields[src].borrow(), |f| &f.iter_hall),
            ),
            FieldDataSource::Owned
            | FieldDataSource::RecordBufferDataCow(_)
            | FieldDataSource::DataCow(_)
            | FieldDataSource::RecordBufferCow(_) => {
                let field_count = fr.field_data.field_count;
                (Ref::map(fr, |f| &f.field_data.headers), field_count)
            }
        }
    }
    pub fn get_field_data_for_iter_hall<'a>(
        &'a self,
        fr: Ref<'a, IterHall>,
    ) -> Ref<'a, AlignedBuf<MAX_FIELD_ALIGN>> {
        match &fr.data_source {
            FieldDataSource::Cow(data_ref)
            | FieldDataSource::DataCow(data_ref) => self
                .get_field_data_for_iter_hall(Ref::map(
                    self.fields[*data_ref].borrow(),
                    |f| &f.iter_hall,
                )),
            FieldDataSource::RecordBufferCow(_)
            | FieldDataSource::RecordBufferDataCow(_)
            | FieldDataSource::Owned => {
                Ref::map(fr, |f| match f.data_source {
                    FieldDataSource::RecordBufferCow(data_ref)
                    | FieldDataSource::RecordBufferDataCow(data_ref) => {
                        &unsafe { &*(*data_ref).get() }.data
                    }
                    FieldDataSource::Owned => &f.field_data.data,
                    _ => unreachable!(),
                })
            }
        }
    }
    // returns false if it was uncow'ed
    fn propagate_clear(
        &mut self,
        msm: &mut MatchSetManager,
        cow_source_id: FieldId,
        field_id: FieldId,
    ) -> bool {
        let cow_source = self.fields[cow_source_id].borrow_mut();
        let mut field = self.fields[field_id].borrow_mut();
        match &field.iter_hall.data_source {
            FieldDataSource::Owned => {
                panic!("propagate_clear called for FieldDataSource::Owned")
            }
            FieldDataSource::Cow(_) => (),
            FieldDataSource::RecordBufferCow(_) => (),
            FieldDataSource::DataCow(data_ref) => {
                if field.get_clear_delay_request_count() > 0 {
                    return false;
                } else {
                    field.iter_hall.data_source =
                        FieldDataSource::Cow(*data_ref);
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
        msm.match_sets[field.match_set]
            .command_buffer
            .drop_field_commands(field_id, &mut field.action_indices);
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
        msm.match_sets[field.match_set]
            .command_buffer
            .drop_field_commands(field_id, &mut field.action_indices);
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
            action_indices: FieldActionIndices::new(min_apf),
            name: Default::default(),
            iter_hall: IterHall::new_with_data(data),
            #[cfg(feature = "debug_logging")]
            producing_transform_id: None,
            #[cfg(feature = "debug_logging")]
            producing_transform_arg: "".to_string(),
            field_refs: Default::default(),
        };
        field.iter_hall.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        self.fields.claim_with_value(RefCell::new(field))
    }

    pub fn apply_field_actions(
        &self,
        msm: &mut MatchSetManager,
        field_id: FieldId,
    ) {
        let field = self.fields[field_id].borrow();
        let match_set = field.match_set;
        let cb = &mut msm.match_sets[match_set].command_buffer;
        drop(field);
        cb.execute(self, field_id);
    }

    pub fn setup_cow(
        &mut self,
        msm: &mut MatchSetManager,
        field_id: FieldId,
        data_source_id: FieldId,
    ) {
        let mut field = self.fields[field_id].borrow_mut();
        if let (Some(cow_src), _) = field.iter_hall.cow_source_field() {
            drop(field);
            self.uncow(msm, cow_src);
            field = self.fields[field_id].borrow_mut();
        }
        let mut data_source = self.fields[data_source_id].borrow_mut();
        data_source.ref_count += 1;
        data_source.iter_hall.cow_targets.push(field_id);
        assert!(field.field_refs.is_empty());
        field.field_refs.extend_from_slice(&data_source.field_refs);
        field.iter_hall.data_source = FieldDataSource::Cow(data_source_id);
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
            FieldDataSource::Cow(_) => {
                let fr = self.get_cow_field_ref(field_id, false);
                let mut iter = Iter::from_start(fr.destructured_field_ref());
                FieldData::copy(&mut iter, &mut |f| f(tgt));
            }
            FieldDataSource::DataCow(data_ref) => {
                std::mem::swap(
                    &mut tgt.field_count,
                    &mut src.iter_hall.field_data.field_count,
                );
                std::mem::swap(
                    &mut tgt.headers,
                    &mut src.iter_hall.field_data.headers,
                );
                let fr = self.get_cow_field_ref(data_ref, false);
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
        let fd = Ref::map(field, |f| &f.iter_hall);
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
    ) {
        let mut src = self.fields[refs_field].borrow_mut();
        let mut tgt = self.fields[refs_target].borrow_mut();
        src.field_refs.push(refs_target);
        tgt.ref_count += 1;
        for fr in &tgt.field_refs {
            self.fields[*fr].borrow_mut().ref_count += 1;
        }
        src.field_refs.extend_from_slice(&tgt.field_refs);
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
        let (cow_src, _) = field.iter_hall.cow_source_field();
        let frs = std::mem::take(&mut field.field_refs);
        drop(field);
        self.fields.release(id);
        if let Some(cow_src) = cow_src {
            self.drop_field_refcount(cow_src, msm);
        }
        for fr in &frs {
            self.drop_field_refcount(*fr, msm);
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
            print!("dropped ref to field {field_id} (rc {})", rc);
            println!();
        }
    }
}

impl Drop for FieldManager {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if !std::thread::panicking() {
            self.fields.release(DUMMY_INPUT_FIELD_ID);
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
        debug_assert!(DUMMY_INPUT_FIELD_ID == id);
        res
    }
}
