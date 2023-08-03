use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    ops::DerefMut,
};

use nonmax::NonMaxU32;
use smallvec::SmallVec;

use crate::utils::{
    nonzero_ext::NonMaxU32Ext, string_store::StringStoreEntry,
    universe::Universe,
};

use super::{
    command_buffer::{ActionProducingFieldIndex, FieldActionIndices},
    field_data::FieldData,
    iter_hall::{IterHall, IterId},
    iters::{FieldIterator, Iter},
    match_set::{MatchSetId, MatchSetManager},
    ref_iter::AutoDerefIter,
};

pub const FIELD_REF_LOOKUP_ITER_ID: IterId = IterId::MIN;

#[derive(Default)]
pub struct Field {
    // used for checking whether we got rug pulled in case of cow
    pub field_id: FieldId,
    pub ref_count: usize,

    pub action_indices: FieldActionIndices,

    pub names: SmallVec<[StringStoreEntry; 4]>,
    // fields potentially referenced by this field.
    // keeps them alive until this field is dropped
    pub field_refs: SmallVec<[FieldId; 4]>,
    pub cow_source: Option<FieldId>,
    pub field_data: IterHall,

    pub match_set: MatchSetId,

    #[cfg(feature = "debug_logging")]
    pub producing_transform: Option<TransformId>,

    // typically called on input fields which we don't want to borrow mut
    pub clear_delay_request_count: Cell<usize>,
    pub has_unconsumed_input: Cell<bool>,
}

pub type FieldId = NonMaxU32;
pub const DUMMY_INPUT_FIELD_ID: FieldId = FieldId::MIN;

impl Field {
    pub fn get_clear_delay_request_count(&self) -> usize {
        self.clear_delay_request_count.get()
    }
    pub fn has_unconsumed_input_or_equals(&self, value: bool) {
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
}

pub struct FieldManager {
    pub fields: Universe<FieldId, RefCell<Field>>,
}

impl FieldManager {
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
        let id = self.fields.peek_claim_id();
        let mut field = Field {
            field_id: id,
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
            cow_source: None,
            field_data: IterHall::new_with_data(data),
            #[cfg(feature = "debug_logging")]
            producing_transform: None,
            field_refs: Default::default(),
        };
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        self.fields.claim_with_value(RefCell::new(field));
        id
    }
    pub fn uncow(&self, match_set_mgr: &mut MatchSetManager, field: FieldId) {
        let mut field = self.fields[field].borrow_mut();
        if let Some(cow_source) = field.cow_source {
            let src = self.fields[cow_source].borrow();
            let mut iter =
                AutoDerefIter::new(self, cow_source, src.field_data.iter());
            IterHall::copy_resolve_refs(
                match_set_mgr,
                &mut iter,
                &mut |func| func(&mut field.field_data),
            );
            field.cow_source = None;
        }
    }

    // this is usually called while iterating over an input field that contains
    // field references we therefore do NOT want to require a mutable
    // reference over the field data, because that forces the caller to kill
    // their iterator instead we `split up` this struct to only require a
    // mutable reference for the MatchSets, which we need to modify the command
    // buffer
    pub fn apply_field_actions(
        &self,
        match_set_mgr: &mut MatchSetManager,
        field: FieldId,
    ) {
        self.uncow(match_set_mgr, field);
        let mut field_ref = self.fields[field].borrow_mut();
        let f = field_ref.deref_mut();
        let match_set = f.match_set;
        let cb = &mut match_set_mgr.match_sets[match_set].command_buffer;
        cb.execute_for_iter_hall(
            field.get() as usize,
            &mut f.field_data,
            &mut f.action_indices,
        );
    }

    pub fn borrow_field_cow(
        &self,
        field_id: FieldId,
        mark_for_unconsumed_input: bool,
    ) -> Ref<Field> {
        let field = self.fields[field_id].borrow();
        field.has_unconsumed_input_or_equals(mark_for_unconsumed_input);
        if let Some(cow_source) = field.cow_source {
            return self.fields[cow_source].borrow();
        }
        field
    }
    pub fn borrow_field_cow_mut(
        &self,
        field_id: FieldId,
        mark_for_unconsumed_input: bool,
    ) -> RefMut<Field> {
        let field = self.fields[field_id].borrow_mut();
        field.has_unconsumed_input_or_equals(mark_for_unconsumed_input);
        if let Some(cow_source) = field.cow_source {
            return self.fields[cow_source].borrow_mut();
        }
        field
    }
    pub fn get_iter_cow_aware<'a>(
        &self,
        field_id: FieldId,
        field: &'a Field,
        iter_id: IterId,
    ) -> Iter<'a, &'a FieldData> {
        if field.field_id != field_id {
            let state = self.fields[field_id]
                .borrow()
                .field_data
                .get_iter_state(iter_id);
            return unsafe { field.field_data.get_iter_from_state(state) };
        }
        return field.field_data.get_iter(iter_id);
    }
    pub fn store_iter_cow_aware<'a>(
        &self,
        field_id: FieldId,
        field: &'a Field,
        iter_id: IterId,
        iter: impl FieldIterator<'a>,
    ) {
        if field.field_id != field_id {
            let iter_base = iter.into_base_iter();
            assert!(field.field_data.iter_is_from_iter_hall(&iter_base));
            let field = self.fields[field_id].borrow();
            unsafe {
                field.field_data.store_iter_unchecked(iter_id, iter_base)
            };
        } else {
            field.field_data.store_iter(iter_id, iter);
        }
    }

    pub fn bump_field_refcount(&self, field_id: FieldId) {
        self.fields[field_id].borrow_mut().ref_count += 1;
    }
    pub fn inc_field_refcount(&self, field_id: FieldId, n: usize) {
        self.fields[field_id].borrow_mut().ref_count += n;
    }
    pub fn register_field_reference(&self, source: FieldId, target: FieldId) {
        self.fields[source].borrow_mut().field_refs.push(target);
        self.fields[target].borrow_mut().ref_count += 1;
    }
}

impl Default for FieldManager {
    fn default() -> Self {
        let mut res = Self {
            fields: Default::default(),
        };
        let id = res.fields.claim_with_value(RefCell::new(Field {
            ref_count: 1,
            ..Default::default()
        }));
        debug_assert!(DUMMY_INPUT_FIELD_ID == id);
        res
    }
}
