use crate::{
    field_data::{
        fd_iter::{FDIter, FDIterator, FDTypedRange, FDTypedValue, TypedSliceIter},
        FieldReference, RunLength,
    },
    utils::universe::Universe,
    worker_thread_session::{Field, FieldId, MatchSet, MatchSetId, FIELD_REF_LOOKUP_ITER_ID},
};
use core::ops::Deref;
use std::cell::{Ref, RefCell};

pub struct FDRefIter<'a> {
    refs_iter: TypedSliceIter<'a, FieldReference>,
    last_field_id: FieldId,

    // SAFETY: We have a chain of lifetime dependencies here:
    // data_iter -> field_ref -> fields
    // As long as we hold entry_data, we can safely hold the others.
    // Since the borrow checker does not understand this, we have to
    // cheat and use unsafe here.
    data_iter: FDIter<'a>,
    field_ref: Ref<'a, Field>,
    fields: &'a Universe<FieldId, RefCell<Field>>,
}

pub struct FieldRefUnpacked<'a> {
    pub field: FieldId,
    pub begin: usize,
    pub end: usize,
    pub data: FDTypedValue<'a>,
    pub run_len: RunLength,
}

impl<'a> FDRefIter<'a> {
    pub fn new(
        refs_iter: TypedSliceIter<'a, FieldReference>,
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        last_field_id: FieldId,
        field_pos: usize,
    ) -> Self {
        let (field_ref, mut data_iter) =
            unsafe { FDRefIter::get_field_ref_and_iter(fields, match_sets, last_field_id) };
        data_iter.move_to_field_pos(field_pos);
        Self {
            refs_iter,
            fields,
            last_field_id,
            data_iter,
            field_ref,
        }
    }
    unsafe fn get_field_ref_and_iter<'b>(
        fields: &'b Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field_id: FieldId,
    ) -> (Ref<'b, Field>, FDIter<'b>) {
        let mut field_ref = fields[field_id].borrow();
        let cb = &mut match_sets[field_ref.match_set].command_buffer;
        let last_acs = cb.last_action_set_id();
        {
            drop(field_ref);
            let mut field_ref_mut = fields[field_id].borrow_mut();
            if field_ref_mut.last_applied_action_set_id != last_acs {
                let start = field_ref_mut.last_applied_action_set_id + 1;
                field_ref_mut.last_applied_action_set_id = last_acs;
                cb.execute(std::iter::once(field_ref_mut), start, last_acs);
            }
            field_ref = fields[field_id].borrow();
        }
        let field_ref_laundered = unsafe { &*(field_ref.deref() as *const Field) as &'b Field };
        let data_iter = field_ref_laundered
            .field_data
            .get_iter(FIELD_REF_LOOKUP_ITER_ID);
        (fields[field_id].borrow(), data_iter)
    }
    fn move_to_field(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field: FieldId,
    ) {
        let (field_ref, data_iter) =
            unsafe { FDRefIter::get_field_ref_and_iter(self.fields, match_sets, field) };
        // SAFETY: we have to reassign data_iter first, because the old one still
        // has a pointer into the data of the old field_ref
        self.data_iter = data_iter;
        self.field_ref = field_ref;
        self.last_field_id = field;
    }
    pub fn move_to_field_keep_pos(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field: FieldId,
    ) {
        if self.last_field_id == field {
            return;
        }
        let field_pos = self.data_iter.get_next_field_pos();
        self.move_to_field(match_sets, field);
        self.data_iter.move_to_field_pos(field_pos);
    }
    pub fn move_to_field_pos(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field: FieldId,
        field_pos: usize,
    ) {
        if self.last_field_id != field {
            self.move_to_field(match_sets, field);
        }
        self.data_iter.move_to_field_pos(field_pos);
    }
    pub fn set_refs_iter(&mut self, refs_iter: TypedSliceIter<'a, FieldReference>) {
        self.refs_iter = refs_iter;
    }
    pub fn typed_range_fwd(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        limit: usize,
    ) -> Option<FieldRefUnpacked<'a>> {
        let (field_ref, rl) = self.refs_iter.peek()?;
        self.move_to_field_keep_pos(match_sets, field_ref.field);
        let tf = self
            .data_iter
            .typed_field_fwd((rl as usize).min(limit) as RunLength)
            .unwrap();
        self.refs_iter.next_n_fields(tf.header.run_length as usize);
        Some(FieldRefUnpacked {
            field: field_ref.field,
            begin: field_ref.begin,
            end: field_ref.end,
            data: tf.value,
            run_len: tf.header.run_length,
        })
    }
}

#[derive(Default)]
pub struct FDRefIterLazy<'a> {
    iter: Option<FDRefIter<'a>>,
}

impl<'a: 'b, 'b> FDRefIterLazy<'a> {
    pub fn typed_range_fwd(
        &mut self,
        match_sets: &'b mut Universe<MatchSetId, MatchSet>,
        limit: usize,
    ) -> Option<FieldRefUnpacked<'a>> {
        self.iter
            .as_mut()
            .and_then(|it| it.typed_range_fwd(match_sets, limit))
    }
    pub fn setup_iter(
        &'b mut self,
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'b mut Universe<MatchSetId, MatchSet>,
        field_pos: usize,
        range: &'b FDTypedRange,
        refs: &'b [FieldReference],
    ) {
        let (range, refs) = unsafe {
            std::mem::transmute::<
                (&'b FDTypedRange, &'b [FieldReference]),
                (&'a FDTypedRange, &'a [FieldReference]),
            >((range, refs))
        };
        let refs_iter = TypedSliceIter::new(
            refs,
            range.headers,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        );
        let field = refs[0].field;

        if let Some(iter) = &mut self.iter {
            iter.set_refs_iter(refs_iter);
            iter.move_to_field_pos(match_sets, field, field_pos);
        } else {
            self.iter = Some(FDRefIter::new(
                refs_iter, fields, match_sets, field, field_pos,
            ))
        }
    }
}
