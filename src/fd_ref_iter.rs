use crate::{
    field_data::{
        fd_iter::{
            FDIter, FDIterator, FDTypedField, FDTypedRange, FDTypedSlice, FDTypedValue,
            TypedSliceIter,
        },
        field_value_flags::{self},
        FieldReference, FieldValueFormat, FieldValueHeader, FieldValueKind, RunLength,
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
    // fields -owns-> field_ref -owns-> data_iter
    // As long as we hold fields, we can safely hold the others.
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
    pub header: FieldValueHeader,
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
                cb.execute_for_iter_halls(std::iter::once(field_ref_mut), start, last_acs);
            } else {
                drop(field_ref_mut);
            }
            field_ref = fields[field_id].borrow();
        }
        let field_ref_laundered = unsafe { &*(field_ref.deref() as *const Field) as &'b Field };
        let data_iter = field_ref_laundered
            .field_data
            .get_iter(FIELD_REF_LOOKUP_ITER_ID);
        (fields[field_id].borrow(), data_iter)
    }
    pub fn reset(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        refs_iter: TypedSliceIter<'a, FieldReference>,
        field: FieldId,
        field_pos: usize,
    ) {
        self.refs_iter = refs_iter;
        self.move_to_field_pos(match_sets, field, field_pos);
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
    pub fn typed_field_fwd(
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
            header: tf.header,
        })
    }
}

#[derive(Default)]
pub struct FDRefIterLazy<'a> {
    iter: Option<FDRefIter<'a>>,
}

pub struct FDRefIterLazyGuard<'a, 'b> {
    iter: &'b mut FDRefIter<'a>,
}

impl<'a, 'b> Drop for FDRefIterLazyGuard<'a, 'b> {
    fn drop(&mut self) {
        // SAFETY:
        // make sure these aren't used after the lifetime of the supplied headers/refs ('b) has ended
        // because this iter only stores pointers, it suffices to set them into a state where they will
        // never be accessed again
        self.iter.refs_iter.clear();
    }
}

impl<'a, 'b> FDRefIterLazyGuard<'a, 'b> {
    pub fn typed_range_fwd(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        limit: usize,
    ) -> Option<FieldRefUnpacked<'a>> {
        self.iter.typed_field_fwd(match_sets, limit)
    }
}

impl<'a: 'b, 'b> FDRefIterLazy<'a> {
    pub fn setup_iter(
        &'b mut self,
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field_pos: usize,
        headers: &'b [FieldValueHeader],
        refs: &'b [FieldReference],
        oversize_start: RunLength,
        oversize_end: RunLength,
    ) -> FDRefIterLazyGuard<'a, 'b> {
        // SAFETY: this is fine because FDRefIterLazyGuard may only live for 'b and will clear
        // the refs in it's drop impl
        let (headers, refs) = unsafe {
            std::mem::transmute::<
                (&'b [FieldValueHeader], &'b [FieldReference]),
                (&'a [FieldValueHeader], &'a [FieldReference]),
            >((headers, refs))
        };
        let refs_iter = TypedSliceIter::new(refs, headers, oversize_start, oversize_end);
        let field = refs[0].field;

        if let Some(iter) = &mut self.iter {
            iter.set_refs_iter(refs_iter);
            iter.move_to_field_pos(match_sets, field, field_pos);
        } else {
            self.iter = Some(FDRefIter::new(
                refs_iter, fields, match_sets, field, field_pos,
            ))
        }
        FDRefIterLazyGuard {
            iter: self.iter.as_mut().unwrap(),
        }
    }
    pub fn setup_iter_from_typed_range(
        &'b mut self,
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field_pos: usize,
        range: &'b FDTypedRange,
        refs: &'b [FieldReference],
    ) -> FDRefIterLazyGuard<'a, 'b> {
        self.setup_iter(
            fields,
            match_sets,
            field_pos,
            range.headers,
            refs,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        )
    }
    pub fn setup_iter_from_typed_field(
        &'b mut self,
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        field_pos: usize,
        field_ref: &'b FieldReference,
        field: &'b FDTypedField,
    ) -> FDRefIterLazyGuard<'a, 'b> {
        self.setup_iter(
            fields,
            match_sets,
            field_pos,
            std::slice::from_ref(&field.header),
            std::slice::from_ref(field_ref),
            0,
            0,
        )
    }
}

pub struct FDAutoDerefIter<'a, I: FDIterator<'a>> {
    iter: I,
    iter_field_id: FieldId,
    ref_iter: FDRefIter<'a>,
    dummy_header: FieldValueHeader,
    dummy_val: FDTypedValue<'a>,
}
pub struct FDTypedRangeWithField<'a> {
    pub base: FDTypedRange<'a>,
    pub field_id: FieldId,
    pub offset: usize,
}
impl<'a> Deref for FDTypedRangeWithField<'a> {
    type Target = FDTypedRange<'a>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a, I: FDIterator<'a>> FDAutoDerefIter<'a, I> {
    pub fn new(
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        iter_field_id: FieldId,
        iter: I,
        refs_field_id: Option<FieldId>,
    ) -> Self {
        let refs_field_id =
            refs_field_id.unwrap_or_else(|| match_sets.any_used().unwrap().err_field_id);
        let ref_iter = FDRefIter::new(
            TypedSliceIter::default(),
            fields,
            match_sets,
            refs_field_id,
            iter.get_next_field_pos(),
        );
        Self {
            iter,
            ref_iter,
            iter_field_id,
            dummy_header: FieldValueHeader {
                fmt: FieldValueFormat::default(),
                run_length: 0,
            },
            dummy_val: FDTypedValue::Unset(()),
        }
    }
    pub fn into_base_iter(self) -> I {
        self.iter
    }
    pub fn move_to_field_pos(&mut self, _field_pos: usize) {
        todo!();
    }
    fn as_slice<'b>(
        data: &'b FDTypedValue<'b>,
        header: &mut FieldValueHeader,
        begin: usize,
        end: usize,
    ) -> FDTypedSlice<'b> {
        let data = match data {
            FDTypedValue::Unset(v) => FDTypedSlice::Unset(std::slice::from_ref(v)),
            FDTypedValue::Null(v) => FDTypedSlice::Null(std::slice::from_ref(v)),
            FDTypedValue::Integer(v) => FDTypedSlice::Integer(std::slice::from_ref(v)),
            FDTypedValue::StreamValueId(v) => FDTypedSlice::StreamValueId(std::slice::from_ref(v)),
            FDTypedValue::Reference(v) => FDTypedSlice::Reference(std::slice::from_ref(v)),
            FDTypedValue::Error(v) => FDTypedSlice::Error(std::slice::from_ref(v)),
            FDTypedValue::Html(v) => FDTypedSlice::Html(std::slice::from_ref(v)),
            FDTypedValue::BytesInline(v) => FDTypedSlice::BytesInline(&v[begin..end]),
            FDTypedValue::TextInline(v) => FDTypedSlice::TextInline(&v[begin..end]),
            FDTypedValue::BytesBuffer(v) => {
                header.fmt.kind = FieldValueKind::BytesInline;
                FDTypedSlice::BytesInline(&v.as_slice()[begin..end])
            }
            FDTypedValue::Object(v) => FDTypedSlice::Object(std::slice::from_ref(v)),
        };
        if header.fmt.kind.is_variable_sized_type() {
            //HACK: this can easily overflow for BytesBuffer
            //TODO: think about a proper solution
            header.fmt.size = (end - begin) as u16;
        }
        data
    }
    pub fn typed_range_fwd<'b>(
        &'b mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        limit: usize,
    ) -> Option<FDTypedRangeWithField<'b>> {
        loop {
            if let Some(fru) = self.ref_iter.typed_field_fwd(match_sets, limit) {
                //TODO: do something more clever to batch this more
                self.dummy_header = fru.header;
                self.dummy_val = fru.data;

                let data =
                    Self::as_slice(&self.dummy_val, &mut self.dummy_header, fru.begin, fru.end);
                return Some(FDTypedRangeWithField {
                    base: FDTypedRange {
                        headers: std::slice::from_ref(&self.dummy_header),
                        data,
                        field_count: fru.header.run_length as usize,
                        first_header_run_length_oversize: 0,
                        last_header_run_length_oversize: 0,
                    },
                    field_id: fru.field,
                    offset: fru.begin,
                });
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(range) = self
                .iter
                .typed_range_fwd(limit, field_value_flags::BYTES_ARE_UTF8)
            {
                if let FDTypedSlice::Reference(refs) = range.data {
                    let refs_iter = TypedSliceIter::from_typed_range(&range, refs);
                    let field_id = refs_iter.peek().unwrap().0.field;
                    self.ref_iter
                        .reset(match_sets, refs_iter, field_id, field_pos);
                    continue;
                }
                return Some(FDTypedRangeWithField {
                    base: range,
                    field_id: self.iter_field_id,
                    offset: 0,
                });
            } else {
                return None;
            }
        }
    }
}
