use crate::{
    field_data::{
        field_value_flags::{self, FieldValueFlags},
        iters::{ Iter, FieldIterator},
        typed_iters::TypedSliceIter,
        FieldReference, FieldValueHeader, FieldValueKind, RunLength, typed::{TypedValue, TypedRange, TypedSlice}
    },
    utils::universe::Universe,
    worker_thread_session::{Field, FieldId, MatchSet, MatchSetId, FIELD_REF_LOOKUP_ITER_ID},
};
use core::ops::Deref;
use std::cell::{Ref, RefCell};

pub struct RefIter<'a> {
    refs_iter: TypedSliceIter<'a, FieldReference>,
    last_field_id: FieldId,

    // SAFETY: We have a chain of lifetime dependencies here:
    // fields -owns-> field_ref -owns-> data_iter
    // As long as we hold fields, we can safely hold the others.
    // Since the borrow checker does not understand this, we have to
    // cheat and use unsafe here.
    data_iter: Iter<'a>,
    field_ref: Ref<'a, Field>,
    fields: &'a Universe<FieldId, RefCell<Field>>,
}

pub struct FieldRefUnpacked<'a> {
    pub field: FieldId,
    pub begin: usize,
    pub end: usize,
    pub data: TypedValue<'a>,
    pub header: FieldValueHeader,
}

impl<'a> RefIter<'a> {
    pub fn new(
        refs_iter: TypedSliceIter<'a, FieldReference>,
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        last_field_id: FieldId,
        field_pos: usize,
    ) -> Self {
        let (field_ref, mut data_iter) =
            unsafe { RefIter::get_field_ref_and_iter(fields, match_sets, last_field_id) };
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
    ) -> (Ref<'b, Field>, Iter<'b>) {
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
            unsafe { RefIter::get_field_ref_and_iter(self.fields, match_sets, field) };
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
    pub fn typed_range_fwd(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<(TypedRange<'a>, &'a [FieldReference])> {
        let (field_ref, field_rl) = self.refs_iter.peek()?;
        let field = field_ref.field;
        let refs_shared_val = field_rl != 1;

        self.move_to_field_keep_pos(match_sets, field);
        let fmt = self.data_iter.get_next_field_format();
        let data_rl = self.data_iter.field_run_length_fwd();
        let data_shared_val = fmt.shared_value() && data_rl != 1;

        let mut refs_rl = self.refs_iter.field_run_length_fwd();
        if refs_shared_val != data_shared_val {
            debug_assert!(refs_rl != 0);
            refs_rl = 1;
        }

        let header_ref = self.data_iter.get_next_header_ref();
        let header_start = self.data_iter.get_next_header_index();
        let oversize_start = self.data_iter.field_run_length_bwd();
        let data_start = self.data_iter.get_next_field_data();
        let refs_data_start = self.refs_iter.data_ptr();

        if refs_rl != data_rl {
            let rl = (refs_rl.min(data_rl) as usize).min(limit) as RunLength;
            let tf = self.data_iter.typed_field_fwd(rl).unwrap();
            self.refs_iter.next_n_fields(tf.header.run_length as usize);
            return Some((
                TypedRange {
                    headers: std::slice::from_ref(header_ref),
                    data: tf.value.as_slice(),
                    field_count: tf.header.run_length as usize,
                    first_header_run_length_oversize: oversize_start,
                    last_header_run_length_oversize: data_rl - rl,
                },
                std::slice::from_ref(field_ref),
            ));
        }
        let mut header_count = 0;
        let mut field_count = 0;
        let mut oversize_end = 0;
        loop {
            field_count += self.data_iter.next_header() as usize;
            self.refs_iter.next();
            header_count += 1;

            if let Some((field_ref, rl)) = self.refs_iter.peek() {
                if field_ref.field != field {
                    break;
                }
                let next_fmt = self.data_iter.get_next_field_format();
                if next_fmt.kind != fmt.kind || next_fmt.flags & flag_mask != fmt.flags & flag_mask
                {
                    break;
                }
                let data_rl = self.data_iter.field_run_length_fwd();
                let data_shared_val = next_fmt.shared_value() && data_rl != 1;

                let mut refs_rl = self.refs_iter.field_run_length_fwd();
                let refs_shared_val = field_rl != 1;
                if refs_rl != data_rl {
                    debug_assert!(refs_rl != 0);
                    refs_rl = 1;
                }

                if refs_rl != data_rl {
                    let rl = (refs_rl.min(data_rl) as usize).min(limit) as RunLength;
                    oversize_end = data_rl - rl;
                    header_count += 1;
                    self.data_iter.next_n_fields(rl as usize);
                    self.refs_iter.next_n_fields(rl as usize);
                    break;
                }
            } else {
                break;
            }
        }
        Some((
            TypedRange {
                headers: unsafe {
                    std::slice::from_raw_parts(header_ref as *const FieldValueHeader, header_count)
                },
                data: unsafe {
                    TypedSlice::new(
                        self.data_iter.field_data_ref(),
                        header_ref.fmt,
                        true,
                        data_start,
                        self.data_iter.get_prev_field_data_end(),
                        field_count,
                    )
                },
                field_count: field_count,
                first_header_run_length_oversize: oversize_start,
                last_header_run_length_oversize: oversize_end,
            },
            std::slice::from_ref(field_ref),
        ))
    }
}

pub struct AutoDerefIter<'a, I: FieldIterator<'a>> {
    iter: I,
    iter_field_id: FieldId,
    ref_iter: RefIter<'a>,
}
pub struct ReferenceAwareTypedRange<'a> {
    pub base: TypedRange<'a>,
    pub refs: Option<&'a [FieldReference]>,
}

impl<'a, I: FieldIterator<'a>> AutoDerefIter<'a, I> {
    pub fn new(
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        iter_field_id: FieldId,
        iter: I,
        refs_field_id: Option<FieldId>,
    ) -> Self {
        let refs_field_id =
            refs_field_id.unwrap_or_else(|| match_sets.any_used().unwrap().err_field_id);
        let ref_iter = RefIter::new(
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
        }
    }
    pub fn into_base_iter(self) -> I {
        self.iter
    }
    pub fn move_to_field_pos(&mut self, _field_pos: usize) {
        todo!();
    }
    fn as_slice<'b>(
        data: &'b TypedValue<'b>,
        header: &mut FieldValueHeader,
        begin: usize,
        end: usize,
    ) -> TypedSlice<'b> {
        let data = match data {
            TypedValue::Unset(v) => TypedSlice::Unset(std::slice::from_ref(v)),
            TypedValue::Null(v) => TypedSlice::Null(std::slice::from_ref(v)),
            TypedValue::Integer(v) => TypedSlice::Integer(std::slice::from_ref(v)),
            TypedValue::StreamValueId(v) => TypedSlice::StreamValueId(std::slice::from_ref(v)),
            TypedValue::Reference(v) => TypedSlice::Reference(std::slice::from_ref(v)),
            TypedValue::Error(v) => TypedSlice::Error(std::slice::from_ref(v)),
            TypedValue::Html(v) => TypedSlice::Html(std::slice::from_ref(v)),
            TypedValue::BytesInline(v) => TypedSlice::BytesInline(&v[begin..end]),
            TypedValue::TextInline(v) => TypedSlice::TextInline(&v[begin..end]),
            TypedValue::BytesBuffer(v) => {
                header.fmt.kind = FieldValueKind::BytesInline;
                TypedSlice::BytesInline(&v.as_slice()[begin..end])
            }
            TypedValue::Object(v) => TypedSlice::Object(std::slice::from_ref(v)),
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
    ) -> Option<ReferenceAwareTypedRange<'b>> {
        loop {
            if let Some((range, refs)) =
                self.ref_iter
                    .typed_range_fwd(match_sets, limit, field_value_flags::BYTES_ARE_UTF8)
            {
                return Some(ReferenceAwareTypedRange {
                    base: range,
                    refs: Some(refs),
                });
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(range) = self
                .iter
                .typed_range_fwd(limit, field_value_flags::BYTES_ARE_UTF8)
            {
                if let TypedSlice::Reference(refs) = range.data {
                    let refs_iter = TypedSliceIter::from_typed_range(&range, refs);
                    let field_id = refs_iter.peek().unwrap().0.field;
                    self.ref_iter
                        .reset(match_sets, refs_iter, field_id, field_pos);
                    continue;
                }
                return Some(ReferenceAwareTypedRange {
                    base: range,
                    refs: None,
                });
            } else {
                return None;
            }
        }
    }
}
