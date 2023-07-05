use crate::{
    field_data::{
        field_value_flags::{self, FieldValueFlags},
        iters::{ Iter, FieldIterator},
        typed_iters::{TypedSliceIter, InlineBytesIter, InlineTextIter},
        FieldReference, FieldValueHeader, RunLength, typed::{TypedValue, TypedRange, TypedSlice}
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

            if let Some((field_ref, field_rl)) = self.refs_iter.peek() {
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
                if data_shared_val != refs_shared_val {
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
            unsafe { std::slice::from_raw_parts(refs_data_start as *const FieldReference, header_count) }
        ))
    }
}

pub struct AutoDerefIter<'a, I: FieldIterator<'a>> {
    iter: I,
    iter_field_id: FieldId,
    ref_iter: RefIter<'a>,
}
pub struct RefAwareTypedRange<'a> {
    pub base: TypedRange<'a>,
    pub refs: Option<&'a [FieldReference]>,
    pub field_id: FieldId
}

/*
impl<'a> Deref for RefAwareTypedRange<'a> {
    type Target = TypedRange<'a>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}*/

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
    pub fn typed_range_fwd<'b>(
        &'b mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        limit: usize,
    ) -> Option<RefAwareTypedRange<'b>> {
        loop {
            if let Some((range, refs)) =
                self.ref_iter
                    .typed_range_fwd(match_sets, limit, field_value_flags::BYTES_ARE_UTF8)
            {
                return Some(RefAwareTypedRange {
                    base: range,
                    refs: Some(refs),
                    field_id: refs[0].field
                });
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(range) = self
                .iter
                .typed_range_fwd(limit, field_value_flags::BYTES_ARE_UTF8)
            {
                if let TypedSlice::Reference(refs) = range.data {
                    let refs_iter = TypedSliceIter::from_range(&range, refs);
                    let field_id = refs_iter.peek().unwrap().0.field;
                    self.ref_iter
                        .reset(match_sets, refs_iter, field_id, field_pos);
                    continue;
                }
                return Some(RefAwareTypedRange {
                    base: range,
                    refs: None,
                    field_id: self.iter_field_id
                });
            } else {
                return None;
            }
        }
    }
}

pub struct RefAwareInlineBytesIter<'a> {
    iter: InlineBytesIter<'a>,
    refs: Option<&'a [FieldReference]>
}

impl<'a> RefAwareInlineBytesIter<'a> {
    pub fn new( data: &'a [u8],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<&'a [FieldReference]>
    ) -> Self {
        Self { iter: InlineBytesIter::new(data, headers, first_oversize, last_oversize), refs }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, data: &'a [u8]) -> Self{
        Self {iter: InlineBytesIter::from_range(&range.base, data), refs: range.refs}
    }
}

impl<'a> Iterator for RefAwareInlineBytesIter<'a> {
    type Item = (&'a [u8], RunLength, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (mut data, rl) = self.iter.next()?;
        let mut offset = 0;
        if let Some(refs) = self.refs {
            let idx = refs.len() - self.iter.headers_remaining() - 1;
            let r = &refs[idx];
            data = &data[r.begin..r.end];
            offset = r.begin;
        }
        Some((data, rl, offset))
    }
}

pub struct RefAwareInlineTextIter<'a> {
    iter: InlineTextIter<'a>,
    refs: Option<&'a [FieldReference]>
}

impl<'a> RefAwareInlineTextIter<'a> {
    pub fn new( data: &'a str,
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<&'a [FieldReference]>
    ) -> Self {
        Self { iter: InlineTextIter::new(data, headers, first_oversize, last_oversize), refs }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, data: &'a str) -> Self{
        Self {iter: InlineTextIter::from_range(&range.base, data), refs: range.refs}
    }
}

impl<'a> Iterator for RefAwareInlineTextIter<'a> {
    type Item = (&'a str, RunLength, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (mut data, rl) = self.iter.next()?;
        let mut offset=0;
        if let Some(refs) = self.refs {
            let idx = refs.len() - self.iter.headers_remaining() - 1;
            let r = &refs[idx];
            data = &data[r.begin..r.end];
            offset = r.begin;
        }
        Some((data,  rl, offset))
    }
}

pub struct RefAwareBytesBufferIter<'a> {
    iter: TypedSliceIter<'a, Vec<u8>>,
    refs: Option<&'a [FieldReference]>
}

impl<'a> RefAwareBytesBufferIter<'a> {
    pub fn new( values: &'a [Vec<u8>],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<&'a [FieldReference]>
    ) -> Self {
        Self { iter: TypedSliceIter::new(values, headers, first_oversize, last_oversize), refs }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, values: &'a [Vec<u8>]) -> Self{
        Self {iter: TypedSliceIter::from_range(&range.base, values), refs: range.refs}
    }
}

impl<'a> Iterator for RefAwareBytesBufferIter<'a> {
    type Item = (&'a [u8], RunLength,  usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (buf, rl) = self.iter.next()?;
        let mut data = buf.as_slice();
        let mut offset = 0;
        if let Some(refs) = self.refs {
            let idx = refs.len() - self.iter.headers_remaining() - 1;
            let r = &refs[idx];
            data = &data[r.begin..r.end];
            offset = r.begin;
        }
        Some((data,  rl, offset))
    }
}

pub struct RefAwareTextBufferIter<'a> {
    iter: TypedSliceIter<'a, String>,
    refs: Option<&'a [FieldReference]>
}

impl<'a> RefAwareTextBufferIter<'a> {
    pub fn new( values: &'a [String],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<&'a [FieldReference]>
    ) -> Self {
        debug_assert!(headers.first().map(|h|h.bytes_are_utf8()).unwrap_or(true));
        Self { iter: TypedSliceIter::new(values, headers, first_oversize, last_oversize), refs }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, values: &'a [String]) -> Self{
        Self {iter: TypedSliceIter::from_range(&range.base, values), refs: range.refs}
    }
}

impl<'a> Iterator for RefAwareTextBufferIter<'a> {
    type Item = (&'a str, RunLength, usize);

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: because we store TextBuffers as Vec<u8> we have to do this
        // check here so the unsafe conversion to str below is sound
        // because it would be possible to construct this
        assert!(self.iter.peek_header()?.bytes_are_utf8());
        let (buf, rl) = self.iter.next()?;
        let mut data = buf.as_str();
        let mut offset=0;
        if let Some(refs) = self.refs {
            let idx = refs.len() - self.iter.headers_remaining() - 1;
            let r = &refs[idx];
            data = &data[r.begin..r.end];
            offset = r.begin;
        }
        Some((data,  rl, offset))
    }
}
