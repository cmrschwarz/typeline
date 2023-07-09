use crate::{
    field_data::{
        field_value_flags::FieldValueFlags,
        iters::{FieldIterator, Iter},
        typed::{TypedRange, TypedSlice, TypedValue, ValidTypedRange},
        typed_iters::{InlineBytesIter, TypedSliceIter},
        FieldReference, FieldValueHeader, RunLength,
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
        if field_ref.last_applied_action_set_id != last_acs {
            drop(field_ref);
            let mut field_ref_mut = fields[field_id].borrow_mut();
            let start = field_ref_mut.last_applied_action_set_id + 1;
            field_ref_mut.last_applied_action_set_id = last_acs;
            cb.execute_for_iter_halls(std::iter::once(field_ref_mut), start, last_acs);
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
        mut limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<(ValidTypedRange<'a>, TypedSliceIter<'a, FieldReference>)> {
        let refs_headers_start = self.refs_iter.peek_header()?;
        let refs_data_start = self.refs_iter.data_ptr();
        let refs_oversize_start = self.refs_iter.field_run_length_bwd();
        let ref_header_idx = self.refs_iter.headers_remaining();
        let (mut field_ref, mut field_rl) = self.refs_iter.next()?;
        let field = field_ref.field;

        self.move_to_field_keep_pos(match_sets, field);
        let fmt = self.data_iter.get_next_field_format();

        let data_start = self.data_iter.get_next_field_data();
        let header_ref = self.data_iter.get_next_header_ref();
        let oversize_start = self.data_iter.field_run_length_bwd();
        let header_idx = self.data_iter.get_next_header_index();

        let mut refs_oversize_end = 0;
        let mut field_count = 0;
        loop {
            let data_stride = self.data_iter.next_n_fields_with_fmt(
                (field_rl as usize).min(limit),
                [fmt.kind],
                flag_mask,
                fmt.flags,
            );
            field_count += data_stride;
            limit -= data_stride;
            if data_stride != field_rl as usize {
                refs_oversize_end = field_rl - data_stride as RunLength;
                break;
            }

            if let Some(v) = self.refs_iter.next() {
                (field_ref, field_rl) = v;
                if field_ref.field != field {
                    break;
                }
            } else {
                break;
            }
        }
        let mut header_count = self.data_iter.get_next_header_index() - header_idx;
        if self.data_iter.field_run_length_bwd() != 0 {
            header_count += 1;
        }
        let mut refs_header_count = ref_header_idx - self.refs_iter.headers_remaining();
        let mut refs_data_len =
            unsafe { self.refs_iter.data_ptr().offset_from(refs_data_start) as usize };
        if self.refs_iter.field_run_length_bwd() != 0 {
            refs_header_count += 1;
            refs_data_len += 1;
        }
        unsafe {
            Some((
                ValidTypedRange::new(TypedRange {
                    headers: std::slice::from_raw_parts(
                        header_ref as *const FieldValueHeader,
                        header_count,
                    ),
                    data: TypedSlice::new(
                        self.data_iter.field_data_ref(),
                        header_ref.fmt,
                        flag_mask,
                        data_start,
                        self.data_iter.get_prev_field_data_end(),
                        field_count,
                    ),
                    field_count: field_count,
                    first_header_run_length_oversize: oversize_start,
                    last_header_run_length_oversize: self.data_iter.field_run_length_fwd_oversize(),
                }),
                TypedSliceIter::new(
                    std::slice::from_raw_parts(refs_data_start, refs_data_len),
                    std::slice::from_raw_parts(
                        refs_headers_start as *const FieldValueHeader,
                        refs_header_count,
                    ),
                    refs_oversize_start,
                    refs_oversize_end,
                ),
            ))
        }
    }
}

pub struct AutoDerefIter<'a, I: FieldIterator<'a>> {
    iter: I,
    iter_field_id: FieldId,
    ref_iter: Option<RefIter<'a>>,
    fields: &'a Universe<FieldId, RefCell<Field>>,
}
pub struct RefAwareTypedRange<'a> {
    pub base: ValidTypedRange<'a>,
    pub refs: Option<TypedSliceIter<'a, FieldReference>>,
    pub field_id: FieldId,
}

impl<'a, I: FieldIterator<'a>> AutoDerefIter<'a, I> {
    pub fn new(
        fields: &'a Universe<FieldId, RefCell<Field>>,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        iter_field_id: FieldId,
        iter: I,
        refs_field_id: Option<FieldId>,
    ) -> Self {
        let ref_iter = refs_field_id.map(|refs_field_id| {
            RefIter::new(
                TypedSliceIter::default(),
                fields,
                match_sets,
                refs_field_id,
                iter.get_next_field_pos(),
            )
        });
        Self {
            iter,
            ref_iter,
            fields,
            iter_field_id,
        }
    }
    pub fn into_base_iter(self) -> I {
        self.iter
    }
    pub fn move_to_field_pos(&mut self, _field_pos: usize) {
        todo!();
    }
    pub fn typed_range_fwd(
        &mut self,
        match_sets: &'_ mut Universe<MatchSetId, MatchSet>,
        limit: usize,
        flags: FieldValueFlags,
    ) -> Option<RefAwareTypedRange<'a>> {
        loop {
            if let Some(ri) = &mut self.ref_iter {
                if let Some((range, refs)) = ri.typed_range_fwd(match_sets, limit, flags) {
                    let (fr, _) = refs.peek().unwrap();
                    return Some(RefAwareTypedRange {
                        base: range,
                        refs: Some(refs),
                        field_id: fr.field,
                    });
                }
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(range) = self.iter.typed_range_fwd(limit, flags) {
                if let TypedSlice::Reference(refs) = range.data {
                    let refs_iter = TypedSliceIter::from_range(&range, refs);
                    let field_id = refs_iter.peek().unwrap().0.field;
                    if let Some(ri) = &mut self.ref_iter {
                        ri.reset(match_sets, refs_iter, field_id, field_pos);
                    } else {
                        self.ref_iter = Some(RefIter::new(
                            refs_iter,
                            self.fields,
                            match_sets,
                            field_id,
                            field_pos,
                        ));
                    }

                    continue;
                }
                return Some(RefAwareTypedRange {
                    base: range,
                    refs: None,
                    field_id: self.iter_field_id,
                });
            } else {
                return None;
            }
        }
    }
}

pub struct RefAwareInlineBytesIter<'a> {
    iter: InlineBytesIter<'a>,
    refs: Option<TypedSliceIter<'a, FieldReference>>,
}

impl<'a> RefAwareInlineBytesIter<'a> {
    pub fn new(
        data: &'a [u8],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<TypedSliceIter<'a, FieldReference>>,
    ) -> Self {
        Self {
            iter: InlineBytesIter::new(data, headers, first_oversize, last_oversize),
            refs,
        }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, data: &'a [u8]) -> Self {
        Self {
            iter: InlineBytesIter::from_range(&range.base, data),
            refs: range.refs.clone(),
        }
    }
}

impl<'a> Iterator for RefAwareInlineBytesIter<'a> {
    type Item = (&'a [u8], RunLength, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut refs_iter) = self.refs {
            let (fr, rl_ref) = refs_iter.peek()?;
            let (data, rl_data) = self.iter.peek()?;
            let run_len = rl_ref.min(rl_data);
            self.iter.next_n_fields(run_len as usize);
            refs_iter.next_n_fields(run_len as usize);
            Some((&data[fr.begin..fr.end], run_len, fr.begin))
        } else {
            let (data, rl) = self.iter.next()?;
            Some((data, rl, 0))
        }
    }
}

pub struct RefAwareInlineTextIter<'a> {
    iter: RefAwareInlineBytesIter<'a>,
}

impl<'a> RefAwareInlineTextIter<'a> {
    pub fn new(
        data: &'a str,
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<TypedSliceIter<'a, FieldReference>>,
    ) -> Self {
        Self {
            iter: RefAwareInlineBytesIter::new(
                data.as_bytes(),
                headers,
                first_oversize,
                last_oversize,
                refs,
            ),
        }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, data: &'a str) -> Self {
        Self {
            iter: RefAwareInlineBytesIter::from_range(&range, data.as_bytes()),
        }
    }
}

impl<'a> Iterator for RefAwareInlineTextIter<'a> {
    type Item = (&'a str, RunLength, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (data, rl, offset) = self.iter.next()?;
        return Some((unsafe { std::str::from_utf8_unchecked(data) }, rl, offset));
    }
}

pub struct RefAwareBytesBufferIter<'a> {
    iter: TypedSliceIter<'a, Vec<u8>>,
    refs: Option<TypedSliceIter<'a, FieldReference>>,
}

impl<'a> RefAwareBytesBufferIter<'a> {
    pub unsafe fn new(
        values: &'a [Vec<u8>],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<TypedSliceIter<'a, FieldReference>>,
    ) -> Self {
        Self {
            iter: TypedSliceIter::new(values, headers, first_oversize, last_oversize),
            refs,
        }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, values: &'a [Vec<u8>]) -> Self {
        Self {
            iter: TypedSliceIter::from_range(&range.base, values),
            refs: range.refs.clone(),
        }
    }
}

impl<'a> Iterator for RefAwareBytesBufferIter<'a> {
    type Item = (&'a [u8], RunLength, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut refs_iter) = self.refs {
            let (fr, rl_ref) = refs_iter.peek()?;
            let (data, rl_data) = self.iter.peek()?;
            let run_len = rl_ref.min(rl_data);
            self.iter.next_n_fields(run_len as usize);
            refs_iter.next_n_fields(run_len as usize);
            Some((&data.as_slice()[fr.begin..fr.end], run_len, fr.begin))
        } else {
            let (data, rl) = self.iter.next()?;
            Some((data, rl, 0))
        }
    }
}

#[cfg(test)]
mod ref_iter_tests {
    use std::cell::RefCell;

    use nonmax::NonMaxUsize;

    use crate::{
        field_data::{
            field_value_flags, push_interface::PushInterface, typed::TypedSlice, FieldData,
            FieldReference, FieldValueFormat, FieldValueHeader, FieldValueKind, RunLength,
        },
        ref_iter::{AutoDerefIter, RefAwareInlineTextIter},
        utils::universe::Universe,
        worker_thread_session::{Field, FieldId, MatchSet, MatchSetId, FIELD_REF_LOOKUP_ITER_ID},
    };

    fn push_field(
        u: &mut Universe<FieldId, RefCell<Field>>,
        fd: FieldData,
        id: Option<FieldId>,
    ) -> FieldId {
        let mut field = Field::default();
        field.field_data.reset_with_data(fd);
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        if let Some(id) = id {
            u.reserve_id_with(id, Default::default, || RefCell::new(field));
            id
        } else {
            u.claim_with_value(RefCell::new(field))
        }
    }
    fn compare_iter_output(
        fd: FieldData,
        fd_refs: FieldData,
        expected: &[(&'static str, RunLength, usize)],
    ) {
        let mut fields = Universe::<FieldId, RefCell<Field>>::default();

        let field_id = push_field(&mut fields, fd, Default::default());
        let refs_field_id = push_field(&mut fields, fd_refs, None);
        let mut match_sets = Universe::<MatchSetId, MatchSet>::default();
        match_sets.claim_with_value(MatchSet {
            stream_batch_size: Default::default(),
            stream_participants: Default::default(),
            working_set_updates: Default::default(),
            working_set: Default::default(),
            command_buffer: Default::default(),
            field_name_map: Default::default(),
        });

        let refs_borrow = fields[refs_field_id].borrow();
        let mut ref_iter = AutoDerefIter::new(
            &fields,
            &mut match_sets,
            field_id,
            refs_borrow.field_data.iter(),
            Some(field_id),
        );
        let range = ref_iter
            .typed_range_fwd(
                &mut match_sets,
                usize::MAX,
                field_value_flags::BYTES_ARE_UTF8,
            )
            .unwrap();
        let iter = match range.base.data {
            TypedSlice::TextInline(v) => RefAwareInlineTextIter::from_range(&range, v),
            _ => panic!("wrong data type"),
        };

        assert_eq!(iter.collect::<Vec<_>>(), expected);
    }
    fn compare_iter_output_parallel_ref(mut fd: FieldData, expected: &[(&'static str, RunLength)]) {
        let mut fd_refs = FieldData::default();
        let fdi = unsafe { fd.internals() };
        for h in fdi.header.iter_mut() {
            if !h.same_value_as_previous() {
                push_ref(&mut fd_refs, 0, h.size as usize, h.run_length as usize);
                let fdi = unsafe { fd_refs.internals() };
                let h_ref = fdi.header.last_mut().unwrap();
                h_ref.flags |=
                    h.flags & (field_value_flags::DELETED | field_value_flags::SHARED_VALUE);
            } else {
                let fdi = unsafe { fd_refs.internals() };
                fdi.header.push(FieldValueHeader {
                    fmt: FieldValueFormat {
                        kind: FieldValueKind::Reference,
                        flags: h.flags
                            & (field_value_flags::DELETED
                                | field_value_flags::SHARED_VALUE
                                | field_value_flags::SAME_VALUE_AS_PREVIOUS),
                        size: std::mem::size_of::<FieldReference>() as u16,
                    },
                    run_length: h.run_length,
                });
            }
        }

        compare_iter_output(
            fd,
            fd_refs,
            &expected
                .iter()
                .map(|(v, rl)| (*v, *rl, 0))
                .collect::<Vec<_>>(),
        );
    }
    fn push_ref(fd: &mut FieldData, begin: usize, end: usize, rl: usize) {
        fd.push_reference(
            FieldReference {
                field: NonMaxUsize::default(),
                begin: begin,
                end: end,
            },
            rl,
            false,
            false,
        );
    }

    #[test]
    fn simple() {
        let mut fd = FieldData::default();
        fd.push_str("a", 1, false, false);
        fd.push_str("bb", 2, false, false);
        fd.push_str("ccc", 3, false, false);
        compare_iter_output_parallel_ref(fd, &[("a", 1), ("bb", 2), ("ccc", 3)]);
    }

    #[test]
    fn shared_ref() {
        let mut fd = FieldData::default();
        fd.push_str("aaa", 1, false, false);
        fd.push_str("bbbb", 2, false, false);
        fd.push_str("ccccc", 3, false, false);
        let mut fdr = FieldData::default();
        push_ref(&mut fdr, 1, 3, 6);
        compare_iter_output(fd, fdr, &[("aa", 1, 1), ("bb", 2, 1), ("cc", 3, 1)]);
    }

    #[test]
    fn with_deletion() {
        let mut fd = FieldData::default();
        fd.push_str("a", 1, false, false);
        fd.push_str("bb", 2, false, false);
        fd.push_str("ccc", 3, false, false);

        let mut fdr = FieldData::default();
        push_ref(&mut fdr, 0, 1, 1);
        push_ref(&mut fdr, 0, 2, 2);
        push_ref(&mut fdr, 0, 3, 3);

        unsafe {
            let fdi = fd.internals();
            fdi.header[1].set_deleted(true);
            *fdi.field_count -= 2;
            let fdi = fdr.internals();
            fdi.header[1].set_deleted(true);
            *fdi.field_count -= 2;
        }

        compare_iter_output(fd, fdr, &[("a", 1, 0), ("ccc", 3, 0)]);
    }

    #[test]
    fn with_same_as_previous() {
        let mut fd = FieldData::default();
        fd.push_str("aaa", 1, false, false);
        unsafe {
            let fdi = fd.internals();
            fdi.header.extend_from_within(0..1);
            fdi.header[1].set_same_value_as_previous(true);
            fdi.header[1].run_length = 5;
            fdi.header[1].set_shared_value(true);
            *fdi.field_count += 5;
        }
        fd.push_str("c", 3, false, false);
        compare_iter_output_parallel_ref(fd, &[("aaa", 1), ("aaa", 5), ("c", 3)]);
    }

    #[test]
    fn with_same_as_previous_after_deleted() {
        let mut fd = FieldData::default();
        fd.push_str("00", 1, false, false);
        fd.push_str("1", 1, false, false);
        unsafe {
            let fdi = fd.internals();
            fdi.header.extend_from_within(1..2);
            fdi.header[2].set_same_value_as_previous(true);
            fdi.header[2].run_length = 5;
            fdi.header[2].set_shared_value(true);
            *fdi.field_count += 5;
            fdi.header[1].set_deleted(true);
            *fdi.field_count -= 1;
        }
        fd.push_str("333", 3, false, false);
        compare_iter_output_parallel_ref(fd, &[("00", 1), ("1", 5), ("333", 3)]);
    }
}
