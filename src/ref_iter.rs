use crate::{
    field_data::{
        field_value_flags::FieldValueFlags,
        iters::{FieldIterator, Iter},
        typed::{TypedRange, TypedSlice, TypedValue, ValidTypedRange},
        typed_iters::{InlineBytesIter, TypedSliceIter},
        FieldReference, FieldValueHeader, RunLength,
    },
    job_session::{
        Field, FieldId, FieldManager, MatchSetManager, FIELD_REF_LOOKUP_ITER_ID, INVALID_FIELD_ID,
    },
    stream_value::StreamValueId,
};
use std::cell::Ref;

pub struct RefIter<'a> {
    refs_iter: TypedSliceIter<'a, FieldReference>,
    last_field_id: FieldId,
    data_iter: Option<Iter<'a>>,
    field_ref: Option<Ref<'a, Field>>,
    field_mgr: &'a FieldManager,
    unconsumed_input: bool,
}

impl<'a> Clone for RefIter<'a> {
    fn clone(&self) -> Self {
        Self {
            refs_iter: self.refs_iter.clone(),
            last_field_id: self.last_field_id.clone(),
            data_iter: self.data_iter.clone(),
            field_ref: if self.field_ref.is_some() {
                Some(self.field_mgr.fields[self.last_field_id].borrow())
            } else {
                None
            },
            field_mgr: self.field_mgr.clone(),
            unconsumed_input: self.unconsumed_input,
        }
    }
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
        field_mgr: &'a FieldManager,
        match_set_mgr: &'_ mut MatchSetManager,
        last_field_id: FieldId,
        field_pos: usize,
        unconsumed_input: bool,
    ) -> Self {
        field_mgr.apply_field_actions(match_set_mgr, last_field_id);
        let (field_ref, mut data_iter) =
            unsafe { Self::get_field_ref_and_iter(field_mgr, last_field_id, unconsumed_input) };
        data_iter.move_to_field_pos(field_pos);
        Self {
            refs_iter,
            field_mgr,
            last_field_id,
            data_iter: Some(data_iter),
            field_ref: Some(field_ref),
            unconsumed_input,
        }
    }
    unsafe fn get_field_ref_and_iter<'b>(
        field_mgr: &'b FieldManager,
        field_id: FieldId,
        unconsumed_input: bool,
    ) -> (Ref<'b, Field>, Iter<'b>) {
        let field_ref = field_mgr.borrow_field_cow(field_id, unconsumed_input);
        // this is explicitly *not* cow aware for now, because that would be unsound
        // it doesn't matter too much, and this whole FIELD_REF_LOOKUP_ITER_ID thing is pretty stupid anyways
        let iter = field_ref.field_data.get_iter(FIELD_REF_LOOKUP_ITER_ID);
        let iter_lifetime_laundered = unsafe { std::mem::transmute::<Iter<'_>, Iter<'b>>(iter) };

        (field_ref, iter_lifetime_laundered)
    }
    pub fn reset(
        &mut self,
        match_set_mgr: &'_ mut MatchSetManager,
        refs_iter: TypedSliceIter<'a, FieldReference>,
        field: FieldId,
        field_pos: usize,
    ) {
        self.refs_iter = refs_iter;
        self.move_to_field_pos(match_set_mgr, field, field_pos);
    }

    fn move_to_field(&mut self, match_set_mgr: &'_ mut MatchSetManager, field_id: FieldId) {
        if self.last_field_id == field_id {
            return;
        }
        self.field_ref
            .take()
            .unwrap()
            .field_data
            .store_iter(FIELD_REF_LOOKUP_ITER_ID, self.data_iter.take().unwrap());
        self.field_mgr.apply_field_actions(match_set_mgr, field_id);
        let (field_ref, data_iter) = unsafe {
            Self::get_field_ref_and_iter(self.field_mgr, field_id, self.unconsumed_input)
        };
        // SAFETY: we have to reassign data_iter first, because the old one still
        // has a pointer into the data of the old field_ref
        self.field_ref = Some(field_ref);
        self.data_iter = Some(data_iter);
        self.last_field_id = field_id;
    }
    pub fn move_to_field_keep_pos(
        &mut self,
        match_set_mgr: &'_ mut MatchSetManager,
        field_id: FieldId,
    ) {
        if self.last_field_id == field_id {
            return;
        }
        self.move_to_field_pos(
            match_set_mgr,
            field_id,
            self.data_iter.as_ref().unwrap().get_next_field_pos(),
        );
    }
    pub fn move_to_field_pos(
        &mut self,
        match_set_mgr: &'_ mut MatchSetManager,
        field_id: FieldId,
        field_pos: usize,
    ) {
        self.move_to_field(match_set_mgr, field_id);
        self.data_iter
            .as_mut()
            .unwrap()
            .move_to_field_pos(field_pos);
    }
    pub fn set_refs_iter(&mut self, refs_iter: TypedSliceIter<'a, FieldReference>) {
        self.refs_iter = refs_iter;
    }
    pub fn typed_field_fwd(
        &mut self,
        match_set_mgr: &'_ mut MatchSetManager,
        limit: usize,
    ) -> Option<FieldRefUnpacked<'a>> {
        let (field_ref, rl) = self.refs_iter.peek()?;
        self.move_to_field_keep_pos(match_set_mgr, field_ref.field);
        let iter = self.data_iter.as_mut().unwrap();
        let tf = iter
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
        match_set_mgr: &'_ mut MatchSetManager,
        mut limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<(ValidTypedRange<'a>, TypedSliceIter<'a, FieldReference>)> {
        let refs_headers_start = self.refs_iter.peek_header()?;
        let refs_data_start = self.refs_iter.data_ptr();
        let refs_oversize_start = self.refs_iter.field_run_length_bwd();
        let ref_header_idx = self.refs_iter.headers_remaining();
        let (mut field_ref, mut field_rl) = self.refs_iter.next()?;
        let field = field_ref.field;
        self.move_to_field_keep_pos(match_set_mgr, field);
        let iter = self.data_iter.as_mut().unwrap();
        let fmt = iter.get_next_field_format();

        let data_start = iter.get_next_field_data();
        let header_ref = iter.get_next_header_ref();
        let oversize_start = iter.field_run_length_bwd();
        let header_idx = iter.get_next_header_index();

        let mut refs_oversize_end = 0;
        let mut field_count = 0;
        loop {
            let data_stride = iter.next_n_fields_with_fmt(
                (field_rl as usize).min(limit),
                [fmt.kind],
                false,
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
        let mut header_count = iter.get_next_header_index() - header_idx;
        if iter.field_run_length_bwd() != 0 {
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
                        iter.field_data_ref(),
                        header_ref.fmt,
                        flag_mask,
                        data_start,
                        iter.get_prev_field_data_end(),
                        field_count,
                    ),
                    field_count: field_count,
                    first_header_run_length_oversize: oversize_start,
                    last_header_run_length_oversize: iter.field_run_length_fwd_oversize(),
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
    pub fn next_n_fields(&mut self, limit: usize) -> usize {
        let ref_skip = self.refs_iter.next_n_fields(limit);
        let data_iter = self.data_iter.as_mut().unwrap();
        if self.refs_iter.peek().map(|(v, _rl)| v.field) == Some(self.last_field_id) {
            let data_skip = data_iter.next_n_fields(ref_skip);
            assert!(data_skip == ref_skip);
        } else {
            self.last_field_id = INVALID_FIELD_ID;
        }
        return ref_skip;
    }
}

#[derive(Clone)]
pub struct AutoDerefIter<'a, I: FieldIterator<'a>> {
    iter: I,
    iter_field_id: FieldId,
    ref_iter: Option<RefIter<'a>>,
    field_mgr: &'a FieldManager,
    unconsumed_input: bool,
}
pub struct RefAwareTypedRange<'a> {
    pub base: ValidTypedRange<'a>,
    pub refs: Option<TypedSliceIter<'a, FieldReference>>,
    pub field_id: FieldId,
}

impl<'a, I: FieldIterator<'a>> AutoDerefIter<'a, I> {
    pub fn new(field_mgr: &'a FieldManager, iter_field_id: FieldId, iter: I) -> Self {
        Self {
            iter,
            ref_iter: None,
            field_mgr,
            iter_field_id,
            unconsumed_input: field_mgr.fields[iter_field_id]
                .borrow()
                .has_unconsumed_input
                .get(),
        }
    }
    pub fn into_base_iter(self) -> I {
        self.iter
    }
    pub fn move_to_field_pos(&mut self, field_pos: usize) {
        self.ref_iter = None;
        self.iter.move_to_field_pos(field_pos);
    }
    pub fn typed_range_fwd(
        &mut self,
        match_set_mgr: &'_ mut MatchSetManager,
        limit: usize,
        flags: FieldValueFlags,
    ) -> Option<RefAwareTypedRange<'a>> {
        loop {
            if let Some(ri) = &mut self.ref_iter {
                if let Some((range, refs)) = ri.typed_range_fwd(match_set_mgr, limit, flags) {
                    let (fr, _) = refs.peek().unwrap();
                    return Some(RefAwareTypedRange {
                        base: range,
                        refs: Some(refs),
                        field_id: fr.field,
                    });
                }
                self.ref_iter = None;
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(range) = self.iter.typed_range_fwd(limit, flags) {
                if let TypedSlice::Reference(refs) = range.data {
                    let refs_iter = TypedSliceIter::from_range(&range, refs);
                    let field_id = refs_iter.peek().unwrap().0.field;
                    if let Some(ri) = &mut self.ref_iter {
                        ri.reset(match_set_mgr, refs_iter, field_id, field_pos);
                    } else {
                        self.ref_iter = Some(RefIter::new(
                            refs_iter,
                            self.field_mgr,
                            match_set_mgr,
                            field_id,
                            field_pos,
                            self.unconsumed_input,
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
    pub fn next_n_fields(&mut self, mut limit: usize) -> usize {
        let mut ri_count = 0;
        if let Some(ri) = &mut self.ref_iter {
            ri_count = ri.next_n_fields(limit);
            if ri_count == limit {
                return limit;
            }
            limit -= ri_count;
        }
        let base_count = self.iter.next_n_fields(limit);
        if base_count > 0 {
            self.ref_iter = None;
        }
        return ri_count + base_count;
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

pub struct RefAwareStreamValueIter<'a> {
    iter: TypedSliceIter<'a, StreamValueId>,
    refs: Option<TypedSliceIter<'a, FieldReference>>,
}

impl<'a> RefAwareStreamValueIter<'a> {
    pub unsafe fn new(
        values: &'a [StreamValueId],
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
    pub fn from_range(range: &'a RefAwareTypedRange, values: &'a [StreamValueId]) -> Self {
        Self {
            iter: TypedSliceIter::from_range(&range.base, values),
            refs: range.refs.clone(),
        }
    }
}

impl<'a> Iterator for RefAwareStreamValueIter<'a> {
    type Item = (StreamValueId, Option<core::ops::Range<usize>>, RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut refs_iter) = self.refs {
            let (fr, rl_ref) = refs_iter.peek()?;
            let (data, rl_data) = self.iter.peek()?;
            let run_len = rl_ref.min(rl_data);
            self.iter.next_n_fields(run_len as usize);
            refs_iter.next_n_fields(run_len as usize);
            Some((*data, Some(fr.begin..fr.end), run_len))
        } else {
            let (data, rl) = self.iter.next()?;
            Some((*data, None, rl))
        }
    }
}

pub struct RefAwareUnfoldRunLength<I, T> {
    iter: I,
    last: Option<T>,
    remaining_run_len: RunLength,
}

impl<I: Iterator<Item = (T, RunLength, usize)>, T: Clone> RefAwareUnfoldRunLength<I, T> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            last: None,
            remaining_run_len: 0,
        }
    }
}

pub trait RefAwareUnfoldIterRunLength<T>: Sized {
    fn unfold_rl(self) -> RefAwareUnfoldRunLength<Self, T>;
}

impl<T: Clone, I: Iterator<Item = (T, RunLength, usize)>> RefAwareUnfoldIterRunLength<T> for I {
    fn unfold_rl(self) -> RefAwareUnfoldRunLength<Self, T> {
        RefAwareUnfoldRunLength::new(self)
    }
}

impl<I: Iterator<Item = (T, RunLength, usize)>, T: Clone> Iterator
    for RefAwareUnfoldRunLength<I, T>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_run_len > 0 {
            self.remaining_run_len -= 1;
            return self.last.clone();
        } else if let Some((v, rl, _offset)) = self.iter.next() {
            self.remaining_run_len = rl - 1;
            self.last = Some(v);
        } else {
            self.last = None;
        }
        self.last.clone()
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
        job_session::{
            Field, FieldId, FieldManager, MatchSet, MatchSetManager, FIELD_REF_LOOKUP_ITER_ID,
        },
        ref_iter::{AutoDerefIter, RefAwareInlineTextIter},
    };

    fn push_field(field_mgr: &mut FieldManager, fd: FieldData, id: Option<FieldId>) -> FieldId {
        let mut field = Field::default();
        field.field_data.reset_with_data(fd);
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        if let Some(id) = id {
            field_mgr
                .fields
                .reserve_id_with(id, Default::default, || RefCell::new(field));
            id
        } else {
            field_mgr.fields.claim_with_value(RefCell::new(field))
        }
    }
    fn compare_iter_output(
        fd: FieldData,
        fd_refs: FieldData,
        expected: &[(&'static str, RunLength, usize)],
    ) {
        let mut field_mgr = FieldManager {
            fields: Default::default(),
        };

        let field_id = push_field(&mut field_mgr, fd, Default::default());
        let refs_field_id = push_field(&mut field_mgr, fd_refs, None);
        let mut match_set_mgr = MatchSetManager {
            match_sets: Default::default(),
        };
        match_set_mgr.match_sets.claim_with_value(MatchSet {
            stream_participants: Default::default(),
            command_buffer: Default::default(),
            field_name_map: Default::default(),
        });

        let refs_borrow = field_mgr.fields[refs_field_id].borrow();
        let mut ref_iter = AutoDerefIter::new(&field_mgr, field_id, refs_borrow.field_data.iter());
        let range = ref_iter
            .typed_range_fwd(
                &mut match_set_mgr,
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
