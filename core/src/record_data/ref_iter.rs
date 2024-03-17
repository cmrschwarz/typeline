use std::{cell::Ref, ops::Range};

use super::{
    field::{
        CowFieldDataRef, Field, FieldId, FieldManager, FieldRefOffset,
        FIELD_REF_LOOKUP_ITER_ID,
    },
    field_data::{field_value_flags, FieldValueType},
    field_value::{FieldReference, SlicedFieldReference},
    iters::{DestructuredFieldDataRef, FieldDataRef},
    match_set::MatchSetManager,
    stream_value::StreamValueId,
};

use crate::record_data::{
    field_data::{
        field_value_flags::FieldValueFlags, FieldValueHeader, RunLength,
    },
    field_value_ref::{
        FieldValueRef, FieldValueSlice, TypedRange, ValidTypedRange,
    },
    field_value_slice_iter::{FieldValueSliceIter, InlineBytesIter},
    iters::{FieldIterator, Iter},
};

pub trait ReferenceFieldValueType: FieldValueType + Clone + 'static {
    fn field_id_offset(&self) -> FieldRefOffset;
    fn range(&self) -> Option<Range<usize>>;
}
impl ReferenceFieldValueType for FieldReference {
    fn field_id_offset(&self) -> FieldRefOffset {
        self.field_ref_offset
    }

    fn range(&self) -> Option<Range<usize>> {
        None
    }
}
impl ReferenceFieldValueType for SlicedFieldReference {
    fn field_id_offset(&self) -> FieldRefOffset {
        self.field_ref_offset
    }

    fn range(&self) -> Option<Range<usize>> {
        Some(self.begin..self.end)
    }
}

pub struct FieldRefUnpacked<'a, R> {
    pub reference: R,
    pub data: FieldValueRef<'a>,
    pub header: FieldValueHeader,
}

pub struct RefIter<'a, R> {
    refs_iter: FieldValueSliceIter<'a, R>,
    refs_field: Ref<'a, Field>,
    last_field_id_offset: FieldRefOffset,
    data_iter: Iter<'a, DestructuredFieldDataRef<'a>>,
    data_cow_ref: CowFieldDataRef<'a>,
    field_mgr: &'a FieldManager,
}

#[derive(Clone)]
pub enum AnyRefIter<'a> {
    FieldRef(RefIter<'a, FieldReference>),
    SlicedFieldRef(RefIter<'a, SlicedFieldReference>),
}

#[derive(Clone)]
pub enum AnyRefSliceIter<'a> {
    FieldRef(FieldValueSliceIter<'a, FieldReference>),
    SlicedFieldRef(FieldValueSliceIter<'a, SlicedFieldReference>),
}

impl<'a, R: ReferenceFieldValueType> FieldRefUnpacked<'a, R> {
    pub fn apply_ref(&self) -> (FieldValueRef<'a>, RunLength) {
        (
            self.reference
                .range()
                .map(|r| self.data.subslice(r))
                .unwrap_or(self.data),
            self.header.run_length,
        )
    }
}

impl<'a, R: ReferenceFieldValueType> Clone for RefIter<'a, R> {
    fn clone(&self) -> Self {
        Self {
            refs_iter: self.refs_iter.clone(),
            refs_field: Ref::clone(&self.refs_field),
            last_field_id_offset: self.last_field_id_offset,
            data_iter: self.data_iter.clone(),
            data_cow_ref: self.data_cow_ref.clone(),
            field_mgr: self.field_mgr,
        }
    }
}

impl<'a, R: ReferenceFieldValueType> RefIter<'a, R> {
    pub fn new(
        refs_field_id: FieldId,
        refs_iter: FieldValueSliceIter<'a, R>,
        field_mgr: &'a FieldManager,
        match_set_mgr: &'_ MatchSetManager,
        last_field_id_offset: FieldRefOffset,
        field_pos: usize,
    ) -> Self {
        let refs_field = field_mgr.fields[refs_field_id].borrow();
        let last_field_id =
            refs_field.field_refs[usize::from(last_field_id_offset)];
        let (data_cow_ref, mut data_iter) = unsafe {
            Self::get_data_ref_and_iter(
                field_mgr,
                match_set_mgr,
                last_field_id,
            )
        };
        data_iter.move_to_field_pos(field_pos);
        Self {
            refs_iter,
            refs_field,
            last_field_id_offset,
            data_iter,
            data_cow_ref,
            field_mgr,
        }
    }
    pub fn reset(
        &mut self,
        match_set_mgr: &'_ MatchSetManager,
        refs_iter: FieldValueSliceIter<'a, R>,
        field_id_offset: FieldRefOffset,
        field_pos: usize,
    ) {
        self.refs_iter = refs_iter;
        self.move_to_field_pos(match_set_mgr, field_id_offset, field_pos);
    }

    fn move_to_field(
        &mut self,
        msm: &'_ MatchSetManager,
        field_id_offset: FieldRefOffset,
    ) {
        if self.last_field_id_offset == field_id_offset {
            return;
        }
        let prev_field_id =
            self.refs_field.field_refs[usize::from(self.last_field_id_offset)];
        let field_id =
            self.refs_field.field_refs[usize::from(field_id_offset)];
        let (cow_ref_new, data_iter_new) = unsafe {
            Self::get_data_ref_and_iter(self.field_mgr, msm, field_id)
        };
        self.field_mgr.store_iter(
            prev_field_id,
            FIELD_REF_LOOKUP_ITER_ID,
            std::mem::replace(&mut self.data_iter, data_iter_new),
        );
        let _ = std::mem::replace(&mut self.data_cow_ref, cow_ref_new);
        self.last_field_id_offset = field_id_offset;
    }
    // SAFETY: caller has to ensure that the cow ref outlives the iter
    unsafe fn get_data_ref_and_iter(
        fm: &FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
    ) -> (CowFieldDataRef<'a>, Iter<'a, DestructuredFieldDataRef<'a>>) {
        let fr = fm.get_cow_field_ref(msm, field_id);
        let iter = fm.lookup_iter(
            field_id,
            unsafe { std::mem::transmute(&fr) },
            FIELD_REF_LOOKUP_ITER_ID,
        );
        unsafe { std::mem::transmute((fr, iter)) }
    }
    pub fn move_to_field_keep_pos(
        &mut self,
        match_set_mgr: &'_ MatchSetManager,
        field_id_offset: FieldRefOffset,
    ) {
        if self.last_field_id_offset == field_id_offset {
            return;
        }
        let pos = self.data_iter.get_next_field_pos();
        self.move_to_field(match_set_mgr, field_id_offset);
        self.data_iter.move_to_field_pos(pos);
    }
    pub fn move_to_field_pos(
        &mut self,
        match_set_mgr: &'_ MatchSetManager,
        field_id_offset: FieldRefOffset,
        field_pos: usize,
    ) {
        if self.last_field_id_offset == field_id_offset {
            self.move_to_field(match_set_mgr, field_id_offset);
        }
        self.data_iter.move_to_field_pos(field_pos);
    }
    pub fn set_refs_iter(&mut self, refs_iter: FieldValueSliceIter<'a, R>) {
        self.refs_iter = refs_iter;
    }
    pub fn typed_field_fwd(
        &mut self,
        match_set_mgr: &'_ mut MatchSetManager,
        limit: usize,
    ) -> Option<FieldRefUnpacked<'a, R>> {
        let (field_ref, rl) = self.refs_iter.peek()?;
        self.move_to_field_keep_pos(
            match_set_mgr,
            field_ref.field_id_offset(),
        );
        let tf = self
            .data_iter
            .typed_field_fwd((rl as usize).min(limit))
            .unwrap();
        self.refs_iter.next_n_fields(tf.header.run_length as usize);
        Some(FieldRefUnpacked {
            reference: field_ref.clone(),
            data: tf.value,
            header: tf.header,
        })
    }
    pub fn typed_range_fwd(
        &mut self,
        match_set_mgr: &'_ MatchSetManager,
        mut limit: usize,
        flag_mask: FieldValueFlags,
    ) -> Option<(ValidTypedRange<'a>, FieldValueSliceIter<'a, R>)> {
        let (mut field_ref, mut field_rl) = self.refs_iter.peek()?;
        let refs_headers_start = self.refs_iter.header_ptr();
        let refs_data_start = self.refs_iter.data_ptr();
        let refs_oversize_start = self.refs_iter.field_run_length_bwd();
        let ref_header_idx = self.refs_iter.headers_remaining();
        let field_id_offset = field_ref.field_id_offset();
        self.move_to_field_keep_pos(match_set_mgr, field_id_offset);
        let fmt = self.data_iter.get_next_field_format();

        let data_start = self.data_iter.get_next_field_data();
        let oversize_start = self.data_iter.field_run_length_bwd();
        let header_idx = self.data_iter.get_next_header_index();

        let mut refs_oversize_end = 0;
        let mut field_count = 0;
        loop {
            let data_stride = self.data_iter.next_n_fields_with_fmt(
                (field_rl as usize).min(limit),
                [fmt.repr],
                false,
                flag_mask,
                fmt.flags,
                false,
            );
            field_count += data_stride;
            limit -= data_stride;
            if data_stride != field_rl as usize {
                self.refs_iter.next_n_fields(data_stride);
                refs_oversize_end = field_rl - data_stride as RunLength;
                break;
            }
            self.refs_iter.next();

            if let Some(v) = self.refs_iter.peek() {
                (field_ref, field_rl) = v;
                if field_ref.field_id_offset() != field_id_offset {
                    break;
                }
            } else {
                break;
            }
        }
        let mut header_count =
            self.data_iter.get_next_header_index() - header_idx;
        if self.data_iter.field_run_length_bwd() != 0 {
            header_count += 1;
        }
        let mut refs_header_count =
            ref_header_idx - self.refs_iter.headers_remaining();
        let mut refs_data_len = unsafe {
            self.refs_iter.data_ptr().offset_from(refs_data_start) as usize
        };
        if self.refs_iter.field_run_length_bwd() != 0 {
            refs_header_count += 1;
            refs_data_len += 1;
        }
        unsafe {
            let (h_s1, h_s2) =
                self.data_iter.field_data_ref().headers().as_slices();
            let headers = if h_s1.len() > header_idx {
                &h_s1[header_idx..header_idx + header_count]
            } else {
                &h_s2[header_idx - h_s1.len()
                    ..header_idx - h_s1.len() + header_count]
            };
            Some((
                ValidTypedRange::new(TypedRange {
                    headers,
                    data: FieldValueSlice::new(
                        self.data_iter.field_data_ref(),
                        fmt,
                        data_start,
                        // HACK // BUG // SAFETY:
                        // this is unsound. we might have skipped over
                        // dead fields with another type
                        self.data_iter.get_prev_field_data_end(),
                        field_count,
                    ),
                    field_count,
                    first_header_run_length_oversize: oversize_start,
                    last_header_run_length_oversize: self
                        .data_iter
                        .field_run_length_fwd_oversize(),
                }),
                FieldValueSliceIter::new(
                    std::slice::from_raw_parts(refs_data_start, refs_data_len),
                    std::slice::from_raw_parts(
                        refs_headers_start,
                        refs_header_count,
                    ),
                    refs_oversize_start,
                    refs_oversize_end,
                ),
            ))
        }
    }
    pub fn next_n_fields(
        &mut self,
        limit: usize,
        allow_ring_wrap: bool,
    ) -> usize {
        let ref_skip = self.refs_iter.next_n_fields(limit);
        if self.refs_iter.peek().map(|(v, _rl)| v.field_id_offset())
            == Some(self.last_field_id_offset)
        {
            let data_skip =
                self.data_iter.next_n_fields(ref_skip, allow_ring_wrap);
            assert!(data_skip == ref_skip);
        }
        ref_skip
    }
    pub fn get_next_field_pos(&self) -> usize {
        self.data_iter.get_next_field_pos()
    }
}

#[derive(Clone)]
pub struct AutoDerefIter<'a, I: FieldIterator<'a>> {
    iter: I,
    iter_field_id: FieldId,
    ref_iter: Option<AnyRefIter<'a>>,
    field_mgr: &'a FieldManager,
}
pub struct RefAwareTypedRange<'a> {
    pub base: ValidTypedRange<'a>,
    pub refs: Option<AnyRefSliceIter<'a>>,
    pub field_ref_offset: Option<FieldRefOffset>,
}

impl<'a, I: FieldIterator<'a>> AutoDerefIter<'a, I> {
    pub fn new(
        field_mgr: &'a FieldManager,
        iter_field_id: FieldId,
        iter: I,
    ) -> Self {
        let iter_field_id = field_mgr.dealias_field_id(iter_field_id);
        Self {
            iter,
            ref_iter: None,
            field_mgr,
            iter_field_id,
        }
    }
    pub fn get_next_field_pos(&mut self) -> usize {
        match &self.ref_iter {
            Some(AnyRefIter::SlicedFieldRef(iter)) => {
                iter.get_next_field_pos()
            }
            Some(AnyRefIter::FieldRef(iter)) => iter.get_next_field_pos(),
            None => self.iter.get_next_field_pos(),
        }
    }
    pub fn move_to_field_pos(&mut self, field_pos: usize) {
        self.ref_iter = None;
        self.iter.move_to_field_pos(field_pos);
    }
    fn setup_for_field_refs_range(
        &mut self,
        match_set_mgr: &MatchSetManager,
        field_pos_before: usize,
        range: &ValidTypedRange<'a>,
    ) -> bool {
        if let FieldValueSlice::FieldReference(refs) = range.data {
            let refs_iter = FieldValueSliceIter::from_valid_range(range, refs);
            let field_id_offset = refs_iter.peek().unwrap().0.field_ref_offset;
            if let Some(AnyRefIter::FieldRef(ri)) = &mut self.ref_iter {
                ri.reset(
                    match_set_mgr,
                    refs_iter,
                    field_id_offset,
                    field_pos_before,
                );
            } else {
                self.ref_iter = Some(AnyRefIter::FieldRef(RefIter::new(
                    self.iter_field_id,
                    refs_iter,
                    self.field_mgr,
                    match_set_mgr,
                    field_id_offset,
                    field_pos_before,
                )));
            }
            return true;
        }
        if let FieldValueSlice::SlicedFieldReference(refs) = range.data {
            let refs_iter = FieldValueSliceIter::from_valid_range(range, refs);
            let field_id_offset = refs_iter.peek().unwrap().0.field_ref_offset;
            if let Some(AnyRefIter::SlicedFieldRef(ri)) = &mut self.ref_iter {
                ri.reset(
                    match_set_mgr,
                    refs_iter,
                    field_id_offset,
                    field_pos_before,
                );
            } else {
                self.ref_iter =
                    Some(AnyRefIter::SlicedFieldRef(RefIter::new(
                        self.iter_field_id,
                        refs_iter,
                        self.field_mgr,
                        match_set_mgr,
                        field_id_offset,
                        field_pos_before,
                    )));
            }
            return true;
        }
        false
    }
    pub fn typed_range_fwd(
        &mut self,
        match_set_mgr: &MatchSetManager,
        limit: usize,
        flags: FieldValueFlags,
    ) -> Option<RefAwareTypedRange<'a>> {
        loop {
            if let Some(ri) = &mut self.ref_iter {
                match ri {
                    AnyRefIter::FieldRef(iter) => {
                        if let Some((range, refs)) =
                            iter.typed_range_fwd(match_set_mgr, limit, flags)
                        {
                            let (fr, _) = refs.peek().unwrap();
                            return Some(RefAwareTypedRange {
                                base: range,
                                refs: Some(AnyRefSliceIter::FieldRef(refs)),
                                field_ref_offset: Some(fr.field_ref_offset),
                            });
                        }
                    }
                    AnyRefIter::SlicedFieldRef(iter) => {
                        if let Some((range, refs)) =
                            iter.typed_range_fwd(match_set_mgr, limit, flags)
                        {
                            let (fr, _) = refs.peek().unwrap();
                            return Some(RefAwareTypedRange {
                                base: range,
                                refs: Some(AnyRefSliceIter::SlicedFieldRef(
                                    refs,
                                )),
                                field_ref_offset: Some(fr.field_ref_offset),
                            });
                        }
                    }
                }
                self.ref_iter = None;
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(range) = self.iter.typed_range_fwd(limit, flags) {
                if self.setup_for_field_refs_range(
                    match_set_mgr,
                    field_pos,
                    &range,
                ) {
                    continue;
                }
                return Some(RefAwareTypedRange {
                    base: range,
                    refs: None,
                    field_ref_offset: None,
                });
            }
            return None;
        }
    }
    pub fn typed_field_fwd(
        &mut self,
        match_set_mgr: &mut MatchSetManager,
        limit: usize,
    ) -> Option<(FieldValueRef<'a>, RunLength, Option<FieldRefOffset>)> {
        loop {
            if let Some(ri) = &mut self.ref_iter {
                match ri {
                    AnyRefIter::FieldRef(iter) => {
                        if let Some(fru) =
                            iter.typed_field_fwd(match_set_mgr, limit)
                        {
                            let (v, rl) = fru.apply_ref();
                            return Some((
                                v,
                                rl,
                                Some(fru.reference.field_ref_offset),
                            ));
                        }
                    }
                    AnyRefIter::SlicedFieldRef(iter) => {
                        if let Some(fru) =
                            iter.typed_field_fwd(match_set_mgr, limit)
                        {
                            let (v, rl) = fru.apply_ref();
                            return Some((
                                v,
                                rl,
                                Some(fru.reference.field_ref_offset),
                            ));
                        }
                    }
                };
                self.ref_iter = None;
            }
            let field_pos = self.iter.get_next_field_pos();
            if let Some(field) = self.iter.typed_field_fwd(limit) {
                if matches!(
                    field.value,
                    FieldValueRef::FieldReference(_)
                        | FieldValueRef::SlicedFieldReference(_)
                ) {
                    self.iter.typed_field_bwd(limit);
                    let range = self
                        .iter
                        .typed_range_fwd(limit, field_value_flags::DEFAULT)
                        .unwrap();
                    self.setup_for_field_refs_range(
                        match_set_mgr,
                        field_pos,
                        &range,
                    )
                    .then_some(())
                    .unwrap();
                    continue;
                }
                return Some((field.value, field.header.run_length, None));
            }
            return None;
        }
    }
    pub fn next_range(
        &mut self,
        msm: &'_ MatchSetManager,
    ) -> Option<RefAwareTypedRange<'a>> {
        self.typed_range_fwd(msm, usize::MAX, field_value_flags::DEFAULT)
    }
    // using `next_range` and nesting RefAwareTypedSliceIters is significantly
    // faster. for example, `scr seqn=1G sum p` gets a 5x speedup
    pub fn next_value(
        &mut self,
        msm: &mut MatchSetManager,
    ) -> Option<(FieldValueRef<'a>, RunLength, Option<FieldRefOffset>)> {
        self.typed_field_fwd(msm, usize::MAX)
    }
    pub fn next_n_fields(&mut self, mut limit: usize) -> usize {
        let mut ri_count = 0;
        if let Some(ri) = &mut self.ref_iter {
            ri_count = match ri {
                AnyRefIter::FieldRef(iter) => iter.next_n_fields(limit, true),
                AnyRefIter::SlicedFieldRef(iter) => {
                    iter.next_n_fields(limit, true)
                }
            };
            if ri_count == limit {
                return limit;
            }
            limit -= ri_count;
        }
        let base_count = self.iter.next_n_fields(limit, true);
        if base_count > 0 {
            self.ref_iter = None;
        }
        ri_count + base_count
    }
    pub fn into_base_iter(self) -> I {
        self.iter
    }
    pub fn clone_base(self) -> I {
        self.iter
    }
    pub fn is_next_valid(&self) -> bool {
        self.iter.is_next_valid()
    }
}

impl<'a, R: FieldDataRef<'a>, I: FieldIterator<'a, FieldDataRefType = R>>
    From<AutoDerefIter<'a, I>> for Iter<'a, R>
{
    fn from(value: AutoDerefIter<'a, I>) -> Self {
        value.iter.into_base_iter()
    }
}

pub struct RefAwareInlineBytesIter<'a> {
    iter: InlineBytesIter<'a>,
    refs: Option<AnyRefSliceIter<'a>>,
}

impl<'a> RefAwareInlineBytesIter<'a> {
    pub fn new(
        data: &'a [u8],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<AnyRefSliceIter<'a>>,
    ) -> Self {
        Self {
            iter: InlineBytesIter::new(
                data,
                headers,
                first_oversize,
                last_oversize,
            ),
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

    // returns a triple of (data, run length, offset)
    // the offset is the position of data in the original data slice
    // this is needed if we want to create a field reference into the
    // original data, because it has to include that offset
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.refs {
            Some(AnyRefSliceIter::FieldRef(refs_iter)) => {
                let (_fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((data, run_len, 0))
            }
            Some(AnyRefSliceIter::SlicedFieldRef(refs_iter)) => {
                let (fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((&data[fr.begin..fr.end], run_len, fr.begin))
            }
            None => {
                let (data, rl) = self.iter.next()?;
                Some((data, rl, 0))
            }
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
        refs: Option<AnyRefSliceIter<'a>>,
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
            iter: RefAwareInlineBytesIter::from_range(range, data.as_bytes()),
        }
    }
}

impl<'a> Iterator for RefAwareInlineTextIter<'a> {
    type Item = (&'a str, RunLength, usize);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let (data, rl, offset) = self.iter.next()?;
        return Some((
            unsafe { std::str::from_utf8_unchecked(data) },
            rl,
            offset,
        ));
    }
}

pub struct RefAwareBytesBufferIter<'a> {
    iter: FieldValueSliceIter<'a, Vec<u8>>,
    refs: Option<AnyRefSliceIter<'a>>,
}

impl<'a> RefAwareBytesBufferIter<'a> {
    pub unsafe fn new(
        values: &'a [Vec<u8>],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<AnyRefSliceIter<'a>>,
    ) -> Self {
        Self {
            iter: unsafe {
                FieldValueSliceIter::new(
                    values,
                    headers,
                    first_oversize,
                    last_oversize,
                )
            },
            refs,
        }
    }
    pub fn from_range(
        range: &'a RefAwareTypedRange,
        values: &'a [Vec<u8>],
    ) -> Self {
        Self {
            iter: FieldValueSliceIter::from_valid_range(&range.base, values),
            refs: range.refs.clone(),
        }
    }
}

impl<'a> Iterator for RefAwareBytesBufferIter<'a> {
    type Item = (&'a [u8], RunLength, usize);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.refs {
            Some(AnyRefSliceIter::FieldRef(refs_iter)) => {
                let (_fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((data, run_len, 0))
            }
            Some(AnyRefSliceIter::SlicedFieldRef(refs_iter)) => {
                let (fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((&data[fr.begin..fr.end], run_len, fr.begin))
            }
            None => {
                let (data, rl) = self.iter.next()?;
                Some((data, rl, 0))
            }
        }
    }
}

pub struct RefAwareTextBufferIter<'a> {
    iter: FieldValueSliceIter<'a, String>,
    refs: Option<AnyRefSliceIter<'a>>,
}

impl<'a> RefAwareTextBufferIter<'a> {
    pub unsafe fn new(
        values: &'a [String],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<AnyRefSliceIter<'a>>,
    ) -> Self {
        Self {
            iter: unsafe {
                FieldValueSliceIter::new(
                    values,
                    headers,
                    first_oversize,
                    last_oversize,
                )
            },
            refs,
        }
    }
    pub fn from_range(
        range: &'a RefAwareTypedRange,
        values: &'a [String],
    ) -> Self {
        Self {
            iter: FieldValueSliceIter::from_valid_range(&range.base, values),
            refs: range.refs.clone(),
        }
    }
}

impl<'a> Iterator for RefAwareTextBufferIter<'a> {
    type Item = (&'a str, RunLength, usize);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.refs {
            Some(AnyRefSliceIter::FieldRef(refs_iter)) => {
                let (_fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((data, run_len, 0))
            }
            Some(AnyRefSliceIter::SlicedFieldRef(refs_iter)) => {
                let (fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((&data[fr.begin..fr.end], run_len, fr.begin))
            }
            None => {
                let (data, rl) = self.iter.next()?;
                Some((data, rl, 0))
            }
        }
    }
}

pub struct RefAwareStreamValueIter<'a> {
    iter: FieldValueSliceIter<'a, StreamValueId>,
    refs: Option<AnyRefSliceIter<'a>>,
}

impl<'a> RefAwareStreamValueIter<'a> {
    pub unsafe fn new(
        values: &'a [StreamValueId],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<AnyRefSliceIter<'a>>,
    ) -> Self {
        Self {
            iter: unsafe {
                FieldValueSliceIter::new(
                    values,
                    headers,
                    first_oversize,
                    last_oversize,
                )
            },
            refs,
        }
    }
    pub fn from_range(
        range: &'a RefAwareTypedRange,
        values: &'a [StreamValueId],
    ) -> Self {
        Self {
            iter: FieldValueSliceIter::from_valid_range(&range.base, values),
            refs: range.refs.clone(),
        }
    }
}

impl<'a> Iterator for RefAwareStreamValueIter<'a> {
    type Item = (StreamValueId, Option<core::ops::Range<usize>>, RunLength);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.refs {
            Some(AnyRefSliceIter::FieldRef(refs_iter)) => {
                let (_fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((*data, None, run_len))
            }
            Some(AnyRefSliceIter::SlicedFieldRef(refs_iter)) => {
                let (fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((*data, Some(fr.begin..fr.end), run_len))
            }
            None => {
                let (data, rl) = self.iter.next()?;
                Some((*data, None, rl))
            }
        }
    }
}

pub struct RefAwareFieldValueSliceIter<'a, T> {
    iter: FieldValueSliceIter<'a, T>,
    refs: Option<FieldValueSliceIter<'a, FieldReference>>,
}

impl<'a, T: FieldValueType + 'static> RefAwareFieldValueSliceIter<'a, T> {
    fn unpack_refs_iter(
        refs: Option<AnyRefSliceIter<'a>>,
    ) -> Option<FieldValueSliceIter<'a, FieldReference>> {
        match refs {
            Some(AnyRefSliceIter::FieldRef(iter)) => Some(iter),
            Some(AnyRefSliceIter::SlicedFieldRef(_)) => {
                panic!(
                    "sliced field references to `{}` are not allowed",
                    T::REPR.to_str()
                )
            }
            None => None,
        }
    }
    pub unsafe fn new(
        values: &'a [T],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
        refs: Option<AnyRefSliceIter<'a>>,
    ) -> Self {
        debug_assert!(T::SUPPORTS_REFS);
        Self {
            iter: unsafe {
                FieldValueSliceIter::new(
                    values,
                    headers,
                    first_oversize,
                    last_oversize,
                )
            },
            refs: Self::unpack_refs_iter(refs),
        }
    }
    pub fn from_range(range: &'a RefAwareTypedRange, values: &'a [T]) -> Self {
        debug_assert!(T::SUPPORTS_REFS);
        Self {
            iter: FieldValueSliceIter::from_valid_range(&range.base, values),
            refs: Self::unpack_refs_iter(range.refs.clone()),
        }
    }
}

impl<'a, T: FieldValueType + 'static> Iterator
    for RefAwareFieldValueSliceIter<'a, T>
{
    type Item = (&'a T, RunLength);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut refs_iter) = self.refs {
            let (_fr, rl_ref) = refs_iter.peek()?;
            let (data, rl_data) = self.iter.peek()?;
            let run_len = rl_ref.min(rl_data);
            self.iter.next_n_fields(run_len as usize);
            refs_iter.next_n_fields(run_len as usize);
            Some((data, run_len))
        } else {
            let (data, rl) = self.iter.next()?;
            Some((data, rl))
        }
    }
}

pub struct RefAwareUnfoldRunLength<I, T> {
    iter: I,
    last: Option<T>,
    remaining_run_len: RunLength,
}

impl<I: Iterator<Item = (T, RunLength, usize)>, T: Clone>
    RefAwareUnfoldRunLength<I, T>
{
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

impl<T: Clone, I: Iterator<Item = (T, RunLength, usize)>>
    RefAwareUnfoldIterRunLength<T> for I
{
    fn unfold_rl(self) -> RefAwareUnfoldRunLength<Self, T> {
        RefAwareUnfoldRunLength::new(self)
    }
}

impl<I: Iterator<Item = (T, RunLength, usize)>, T: Clone> Iterator
    for RefAwareUnfoldRunLength<I, T>
{
    type Item = T;
    #[inline(always)]
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
    use super::super::{
        field::FieldManager,
        match_set::MatchSetManager,
        ref_iter::{AutoDerefIter, RefAwareInlineTextIter},
    };
    use crate::record_data::{
        action_buffer::ActorRef,
        field::FieldRefOffset,
        field_data::{
            field_value_flags, FieldData, FieldValueFormat, FieldValueHeader,
            FieldValueRepr, RunLength,
        },
        field_value::SlicedFieldReference,
        field_value_ref::FieldValueSlice,
        iters::Iter,
        push_interface::PushInterface,
    };

    fn compare_iter_output(
        fd: FieldData,
        fd_refs: FieldData,
        expected: &[(&'static str, RunLength, usize)],
    ) {
        let mut match_set_mgr = MatchSetManager::default();
        let ms_id = match_set_mgr.match_sets.claim();
        let mut field_mgr = FieldManager::default();

        let field_id = field_mgr.add_field_with_data(
            &mut match_set_mgr,
            ms_id,
            None,
            ActorRef::default(),
            fd,
        );
        let refs_field_id = field_mgr.add_field_with_data(
            &mut match_set_mgr,
            ms_id,
            None,
            ActorRef::default(),
            fd_refs,
        );
        field_mgr.register_field_reference(refs_field_id, field_id);

        {
            let fr = field_mgr.get_cow_field_ref_raw(refs_field_id);
            let iter = Iter::from_start(fr.destructured_field_ref());
            let mut ref_iter =
                AutoDerefIter::new(&field_mgr, refs_field_id, iter);
            let range = ref_iter
                .typed_range_fwd(
                    &match_set_mgr,
                    usize::MAX,
                    field_value_flags::DEFAULT,
                )
                .unwrap();
            let iter = match range.base.data {
                FieldValueSlice::TextInline(v) => {
                    RefAwareInlineTextIter::from_range(&range, v)
                }
                _ => panic!("wrong data type"),
            };
            assert_eq!(iter.collect::<Vec<_>>(), expected);
        }

        field_mgr.drop_field_refcount(field_id, &mut match_set_mgr);
        field_mgr.drop_field_refcount(refs_field_id, &mut match_set_mgr);
    }
    fn compare_iter_output_parallel_ref(
        mut fd: FieldData,
        expected: &[(&'static str, RunLength)],
    ) {
        let mut fd_refs = FieldData::default();
        for h in &mut fd.headers {
            if h.same_value_as_previous() {
                fd_refs.headers.push_back(FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::SlicedFieldReference,
                        flags: h.flags
                            & (field_value_flags::DELETED
                                | field_value_flags::SHARED_VALUE
                                | field_value_flags::SAME_VALUE_AS_PREVIOUS),
                        size: std::mem::size_of::<SlicedFieldReference>()
                            as u16,
                    },
                    run_length: h.run_length,
                });
            } else {
                push_ref(
                    &mut fd_refs,
                    0,
                    h.size as usize,
                    h.run_length as usize,
                );
                let h_ref = fd_refs.headers.back_mut().unwrap();
                h_ref.flags |= h.flags
                    & (field_value_flags::DELETED
                        | field_value_flags::SHARED_VALUE);
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
        fd.push_sliced_field_reference(
            SlicedFieldReference {
                field_ref_offset: FieldRefOffset::from(0u8),
                begin,
                end,
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
        compare_iter_output_parallel_ref(
            fd,
            &[("a", 1), ("bb", 2), ("ccc", 3)],
        );
    }

    #[test]
    fn shared_ref() {
        let mut fd = FieldData::default();
        fd.push_str("aaa", 1, false, false);
        fd.push_str("bbbb", 2, false, false);
        fd.push_str("ccccc", 3, false, false);
        let mut fdr = FieldData::default();
        push_ref(&mut fdr, 1, 3, 6);
        compare_iter_output(
            fd,
            fdr,
            &[("aa", 1, 1), ("bb", 2, 1), ("cc", 3, 1)],
        );
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

        fd.headers[1].set_deleted(true);
        fd.field_count -= 2;
        fdr.headers[1].set_deleted(true);
        fdr.field_count -= 2;

        compare_iter_output(fd, fdr, &[("a", 1, 0), ("ccc", 3, 0)]);
    }

    #[test]
    fn with_same_as_previous() {
        let mut fd = FieldData::default();
        fd.push_str("aaa", 1, false, false);

        fd.headers.push_back(fd.headers[0]);
        fd.headers[1].set_same_value_as_previous(true);
        fd.headers[1].run_length = 5;
        fd.headers[1].set_shared_value(true);
        fd.field_count += 5;

        fd.push_str("c", 3, false, false);
        compare_iter_output_parallel_ref(
            fd,
            &[("aaa", 1), ("aaa", 5), ("c", 3)],
        );
    }

    #[test]
    fn with_same_as_previous_after_deleted() {
        let mut fd = FieldData::default();
        fd.push_str("00", 1, false, false);
        fd.push_str("1", 1, false, false);
        fd.headers.push_back(fd.headers[1]);
        fd.headers[2].set_same_value_as_previous(true);
        fd.headers[2].run_length = 5;
        fd.headers[2].set_shared_value(true);
        fd.field_count += 5;
        fd.headers[1].set_deleted(true);
        fd.field_count -= 1;
        fd.push_str("333", 3, false, false);
        compare_iter_output_parallel_ref(
            fd,
            &[("00", 1), ("1", 5), ("333", 3)],
        );
    }
}
