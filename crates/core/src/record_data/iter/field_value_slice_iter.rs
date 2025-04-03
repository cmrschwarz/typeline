use std::{marker::PhantomData, ptr::NonNull};

use super::{
    super::{
        field_data::{FieldValueHeader, FieldValueType, RunLength},
        field_value_ref::{TypedRange, ValidTypedRange},
    },
    ref_iter::RefAwareTypedRange,
};
use crate::record_data::field_value_ref::{
    FieldValueBlock, FieldValueRef, FieldValueSlice,
};
use metamatch::metamatch;

#[derive(Clone)]
pub struct FieldValueRangeIter<'a, T> {
    values: NonNull<T>,
    header: *const FieldValueHeader,
    header_end: *const FieldValueHeader,
    header_rl_rem: RunLength,
    last_oversize: RunLength,
    _phantom_data: PhantomData<&'a FieldValueHeader>,
}

#[derive(Clone)]
pub struct InlineBytesIter<'a> {
    values: NonNull<u8>,
    header: *const FieldValueHeader,
    header_end: *const FieldValueHeader,
    header_rl_rem: RunLength,
    last_oversize: RunLength,
    _phantom_data: PhantomData<&'a FieldValueHeader>,
}

#[derive(Default)]
pub struct InlineTextIter<'a> {
    iter: InlineBytesIter<'a>,
}

impl<'a, T> Default for FieldValueRangeIter<'a, T> {
    fn default() -> Self {
        Self {
            values: NonNull::dangling(),
            header: std::ptr::null(),
            header_end: std::ptr::null(),
            header_rl_rem: 0,
            last_oversize: 0,
            _phantom_data: PhantomData,
        }
    }
}

impl<'a, T: FieldValueType + 'static> FieldValueRangeIter<'a, T> {
    pub unsafe fn new(
        values: &'a [T],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        let mut header_rl_rem = 0;
        if !headers.is_empty() {
            header_rl_rem = headers[0].run_length - first_oversize
        };
        if headers.len() == 1 {
            header_rl_rem -= last_oversize;
        }
        let headers_range = headers.as_ptr_range();
        let values = if values.is_empty() {
            NonNull::dangling()
        } else {
            unsafe { NonNull::new_unchecked(values.as_ptr().cast_mut()) }
        };
        Self {
            values,
            header: headers_range.start,
            header_end: headers_range.end,
            header_rl_rem,
            last_oversize,
            _phantom_data: PhantomData,
        }
    }
    pub fn from_range(
        range: &RefAwareTypedRange<'a>,
        values: &'a [T],
    ) -> Self {
        Self::from_valid_range(&range.base, values)
    }
    pub fn from_valid_range(
        range: &ValidTypedRange<'a>,
        values: &'a [T],
    ) -> Self {
        // we explicitly *don't* check for SUPPORTS_REFS here
        // because this is used in the RefAware version of this iterator
        // as a base, where this check would then fail
        assert!(range.data.matches_values(values));
        unsafe {
            Self::new(
                values,
                range.headers,
                range.first_header_run_length_oversize,
                range.last_header_run_length_oversize,
            )
        }
    }
    pub fn peek(&self) -> Option<(&'a T, RunLength)> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let rl = if (*self.header).shared_value() {
                self.header_rl_rem
            } else {
                1
            };
            Some((self.values.as_ref(), rl))
        }
    }
    pub fn peek_header(&self) -> Option<&'a FieldValueHeader> {
        if self.header == self.header_end {
            return None;
        }
        Some(unsafe { &*self.header })
    }
    pub fn field_run_length_bwd(&self) -> RunLength {
        if self.header == self.header_end {
            return 0;
        }
        unsafe { *self.header }.run_length - self.header_rl_rem
    }
    pub fn field_run_length_fwd(&self) -> RunLength {
        self.header_rl_rem
    }
    pub fn data_ptr(&self) -> *const T {
        self.values.as_ptr()
    }
    pub fn header_ptr(&self) -> *const FieldValueHeader {
        self.header
    }
    pub fn next_no_sv(&mut self) -> Option<&'a T> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let value = self.values.as_ref();
            if self.header_rl_rem == 1 {
                self.next_header();
            } else {
                self.header_rl_rem -= 1;
                if !(*self.header).shared_value() {
                    self.next_value();
                }
            }
            Some(value)
        }
    }
    #[inline(always)]
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        let mut skip_rem = n;
        loop {
            if self.header == self.header_end {
                return n - skip_rem;
            }
            if self.header_rl_rem as usize > skip_rem {
                self.header_rl_rem -= skip_rem as RunLength;
                unsafe {
                    if !(*self.header).shared_value() {
                        self.advance_value(skip_rem);
                    }
                }
                return n;
            }
            skip_rem -= self.header_rl_rem as usize;
            self.next_header();
        }
    }
    pub fn next_header(&mut self) {
        if self.header == self.header_end {
            return;
        }
        if self.header_rl_rem > 1 {
            let h = unsafe { *self.header };
            if !h.shared_value() {
                unsafe {
                    self.advance_value((self.header_rl_rem - 1) as usize)
                };
            }
        }
        let mut h;
        loop {
            self.header = unsafe { self.header.add(1) };
            if self.header == self.header_end {
                unsafe { self.next_value() };
                return;
            }
            h = unsafe { *self.header };
            if !h.deleted() {
                break;
            }
            // we allow deleted ZSTs (like the GroupSeparator) to not
            // interrupt a range, so we have to check for that here
            if h.fmt.size != 0 {
                unsafe {
                    self.advance_value(h.unique_data_element_count() as usize)
                };
            }
        }
        self.header_rl_rem = h.run_length;
        unsafe {
            if !h.same_value_as_previous() {
                self.next_value();
            }
            if self.header.add(1) == self.header_end {
                self.header_rl_rem -= self.last_oversize;
            }
        }
    }
    unsafe fn advance_value(&mut self, n: usize) {
        self.values =
            unsafe { NonNull::new_unchecked(self.values.as_ptr().add(n)) };
    }
    unsafe fn next_value(&mut self) {
        unsafe { self.advance_value(1) };
    }
    pub fn next_block(&mut self) -> Option<FieldValueBlock<T>> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let value = self.values.as_ref();
            let h = *self.header;
            if h.shared_value() {
                let rl = self.header_rl_rem;
                self.next_header();
                return Some(FieldValueBlock::WithRunLength(value, rl));
            }
            let res = std::slice::from_raw_parts(
                self.data_ptr(),
                self.header_rl_rem as usize,
            );
            self.next_header();
            Some(FieldValueBlock::Plain(res))
        }
    }
    pub fn has_next(&mut self) -> bool {
        self.header_rl_rem > 0 || self.header != self.header_end
    }
    pub fn clear(&mut self) {
        self.header_rl_rem = 0;
        self.header = self.header_end;
    }
    pub fn headers_remaining(&self) -> usize {
        unsafe { self.header_end.offset_from(self.header) as usize }
    }
}

impl<'a, T: FieldValueType + 'static> Iterator for FieldValueRangeIter<'a, T> {
    type Item = (&'a T, RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let value = self.values.as_ref();
            let h = *self.header;
            if h.shared_value() {
                let rl = self.header_rl_rem;
                self.next_header();
                return Some((value, rl));
            }
            self.header_rl_rem -= 1;
            if self.header_rl_rem == 0 {
                self.next_header();
            } else {
                self.next_value(); // shared value was handled above
            }
            Some((value, 1))
        }
    }
}

impl<'a> Default for InlineBytesIter<'a> {
    fn default() -> Self {
        Self {
            values: NonNull::dangling(),
            header: std::ptr::null(),
            header_end: std::ptr::null(),
            header_rl_rem: 0,
            last_oversize: 0,
            _phantom_data: PhantomData,
        }
    }
}

impl<'a> InlineBytesIter<'a> {
    pub fn new(
        values: &'a [u8],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        let mut header_rl_rem = 0;
        if !headers.is_empty() {
            header_rl_rem = headers[0].run_length - first_oversize
        };
        if headers.len() == 1 {
            header_rl_rem -= last_oversize;
        }
        let headers_range = headers.as_ptr_range();
        let values = if values.is_empty() {
            NonNull::dangling()
        } else {
            unsafe { NonNull::new_unchecked(values.as_ptr().cast_mut()) }
        };
        Self {
            values,
            header: headers_range.start,
            header_end: headers_range.end,
            header_rl_rem,
            last_oversize,
            _phantom_data: PhantomData,
        }
    }
    pub fn from_range(range: &TypedRange<'a>, values: &'a [u8]) -> Self {
        Self::new(
            values,
            range.headers,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        )
    }
    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        let h = self.peek_header()?;
        unsafe {
            Some((
                std::slice::from_raw_parts(
                    self.values.as_ptr(),
                    h.size as usize,
                ),
                if h.shared_value() { h.run_length } else { 1 },
            ))
        }
    }
    pub fn peek_header(&self) -> Option<&'a FieldValueHeader> {
        if self.header == self.header_end {
            return None;
        }
        Some(unsafe { &*self.header })
    }
    pub fn field_run_length_bwd(&self) -> RunLength {
        if self.header == self.header_end {
            return 0;
        }
        unsafe { *self.header }.run_length - self.header_rl_rem
    }
    pub fn field_run_length_fwd(&self) -> RunLength {
        self.header_rl_rem
    }
    pub fn data_ptr(&self) -> *const u8 {
        self.values.as_ptr()
    }
    pub fn next_no_sv(&mut self) -> Option<&'a [u8]> {
        if self.header == self.header_end {
            return None;
        }
        let value;
        unsafe {
            let h = *self.header;
            let value_size = h.size as usize;
            value =
                std::slice::from_raw_parts(self.values.as_ptr(), value_size);
            self.header_rl_rem -= 1;
            if !h.shared_value() {
                self.advance_value(value_size);
            }
        }
        if self.header_rl_rem == 0 {
            self.next_header();
        }
        Some(value)
    }
    #[inline]
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        let mut skip_rem = n;
        loop {
            if self.header == self.header_end {
                return n - skip_rem;
            }
            if self.header_rl_rem as usize > skip_rem {
                self.header_rl_rem -= skip_rem as RunLength;
                unsafe {
                    let h = *self.header;
                    if !h.shared_value() {
                        self.advance_value(skip_rem * h.size as usize);
                    }
                }
                return n;
            }
            skip_rem -= self.header_rl_rem as usize;
            self.next_header();
        }
    }
    pub fn next_header(&mut self) {
        if self.header == self.header_end {
            return;
        }
        let h = unsafe { *self.header };
        let mut prev_size = h.size as usize;
        if self.header_rl_rem > 1 && !h.shared_value() {
            unsafe {
                self.advance_value(
                    (self.header_rl_rem - 1) as usize * prev_size,
                )
            };
        }
        let mut h;
        loop {
            self.header = unsafe { self.header.add(1) };
            if self.header == self.header_end {
                unsafe { self.advance_value(prev_size) };
                return;
            }
            h = unsafe { *self.header };
            if !h.deleted() {
                break;
            }
            if !h.same_value_as_previous() {
                unsafe {
                    self.advance_value(h.fmt.leading_padding() + prev_size)
                };
                if !h.shared_value() {
                    unsafe {
                        self.advance_value(
                            (h.run_length - 1) as usize * h.size as usize,
                        )
                    };
                }
            }
            prev_size = h.size as usize;
        }
        self.header_rl_rem = h.run_length;
        unsafe {
            if !h.same_value_as_previous() {
                self.advance_value(prev_size + h.fmt.leading_padding());
            }
            if self.header.add(1) == self.header_end {
                self.header_rl_rem -= self.last_oversize;
            }
        }
    }
    #[inline(always)]
    unsafe fn advance_value(&mut self, n: usize) {
        self.values =
            unsafe { NonNull::new_unchecked(self.values.as_ptr().add(n)) };
    }
    pub fn has_next(&mut self) -> bool {
        self.header_rl_rem > 0 || self.header != self.header_end
    }
    pub fn clear(&mut self) {
        self.header_rl_rem = 0;
        self.header = self.header_end;
    }
    pub fn headers_remaining(&self) -> usize {
        unsafe { self.header_end.offset_from(self.header) as usize }
    }
}

impl<'a> Iterator for InlineBytesIter<'a> {
    type Item = (&'a [u8], RunLength);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let h = *self.header;
            let value_size = h.size as usize;
            let value =
                std::slice::from_raw_parts(self.values.as_ptr(), value_size);
            if h.shared_value() {
                let rl = self.header_rl_rem;
                self.next_header();
                return Some((value, rl));
            }
            self.header_rl_rem -= 1;
            if self.header_rl_rem == 0 {
                self.next_header();
            } else {
                self.advance_value(value_size);
            }
            Some((value, 1))
        }
    }
}

impl<'a> InlineTextIter<'a> {
    pub fn new(
        values: &'a str,
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        Self {
            iter: InlineBytesIter::new(
                values.as_bytes(),
                headers,
                first_oversize,
                last_oversize,
            ),
        }
    }
    pub fn from_range(range: &TypedRange<'a>, values: &'a str) -> Self {
        Self {
            iter: InlineBytesIter::from_range(range, values.as_bytes()),
        }
    }
    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        let (v, rl) = self.iter.peek()?;
        Some((unsafe { std::str::from_utf8_unchecked(v) }, rl))
    }
    pub fn peek_header(&self) -> Option<&'a FieldValueHeader> {
        self.iter.peek_header()
    }
    pub fn field_run_length_bwd(&self) -> RunLength {
        self.iter.field_run_length_bwd()
    }
    pub fn field_run_length_fwd(&self) -> RunLength {
        self.iter.field_run_length_fwd()
    }
    pub fn data_ptr(&self) -> *const u8 {
        self.iter.data_ptr()
    }
    pub fn next_no_sv(&mut self) -> Option<&'a str> {
        let v = self.iter.next_no_sv()?;
        Some(unsafe { std::str::from_utf8_unchecked(v) })
    }
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        self.iter.next_n_fields(n)
    }
    pub fn next_header(&mut self) {
        self.iter.next_header()
    }
    pub fn has_next(&mut self) -> bool {
        self.iter.has_next()
    }
    pub fn clear(&mut self) {
        self.iter.clear()
    }
    pub fn headers_remaining(&self) -> usize {
        self.iter.headers_remaining()
    }
}

impl<'a> Iterator for InlineTextIter<'a> {
    type Item = (&'a str, RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        let (v, rl) = self.iter.next()?;
        let v = unsafe { std::str::from_utf8_unchecked(v) };
        Some((v, rl))
    }
}

pub struct FieldValueSliceIter<'a> {
    slice: FieldValueSlice<'a>,
}

impl<'a> FieldValueSliceIter<'a> {
    pub fn new(slice: FieldValueSlice<'a>) -> Self {
        Self { slice }
    }
}

impl<'a> Iterator for FieldValueSliceIter<'a> {
    type Item = FieldValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        metamatch!(match &mut self.slice {
            FieldValueSlice::Undefined(n) | FieldValueSlice::Null(n) => {
                if *n == 0 {
                    None
                } else {
                    *n -= 1;
                    Some(FieldValueRef::Null)
                }
            }
            #[expand(for (REP, KIND) in [
                (TextInline, Text), (BytesInline, Bytes)]
            )]
            FieldValueSlice::REP(val) => {
                let res = FieldValueRef::KIND(val);
                self.slice = FieldValueSlice::Null(0);
                Some(res)
            }
            #[expand(for (REP, KIND) in [
                (TextBuffer, Text),
                (BytesBuffer, Bytes),
            ])]
            FieldValueSlice::REP(v) => {
                if v.is_empty() {
                    None
                } else {
                    let res = &v[0];
                    *v = &v[1..];
                    Some(FieldValueRef::KIND(res))
                }
            }
            #[expand(for REP in [
                Bool, Int, BigInt, Float, BigRational,
                Object, Array, Argument, OpDecl, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference
            ])]
            FieldValueSlice::REP(v) => {
                if v.is_empty() {
                    None
                } else {
                    let res = &v[0];
                    *v = &v[1..];
                    Some(FieldValueRef::REP(res))
                }
            }
        })
    }
}

#[cfg(test)]
mod test_slice_iter {
    use crate::record_data::{
        field_data::{FieldData, FieldValueType, RunLength},
        push_interface::PushInterface,
    };

    use super::FieldValueRangeIter;

    fn compare_iter_output<
        T: Eq + std::fmt::Debug + Clone + FieldValueType + 'static,
    >(
        fd: &mut FieldData,
        expected: &[(T, RunLength)],
    ) {
        fd.headers.make_contiguous();
        fd.data.make_contiguous();
        let iter = unsafe {
            FieldValueRangeIter::new(
                std::slice::from_raw_parts(
                    fd.data.head_ptr().cast::<T>(),
                    fd.data.len() / std::mem::size_of::<T>(),
                ),
                fd.headers.as_slices().0,
                0,
                0,
            )
        };
        assert_eq!(
            iter.map(|(v, rl)| (v.clone(), rl)).collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn simple() {
        let mut fd = FieldData::default();
        fd.push_int(1, 1, false, false);
        fd.push_int(2, 2, false, false);
        fd.push_int(3, 3, false, false);
        compare_iter_output::<i64>(&mut fd, &[(1, 1), (2, 2), (3, 3)]);
    }

    #[test]
    fn with_deletion() {
        let mut fd = FieldData::default();
        fd.push_int(1, 1, false, false);
        fd.push_int(2, 2, false, false);
        fd.push_int(3, 3, false, false);
        fd.headers[1].set_deleted(true);
        fd.field_count -= 2;
        compare_iter_output::<i64>(&mut fd, &[(1, 1), (3, 3)]);
    }

    #[test]
    fn with_same_as_previous() {
        let mut fd = FieldData::default();
        fd.push_int(1, 1, false, false);
        fd.headers.push_back(fd.headers[0]);
        fd.headers[1].set_same_value_as_previous(true);
        fd.headers[1].set_shared_value(true);
        fd.headers[1].run_length = 5;
        fd.field_count += 5;
        fd.push_int(3, 3, false, false);
        compare_iter_output::<i64>(&mut fd, &[(1, 1), (1, 5), (3, 3)]);
    }

    #[test]
    fn with_same_as_previous_after_deleted() {
        let mut fd = FieldData::default();
        fd.push_int(0, 1, false, false);
        fd.push_int(1, 1, false, false);
        fd.headers.push_back(fd.headers[1]);
        fd.headers[2].set_same_value_as_previous(true);
        fd.headers[2].run_length = 5;
        fd.headers[2].set_shared_value(true);
        fd.field_count += 5;
        fd.headers[1].set_deleted(true);
        fd.field_count -= 1;
        fd.push_int(3, 3, false, false);
        compare_iter_output::<i64>(&mut fd, &[(0, 1), (1, 5), (3, 3)]);
    }
}

#[cfg(test)]
mod test_text_iter {
    use bstr::ByteSlice;

    use crate::record_data::{
        field_data::{FieldData, RunLength},
        iter::field_value_slice_iter::InlineTextIter,
        push_interface::PushInterface,
    };

    fn compare_iter_output(
        fd: &mut FieldData,
        expected: &[(&'static str, RunLength)],
    ) {
        fd.headers.make_contiguous();
        fd.data.make_contiguous();
        let data = unsafe {
            std::slice::from_raw_parts(fd.data.head_ptr(), fd.data.len())
                .to_str()
                .unwrap()
        };

        let iter = InlineTextIter::new(data, fd.headers.as_slices().0, 0, 0);
        assert_eq!(iter.collect::<Vec<_>>(), expected);
    }

    #[test]
    fn simple() {
        let mut fd = FieldData::default();
        fd.push_str("a", 1, false, false);
        fd.push_str("bb", 2, false, false);
        fd.push_str("ccc", 3, false, false);
        compare_iter_output(&mut fd, &[("a", 1), ("bb", 2), ("ccc", 3)]);
    }

    #[test]
    fn with_deletion() {
        let mut fd = FieldData::default();
        fd.push_str("a", 1, false, false);
        fd.push_str("bb", 2, false, false);
        fd.push_str("ccc", 3, false, false);
        fd.headers[1].set_deleted(true);
        fd.field_count -= 2;
        compare_iter_output(&mut fd, &[("a", 1), ("ccc", 3)]);
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
        compare_iter_output(&mut fd, &[("aaa", 1), ("aaa", 5), ("c", 3)]);
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
        compare_iter_output(&mut fd, &[("00", 1), ("1", 5), ("333", 3)]);
    }
}
