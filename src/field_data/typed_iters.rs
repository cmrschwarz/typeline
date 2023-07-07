use std::{marker::PhantomData, ptr::NonNull};

use super::{typed::TypedRange, FieldValueHeader, RunLength};

#[derive(Clone)]
pub struct TypedSliceIter<'a, T> {
    values: NonNull<T>,
    header: *const FieldValueHeader,
    header_end: *const FieldValueHeader,
    header_rl_rem: RunLength,
    last_oversize: RunLength,
    _phantom_data: PhantomData<&'a FieldValueHeader>,
}

impl<'a, T> Default for TypedSliceIter<'a, T> {
    fn default() -> Self {
        Self {
            values: NonNull::dangling(),
            header: std::ptr::null(),
            header_end: std::ptr::null(),
            header_rl_rem: 0,
            last_oversize: 0,
            _phantom_data: Default::default(),
        }
    }
}

impl<'a, T> TypedSliceIter<'a, T> {
    pub fn new(
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
            NonNull::from(&values[0])
        };
        Self {
            values,
            header: headers_range.start,
            header_end: headers_range.end,
            header_rl_rem,
            last_oversize,
            _phantom_data: PhantomData::default(),
        }
    }
    pub fn from_range(range: &TypedRange<'a>, values: &'a [T]) -> Self {
        Self::new(
            values,
            range.headers,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        )
    }
    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let rl = if (*self.header).shared_value() {
                self.header_rl_rem
            } else {
                1
            };
            return Some((self.values.as_ref(), rl));
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
            return Some(value);
        }
    }
    pub fn next_n_fields(&mut self, mut n: usize) {
        if self.header == self.header_end {
            return;
        }
        loop {
            if self.header_rl_rem as usize > n {
                self.header_rl_rem -= n as RunLength;
                unsafe {
                    if !(*self.header).shared_value() {
                        self.advance_value(n);
                    }
                }
                return;
            }
            n -= self.header_rl_rem as usize;
            unsafe {
                if !(*self.header).shared_value() {
                    self.advance_value(self.header_rl_rem as usize);
                } else if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
                self.next_header();
            }
        }
    }
    pub fn next_header(&mut self) {
        if self.header_rl_rem > 1 {
            let h = unsafe { *self.header };
            if !h.shared_value() {
                unsafe { self.advance_value((self.header_rl_rem - 1) as usize) };
            }
        }
        let mut h;
        loop {
            let next = unsafe { self.header.add(1) };
            if next == self.header_end {
                return;
            }
            self.header = next;
            h = unsafe { *self.header };
            if !h.deleted() {
                break;
            }
            unsafe { self.advance_value(h.unique_data_element_count() as usize) };
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
        self.values = NonNull::new_unchecked(self.values.as_ptr().add(n));
    }
    unsafe fn next_value(&mut self) {
        self.advance_value(1);
    }
    pub fn has_next(&mut self) -> bool {
        return self.header_rl_rem > 0 || self.header != self.header_end;
    }
    pub fn clear(&mut self) {
        self.header_rl_rem = 0;
        self.header = self.header_end;
    }
    pub fn headers_remaining(&self) -> usize {
        unsafe { self.header_end.offset_from(self.header) as usize }
    }
}

impl<'a, T: 'a> Iterator for TypedSliceIter<'a, T> {
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
                self.next_value(); //shared value was handled above
            }
            return Some((value, 1));
        }
    }
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

impl<'a> Default for InlineBytesIter<'a> {
    fn default() -> Self {
        Self {
            values: NonNull::dangling(),
            header: std::ptr::null(),
            header_end: std::ptr::null(),
            header_rl_rem: 0,
            last_oversize: 0,
            _phantom_data: Default::default(),
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
            NonNull::from(&values[0])
        };
        Self {
            values,
            header: headers_range.start,
            header_end: headers_range.end,
            header_rl_rem,
            last_oversize,
            _phantom_data: PhantomData::default(),
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
            return Some((
                std::slice::from_raw_parts(self.values.as_ptr(), h.size as usize),
                if h.shared_value() { h.run_length } else { 1 },
            ));
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
            value = std::slice::from_raw_parts(self.values.as_ptr(), value_size);
            self.header_rl_rem -= 1;
            if !h.shared_value() {
                self.advance_value(value_size);
            }
        }
        if self.header_rl_rem == 0 {
            self.next_header();
        }
        return Some(value);
    }
    pub fn next_n_fields(&mut self, mut n: usize) {
        if self.header == self.header_end {
            return;
        }
        loop {
            if self.header_rl_rem as usize > n {
                self.header_rl_rem -= n as RunLength;
                unsafe {
                    let h = *self.header;
                    if !h.shared_value() {
                        self.advance_value(n * h.size as usize);
                    }
                }
                return;
            }
            n -= self.header_rl_rem as usize;
            self.next_header();
        }
    }
    pub fn next_header(&mut self) {
        if self.header_rl_rem > 0 {
            let h = unsafe { *self.header };
            if !h.same_value_as_previous() {
                let elem_count = if h.shared_value() {
                    1
                } else {
                    self.header_rl_rem
                };
                unsafe { self.advance_value(elem_count as usize * h.size as usize) };
            }
        }
        let mut h;
        loop {
            let next = unsafe { self.header.add(1) };
            if next == self.header_end {
                return;
            }
            self.header = next;
            h = unsafe { *self.header };
            if !h.deleted() {
                break;
            }
            unsafe { self.advance_value(h.unique_data_element_count() as usize * h.size as usize) };
        }
        if !h.same_value_as_previous() {
            unsafe { self.advance_value(h.size as usize) };
        }
        self.header_rl_rem = h.run_length;
        if unsafe { self.header.add(1) } == self.header_end {
            self.header_rl_rem -= self.last_oversize;
        }
    }
    unsafe fn advance_value(&mut self, n: usize) {
        self.values = NonNull::new_unchecked(self.values.as_ptr().add(n));
    }
    pub fn has_next(&mut self) -> bool {
        return self.header_rl_rem > 0 || self.header != self.header_end;
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

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let h = *self.header;
            let value_size = h.size as usize;
            let value = std::slice::from_raw_parts(self.values.as_ptr(), value_size);
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
            return Some((value, 1));
        }
    }
}

#[derive(Default)]
pub struct InlineTextIter<'a> {
    iter: InlineBytesIter<'a>,
}

impl<'a> InlineTextIter<'a> {
    pub fn new(
        values: &'a str,
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        Self {
            iter: InlineBytesIter::new(values.as_bytes(), headers, first_oversize, last_oversize),
        }
    }
    pub fn from_range(range: &TypedRange<'a>, values: &'a str) -> Self {
        Self {
            iter: InlineBytesIter::from_range(range, values.as_bytes()),
        }
    }
    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        let (v, rl) = self.iter.peek()?;
        return Some((unsafe { std::str::from_utf8_unchecked(v) }, rl));
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
        return Some(unsafe { std::str::from_utf8_unchecked(v) });
    }
    pub fn next_n_fields(&mut self, n: usize) {
        self.iter.next_n_fields(n)
    }
    pub fn next_header(&mut self) {
        self.iter.next_header()
    }
    pub fn has_next(&mut self) -> bool {
        return self.iter.has_next();
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
        return Some((unsafe { std::str::from_utf8_unchecked(v) }, rl));
    }
}
