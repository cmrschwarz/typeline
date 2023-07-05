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
    pub fn from_typed_range(range: &TypedRange<'a>, values: &'a [T]) -> Self {
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
            self.header_rl_rem -= 1;
            if self.header_rl_rem == 0 {
                self.next_header();
                if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
            } else if !(*self.header).shared_value() {
                self.next_value();
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
    unsafe fn next_header(&mut self) {
        self.header = self.header.add(1);
        if self.header == self.header_end {
            return;
        }
        let h = *self.header;
        self.header_rl_rem = h.run_length;
        if self.header.add(1) == self.header_end {
            self.header_rl_rem -= self.last_oversize;
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
}

impl<'a, T: 'a> Iterator for TypedSliceIter<'a, T> {
    type Item = (&'a T, RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_end {
            return None;
        }
        unsafe {
            let value = self.values.as_ref();
            if (*self.header).shared_value() {
                let rl = self.header_rl_rem;
                self.next_header();
                if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
                return Some((value, rl));
            }
            self.header_rl_rem -= 1;
            if self.header_rl_rem == 0 {
                self.next_header();
                if !(*self.header).same_value_as_previous() {
                    self.next_value();
                }
            } else {
                self.next_value();
            }
            return Some((value, 1));
        }
    }
}

#[derive(Clone)]
pub struct InlineBytesIter<'a> {
    //TODO: rework this similarly to typed slice
    data: &'a [u8],
    headers: &'a [FieldValueHeader],
    headers_idx: usize,
    first_oversize: RunLength,
    last_oversize: RunLength,
    header_rl_offset: RunLength,
    header_rl_total: RunLength,
    header_value_size: u16,
    data_offset: usize,
}

impl<'a> InlineBytesIter<'a> {
    pub fn new(
        data: &'a [u8],
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        Self {
            data,
            headers: headers,
            headers_idx: 0,
            first_oversize,
            last_oversize,
            header_rl_offset: 0,
            header_rl_total: 0,
            header_value_size: 0,
            data_offset: 0,
        }
    }
    pub fn from_typed_range(range: &'a TypedRange<'a>, data: &'a [u8]) -> Self {
        Self::new(
            data,
            range.headers,
            range.first_header_run_length_oversize,
            range.last_header_run_length_oversize,
        )
    }
}

impl<'a> Iterator for InlineBytesIter<'a> {
    type Item = (&'a [u8], RunLength);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header_rl_offset != self.header_rl_total {
            self.header_rl_offset += 1;
            let data_offset_prev = self.data_offset;
            self.data_offset += self.header_value_size as usize;
            return Some((&self.data[data_offset_prev..self.data_offset], 1));
        }
        if self.headers_idx == self.headers.len() {
            return None;
        }
        let h = self.headers[self.headers_idx];
        self.headers_idx += 1;
        if h.shared_value() {
            let data_offset_prev = self.data_offset;
            if !h.same_value_as_previous() {
                self.data_offset += h.size as usize;
            }
            let mut rl = h.run_length;
            if self.headers_idx == 1 {
                rl -= self.first_oversize;
            }
            if self.headers_idx == self.headers.len() {
                rl -= self.last_oversize;
            }
            return Some((
                &self.data[data_offset_prev..data_offset_prev + h.size as usize],
                rl,
            ));
        }
        self.header_rl_offset = 0;
        self.header_rl_total = h.run_length;
        if self.headers_idx == 1 {
            self.header_rl_offset += self.first_oversize;
        }
        if self.headers_idx == self.headers.len() {
            self.data_offset += self.last_oversize as usize * h.size as usize;
            self.header_rl_total -= self.last_oversize;
        }
        self.header_value_size = h.size;
        self.header_rl_offset += 1;
        let data_offset_prev = self.data_offset;
        self.data_offset += h.size as usize;
        return Some((&self.data[data_offset_prev..self.data_offset], 1));
    }
}

pub struct InlineTextIter<'a> {
    iter: InlineBytesIter<'a>,
}

impl<'a> InlineTextIter<'a> {
    pub fn new(
        data: &'a str,
        headers: &'a [FieldValueHeader],
        first_oversize: RunLength,
        last_oversize: RunLength,
    ) -> Self {
        Self {
            iter: InlineBytesIter::new(data.as_bytes(), headers, first_oversize, last_oversize),
        }
    }
    pub fn from_typed_range(range: &'a TypedRange<'a>, data: &'a str) -> Self {
        Self {
            iter: InlineBytesIter::from_typed_range(range, data.as_bytes()),
        }
    }
}

impl<'a> Iterator for InlineTextIter<'a> {
    type Item = (&'a str, RunLength);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(v, rl)| (unsafe { std::str::from_utf8_unchecked(v) }, rl))
    }
}
