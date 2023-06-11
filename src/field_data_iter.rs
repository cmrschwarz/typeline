use std::{marker::PhantomData, ptr::NonNull};

use crate::field_data::{
    get_field_value_ref, FieldData, FieldValueHeader, FieldValueKind, FieldValueRef, RunLength,
};

#[derive(Clone)]
pub struct RawFieldDataIter<'a> {
    header: *const FieldValueHeader,
    header_r_end: *const FieldValueHeader,
    data: NonNull<u8>,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a> Iterator for RawFieldDataIter<'a> {
    type Item = (FieldValueHeader, *mut u8);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe {
            self.header = self.header.sub(1);
            let h = *self.header;
            let ptr = h
                .kind
                .align_ptr(self.data.as_ptr().sub(h.data_size() as usize));
            self.data = NonNull::new_unchecked(ptr);
            Some((h, ptr))
        }
    }
}

impl<'a> RawFieldDataIter<'a> {
    pub fn new(fd: &'a FieldData) -> RawFieldDataIter<'a> {
        let h_range = fd.header.as_ptr_range();
        RawFieldDataIter {
            header: h_range.end,
            header_r_end: h_range.start,
            data: unsafe { NonNull::new_unchecked(fd.data.as_ptr_range().end as *mut u8) },
            _phantom_data: PhantomData,
        }
    }
    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe {
            let h = *self.header.sub(1);
            let ptr = h
                .kind
                .align_ptr(self.data.as_ptr().sub(h.data_size() as usize));
            Some((h, ptr))
        }
    }
    pub fn peek_header(&self) -> Option<FieldValueHeader> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe { Some(*self.header.sub(1)) }
    }
    pub fn previous_data_begin(&self) -> *mut u8 {
        self.data.as_ptr()
    }
}

// SAFETY:
// this trait is unsafe because other (especially transformative) iterators
// of this trait assume that everyone upholds the FieldData contracts
pub unsafe trait FieldDataIterator<'a>:
    Iterator<Item = (FieldValueHeader, Self::ValueType)> + Sized
{
    type ValueType: Copy;
    fn bounded(self, remaining_len: usize) -> BoundedFieldDataIterator<'a, Self> {
        BoundedFieldDataIterator {
            iter: self,
            remaining_len,
            _phantom_data: PhantomData::default(),
        }
    }
    fn unfold_values(self) -> UnfoldedValuesFieldDataIterator<'a, Self> {
        UnfoldedValuesFieldDataIterator {
            iter: self,
            cached_value: None,
            remaining_run_len: 0,
        }
    }
    fn len_only(self) -> LenOnlyFieldDataIterator<'a, Self> {
        LenOnlyFieldDataIterator {
            iter: self,
            _phantom_data: PhantomData::default(),
        }
    }
    fn peek(&self) -> Option<Self::Item>;
    fn drop_n_fields(&mut self, n: usize) -> usize;
}

#[derive(Clone)]
pub struct RleTypesFieldDataIter<'a> {
    iter: RawFieldDataIter<'a>,
    handled_run_len: RunLength,
}

impl<'a> RleTypesFieldDataIter<'a> {
    pub fn new(iter: RawFieldDataIter<'a>) -> Self {
        Self {
            iter,
            handled_run_len: 0,
        }
    }
}

impl<'a> Iterator for RleTypesFieldDataIter<'a> {
    type Item = (FieldValueHeader, *mut u8);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some((mut h, v)) => {
                h.run_length -= self.handled_run_len;
                Some((h, v))
            }
        }
    }
}
unsafe impl<'a> FieldDataIterator<'a> for RleTypesFieldDataIter<'a> {
    type ValueType = *mut u8;

    fn peek(&self) -> Option<Self::Item> {
        match self.iter.peek() {
            None => None,
            Some((mut h, v)) => {
                h.run_length -= self.handled_run_len;
                Some((h, v))
            }
        }
    }

    fn drop_n_fields(&mut self, n: usize) -> usize {
        let mut drops_remaining = n;
        while let Some(h) = self.iter.peek_header() {
            if h.run_length as usize > drops_remaining {
                self.handled_run_len = drops_remaining as RunLength;
                return n;
            }
            drops_remaining -= h.run_length as usize;
            self.iter.next();
        }
        self.handled_run_len = 0;
        n - drops_remaining
    }
}

#[derive(Clone)]
pub struct FieldDataIter<'a> {
    iter: RawFieldDataIter<'a>,
    last_header: FieldValueHeader,
}

impl<'a> FieldDataIter<'a> {
    pub fn new(iter: RawFieldDataIter<'a>) -> Self {
        Self {
            iter,
            last_header: FieldValueHeader {
                kind: FieldValueKind::Unset,
                shared_value: false,
                size: 0,
                run_length: 0,
            },
        }
    }
}

impl<'a> Iterator for FieldDataIter<'a> {
    type Item = (FieldValueHeader, *mut u8);

    fn next(&mut self) -> Option<Self::Item> {
        let rl_rem = self.last_header.run_length;
        if rl_rem > 0 {
            let ptr = unsafe { self.iter.previous_data_begin().add(rl_rem as usize) };
            self.last_header.run_length -= 1;
            let mut h = self.last_header;
            h.run_length = 1;
            return Some((h, ptr));
        }
        if let Some((mut h, v)) = self.iter.next() {
            self.last_header = h;
            if h.shared_value {
                self.last_header.run_length = 0;
                return Some((h, v));
            }
            self.last_header.run_length -= 1;
            h.run_length = 1;
            return Some((h, v));
        }
        None
    }
}

unsafe impl<'a> FieldDataIterator<'a> for FieldDataIter<'a> {
    type ValueType = *mut u8;
    fn drop_n_fields(&mut self, n: usize) -> usize {
        let mut drops_remaining = n;
        if self.last_header.run_length as usize > drops_remaining {
            self.last_header.run_length -= drops_remaining as RunLength;
            return n;
        }
        drops_remaining -= self.last_header.run_length as usize;
        self.last_header.run_length = 0;
        while let Some(h) = self.iter.peek_header() {
            if h.run_length as usize > drops_remaining {
                self.last_header = h;
                self.last_header.run_length -= drops_remaining as RunLength;
                return n;
            }
            drops_remaining -= h.run_length as usize;
            self.iter.next();
        }
        self.last_header.run_length = 0;
        n - drops_remaining as usize
    }
    fn peek(&self) -> Option<<Self as Iterator>::Item> {
        let rl_rem = self.last_header.run_length;
        if rl_rem > 0 {
            let ptr = unsafe { self.iter.previous_data_begin().add(rl_rem as usize) };
            let mut h = self.last_header;
            h.run_length = 1;
            return Some((h, ptr));
        }
        if let Some((mut h, v)) = self.iter.peek() {
            if h.shared_value {
                return Some((h, v));
            }
            h.run_length = 1;
            return Some((h, v));
        }
        None
    }
}

#[derive(Clone)]
pub struct BoundedFieldDataIterator<'a, I: FieldDataIterator<'a>> {
    iter: I,
    remaining_len: usize,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a, I: FieldDataIterator<'a>> Iterator for BoundedFieldDataIterator<'a, I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_len == 0 {
            return None;
        }
        match self.iter.next() {
            None => {
                self.remaining_len = 0;
                None
            }
            Some((mut h, val)) => {
                if h.run_length as usize > self.remaining_len {
                    h.run_length = self.remaining_len as RunLength;
                }
                self.remaining_len -= h.run_length as usize;
                Some((h, val))
            }
        }
    }
}

unsafe impl<'a, I: FieldDataIterator<'a>> FieldDataIterator<'a>
    for BoundedFieldDataIterator<'a, I>
{
    type ValueType = I::ValueType;
    fn peek(&self) -> Option<Self::Item> {
        if self.remaining_len == 0 {
            return None;
        }
        self.iter.peek()
    }

    fn drop_n_fields(&mut self, mut n: usize) -> usize {
        if n < self.remaining_len {
            n = self.remaining_len;
        }
        self.iter.drop_n_fields(n)
    }
}

pub trait IntoFieldValueRefIter<'a, I: FieldDataIterator<'a, ValueType = *mut u8>> {
    fn value_refs(self) -> FieldValueRefIter<'a, I>;
}
impl<'a, I: FieldDataIterator<'a, ValueType = *mut u8>> IntoFieldValueRefIter<'a, I> for I {
    fn value_refs(self) -> FieldValueRefIter<'a, I> {
        FieldValueRefIter {
            iter: self,
            _phantom_data: PhantomData::default(),
        }
    }
}

#[derive(Clone)]
pub struct FieldValueRefIter<'a, I> {
    iter: I,
    _phantom_data: PhantomData<&'a FieldData>,
}

unsafe fn resolve_field_ref<'a>(
    v: Option<(FieldValueHeader, *mut u8)>,
) -> Option<(FieldValueHeader, FieldValueRef<'a>)> {
    match v {
        Some((h, ptr)) => Some((h, unsafe { get_field_value_ref(h, ptr) })),
        _ => None,
    }
}

impl<'a, I: FieldDataIterator<'a, ValueType = *mut u8>> Iterator for FieldValueRefIter<'a, I> {
    type Item = (FieldValueHeader, FieldValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe { resolve_field_ref(self.iter.next()) }
    }
}

unsafe impl<'a, I: FieldDataIterator<'a, ValueType = *mut u8>> FieldDataIterator<'a>
    for FieldValueRefIter<'a, I>
{
    type ValueType = FieldValueRef<'a>;

    fn peek(&self) -> Option<Self::Item> {
        unsafe { resolve_field_ref(self.iter.peek()) }
    }

    fn drop_n_fields(&mut self, n: usize) -> usize {
        todo!()
    }
}

pub struct UnfoldedValuesFieldDataIterator<'a, I: FieldDataIterator<'a>> {
    iter: I,
    cached_value: Option<I::ValueType>,
    remaining_run_len: RunLength,
}

impl<'a, I: FieldDataIterator<'a>> Iterator for UnfoldedValuesFieldDataIterator<'a, I> {
    type Item = I::ValueType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_run_len > 0 {
            self.remaining_run_len -= 1;
            return self.cached_value;
        }
        if let Some((h, val)) = self.iter.next() {
            self.cached_value = Some(val);
            self.remaining_run_len = h.run_length - 1;
            return self.cached_value;
        }
        None
    }
}

impl<'a, I: FieldDataIterator<'a, ValueType = FieldValueRef<'a>>>
    UnfoldedValuesFieldDataIterator<'a, I>
{
    pub fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.remaining_run_len > 0 {
            return self.cached_value;
        }
        if let Some((h, val)) = self.iter.next() {
            self.cached_value = Some(val);
            self.remaining_run_len = h.run_length;
            return self.cached_value;
        }
        None
    }
    pub fn drop_n(&mut self, n: usize) -> usize {
        self.iter.drop_n_fields(n)
    }
}

pub struct LenOnlyFieldDataIterator<'a, I: FieldDataIterator<'a>> {
    iter: I,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a, I: FieldDataIterator<'a>> Iterator for LenOnlyFieldDataIterator<'a, I> {
    type Item = (RunLength, I::ValueType);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(h, v)| (h.run_length, v))
    }
}

impl<'a, I: FieldDataIterator<'a, ValueType = FieldValueRef<'a>>> LenOnlyFieldDataIterator<'a, I> {
    pub fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iter.peek().map(|(h, v)| (h.run_length, v))
    }
    pub fn drop_n_fields(&mut self, n: usize) -> usize {
        self.iter.drop_n_fields(n)
    }
}
