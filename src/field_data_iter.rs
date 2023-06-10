use std::{marker::PhantomData, ptr::NonNull};

use crate::field_data::{
    get_field_value_ref, FieldData, FieldValueHeader, FieldValueRef, RunLength,
};

pub trait FieldDataIterator<'a>: Iterator<Item = (RunLength, FieldValueRef<'a>)> + Sized {
    fn bounded(self, remaining_len: usize) -> BoundedFieldDataIterator<'a, Self> {
        BoundedFieldDataIterator {
            iter: self,
            remaining_len,
            _phantom_data: PhantomData::default(),
        }
    }
    fn counted(self) -> CountedFieldDataIterator<'a, Self> {
        CountedFieldDataIterator {
            iter: self,
            len_consumed: 0,
            _phantom_data: PhantomData::default(),
        }
    }
    fn values_only(self) -> ValuesOnlyFieldDataIterator<'a, Self> {
        ValuesOnlyFieldDataIterator {
            iter: self,
            cached_value: FieldValueRef::Unset,
            remaining_run_len: 0,
        }
    }
    fn peek(&self) -> Option<Self::Item>;
    fn peek_field(&mut self) -> Option<FieldValueRef<'a>>;
    fn next_field(&mut self) -> Option<FieldValueRef<'a>>;
    fn drop_n_fields(&mut self, n: usize) -> usize;
    fn remaining_run_len(&mut self) -> RunLength;
}

#[derive(Clone)]
pub struct FieldDataIter<'a> {
    header: *const FieldValueHeader,
    header_r_end: *const FieldValueHeader,
    data: NonNull<u8>,
    handled_run_len: RunLength,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a> Iterator for FieldDataIter<'a> {
    type Item = (RunLength, FieldValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_r_end {
            return None;
        }
        let (h, hp, val) = unsafe {
            let hp = self.header.sub(1);
            let h = *hp;
            let ptr = self.data.as_ptr().sub(h.size as usize);
            let val = get_field_value_ref(h, ptr);
            self.data = NonNull::new_unchecked(ptr);
            (h, hp, val)
        };
        if h.shared_value {
            self.header = hp;
            let rl = h.run_length - self.handled_run_len;
            self.handled_run_len = 0;
            Some((rl, val))
        } else {
            self.handled_run_len += 1;
            if self.handled_run_len == h.run_length {
                self.handled_run_len = 0;
                self.header = hp;
            }
            Some((1, val))
        }
    }
}

impl<'a> FieldDataIterator<'a> for FieldDataIter<'a> {
    fn drop_n_fields(&mut self, n: usize) -> usize {
        let mut fields_to_drop = n;
        loop {
            if self.header == self.header_r_end {
                return n - fields_to_drop;
            }
            let (h, hp) = unsafe {
                let hp = self.header.sub(1);
                let h = *hp;
                (h, hp)
            };
            if (h.run_length as usize) < fields_to_drop {
                fields_to_drop -= h.run_length as usize;
                self.header = hp;
                continue;
            }
            if h.run_length as usize == fields_to_drop {
                self.header = hp;
                self.handled_run_len = 0;
                return n;
            }
            self.handled_run_len = fields_to_drop as RunLength;
            return n;
        }
    }
    fn peek(&self) -> Option<<Self as Iterator>::Item> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe {
            let h = *self.header.sub(1);
            let val = get_field_value_ref(h, self.data.as_ptr().sub(h.size as usize));
            if h.shared_value {
                Some((h.run_length, val))
            } else {
                Some((1, val))
            }
        }
    }
    fn peek_field(&mut self) -> Option<FieldValueRef<'a>> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe {
            let h = *self.header.sub(1);
            Some(get_field_value_ref(
                h,
                self.data.as_ptr().sub(h.size as usize),
            ))
        }
    }

    fn next_field(&mut self) -> Option<FieldValueRef<'a>> {
        if self.header == self.header_r_end {
            return None;
        }
        let (h, hp, ptr, val) = unsafe {
            let hp = self.header.sub(1);
            let h = *hp;
            let ptr = self.data.as_ptr().sub(h.size as usize);
            let val = get_field_value_ref(h, ptr);
            (h, hp, ptr, val)
        };
        self.handled_run_len += 1;
        if self.handled_run_len == h.run_length {
            self.handled_run_len = 0;
            self.header = hp;
        }
        if !h.shared_value || self.handled_run_len == 0 {
            unsafe {
                self.data = NonNull::new_unchecked(ptr);
            }
        }
        Some(val)
    }

    fn remaining_run_len(&mut self) -> RunLength {
        if self.header == self.header_r_end {
            return 0;
        }
        let curr_run_len = unsafe { (*self.header.sub(1)).run_length };
        return curr_run_len - self.handled_run_len;
    }
}

impl<'a> FieldDataIter<'a> {
    pub fn new(field_data: &'a FieldData) -> FieldDataIter<'a> {
        Self {
            header: field_data.header.as_ptr_range().end,
            header_r_end: field_data.header.as_ptr_range().start,
            data: unsafe { NonNull::new_unchecked(field_data.data.as_ptr_range().end as *mut u8) },
            handled_run_len: 0,
            _phantom_data: PhantomData::default(),
        }
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
            Some((mut len, val)) => {
                if len as usize > self.remaining_len {
                    len = self.remaining_len as RunLength;
                }
                self.remaining_len -= len as usize;
                Some((len, val))
            }
        }
    }
}

impl<'a, I: FieldDataIterator<'a>> FieldDataIterator<'a> for BoundedFieldDataIterator<'a, I> {
    fn peek(&self) -> Option<Self::Item> {
        if self.remaining_len == 0 {
            return None;
        }
        self.iter.peek()
    }

    fn peek_field(&mut self) -> Option<FieldValueRef<'a>> {
        if self.remaining_len == 0 {
            return None;
        }
        self.iter.peek_field()
    }

    fn next_field(&mut self) -> Option<FieldValueRef<'a>> {
        if self.remaining_len == 0 {
            return None;
        }
        self.remaining_len -= 1;
        self.iter.next_field()
    }

    fn drop_n_fields(&mut self, mut n: usize) -> usize {
        if n < self.remaining_len {
            n = self.remaining_len;
        }
        self.iter.drop_n_fields(n)
    }
    fn remaining_run_len(&mut self) -> RunLength {
        (self.iter.remaining_run_len() as usize).max(self.remaining_len) as RunLength
    }
}

#[derive(Clone)]
pub struct CountedFieldDataIterator<'a, I: FieldDataIterator<'a>> {
    iter: I,
    len_consumed: usize,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a, I: FieldDataIterator<'a>> CountedFieldDataIterator<'a, I> {
    pub fn set_consumed_len(&mut self, len: usize) {
        self.len_consumed = len;
    }
    pub fn consumed_len(&self) -> usize {
        self.len_consumed
    }
    pub fn skip_to_nth(&mut self, n: usize) -> Option<<Self as Iterator>::Item> {
        while let Some((mut len, val)) = self.iter.next() {
            if self.len_consumed + len as usize >= n {
                len -= (n - self.len_consumed) as RunLength;
                return Some((len, val));
            }
        }
        None
    }
}

impl<'a, I: FieldDataIterator<'a>> Iterator for CountedFieldDataIterator<'a, I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next();
        if let Some((len, _)) = res {
            self.len_consumed += len as usize;
        }
        res
    }
}

impl<'a, I: FieldDataIterator<'a>> FieldDataIterator<'a> for CountedFieldDataIterator<'a, I> {
    fn peek(&self) -> Option<Self::Item> {
        self.iter.peek()
    }

    fn peek_field(&mut self) -> Option<FieldValueRef<'a>> {
        self.iter.peek_field()
    }

    fn next_field(&mut self) -> Option<FieldValueRef<'a>> {
        self.len_consumed += 1;
        self.iter.next_field()
    }

    fn drop_n_fields(&mut self, n: usize) -> usize {
        let dropped = self.iter.drop_n_fields(n);
        self.len_consumed += dropped;
        dropped
    }

    fn remaining_run_len(&mut self) -> RunLength {
        self.iter.remaining_run_len()
    }
}

pub struct ValuesOnlyFieldDataIterator<'a, I: FieldDataIterator<'a>> {
    iter: I,
    cached_value: FieldValueRef<'a>,
    remaining_run_len: RunLength,
}

impl<'a, I: FieldDataIterator<'a>> Iterator for ValuesOnlyFieldDataIterator<'a, I> {
    type Item = FieldValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_run_len > 0 {
            self.remaining_run_len -= 1;
            return Some(self.cached_value);
        }
        if let Some((len, val)) = self.iter.next() {
            self.cached_value = val;
            self.remaining_run_len = len - 1;
            return Some(val);
        }
        None
    }
}

impl<'a, I: FieldDataIterator<'a>> ValuesOnlyFieldDataIterator<'a, I> {
    pub fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.remaining_run_len > 0 {
            return Some(self.cached_value);
        }
        if let Some((len, val)) = self.iter.next() {
            (self.remaining_run_len, self.cached_value) = (len, val);
            return Some(val);
        }
        None
    }
    pub fn drop_n(&mut self, n: usize) -> usize {
        self.iter.drop_n_fields(n)
    }
}
