use std::{marker::PhantomData, mem::size_of, ptr::NonNull, slice};

use crate::field_data::{
    FieldData, FieldValueHeader, FieldValueHeaderFlags, FieldValueHeaderFormat, FieldValueKind,
    FieldValueKindIntegralType, RunLength,
};

#[derive(Clone)]
pub struct RawFieldDataIter<'a> {
    header: *const FieldValueHeader,
    header_r_end: *const FieldValueHeader,
    data: NonNull<u8>,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a> Iterator for RawFieldDataIter<'a> {
    type Item = (FieldValueHeader, *const u8);

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
    pub fn peek_header_fmt(&self) -> Option<FieldValueHeaderFormat> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe { Some((*self.header.sub(1)).fmt) }
    }
    pub fn prev_data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
}

// SAFETY:
// these traits are unsafe because other (especially transformative) iterators
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
    fn header_to_len(self) -> HeaderToLenFieldDataIterator<'a, Self> {
        HeaderToLenFieldDataIterator {
            iter: self,
            _phantom_data: PhantomData::default(),
        }
    }
    fn peek(&self) -> Option<Self::Item>;
    fn peek_header(&self) -> Option<FieldValueHeader>;
    fn peek_header_fmt(&self) -> Option<FieldValueHeaderFormat>;
    fn drop_n_fields(&mut self, n: usize) -> usize;
    fn prev_data_ptr(&self) -> *const u8;
}

pub unsafe trait NoRleTypesFieldDataIterator<'a>: FieldDataIterator<'a> {
    fn unfold_values(self) -> UnfoldedValuesFieldDataIterator<'a, Self> {
        UnfoldedValuesFieldDataIterator {
            iter: self,
            cached_value: None,
            remaining_run_len: 0,
        }
    }
}

#[derive(Clone)]
pub struct FieldDataIter<'a> {
    iter: RawFieldDataIter<'a>,
    handled_run_len: RunLength,
}

impl<'a> FieldDataIter<'a> {
    pub fn new(iter: RawFieldDataIter<'a>) -> Self {
        Self {
            iter,
            handled_run_len: 0,
        }
    }
}

impl<'a> Iterator for FieldDataIter<'a> {
    type Item = (FieldValueHeader, *const u8);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some((mut h, v)) => {
                h.run_length -= self.handled_run_len;
                self.handled_run_len = 0;
                Some((h, v))
            }
        }
    }
}
unsafe impl<'a> FieldDataIterator<'a> for FieldDataIter<'a> {
    type ValueType = *const u8;

    fn peek(&self) -> Option<Self::Item> {
        match self.iter.peek() {
            None => None,
            Some((mut h, v)) => {
                h.run_length -= self.handled_run_len;
                Some((h, v))
            }
        }
    }
    fn peek_header(&self) -> Option<FieldValueHeader> {
        match self.iter.peek_header() {
            None => None,
            Some(mut h) => {
                h.run_length -= self.handled_run_len;
                Some(h)
            }
        }
    }

    fn peek_header_fmt(&self) -> Option<FieldValueHeaderFormat> {
        self.iter.peek_header_fmt()
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

    fn prev_data_ptr(&self) -> *const u8 {
        self.iter.prev_data_ptr()
    }
}

#[derive(Clone)]
pub struct NoRleTypesFieldDataIter<'a> {
    iter: RawFieldDataIter<'a>,
    last_header: FieldValueHeader,
}

impl<'a> NoRleTypesFieldDataIter<'a> {
    pub fn new(iter: RawFieldDataIter<'a>) -> Self {
        Self {
            iter,
            last_header: Default::default(),
        }
    }
}

impl<'a> Iterator for NoRleTypesFieldDataIter<'a> {
    type Item = (FieldValueHeader, *const u8);

    fn next(&mut self) -> Option<Self::Item> {
        let mut h;
        let mut ptr;
        if self.last_header.run_length > 0 {
            self.last_header.run_length -= 1;
            h = self.last_header;
            ptr = self.prev_data_ptr();
        } else if let Some((h_next, v)) = self.iter.next() {
            self.last_header = h_next;
            if h_next.shared_value() {
                self.last_header.run_length = 0;
                return Some((h_next, v));
            }
            self.last_header.run_length -= 1;
            h = h_next;
            ptr = v;
        } else {
            return None;
        }
        h.run_length = 1;
        ptr = unsafe {
            ptr.add(self.last_header.run_length as usize * self.last_header.size as usize)
        };
        Some((h, ptr))
    }
}
unsafe impl<'a> FieldDataIterator<'a> for NoRleTypesFieldDataIter<'a> {
    type ValueType = *const u8;
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
            let ptr = unsafe { self.prev_data_ptr() };
            let mut h = self.last_header;
            h.run_length = 1;
            return Some((h, ptr));
        }
        if let Some((mut h, v)) = self.iter.peek() {
            if h.shared_value() {
                return Some((h, v));
            }
            h.run_length = 1;
            return Some((h, v));
        }
        None
    }
    fn peek_header(&self) -> Option<FieldValueHeader> {
        let rl_rem = self.last_header.run_length;
        if rl_rem > 0 {
            let mut h = self.last_header;
            h.run_length = 1;
            return Some(h);
        }
        if let Some(mut h) = self.iter.peek_header() {
            if h.shared_value() {
                return Some(h);
            }
            h.run_length = 1;
            return Some(h);
        }
        None
    }
    fn peek_header_fmt(&self) -> Option<FieldValueHeaderFormat> {
        let rl_rem = self.last_header.run_length;
        if rl_rem > 0 {
            return Some(self.last_header.fmt);
        }
        self.iter.peek_header_fmt()
    }
    fn prev_data_ptr(&self) -> *const u8 {
        unsafe {
            self.iter
                .prev_data_ptr()
                .add(self.last_header.run_length as usize * self.last_header.size as usize)
        }
    }
}
unsafe impl<'a> NoRleTypesFieldDataIterator<'a> for NoRleTypesFieldDataIter<'a> {}

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
    fn peek_header(&self) -> Option<FieldValueHeader> {
        if self.remaining_len == 0 {
            return None;
        }
        self.iter.peek_header()
    }
    fn peek_header_fmt(&self) -> Option<FieldValueHeaderFormat> {
        if self.remaining_len == 0 {
            return None;
        }
        self.iter.peek_header_fmt()
    }

    fn drop_n_fields(&mut self, mut n: usize) -> usize {
        if n < self.remaining_len {
            n = self.remaining_len;
        }
        let dropped = self.iter.drop_n_fields(n);
        self.remaining_len -= dropped;
        dropped
    }
    fn prev_data_ptr(&self) -> *const u8 {
        self.iter.prev_data_ptr()
    }
}
unsafe impl<'a, I: NoRleTypesFieldDataIterator<'a>> NoRleTypesFieldDataIterator<'a>
    for BoundedFieldDataIterator<'a, I>
{
}

pub trait IntoFieldValueRefIter<'a, I: FieldDataIterator<'a, ValueType = *const u8>> {}

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

impl<'a, I: FieldDataIterator<'a>> UnfoldedValuesFieldDataIterator<'a, I> {
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

pub struct HeaderToLenFieldDataIterator<'a, I: FieldDataIterator<'a>> {
    iter: I,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a, I: FieldDataIterator<'a>> Iterator for HeaderToLenFieldDataIterator<'a, I> {
    type Item = (RunLength, I::ValueType);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(h, v)| (h.run_length, v))
    }
}

impl<'a, I: FieldDataIterator<'a>> HeaderToLenFieldDataIterator<'a, I> {
    pub fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iter.peek().map(|(h, v)| (h.run_length, v))
    }
    pub fn drop_n_fields(&mut self, n: usize) -> usize {
        self.iter.drop_n_fields(n)
    }
}

struct InlineBytesFieldDataIterator<
    'a,
    const FLAGS_MASK: FieldValueHeaderFlags::Type,
    const FLAGS_VALUE: FieldValueHeaderFlags::Type,
    I: FieldDataIterator<'a, ValueType = *const u8>,
> {
    iter: I,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<
        'a,
        const FLAGS_MASK: FieldValueHeaderFlags::Type,
        const FLAGS_VALUE: FieldValueHeaderFlags::Type,
        I: FieldDataIterator<'a, ValueType = *const u8>,
    > Iterator for InlineBytesFieldDataIterator<'a, FLAGS_MASK, FLAGS_VALUE, I>
{
    type Item = (FieldValueHeader, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(h) = self.iter.peek_header_fmt() {
            if h.kind != FieldValueKind::BytesInline || h.flags & FLAGS_MASK != FLAGS_VALUE {
                return None;
            }
            if let Some((h, v)) = self.iter.next() {
                return Some((h, unsafe {
                    slice::from_raw_parts(v as *const u8, h.size as usize)
                }));
            }
        }
        None
    }
}

struct FixedTypeSlicesFieldDataIterator<
    'a,
    const KIND: FieldValueKindIntegralType,
    const FLAGS_MASK: FieldValueHeaderFlags::Type,
    const FLAGS_VALUE: FieldValueHeaderFlags::Type,
    T: Sized,
    I: FieldDataIterator<'a, ValueType = *const u8>,
> {
    iter: I,
    _phantom_data: PhantomData<&'a T>,
}

impl<
        'a,
        const KIND: FieldValueKindIntegralType,
        const FLAGS_MASK: FieldValueHeaderFlags::Type,
        const FLAGS_VALUE: FieldValueHeaderFlags::Type,
        T: Sized,
        I: FieldDataIterator<'a, ValueType = *const u8>,
    > Iterator for FixedTypeSlicesFieldDataIterator<'a, KIND, FLAGS_MASK, FLAGS_VALUE, T, I>
{
    type Item = (FieldValueHeader, &'a [T]);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((h, v)) = self.iter.next() {
            if h.kind as FieldValueKindIntegralType != KIND || h.flags & FLAGS_MASK != FLAGS_VALUE {
                return None;
            }
            return Some((h, unsafe {
                slice::from_raw_parts(v as *const T, h.data_element_count())
            }));
        }
        None
    }
}

struct FixedTypeFieldDataIterator<
    'a,
    const KIND: FieldValueKindIntegralType,
    const FLAGS_MASK: FieldValueHeaderFlags::Type,
    const FLAGS_VALUE: FieldValueHeaderFlags::Type,
    T: Sized,
    I: FieldDataIterator<'a, ValueType = *const u8>,
> {
    iter: I,
    last_header: FieldValueHeader,
    _phantom_data: PhantomData<&'a T>,
}

impl<
        'a,
        const KIND: FieldValueKindIntegralType,
        const FLAGS_MASK: FieldValueHeaderFlags::Type,
        const FLAGS_VALUE: FieldValueHeaderFlags::Type,
        T: Sized,
        I: FieldDataIterator<'a, ValueType = *const u8>,
    > Iterator for FixedTypeFieldDataIterator<'a, KIND, FLAGS_MASK, FLAGS_VALUE, T, I>
{
    type Item = (FieldValueHeader, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let mut h;
        let ptr;
        if self.last_header.run_length > 0 {
            h = self.last_header;
            h.run_length = 1;
            ptr = self.iter.prev_data_ptr();
        } else if let Some((h_next, v)) = self.iter.next() {
            h = h_next;
            if h.kind as FieldValueKindIntegralType != KIND || h.flags & FLAGS_MASK != FLAGS_VALUE {
                return None;
            }
            self.last_header = h;
            if h.shared_value() {
                self.last_header.run_length = 0;
            } else {
                self.last_header.run_length -= 1;
                h.run_length = 1;
            }
            ptr = v;
        } else {
            return None;
        }
        return Some((h, unsafe {
            &*(ptr.add(self.last_header.run_length as usize * size_of::<T>()) as *const T)
        }));
    }
}
