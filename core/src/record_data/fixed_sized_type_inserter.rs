use std::marker::PhantomData;

use crate::utils::range_contains;

use super::{
    field_data::{
        field_value_flags::{self, DELETED},
        FieldData, FieldValueFlags, FieldValueFormat, FieldValueRepr,
        FieldValueSize, FieldValueType, MAX_FIELD_ALIGN,
    },
    push_interface::PushInterface,
};

pub struct RawFixedSizedTypeInserter<'a> {
    fd: &'a mut FieldData,
    count: usize,
    max: usize,
    data_ptr: *mut u8,
}

impl<'a> RawFixedSizedTypeInserter<'a> {
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            fd,
            count: 0,
            max: 0,
            data_ptr: std::ptr::null_mut(),
        }
    }
    unsafe fn commit(&mut self, fmt: FieldValueFormat) {
        self.max = 0;
        if self.count == 0 {
            return;
        }
        let new_len = self.fd.data.len() + self.count * fmt.size as usize;
        unsafe {
            self.fd.data.set_len(new_len);
            let padding = self.fd.pad_to_align(fmt.repr);
            if padding != 0 {
                self.fd.add_header_padded_for_multiple_values(
                    fmt, self.count, padding,
                );
            } else {
                self.fd.add_header_for_multiple_values(
                    fmt,
                    self.count,
                    fmt.flags | DELETED,
                )
            }
        };
        self.fd.field_count += self.count;
        self.count = 0;
    }
    unsafe fn drop_and_reserve(
        &mut self,
        element_size: usize,
        max_inserts: usize,
    ) {
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + max_inserts * element_size);
        self.fd
            .data
            .reserve_contiguous(MAX_FIELD_ALIGN + element_size, 0);
        self.data_ptr = self.fd.data.tail_ptr_mut();
        self.max = (self.fd.data.contiguous_tail_space_available()
            - MAX_FIELD_ALIGN)
            / element_size;
        self.count = 0;
    }
    unsafe fn commit_and_reserve(
        &mut self,
        fmt: FieldValueFormat,
        new_max_inserts: usize,
    ) {
        unsafe {
            self.commit(fmt);
            self.drop_and_reserve(new_max_inserts, fmt.size as usize);
        }
    }
    #[inline(always)]
    unsafe fn push<T>(&mut self, v: T) {
        unsafe {
            debug_assert!(range_contains(
                self.fd.data.buffer_range(),
                self.data_ptr..self.data_ptr.add(std::mem::size_of::<T>())
            ));
            std::ptr::copy_nonoverlapping(
                std::ptr::addr_of!(v),
                self.data_ptr.cast(),
                1,
            );
            self.data_ptr = self.data_ptr.add(std::mem::size_of::<T>());
        }
        std::mem::forget(v);
        self.count += 1;
    }
}

pub struct FixedSizeTypeInserter<'a, T: FieldValueType + PartialEq + Clone> {
    raw: RawFixedSizedTypeInserter<'a>,
    _phantom: PhantomData<T>,
}

impl<'a, T: FieldValueType + PartialEq + Clone> FixedSizeTypeInserter<'a, T> {
    pub fn new(fd: &'a mut FieldData) -> Self {
        assert!(!T::ZST && !T::DST);
        Self {
            raw: RawFixedSizedTypeInserter::new(fd),
            _phantom: PhantomData,
        }
    }
    pub fn element_size() -> usize {
        std::mem::size_of::<T>()
    }
    pub fn element_format() -> FieldValueFormat {
        FieldValueFormat {
            repr: T::REPR,
            flags: field_value_flags::DEFAULT,
            size: Self::element_size() as FieldValueSize,
        }
    }
    pub fn commit(&mut self) {
        unsafe {
            self.raw.commit(Self::element_format());
        }
    }
    pub fn drop_and_reserve(&mut self, new_max_inserts: usize) {
        unsafe {
            self.raw
                .drop_and_reserve(Self::element_size(), new_max_inserts);
        }
    }
    pub fn commit_and_reserve(&mut self, new_max_inserts: usize) {
        unsafe {
            self.raw
                .commit_and_reserve(Self::element_format(), new_max_inserts);
        }
    }
    #[inline(always)]
    pub fn push(&mut self, v: T) {
        if self.raw.count >= self.raw.max {
            self.commit_and_reserve(
                (self.raw.fd.data.len() / std::mem::size_of::<T>()).min(4),
            );
        }
        unsafe {
            self.raw.push(v);
        }
    }
    #[inline(always)]
    pub fn push_with_rl(&mut self, v: T, rl: usize) {
        if rl == 0 {
            return;
        }
        if rl == 1 {
            self.push(v);
            return;
        }
        let max_rem = self.raw.max - self.raw.count;
        self.commit();
        unsafe {
            self.raw.fd.push_fixed_size_type_unchecked(
                T::REPR,
                v,
                rl,
                true,
                false,
            );
        }
        self.drop_and_reserve(max_rem);
    }

    pub fn extend<I: Iterator<Item = T>>(&mut self, mut iter: I) {
        if let (_, Some(count)) = iter.size_hint() {
            self.commit_and_reserve(count);
            unsafe {
                let mut ptr = self.raw.data_ptr.cast::<T>();
                for v in (&mut iter).take(count) {
                    std::ptr::write(ptr, v);
                    ptr = ptr.add(1);
                }
                self.raw.count +=
                    ptr.offset_from(self.raw.data_ptr.cast()) as usize;
                self.raw.data_ptr = ptr.cast();
            }
        }
        for v in &mut iter {
            self.push(v);
        }
    }
}

impl<'a, T: FieldValueType + PartialEq + Clone> Drop
    for FixedSizeTypeInserter<'a, T>
{
    fn drop(&mut self) {
        self.commit()
    }
}

pub struct RawZeroSizedTypeInserter<'a> {
    fd: &'a mut FieldData,
    count: usize,
}

impl<'a> RawZeroSizedTypeInserter<'a> {
    pub fn new(fd: &'a mut FieldData) -> Self {
        Self { fd, count: 0 }
    }
    pub fn push(&mut self, count: usize) {
        self.count += count;
    }
    pub unsafe fn commit(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
    ) {
        if self.count == 0 {
            return;
        }
        unsafe {
            self.fd.push_zst_unchecked(kind, flags, self.count, true);
        }
        self.count = 0;
    }
}

pub unsafe trait ZeroSizedTypeInserter<'a>: Sized {
    const KIND: FieldValueRepr;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    fn get_raw(&mut self) -> &mut RawZeroSizedTypeInserter<'a>;
    fn new(fd: &'a mut FieldData) -> Self;
    fn commit(&mut self) {
        unsafe {
            self.get_raw().commit(Self::KIND, Self::FLAGS);
        }
    }
    #[inline(always)]
    fn push(&mut self, count: usize) {
        self.get_raw().push(count);
    }
}

pub struct NullsInserter<'a> {
    raw: RawZeroSizedTypeInserter<'a>,
}
unsafe impl<'a> ZeroSizedTypeInserter<'a> for NullsInserter<'a> {
    const KIND: FieldValueRepr = FieldValueRepr::Null;
    #[inline(always)]
    fn get_raw(&mut self) -> &mut RawZeroSizedTypeInserter<'a> {
        &mut self.raw
    }
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            raw: RawZeroSizedTypeInserter::new(fd),
        }
    }
}
impl<'a> Drop for NullsInserter<'a> {
    fn drop(&mut self) {
        self.commit();
    }
}
