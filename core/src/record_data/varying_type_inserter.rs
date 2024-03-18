use std::ops::DerefMut;

use crate::record_data::field_data::INLINE_STR_MAX_LEN;

use super::{
    bytes_insertion_stream::{
        BytesInsertionStream, MaybeTextInsertionStream, TextInsertionStream,
    },
    field_data::{
        field_value_flags::{self, FieldValueFlags},
        FieldData, FieldValueFormat, FieldValueRepr, FieldValueSize,
        FieldValueType, MAX_FIELD_ALIGN,
    },
    field_value::FieldValue,
    push_interface::PushInterface,
};

pub struct VaryingTypeInserter<FD: DerefMut<Target = FieldData>> {
    fd: FD,
    count: usize,
    max: usize,
    data_ptr: *mut u8,
    fmt: FieldValueFormat,
}

unsafe impl<FD: DerefMut<Target = FieldData>> Send
    for VaryingTypeInserter<FD>
{
}
unsafe impl<FD: DerefMut<Target = FieldData>> Sync
    for VaryingTypeInserter<FD>
{
}

impl<FD: DerefMut<Target = FieldData>> VaryingTypeInserter<FD> {
    pub fn new(fd: FD) -> Self {
        Self {
            fd,
            count: 0,
            max: 0,
            data_ptr: std::ptr::null_mut(),
            fmt: FieldValueFormat::default(),
        }
    }
    pub fn with_reservation(
        fd: FD,
        fmt: FieldValueFormat,
        reserved_elements: usize,
    ) -> Self {
        let mut v = Self::new(fd);
        v.drop_and_reserve(reserved_elements, fmt);
        v
    }
    pub unsafe fn drop_and_reserve_unchecked(
        &mut self,
        reserved_elements: usize,
        fmt: FieldValueFormat,
    ) {
        // TODO: we might want to restrict reservation size
        // in case there is one very large string
        self.count = 0;
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + reserved_elements * fmt.size as usize);
        self.fd
            .data
            .reserve_contiguous(MAX_FIELD_ALIGN + fmt.size as usize, 0);
        self.max = (self.fd.data.contiguous_tail_space_available()
            - MAX_FIELD_ALIGN)
            / fmt.size as usize;
        self.data_ptr = self.fd.data.tail_ptr_mut();
        self.fmt = fmt;
    }
    fn sanitize_format(fmt: FieldValueFormat) {
        if fmt.repr.is_variable_sized_type() {
            assert!(fmt.size <= INLINE_STR_MAX_LEN as FieldValueSize);
        }
    }
    pub unsafe fn drop_and_reserve_reasonable_unchecked(
        &mut self,
        fmt: FieldValueFormat,
    ) {
        self.fmt = fmt;
        self.count = 0;
        let elem_size = fmt.size as usize;
        if elem_size == 0 {
            self.data_ptr = self.fd.data.tail_ptr_mut();
            self.max = usize::MAX;
            return;
        }
        let curr_field_len = self
            .fd
            .data
            .len()
            .max(std::mem::size_of::<FieldValue>() * 4);
        let reserved_len = (curr_field_len / elem_size).max(2);
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + elem_size * reserved_len);
        self.fd
            .data
            .reserve_contiguous(MAX_FIELD_ALIGN + elem_size, 0);
        self.max = (self.fd.data.contiguous_tail_space_available()
            - MAX_FIELD_ALIGN)
            / elem_size;
        self.data_ptr = self.fd.data.tail_ptr_mut();
    }
    pub fn drop_and_reserve_reasonable(&mut self, fmt: FieldValueFormat) {
        Self::sanitize_format(fmt);
        unsafe { self.drop_and_reserve_reasonable_unchecked(fmt) }
    }
    pub fn drop_and_reserve(
        &mut self,
        reserved_elements: usize,
        fmt: FieldValueFormat,
    ) {
        Self::sanitize_format(fmt);
        unsafe {
            self.drop_and_reserve_unchecked(reserved_elements, fmt);
        }
    }
    pub fn commit(&mut self) {
        if self.count == 0 {
            return;
        }
        self.max -= self.count;
        if self.fmt.repr.is_zst() {
            self.fd.push_zst(self.fmt.repr, self.count, true);
            self.count = 0;
            return;
        }
        let new_len = self.fd.data.len() + self.fmt.size as usize * self.count;
        unsafe {
            self.fd.data.set_len(new_len);
            self.fd.add_header_for_multiple_values(
                self.fmt,
                self.count,
                field_value_flags::DELETED,
            );
            self.fd.field_count += self.count;
        }
        self.count = 0;
    }
}

unsafe impl<FD: DerefMut<Target = FieldData>> PushInterface
    for VaryingTypeInserter<FD>
{
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueRepr,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        if run_length == 0 {
            return self.data_ptr;
        }
        let fmt = FieldValueFormat {
            repr: kind,
            flags: field_value_flags::DEFAULT,
            size: data_len as FieldValueSize,
        };
        if run_length > 1 || self.fmt != fmt || !try_header_rle {
            self.commit();
            if run_length > 1 {
                let res = unsafe {
                    self.fd.push_variable_sized_type_uninit(
                        kind,
                        data_len,
                        run_length,
                        try_header_rle,
                    )
                };
                self.fmt.repr = FieldValueRepr::Null;
                return res;
            }
            unsafe {
                self.drop_and_reserve_reasonable_unchecked(fmt);
            }
        } else if self.count == self.max {
            self.commit();
            unsafe {
                self.drop_and_reserve_reasonable_unchecked(fmt);
            }
        }
        let res = self.data_ptr;
        unsafe {
            self.data_ptr = self.data_ptr.add(data_len);
        }
        self.count += 1;
        res
    }
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        repr: FieldValueRepr,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if run_length == 0 {
            return;
        }
        unsafe {
            if try_data_rle {
                self.commit();
                self.fd.push_variable_sized_type_unchecked(
                    repr,
                    data,
                    run_length,
                    try_header_rle,
                    try_data_rle,
                );
                self.drop_and_reserve_reasonable_unchecked(FieldValueFormat {
                    repr,
                    flags: field_value_flags::DEFAULT,
                    size: data.len() as FieldValueSize,
                });
                return;
            }
            let data_ptr = self.push_variable_sized_type_uninit(
                repr,
                data.len(),
                run_length,
                try_header_rle,
            );
            std::ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());
        }
    }
    unsafe fn push_fixed_size_type_unchecked<T: PartialEq + FieldValueType>(
        &mut self,
        repr: FieldValueRepr,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if run_length == 0 {
            return;
        }
        let fmt = FieldValueFormat {
            repr,
            flags: field_value_flags::DEFAULT,
            size: std::mem::size_of::<T>() as FieldValueSize,
        };
        if run_length > 1 || self.fmt != fmt || !try_header_rle || try_data_rle
        {
            self.commit();
            if run_length > 1 {
                self.fd.push_fixed_size_type(
                    data,
                    run_length,
                    try_header_rle,
                    try_data_rle,
                );
                self.fmt.repr = FieldValueRepr::Null;
                return;
            }
            unsafe {
                self.drop_and_reserve_reasonable_unchecked(fmt);
            }
        } else if self.count == self.max {
            self.commit();
            unsafe {
                self.drop_and_reserve_reasonable_unchecked(fmt);
            }
        }
        unsafe {
            std::ptr::write(self.data_ptr.cast(), data);
            self.data_ptr = self.data_ptr.add(fmt.size as usize);
        }
        self.count += 1;
    }
    unsafe fn push_zst_unchecked(
        &mut self,
        repr: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        if run_length == 0 {
            return;
        }
        if self.fmt.repr != repr || flags != self.fmt.flags || !try_header_rle
        {
            self.commit();
            self.fmt = FieldValueFormat {
                repr,
                flags,
                size: 0,
            };
        }
        self.count += run_length;
        self.max = self.max.max(self.count);
    }

    fn bytes_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> BytesInsertionStream {
        self.commit();
        self.fd.bytes_insertion_stream(run_length)
    }

    fn text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> TextInsertionStream {
        self.commit();
        self.fd.text_insertion_stream(run_length)
    }
    fn maybe_text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> MaybeTextInsertionStream {
        self.commit();
        self.fd.maybe_text_insertion_stream(run_length)
    }
}

impl<FD: DerefMut<Target = FieldData>> Drop for VaryingTypeInserter<FD> {
    fn drop(&mut self) {
        self.commit()
    }
}
