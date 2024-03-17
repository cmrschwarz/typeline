use super::{
    field_data::{
        field_value_flags::{self, DELETED},
        FieldData, FieldValueFormat, FieldValueRepr, FieldValueSize,
    },
    push_interface::PushInterface,
};

pub struct RawVariableSizedTypeInserter<'a> {
    fd: &'a mut FieldData,
    count: usize,
    max: usize,
    expected_size: usize,
    data_ptr: *mut u8,
}

impl<'a> RawVariableSizedTypeInserter<'a> {
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            fd,
            count: 0,
            max: 0,
            expected_size: 0,
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
            self.fd.add_header_for_multiple_values(
                fmt,
                self.count,
                fmt.flags | DELETED,
            )
        };
        self.fd.field_count += self.count;
        self.count = 0;
    }
    unsafe fn drop_and_reserve(
        &mut self,
        max_inserts: usize,
        new_expected_size: usize,
    ) {
        self.fd.data.reserve(max_inserts * new_expected_size);
        self.data_ptr = self.fd.data.tail_ptr_mut();
        self.max =
            self.fd.data.contiguous_tail_space_available() / new_expected_size;
        self.count = 0;
        self.expected_size = new_expected_size;
    }
    unsafe fn commit_and_reserve(
        &mut self,
        fmt: FieldValueFormat,
        new_max_inserts: usize,
        new_expected_size: usize,
    ) {
        unsafe {
            self.commit(fmt);
            self.drop_and_reserve(new_max_inserts, new_expected_size);
        }
    }
    // assumes that v.len() == self.expected_size
    // and self.count < self.max
    #[inline(always)]
    unsafe fn push(&mut self, v: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(v.as_ptr(), self.data_ptr, v.len());
            self.data_ptr = self.data_ptr.add(v.len());
        }
        self.count += 1;
    }
}

pub unsafe trait VariableSizeTypeInserter<'a>: Sized {
    const KIND: FieldValueRepr;
    type ElementType: ?Sized;
    fn new(fd: &'a mut FieldData) -> Self;
    fn get_raw(&mut self) -> &mut RawVariableSizedTypeInserter<'a>;
    fn element_as_bytes(v: &Self::ElementType) -> &[u8];
    fn current_element_format(&mut self) -> FieldValueFormat {
        FieldValueFormat {
            repr: Self::KIND,
            flags: field_value_flags::DEFAULT,
            size: self.get_raw().expected_size as FieldValueSize,
        }
    }
    fn commit(&mut self) {
        let fmt = self.current_element_format();
        unsafe {
            self.get_raw().commit(fmt);
        }
    }
    fn drop_and_reserve(
        &mut self,
        new_max_inserts: usize,
        new_expected_size: usize,
    ) {
        unsafe {
            self.get_raw()
                .drop_and_reserve(new_max_inserts, new_expected_size);
        }
    }
    fn commit_and_reserve(
        &mut self,
        new_max_inserts: usize,
        new_expected_size: usize,
    ) {
        let fmt = self.current_element_format();
        unsafe {
            self.get_raw().commit_and_reserve(
                fmt,
                new_max_inserts,
                new_expected_size,
            );
        }
    }
    #[inline(always)]
    fn push_same_size(&mut self, v: &Self::ElementType) {
        let v = Self::element_as_bytes(v);
        assert!(self.get_raw().count < self.get_raw().max);
        assert!(v.len() == self.get_raw().expected_size);
        unsafe {
            self.get_raw().push(v);
        }
    }
    #[inline(always)]
    fn push_may_rereserve(&mut self, v: &Self::ElementType) {
        let v_bytes = Self::element_as_bytes(v);
        let raw = self.get_raw();
        if v_bytes.len() == raw.expected_size {
            self.push_same_size(v);
            return;
        }
        assert!(raw.count < raw.max);
        let count_rem = raw.max - raw.count;
        let fmt = self.current_element_format();
        unsafe {
            self.get_raw()
                .commit_and_reserve(fmt, count_rem, v_bytes.len());
            self.get_raw().push(v_bytes);
        }
    }
    fn push_with_rl(&mut self, v: &Self::ElementType, rl: usize) {
        if rl == 1 {
            self.push_may_rereserve(v);
            return;
        }
        let v = Self::element_as_bytes(v);
        let max_rem = self.get_raw().max - self.get_raw().count;
        self.commit();
        unsafe {
            self.get_raw().fd.push_variable_sized_type_unchecked(
                Self::KIND,
                v,
                rl,
                true,
                false,
            );
        }
        self.drop_and_reserve(max_rem, v.len());
    }
}

pub struct InlineStringInserter<'a> {
    raw: RawVariableSizedTypeInserter<'a>,
}
unsafe impl<'a> VariableSizeTypeInserter<'a> for InlineStringInserter<'a> {
    const KIND: FieldValueRepr = FieldValueRepr::TextInline;
    type ElementType = str;
    #[inline(always)]
    fn get_raw(&mut self) -> &mut RawVariableSizedTypeInserter<'a> {
        &mut self.raw
    }
    #[inline(always)]
    fn element_as_bytes(v: &Self::ElementType) -> &[u8] {
        v.as_bytes()
    }
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            raw: RawVariableSizedTypeInserter::new(fd),
        }
    }
}
impl<'a> Drop for InlineStringInserter<'a> {
    fn drop(&mut self) {
        self.commit();
    }
}

pub struct InlineBytesInserter<'a> {
    raw: RawVariableSizedTypeInserter<'a>,
}
unsafe impl<'a> VariableSizeTypeInserter<'a> for InlineBytesInserter<'a> {
    const KIND: FieldValueRepr = FieldValueRepr::BytesInline;
    type ElementType = [u8];
    #[inline(always)]
    fn get_raw(&mut self) -> &mut RawVariableSizedTypeInserter<'a> {
        &mut self.raw
    }
    #[inline(always)]
    fn element_as_bytes(v: &Self::ElementType) -> &[u8] {
        v
    }
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            raw: RawVariableSizedTypeInserter::new(fd),
        }
    }
}
impl<'a> Drop for InlineBytesInserter<'a> {
    fn drop(&mut self) {
        self.commit();
    }
}
