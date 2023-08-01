use std::mem::ManuallyDrop;

use crate::{
    operators::errors::OperatorApplicationError,
    record_data::field_data::{
        field_value_flags::{BYTES_ARE_UTF8, DELETED, SHARED_VALUE},
        INLINE_STR_MAX_LEN,
    },
    stream_value::StreamValueId,
    utils::as_u8_slice,
};

use super::field_data::{
    field_value_flags, FieldData, FieldReference, FieldValueFlags,
    FieldValueFormat, FieldValueHeader, FieldValueKind, FieldValueSize,
    RunLength, MAX_FIELD_ALIGN,
};

pub unsafe trait RawPushInterface {
    unsafe fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    );
    unsafe fn push_fixed_size_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    );
    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    );
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
    ) -> *mut u8;
}
impl FieldData {
    #[inline(always)]
    pub unsafe fn push_header_raw(
        &mut self,
        fmt: FieldValueFormat,
        mut run_length: usize,
    ) {
        debug_assert!(run_length > 0);
        while run_length > RunLength::MAX as usize {
            self.header.push(FieldValueHeader {
                fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
        }
        self.header.push(FieldValueHeader {
            fmt,
            run_length: run_length as RunLength,
        });
    }
    pub unsafe fn push_header_raw_same_value_after_first(
        &mut self,
        mut fmt: FieldValueFormat,
        run_length: usize,
    ) {
        debug_assert!(run_length > 0);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.header.push(FieldValueHeader {
            fmt,
            run_length: rl_to_push as RunLength,
        });
        if rl_to_push != run_length {
            fmt.set_same_value_as_previous(true);
            unsafe { self.push_header_raw(fmt, run_length - rl_to_push) };
        }
    }
    pub unsafe fn add_header_for_single_value(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        header_rle: bool,
        data_rle: bool,
    ) {
        debug_assert!(fmt.shared_value());
        if !header_rle && !data_rle {
            unsafe {
                self.push_header_raw_same_value_after_first(fmt, run_length)
            };
            return;
        }
        // safe to unwrap here, otherwise we would have gone into the
        // branch above since header rle only makes sense with a previous
        // header
        let last_header = unsafe { self.header.last_mut().unwrap_unchecked() };
        if last_header.run_length == 1 {
            last_header.set_shared_value(data_rle);
        }
        if last_header.shared_value() || run_length > 1 {
            if data_rle {
                let rl_rem = last_header.run_len_rem() as usize;
                if rl_rem > run_length {
                    last_header.run_length += run_length as RunLength;
                    return;
                }
                last_header.run_length = RunLength::MAX;
                run_length -= rl_rem;
                fmt.set_same_value_as_previous(true);
                unsafe {
                    self.push_header_raw(fmt, run_length);
                }
                return;
            }
            unsafe {
                self.push_header_raw_same_value_after_first(fmt, run_length);
            }
            return;
        }
        if data_rle {
            // guaranteed to be at least two, otherwise we would have gone into
            // the shared value branch
            last_header.run_length -= 1;
            unsafe {
                self.push_header_raw_same_value_after_first(
                    fmt,
                    run_length + 1,
                );
            }
            return;
        }
        if header_rle
            && last_header.run_length as usize + run_length
                < RunLength::MAX as usize
        {
            last_header.run_length += run_length as RunLength;
            return;
        }
        unsafe {
            self.push_header_raw_same_value_after_first(fmt, run_length);
        }
    }
    pub unsafe fn add_header_padded_for_single_value(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        padding: usize,
    ) {
        debug_assert!(fmt.shared_value());
        fmt.set_leading_padding(padding);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.header.push(FieldValueHeader {
            fmt,
            run_length: rl_to_push as RunLength,
        });
        if run_length == rl_to_push {
            return;
        }
        run_length -= rl_to_push;
        fmt.set_leading_padding(0);
        fmt.set_same_value_as_previous(true);
        unsafe {
            self.push_header_raw(fmt, run_length);
        }
    }
    pub unsafe fn add_header_padded_for_multiple_values(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        padding: usize,
    ) {
        debug_assert!(!fmt.shared_value());
        fmt.set_leading_padding(padding);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.header.push(FieldValueHeader {
            fmt,
            run_length: rl_to_push as RunLength,
        });
        if run_length == rl_to_push {
            return;
        }
        run_length -= rl_to_push;
        fmt.set_leading_padding(0);
        unsafe {
            self.push_header_raw(fmt, run_length);
        }
    }

    pub unsafe fn add_header_for_multiple_values(
        &mut self,
        fmt: FieldValueFormat,
        mut run_length: usize,
        format_flags_mask: FieldValueFlags,
    ) {
        debug_assert!(!fmt.shared_value());
        match self.header.last_mut() {
            None => (),
            Some(last_header) => {
                if last_header.kind == fmt.kind
                    && last_header.size == fmt.size
                    && last_header.flags & format_flags_mask
                        == fmt.flags & format_flags_mask
                {
                    if last_header.run_length == 1 {
                        last_header.set_shared_value(false);
                    }
                    if !last_header.shared_value() {
                        let rl_rem = last_header.run_len_rem();
                        if rl_rem as usize > run_length {
                            last_header.run_length += run_length as RunLength;
                            return;
                        }
                        last_header.run_length = RunLength::MAX;
                        run_length -= rl_rem as usize;
                    }
                }
            }
        }
        unsafe {
            self.push_header_raw(fmt, run_length);
        }
    }

    pub fn dup_last_value(&mut self, run_length: usize) {
        if run_length == 0 {
            return;
        }
        let last_header = self.header.last_mut().unwrap();
        // command buffer should clear data after last non deleted
        debug_assert!(!last_header.deleted());
        unsafe {
            if last_header.run_length > 1 && !last_header.shared_value() {
                last_header.run_length -= 1;
                let mut fmt = last_header.fmt;
                fmt.set_shared_value(true);
                self.push_header_raw(fmt, run_length + 1);
            } else {
                last_header.set_shared_value(true);
                let rl_rem = last_header.run_len_rem();
                if last_header.run_len_rem() as usize > run_length {
                    last_header.run_length += run_length as RunLength;
                } else {
                    last_header.run_length = RunLength::MAX;
                    let fmt = last_header.fmt;
                    self.push_header_raw(fmt, run_length - rl_rem as usize);
                }
            }
        }
    }
    pub fn drop_last_value(&mut self, mut run_length: usize) {
        loop {
            if run_length == 0 {
                return;
            }
            let last_header = self.header.last_mut().unwrap();
            if last_header.run_length as usize > run_length {
                if !last_header.deleted() {
                    last_header.run_length -= run_length as RunLength;
                }
                if !last_header.shared_value() {
                    self.data.truncate(
                        self.data.len()
                            - last_header.size as usize * run_length,
                    );
                }
                return;
            }
            if !last_header.deleted() {
                run_length -= last_header.run_length as usize;
            }
            if !last_header.same_value_as_previous() {
                self.data
                    .truncate(self.data.len() - last_header.total_size());
            }
            self.header.pop();
        }
    }
}

unsafe impl RawPushInterface for FieldData {
    unsafe fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        debug_assert!(kind.is_variable_sized_type());
        debug_assert!(data.len() <= INLINE_STR_MAX_LEN);
        self.field_count += run_length;
        let size = data.len() as FieldValueSize;
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags =
            BYTES_ARE_UTF8 | DELETED;

        let mut header_rle = false;
        let mut data_rle = false;

        if try_header_rle || try_data_rle {
            if let Some(h) = self.header.last_mut() {
                if h.kind == kind
                    && h.size == size
                    && h.flags & MUST_MATCH_HEADER_FLAGS
                        == flags & MUST_MATCH_HEADER_FLAGS
                {
                    header_rle = true;
                    if try_data_rle {
                        let len = h.size as usize;
                        let prev_data = unsafe {
                            std::slice::from_raw_parts(
                                self.data.as_ptr_range().end.sub(len),
                                len,
                            )
                        };
                        data_rle = prev_data == data;
                    }
                }
            }
        }
        let fmt = FieldValueFormat {
            kind,
            flags: flags | SHARED_VALUE,
            size,
        };
        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, data_rle,
            );
        }
        if !data_rle {
            self.data.extend_from_slice(data);
        }
    }
    unsafe fn push_fixed_size_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        debug_assert!(kind.is_fixed_size_type());
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags = DELETED;
        self.field_count += run_length;
        let mut data_rle = false;
        let mut header_rle = false;
        let fmt = FieldValueFormat {
            kind,
            flags: flags | SHARED_VALUE,
            size: std::mem::size_of::<T>() as FieldValueSize,
        };
        if kind.needs_alignment() {
            let align = unsafe { self.pad_to_align() };
            if align != 0 {
                unsafe {
                    self.add_header_padded_for_single_value(
                        fmt, run_length, align,
                    );
                }
                if !data_rle {
                    let data = ManuallyDrop::new(data);
                    self.data.extend_from_slice(unsafe { as_u8_slice(&data) });
                }
                return;
            }
        }
        if try_header_rle || try_data_rle {
            if let Some(h) = self.header.last_mut() {
                if h.kind == kind
                    && h.flags & MUST_MATCH_HEADER_FLAGS
                        == flags & MUST_MATCH_HEADER_FLAGS
                {
                    header_rle = true;
                    if try_data_rle {
                        data_rle = unsafe {
                            data == *(self
                                .data
                                .as_ptr_range()
                                .end
                                .sub(std::mem::size_of::<T>())
                                as *const T)
                        };
                    }
                }
            }
        }
        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, data_rle,
            );
        }
        if !data_rle {
            let data = ManuallyDrop::new(data);
            self.data.extend_from_slice(unsafe { as_u8_slice(&data) });
        }
    }
    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags = DELETED;
        debug_assert!(kind.is_zst());
        self.field_count += run_length;
        let fmt = FieldValueFormat {
            kind,
            flags: flags | SHARED_VALUE,
            size: 0,
        };
        let mut header_rle = false;
        if try_header_rle {
            if let Some(h) = self.header.last_mut() {
                header_rle = h.kind == kind
                    && h.flags & MUST_MATCH_HEADER_FLAGS
                        == flags & MUST_MATCH_HEADER_FLAGS;
            }
        }
        // when we have header_rle, that implies data_rle here, because
        // the type has no value
        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, header_rle,
            );
        }
    }

    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
    ) -> *mut u8 {
        self.field_count += run_length;
        debug_assert!(data_len <= INLINE_STR_MAX_LEN);
        let size = data_len as FieldValueSize;
        unsafe {
            self.push_header_raw_same_value_after_first(
                FieldValueFormat { kind, flags, size },
                run_length,
            );
        }
        self.data.reserve(data_len);
        let res = self.data.get_head_ptr_mut();
        unsafe { self.data.set_len(self.data.len() + data_len) };
        res
    }
}

pub trait PushInterface: RawPushInterface {
    fn push_inline_bytes(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_variable_sized_type(
                FieldValueKind::BytesInline,
                field_value_flags::DEFAULT,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_inline_str(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_variable_sized_type(
                FieldValueKind::BytesInline,
                field_value_flags::BYTES_ARE_UTF8,
                data.as_bytes(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_string(
        &mut self,
        data: String,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::BytesBuffer,
                field_value_flags::BYTES_ARE_UTF8,
                data.into_bytes(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_bytes_buffer(
        &mut self,
        data: Vec<u8>,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::BytesBuffer,
                field_value_flags::DEFAULT,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_str_as_buffer(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::BytesBuffer,
                field_value_flags::BYTES_ARE_UTF8,
                data.as_bytes().to_vec(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_bytes_as_buffer(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::BytesBuffer,
                field_value_flags::DEFAULT,
                data.to_vec(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_str(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if data.len() <= INLINE_STR_MAX_LEN {
            self.push_inline_str(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        } else {
            self.push_str_as_buffer(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_bytes(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if data.len() <= INLINE_STR_MAX_LEN {
            self.push_inline_bytes(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        } else {
            self.push_bytes_as_buffer(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_int(
        &mut self,
        data: i64,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::Integer,
                field_value_flags::DEFAULT,
                data.to_owned(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_stream_value_id(
        &mut self,
        id: StreamValueId,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::StreamValueId,
                field_value_flags::DEFAULT,
                id,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_error(
        &mut self,
        err: OperatorApplicationError,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::Error,
                field_value_flags::DEFAULT,
                err,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_reference(
        &mut self,
        reference: FieldReference,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::Reference,
                field_value_flags::DEFAULT,
                reference,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_null(&mut self, run_length: usize, try_header_rle: bool) {
        unsafe {
            self.push_zst_unchecked(
                FieldValueKind::Null,
                field_value_flags::DEFAULT,
                run_length,
                try_header_rle,
            );
        }
    }
    fn push_success(&mut self, run_length: usize, try_header_rle: bool) {
        unsafe {
            self.push_zst_unchecked(
                FieldValueKind::Success,
                field_value_flags::DEFAULT,
                run_length,
                try_header_rle,
            );
        }
    }
    fn push_zst(
        &mut self,
        kind: FieldValueKind,
        run_length: usize,
        try_header_rle: bool,
    ) {
        assert!(kind.is_zst());
        unsafe {
            self.push_zst_unchecked(
                kind,
                field_value_flags::DEFAULT,
                run_length,
                try_header_rle,
            );
        }
    }
}

impl<T: RawPushInterface> PushInterface for T {}

#[cfg(test)]
mod test {
    use crate::record_data::field_data::FieldData;

    use super::PushInterface;

    #[test]
    fn no_header_rle_for_distinct_shared_values() {
        let mut fd = FieldData::default();
        fd.push_int(1, 2, true, false);
        fd.push_int(2, 2, true, false);
        assert!(fd.header.len() == 2);
    }
}

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
            let mut padding = 0;
            if fmt.kind.needs_alignment() {
                padding = self.fd.pad_to_align();
            }
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
    }
    unsafe fn drop_and_reserve(
        &mut self,
        element_size: usize,
        max_inserts: usize,
    ) {
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + max_inserts * element_size);
        self.data_ptr = self.fd.data.get_head_ptr_mut();
        self.max = max_inserts;
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
            std::ptr::copy_nonoverlapping(
                &v as *const T,
                self.data_ptr as *mut T,
                1,
            );
            self.data_ptr = self.data_ptr.add(std::mem::size_of::<T>());
        }
        std::mem::forget(v);
        self.count += 1;
    }
}

pub unsafe trait FixedSizeTypeInserter<'a>: Sized {
    const KIND: FieldValueKind;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    type ValueType;
    fn get_raw(&mut self) -> &mut RawFixedSizedTypeInserter<'a>;
    fn new(fd: &'a mut FieldData) -> Self;
    fn element_size() -> usize {
        std::mem::size_of::<Self::ValueType>()
    }
    fn element_format() -> FieldValueFormat {
        FieldValueFormat {
            kind: Self::KIND,
            flags: Self::FLAGS,
            size: Self::element_size() as FieldValueSize,
        }
    }
    fn commit(&mut self) {
        unsafe {
            self.get_raw().commit(Self::element_format());
        }
    }
    fn drop_and_reserve(&mut self, new_max_inserts: usize) {
        unsafe {
            self.get_raw()
                .drop_and_reserve(Self::element_size(), new_max_inserts);
        }
    }
    fn commit_and_reserve(&mut self, new_max_inserts: usize) {
        unsafe {
            self.get_raw()
                .commit_and_reserve(Self::element_format(), new_max_inserts);
        }
    }
    #[inline(always)]
    fn push<T>(&mut self, v: T) {
        assert!(self.get_raw().count < self.get_raw().max);
        unsafe {
            self.get_raw().push(v);
        }
    }
    #[inline(always)]
    fn push_with_rl<T: PartialEq + Clone>(&mut self, v: T, rl: usize) {
        if rl == 1 {
            self.push(v);
            return;
        }
        let max_rem = self.get_raw().max - self.get_raw().count;
        self.commit();
        unsafe {
            self.get_raw().fd.push_fixed_size_type(
                Self::KIND,
                Self::FLAGS,
                v,
                rl,
                true,
                false,
            );
        }
        self.drop_and_reserve(max_rem);
    }
}

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
    }
    unsafe fn drop_and_reserve(
        &mut self,
        max_inserts: usize,
        new_expected_size: usize,
    ) {
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + max_inserts * new_expected_size);
        self.data_ptr = self.fd.data.get_head_ptr_mut();
        self.max = max_inserts;
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
    const KIND: FieldValueKind;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    type ElementType: ?Sized;
    fn new(fd: &'a mut FieldData) -> Self;
    fn get_raw(&mut self) -> &mut RawVariableSizedTypeInserter<'a>;
    fn element_as_bytes(v: &Self::ElementType) -> &[u8];
    fn current_element_format(&mut self) -> FieldValueFormat {
        FieldValueFormat {
            kind: Self::KIND,
            flags: Self::FLAGS,
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
            self.get_raw().fd.push_variable_sized_type(
                Self::KIND,
                Self::FLAGS,
                v,
                rl,
                true,
                false,
            );
        }
        self.drop_and_reserve(max_rem, v.len());
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
        kind: FieldValueKind,
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
    const KIND: FieldValueKind;
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

pub struct IntegerInserter<'a> {
    raw: RawFixedSizedTypeInserter<'a>,
}
unsafe impl<'a> FixedSizeTypeInserter<'a> for IntegerInserter<'a> {
    const KIND: FieldValueKind = FieldValueKind::Integer;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    type ValueType = i64;
    fn get_raw(&mut self) -> &mut RawFixedSizedTypeInserter<'a> {
        &mut self.raw
    }
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            raw: RawFixedSizedTypeInserter::new(fd),
        }
    }
}
impl<'a> Drop for IntegerInserter<'a> {
    fn drop(&mut self) {
        self.commit();
    }
}

pub struct FieldReferenceInserter<'a> {
    raw: RawFixedSizedTypeInserter<'a>,
}
unsafe impl<'a> FixedSizeTypeInserter<'a> for FieldReferenceInserter<'a> {
    const KIND: FieldValueKind = FieldValueKind::Reference;
    type ValueType = FieldReference;

    fn get_raw(&mut self) -> &mut RawFixedSizedTypeInserter<'a> {
        &mut self.raw
    }
    fn new(fd: &'a mut FieldData) -> Self {
        Self {
            raw: RawFixedSizedTypeInserter::new(fd),
        }
    }
}
impl<'a> Drop for FieldReferenceInserter<'a> {
    fn drop(&mut self) {
        self.commit();
    }
}

pub struct InlineStringInserter<'a> {
    raw: RawVariableSizedTypeInserter<'a>,
}
unsafe impl<'a> VariableSizeTypeInserter<'a> for InlineStringInserter<'a> {
    const KIND: FieldValueKind = FieldValueKind::BytesInline;
    const FLAGS: FieldValueFlags = field_value_flags::BYTES_ARE_UTF8;
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
    const KIND: FieldValueKind = FieldValueKind::BytesInline;
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

pub struct NullsInserter<'a> {
    raw: RawZeroSizedTypeInserter<'a>,
}
unsafe impl<'a> ZeroSizedTypeInserter<'a> for NullsInserter<'a> {
    const KIND: FieldValueKind = FieldValueKind::Null;
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

pub struct VaryingTypeInserter<'a> {
    fd: &'a mut FieldData,
    count: usize,
    max: usize,
    data_ptr: *mut u8,
    fmt: FieldValueFormat,
    re_reserve_count: RunLength,
}

impl<'a> VaryingTypeInserter<'a> {
    pub fn new(fd: &'a mut FieldData, re_reserve_count: RunLength) -> Self {
        Self {
            fd,
            count: 0,
            max: 0,
            data_ptr: std::ptr::null_mut(),
            fmt: Default::default(),
            re_reserve_count,
        }
    }
    pub unsafe fn drop_and_reserve_unchecked(
        &mut self,
        reserved_elements: usize,
        fmt: FieldValueFormat,
    ) {
        self.max = reserved_elements;
        self.count = 0;
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + reserved_elements * fmt.size as usize);
        self.data_ptr = self.fd.data.get_head_ptr_mut();
        self.fmt = fmt;
    }
    pub fn drop_and_reserve(
        &mut self,
        reserved_elements: usize,
        kind: FieldValueKind,
        dynamic_size: usize,
        bytes_are_utf8: bool,
    ) {
        let mut fmt = FieldValueFormat {
            kind,
            ..Default::default()
        };
        fmt.size = if kind.is_variable_sized_type() {
            assert!(dynamic_size < INLINE_STR_MAX_LEN);
            dynamic_size
        } else {
            kind.size()
        } as FieldValueSize;
        if bytes_are_utf8 {
            assert!([
                FieldValueKind::BytesInline,
                FieldValueKind::BytesBuffer,
                FieldValueKind::BytesFile
            ]
            .contains(&kind));
            fmt.flags = field_value_flags::BYTES_ARE_UTF8;
        }
        unsafe {
            self.drop_and_reserve_unchecked(reserved_elements, fmt);
        }
    }
    pub fn commit(&mut self) {
        self.max = 0;
        if self.count == 0 {
            return;
        }
        if self.fmt.kind.is_zst() {
            self.fd.push_zst(self.fmt.kind, self.count, true);
            self.count = 0;
            return;
        }
        let new_len = self.fd.data.len() + self.fmt.size as usize * self.count;
        unsafe {
            self.fd.data.set_len(new_len);
            self.fd.add_header_for_multiple_values(
                self.fmt,
                self.count,
                self.fmt.flags,
            );
        }
        self.count = 0;
    }
    pub fn push_zst(&mut self, kind: FieldValueKind, count: usize) {
        assert!(kind.is_zst());
        if self.fmt.kind != kind {
            let len_rem = self.max - self.count;
            self.commit();
            self.max = len_rem;
            self.fmt = FieldValueFormat {
                kind,
                ..Default::default()
            };
        }
        if self.count + count > self.max {
            assert!(
                self.re_reserve_count as usize
                    >= self.count + count - self.max
            );
            self.max += self.re_reserve_count as usize;
        }
        self.count += count;
    }
    pub unsafe fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        bytes_are_utf8: bool,
        bytes: &[u8],
        rl: usize,
    ) {
        assert!(bytes.len() <= INLINE_STR_MAX_LEN);
        let fmt = FieldValueFormat {
            kind,
            flags: if bytes_are_utf8 {
                field_value_flags::BYTES_ARE_UTF8
            } else {
                field_value_flags::DEFAULT
            },
            size: bytes.len() as FieldValueSize,
        };
        if rl > 1 || self.fmt != fmt {
            let reserved_left = self.max - self.count;
            self.commit();
            if rl > 1 {
                unsafe {
                    self.fd.push_variable_sized_type(
                        kind, fmt.flags, bytes, rl, true, false,
                    );
                }
                self.max = reserved_left;
                self.fmt.kind = FieldValueKind::Null;
                return;
            }
            // TODO: maybe set some limit on the reserved len here?
            unsafe {
                self.drop_and_reserve_unchecked(reserved_left, fmt);
            }
        } else if self.count == self.max {
            self.commit();
            unsafe {
                self.drop_and_reserve_unchecked(
                    self.re_reserve_count as usize,
                    fmt,
                );
            }
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.data_ptr,
                bytes.len(),
            );
            self.data_ptr = self.data_ptr.add(bytes.len());
        }
        self.count += 1;
    }
    pub unsafe fn push_fixed_sized_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        v: T,
        rl: usize,
    ) {
        let size = std::mem::size_of::<T>();
        let fmt = FieldValueFormat {
            kind,
            flags: field_value_flags::DEFAULT,
            size: size as FieldValueSize,
        };
        if rl > 1 || self.fmt != fmt {
            let reserved_left = self.max - self.count;
            self.commit();
            if rl > 1 {
                unsafe {
                    self.fd.push_fixed_size_type(
                        kind,
                        field_value_flags::DEFAULT,
                        v,
                        rl,
                        true,
                        false,
                    );
                }
                self.max = reserved_left;
                self.fmt.kind = FieldValueKind::Null;
                return;
            }
            unsafe {
                self.drop_and_reserve_unchecked(reserved_left, fmt);
            }
        } else if self.count == self.max {
            self.commit();
            unsafe {
                self.drop_and_reserve_unchecked(
                    self.re_reserve_count as usize,
                    fmt,
                );
            }
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                &v as *const T,
                self.data_ptr as *mut T,
                1,
            );
            self.data_ptr = self.data_ptr.add(size);
        }
        std::mem::forget(v);
        self.count += 1;
    }
}

impl<'a> VaryingTypeInserter<'a> {
    pub fn push_inline_str(&mut self, v: &str, rl: usize) {
        unsafe {
            self.push_variable_sized_type(
                FieldValueKind::BytesInline,
                true,
                v.as_bytes(),
                rl,
            )
        }
    }
    pub fn push_inline_bytes(&mut self, v: &[u8], rl: usize) {
        unsafe {
            self.push_variable_sized_type(
                FieldValueKind::BytesInline,
                false,
                v,
                rl,
            )
        }
    }
    pub fn push_int(&mut self, v: i64, rl: usize) {
        unsafe { self.push_fixed_sized_type(FieldValueKind::Integer, v, rl) }
    }
    pub fn push_field_reference(&mut self, v: FieldReference, rl: usize) {
        unsafe { self.push_fixed_sized_type(FieldValueKind::Reference, v, rl) }
    }
    pub fn push_null(&mut self, rl: usize) {
        self.push_zst(FieldValueKind::Null, rl)
    }
}

impl<'a> Drop for VaryingTypeInserter<'a> {
    fn drop(&mut self) {
        self.commit()
    }
}
