use std::{marker::PhantomData, mem::ManuallyDrop, ops::DerefMut};

use super::{
    custom_data::CustomDataBox,
    field_data::{
        field_value_flags, FieldData, FieldValueFlags, FieldValueFormat,
        FieldValueHeader, FieldValueRepr, FieldValueSize, FieldValueType,
        RunLength, MAX_FIELD_ALIGN,
    },
    field_value::{FieldValue, SlicedFieldReference},
    stream_value::StreamValueId,
};
use crate::{
    operators::errors::OperatorApplicationError,
    record_data::field_data::{
        field_value_flags::{BYTES_ARE_UTF8, DELETED, SHARED_VALUE},
        INLINE_STR_MAX_LEN,
    },
    utils::as_u8_slice,
};

pub unsafe trait PushInterface {
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8;
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    );
    unsafe fn push_fixed_size_type_unchecked<
        T: PartialEq + Clone + FieldValueType,
    >(
        &mut self,
        repr: FieldValueRepr,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    );
    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    );
    fn push_inline_bytes(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(data.len() < INLINE_STR_MAX_LEN);
        unsafe {
            self.push_variable_sized_type_unchecked(
                FieldValueRepr::BytesInline,
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
        assert!(data.len() < INLINE_STR_MAX_LEN);
        unsafe {
            self.push_variable_sized_type_unchecked(
                FieldValueRepr::BytesInline,
                field_value_flags::BYTES_ARE_UTF8,
                data.as_bytes(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_fixed_size_type<T: PartialEq + Clone + FieldValueType>(
        &mut self,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(!T::DST && !T::ZST);
        unsafe {
            self.push_fixed_size_type_unchecked(
                T::REPR,
                field_value_flags::DEFAULT,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_zst(
        &mut self,
        kind: FieldValueRepr,
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

    // string / bytes convenience wrappers
    fn push_str_as_buffer(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type_unchecked(
                FieldValueRepr::BytesBuffer,
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
            self.push_fixed_size_type_unchecked(
                FieldValueRepr::BytesBuffer,
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

    // fixed sized types / ZST convenience wrappers
    fn push_string(
        &mut self,
        data: String,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type_unchecked(
                FieldValueRepr::BytesBuffer,
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
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }

    fn push_custom(
        &mut self,
        data: CustomDataBox,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_int(
        &mut self,
        data: i64,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_stream_value_id(
        &mut self,
        id: StreamValueId,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            id,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_error(
        &mut self,
        err: OperatorApplicationError,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            err,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_reference(
        &mut self,
        reference: SlicedFieldReference,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            reference,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_null(&mut self, run_length: usize, try_header_rle: bool) {
        self.push_zst(FieldValueRepr::Null, run_length, try_header_rle);
    }
    fn push_undefined(&mut self, run_length: usize, try_header_rle: bool) {
        self.push_zst(FieldValueRepr::Undefined, run_length, try_header_rle);
    }
    fn push_field_value_clone(
        &mut self,
        v: &FieldValue,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        match v {
            FieldValue::Null => self.push_null(run_length, try_header_rle),
            FieldValue::Undefined => {
                self.push_undefined(run_length, try_header_rle)
            }
            FieldValue::Int(v) => {
                self.push_int(*v, run_length, try_header_rle, try_data_rle)
            }
            FieldValue::BigInt(v) => self.push_fixed_size_type(
                v.clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Float(v) => self.push_fixed_size_type(
                *v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Rational(v) => self.push_fixed_size_type(
                (**v).clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Text(v) => {
                self.push_str(v, run_length, try_header_rle, try_data_rle)
            }
            FieldValue::Bytes(v) => {
                self.push_bytes(v, run_length, try_header_rle, try_data_rle)
            }
            FieldValue::Array(v) => self.push_fixed_size_type(
                v.clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Object(v) => self.push_fixed_size_type(
                v.clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Custom(v) => self.push_fixed_size_type(
                v.clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Error(v) => self.push_fixed_size_type(
                v.clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::FieldReference(v) => self.push_fixed_size_type(
                *v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::SlicedFieldReference(v) => self.push_fixed_size_type(
                *v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
        }
    }
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
            self.headers.push(FieldValueHeader {
                fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
        }
        self.headers.push(FieldValueHeader {
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
        self.headers.push(FieldValueHeader {
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
        let last_header =
            unsafe { self.headers.last_mut().unwrap_unchecked() };
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
        self.headers.push(FieldValueHeader {
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
        self.headers.push(FieldValueHeader {
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
        match self.headers.last_mut() {
            None => (),
            Some(last_header) => {
                if last_header.repr == fmt.repr
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
        let last_header = self.headers.last_mut().unwrap();
        // command buffer should clear data after last non deleted
        debug_assert!(!last_header.deleted());
        self.field_count += run_length;
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
        self.field_count -= run_length;
        loop {
            if run_length == 0 {
                return;
            }
            let last_header = self.headers.last_mut().unwrap();
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
            self.headers.pop();
        }
    }
}

unsafe impl PushInterface for FieldData {
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        repr: FieldValueRepr,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        debug_assert!(repr.is_variable_sized_type());
        debug_assert!(data_len <= INLINE_STR_MAX_LEN);
        self.field_count += run_length;
        let fmt = FieldValueFormat {
            repr,
            flags: flags | SHARED_VALUE,
            size: data_len as FieldValueSize,
        };
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags =
            BYTES_ARE_UTF8 | DELETED;

        let mut header_rle = false;

        if try_header_rle {
            if let Some(h) = self.headers.last_mut() {
                if h.repr == repr
                    && h.size == fmt.size
                    && h.flags & MUST_MATCH_HEADER_FLAGS
                        == flags & MUST_MATCH_HEADER_FLAGS
                {
                    header_rle = true;
                }
            }
        }

        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, false,
            );
        }
        self.data.reserve(data_len);
        let res = self.data.get_head_ptr_mut();
        unsafe { self.data.set_len(self.data.len() + data_len) };
        res
    }
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: FieldValueRepr,
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
            if let Some(h) = self.headers.last_mut() {
                if h.repr == kind
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
            repr: kind,
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
    unsafe fn push_fixed_size_type_unchecked<
        T: PartialEq + Clone + FieldValueType,
    >(
        &mut self,
        repr: FieldValueRepr,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(repr == T::REPR);
        debug_assert!(repr.is_fixed_size_type());
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags = DELETED;
        self.field_count += run_length;
        let mut data_rle = false;
        let mut header_rle = false;
        let fmt = FieldValueFormat {
            repr,
            flags: flags | SHARED_VALUE,
            size: std::mem::size_of::<T>() as FieldValueSize,
        };
        if repr.needs_alignment() {
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
            if let Some(h) = self.headers.last_mut() {
                if h.repr == repr
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
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags = DELETED;
        debug_assert!(kind.is_zst());
        self.field_count += run_length;
        let fmt = FieldValueFormat {
            repr: kind,
            flags: flags | SHARED_VALUE,
            size: 0,
        };
        let mut header_rle = false;
        if try_header_rle {
            if let Some(h) = self.headers.last_mut() {
                header_rle = h.repr == kind
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
            if fmt.repr.needs_alignment() {
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

pub struct FixedSizeTypeInserter<'a, T: FieldValueType + PartialEq + Clone> {
    raw: RawFixedSizedTypeInserter<'a>,
    _phantom: PhantomData<T>,
}

impl<'a, T: FieldValueType + PartialEq + Clone> FixedSizeTypeInserter<'a, T> {
    pub fn new(fd: &'a mut FieldData) -> Self {
        assert!(T::ZST == false && T::DST == false);
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
        assert!(self.raw.count < self.raw.max);
        unsafe {
            self.raw.push(v);
        }
    }
    #[inline(always)]
    pub fn push_with_rl(&mut self, v: T, rl: usize) {
        if rl == 1 {
            self.push(v);
            return;
        }
        let max_rem = self.raw.max - self.raw.count;
        self.commit();
        unsafe {
            self.raw.fd.push_fixed_size_type_unchecked(
                T::REPR,
                field_value_flags::DEFAULT,
                v,
                rl,
                true,
                false,
            );
        }
        self.drop_and_reserve(max_rem);
    }
}

impl<'a, T: FieldValueType + PartialEq + Clone> Drop
    for FixedSizeTypeInserter<'a, T>
{
    fn drop(&mut self) {
        self.commit()
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
        self.fd.field_count += self.count;
        self.count = 0;
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
    const KIND: FieldValueRepr;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    type ElementType: ?Sized;
    fn new(fd: &'a mut FieldData) -> Self;
    fn get_raw(&mut self) -> &mut RawVariableSizedTypeInserter<'a>;
    fn element_as_bytes(v: &Self::ElementType) -> &[u8];
    fn current_element_format(&mut self) -> FieldValueFormat {
        FieldValueFormat {
            repr: Self::KIND,
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
            self.get_raw().fd.push_variable_sized_type_unchecked(
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

pub struct InlineStringInserter<'a> {
    raw: RawVariableSizedTypeInserter<'a>,
}
unsafe impl<'a> VariableSizeTypeInserter<'a> for InlineStringInserter<'a> {
    const KIND: FieldValueRepr = FieldValueRepr::BytesInline;
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
        self.max = reserved_elements;
        self.count = 0;
        self.fd
            .data
            .reserve(MAX_FIELD_ALIGN + reserved_elements * fmt.size as usize);
        self.data_ptr = self.fd.data.get_head_ptr_mut();
        self.fmt = fmt;
    }
    fn sanitize_format(fmt: FieldValueFormat) {
        if fmt.bytes_are_utf8() {
            assert!([
                FieldValueRepr::BytesInline,
                FieldValueRepr::BytesBuffer,
                FieldValueRepr::BytesFile
            ]
            .contains(&fmt.repr));
        }
        assert!(fmt.flags & !field_value_flags::CONTENT_FLAGS == 0);
        if fmt.repr.is_variable_sized_type() {
            assert!(fmt.size <= INLINE_STR_MAX_LEN as FieldValueSize);
        }
    }
    pub unsafe fn drop_and_reserve_reasonable_unchecked(
        &mut self,
        fmt: FieldValueFormat,
    ) {
        let size = self
            .fd
            .data
            .len()
            .max(std::mem::size_of::<FieldValue>() * 4);
        let len = (size / fmt.size as usize).max(2);
        self.fd.data.reserve(fmt.size as usize * len);
        self.data_ptr = self.fd.data.get_head_ptr_mut();
        self.max = len;
        self.fmt = fmt;
        self.count = 0;
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
                field_value_flags::CONTENT_FLAGS,
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
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        let fmt = FieldValueFormat {
            repr: kind,
            flags,
            size: data_len as FieldValueSize,
        };
        if run_length > 1 || self.fmt != fmt || !try_header_rle {
            self.commit();
            if run_length > 1 {
                let res = unsafe {
                    self.fd.push_variable_sized_type_uninit(
                        kind,
                        fmt.flags,
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
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            if try_data_rle {
                self.commit();
                self.fd.push_variable_sized_type_unchecked(
                    repr,
                    flags,
                    data,
                    run_length,
                    try_header_rle,
                    try_data_rle,
                );
                self.drop_and_reserve_reasonable_unchecked(FieldValueFormat {
                    repr,
                    flags,
                    size: data.len() as FieldValueSize,
                });
                return;
            }
            let data_ptr = self.push_variable_sized_type_uninit(
                repr,
                flags,
                data.len(),
                run_length,
                try_header_rle,
            );
            std::ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());
        }
    }
    unsafe fn push_fixed_size_type_unchecked<
        T: PartialEq + Clone + FieldValueType,
    >(
        &mut self,
        repr: FieldValueRepr,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        let fmt = FieldValueFormat {
            repr,
            flags,
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
            std::ptr::write(self.data_ptr as *mut T, data);
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
}

impl<FD: DerefMut<Target = FieldData>> Drop for VaryingTypeInserter<FD> {
    fn drop(&mut self) {
        self.commit()
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::{
        field_data::FieldData, push_interface::PushInterface,
    };

    #[test]
    fn no_header_rle_for_distinct_shared_values() {
        let mut fd = FieldData::default();
        fd.push_int(1, 2, true, false);
        fd.push_int(2, 2, true, false);
        assert!(fd.headers.len() == 2);
    }
}
