use std::mem::ManuallyDrop;

use crate::{
    field_data::{
        field_value_flags::{self, BYTES_ARE_UTF8, SHARED_VALUE},
        INLINE_STR_MAX_LEN,
    },
    operators::errors::OperatorApplicationError,
    stream_value::StreamValueId,
};

use super::{
    as_u8_slice, iter_hall::IterHall, FieldData, FieldReference,
    FieldValueFlags, FieldValueFormat, FieldValueHeader, FieldValueKind,
    FieldValueSize, RunLength,
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
    unsafe fn push_fixed_size_type<T: PartialEq + Clone + Unpin>(
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
                fmt: fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
        }
        self.header.push(FieldValueHeader {
            fmt: fmt,
            run_length: run_length as RunLength,
        });
    }
    #[inline(always)]
    pub unsafe fn push_header_raw_same_value_after_first(
        &mut self,
        mut fmt: FieldValueFormat,
        run_length: usize,
    ) {
        debug_assert!(run_length > 0);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.header.push(FieldValueHeader {
            fmt: fmt,
            run_length: rl_to_push as RunLength,
        });
        if rl_to_push != run_length {
            fmt.set_same_value_as_previous(true);
            self.push_header_raw(fmt, run_length - rl_to_push);
        }
    }
    #[inline(always)]
    pub unsafe fn add_header_for_single_value(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        header_rle: bool,
        data_rle: bool,
    ) {
        debug_assert!(fmt.shared_value());
        if !header_rle && !data_rle {
            self.push_header_raw_same_value_after_first(fmt, run_length);
            return;
        }
        let last_header = self.header.last_mut().unwrap();
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
                self.push_header_raw(fmt, run_length);
                return;
            }
            self.push_header_raw_same_value_after_first(fmt, run_length);
            return;
        }
        if data_rle {
            // guaranteed to be at least two, otherwise we would have gone into
            // the shared value branch
            last_header.run_length -= 1;
            self.push_header_raw_same_value_after_first(fmt, run_length + 1);
            return;
        }
        if header_rle
            && last_header.run_length as usize + run_length
                < RunLength::MAX as usize
        {
            last_header.run_length += run_length as RunLength;
            return;
        }
        self.push_header_raw_same_value_after_first(fmt, run_length);
    }
    pub unsafe fn add_header_padded_for_single_value(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        padding: usize,
    ) {
        fmt.set_leading_padding(padding);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.header.push(FieldValueHeader {
            fmt: fmt,
            run_length: rl_to_push as RunLength,
        });
        if run_length == rl_to_push {
            return;
        }
        run_length -= rl_to_push;
        fmt.set_leading_padding(0);
        fmt.set_same_value_as_previous(true);
        self.push_header_raw(fmt, run_length);
    }
    pub fn dup_last_value(&mut self, run_length: usize) {
        if run_length == 0 {
            return;
        }
        let last_header = self.header.last_mut().unwrap();
        // command buffer should clear data after last non deleted
        debug_assert!(last_header.deleted() == false);
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

        let mut header_rle = false;
        let mut data_rle = false;

        if try_header_rle || try_data_rle {
            if let Some(h) = self.header.last_mut() {
                if h.kind == kind
                    && h.size == size
                    && (h.flags & BYTES_ARE_UTF8) == (flags & BYTES_ARE_UTF8)
                {
                    header_rle = true;
                    if try_data_rle {
                        let len = h.size as usize;
                        let prev_data = unsafe {
                            std::slice::from_raw_parts(
                                self.data.as_ptr_range().end.sub(len)
                                    as *const u8,
                                len,
                            )
                        };
                        data_rle = prev_data == data;
                    }
                }
            }
        }
        let fmt = FieldValueFormat {
            kind: kind,
            flags: flags | SHARED_VALUE,
            size,
        };
        self.add_header_for_single_value(
            fmt, run_length, header_rle, data_rle,
        );
        if !data_rle {
            self.data.extend_from_slice(data);
        }
    }
    unsafe fn push_fixed_size_type<T: PartialEq + Clone + Unpin>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        debug_assert!(kind.is_fixed_size_type());
        self.field_count += run_length;
        let mut data_rle = false;
        let mut header_rle = false;
        let fmt = FieldValueFormat {
            kind: kind,
            flags: flags | SHARED_VALUE,
            size: std::mem::size_of::<T>() as FieldValueSize,
        };
        if kind.needs_alignment() {
            let align = unsafe { self.pad_to_align() };
            if align != 0 {
                self.add_header_padded_for_single_value(
                    fmt, run_length, align,
                );
                if !data_rle {
                    let data = ManuallyDrop::new(data);
                    self.data.extend_from_slice(unsafe { as_u8_slice(&data) });
                }
                return;
            }
        }
        if try_header_rle || try_data_rle {
            if let Some(h) = self.header.last_mut() {
                if h.kind == kind {
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
        self.add_header_for_single_value(
            fmt, run_length, header_rle, data_rle,
        );
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
        debug_assert!(kind.is_zst());
        self.field_count += run_length;
        let fmt = FieldValueFormat {
            kind: kind,
            flags: flags | SHARED_VALUE,
            size: 0,
        };
        let mut header_rle = false;
        if try_header_rle {
            if let Some(h) = self.header.last_mut() {
                header_rle = h.kind == kind;
            }
        }
        // when we have header_rle, that implies data_rle here, because
        // the type has no value
        self.add_header_for_single_value(
            fmt, run_length, header_rle, header_rle,
        );
    }

    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
    ) -> *mut u8 {
        self.field_count += run_length as usize;
        debug_assert!(data_len <= INLINE_STR_MAX_LEN);
        let size = data_len as FieldValueSize;
        self.push_header_raw_same_value_after_first(
            FieldValueFormat { kind, flags, size },
            run_length,
        );
        self.data.reserve(data_len);
        let res = self.data.as_mut_ptr_range().end;
        // really unfortunate that rust considers it undefined to
        // call set_len before initializing, but for now we oblige...
        // fdi.data.set_len(self.data.len() + data_len)
        self.data.extend(std::iter::repeat(0).take(data_len));
        res
    }
}
unsafe impl RawPushInterface for IterHall {
    unsafe fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.fd.push_variable_sized_type(
            kind,
            flags,
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }

    unsafe fn push_fixed_size_type<T: PartialEq + Clone + Unpin>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.fd.push_fixed_size_type(
            kind,
            flags,
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }

    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        self.fd
            .push_zst_unchecked(kind, flags, run_length, try_header_rle);
    }
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data_len: usize,
        run_length: usize,
    ) -> *mut u8 {
        self.fd
            .push_variable_sized_type_uninit(kind, flags, data_len, run_length)
    }
}
impl IterHall {
    pub fn dup_last_value(&mut self, run_length: usize) {
        self.fd.dup_last_value(run_length);
    }
    pub fn drop_last_value(&mut self, run_length: usize) {
        self.fd.drop_last_value(run_length);
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
                FieldValueKind::Success,
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
    use crate::field_data::FieldData;

    use super::PushInterface;

    #[test]
    fn no_header_rle_for_distinct_shared_values() {
        let mut fd = FieldData::default();
        fd.push_int(1, 2, true, false);
        fd.push_int(2, 2, true, false);
        assert!(fd.header.len() == 2);
    }
}
