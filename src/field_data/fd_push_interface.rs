use std::mem::ManuallyDrop;

use crate::{
    field_data::{
        field_value_flags::{self, SHARED_VALUE},
        INLINE_STR_MAX_LEN,
    },
    operations::errors::OperatorApplicationError,
    stream_field_data::StreamValueId,
};

use super::{
    fd_iter_hall::FDIterHall, FieldData, FieldReference, FieldValueFlags, FieldValueFormat,
    FieldValueHeader, FieldValueKind, FieldValueSize, RunLength,
};

// SAFETY: this trait must be made private because it's functions have no way
// to check whether the supplied data matches the given FieldValueKind
// which makes them unsound for a public API.
// This feels preferrable in this case over making basically everything in this
// module unsafe, which would severely obscure the *really* unsafe things
mod private_impl {
    use crate::field_data::{FieldValueFlags, FieldValueKind};
    pub trait FDUnsafePushInterface {
        fn push_variable_sized_type(
            &mut self,
            kind: FieldValueKind,
            flags: FieldValueFlags,
            data: &[u8],
            run_length: usize,
            try_rle: bool,
        );
        fn push_fixed_size_type<T: PartialEq + Clone>(
            &mut self,
            kind: FieldValueKind,
            flags: FieldValueFlags,
            data: T,
            run_length: usize,
            try_rle: bool,
        );
        fn push_zst(
            &mut self,
            kind: FieldValueKind,
            flags: FieldValueFlags,
            run_length: usize,
            try_rle: bool,
        );
    }
}

impl private_impl::FDUnsafePushInterface for FieldData {
    fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_rle: bool,
    ) {
        let mut run_length = run_length;
        assert!(data.len() <= INLINE_STR_MAX_LEN);
        let mut fmt = FieldValueFormat {
            kind: kind,
            flags: flags | SHARED_VALUE,
            size: data.len() as FieldValueSize,
        };
        let mut data_shared = false;
        if try_rle {
            if let Some(h) = self.header.last_mut() {
                if h.fmt == fmt {
                    let len = h.size as usize;
                    let prev_data = unsafe {
                        std::slice::from_raw_parts(self.data.as_ptr().sub(len) as *const u8, len)
                    };
                    if prev_data == data {
                        let rem = (RunLength::MAX - h.run_length) as usize;
                        if rem > run_length {
                            h.run_length += run_length as RunLength;
                            return;
                        } else {
                            run_length -= rem;
                            h.run_length = RunLength::MAX;
                            fmt.set_same_value_as_previous(true);
                            data_shared = true;
                        }
                    }
                }
            }
        }
        while run_length > RunLength::MAX as usize {
            self.header.push(FieldValueHeader {
                fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
            fmt.set_same_value_as_previous(true);
        }
        self.header.push(FieldValueHeader {
            fmt,
            run_length: run_length as RunLength,
        });
        if !data_shared {
            self.data.extend_from_slice(data);
        }
    }
    fn push_fixed_size_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_rle: bool,
    ) {
        let mut run_length = run_length;
        let mut same_value = false;
        if let Some(h) = self.header.last_mut() {
            let mut try_amend_header = false;
            if kind.needs_alignment() && !h.kind.needs_alignment() {
                h.set_alignment_after(kind.align_size_up(self.data.len()) - self.data.len());
            } else if h.kind == kind {
                if try_rle {
                    let prev_data = unsafe {
                        &*(self
                            .data
                            .as_ptr()
                            .sub(h.size as usize + h.alignment_after())
                            as *const T)
                    };
                    if prev_data == &data {
                        same_value = true;
                    }
                    if !h.shared_value() {
                        h.run_length -= 1;
                        if h.run_length == 1 {
                            h.set_shared_value(true);
                        }
                        run_length += 1;
                    } else {
                        same_value = true;
                        try_amend_header = true;
                    }
                } else {
                    if !h.shared_value() {
                        try_amend_header = true;
                    } else if h.run_length == 1 {
                        try_amend_header = true;
                        h.set_shared_value(false);
                    }
                }
            }
            if try_amend_header {
                let space_rem = (RunLength::MAX - h.run_length) as usize;
                if space_rem > run_length {
                    h.run_length += run_length as RunLength;
                    if same_value {
                        return;
                    }
                } else {
                    run_length -= space_rem;
                    h.run_length = RunLength::MAX;
                }
            }
        }
        let mut fmt = FieldValueFormat {
            kind,
            flags: flags | SHARED_VALUE,
            size: std::mem::size_of::<T>() as FieldValueSize,
        };
        while run_length > RunLength::MAX as usize {
            self.header.push(FieldValueHeader {
                fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
            fmt.set_same_value_as_previous(true);
        }
        self.header.push(FieldValueHeader {
            fmt,
            run_length: run_length as RunLength,
        });
        if !same_value {
            if kind.needs_alignment() {
                self.pad_to_align();
            }
            let data = ManuallyDrop::new(data);
            self.data.extend_from_slice(unsafe {
                std::slice::from_raw_parts(
                    (&data as *const ManuallyDrop<T>) as *const u8,
                    std::mem::size_of::<T>(),
                )
            });
        }
    }
    fn push_zst(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        run_length: usize,
        try_rle: bool,
    ) {
        debug_assert!(kind == FieldValueKind::Null || kind == FieldValueKind::Unset);
        self.push_fixed_size_type(kind, flags, (), run_length, try_rle);
    }
}
impl private_impl::FDUnsafePushInterface for FDIterHall {
    fn push_variable_sized_type(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: &[u8],
        run_length: usize,
        try_rle: bool,
    ) {
        self.field_count += run_length;
        self.fd
            .push_variable_sized_type(kind, flags, data, run_length, try_rle);
    }

    fn push_fixed_size_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_rle: bool,
    ) {
        self.field_count += run_length;
        self.fd
            .push_fixed_size_type(kind, flags, data, run_length, try_rle);
    }

    fn push_zst(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        run_length: usize,
        try_rle: bool,
    ) {
        self.field_count += run_length;
        self.fd.push_zst(kind, flags, run_length, try_rle);
    }
}

pub trait FDPushInterface: private_impl::FDUnsafePushInterface {
    fn push_inline_bytes(&mut self, data: &[u8], run_length: usize, try_rle: bool) {
        self.push_variable_sized_type(
            FieldValueKind::BytesInline,
            field_value_flags::DEFAULT,
            data,
            run_length,
            try_rle,
        );
    }
    fn push_inline_str(&mut self, data: &str, run_length: usize, try_rle: bool) {
        self.push_variable_sized_type(
            FieldValueKind::BytesInline,
            field_value_flags::DEFAULT,
            data.as_bytes(),
            run_length,
            try_rle,
        );
    }

    fn push_str_buffer(&mut self, data: &str, run_length: usize, try_rle: bool) {
        self.push_fixed_size_type(
            FieldValueKind::BytesBuffer,
            field_value_flags::BYTES_ARE_UTF8,
            data.to_owned(),
            run_length,
            try_rle,
        );
    }
    fn push_str(&mut self, data: &str, run_length: usize, try_rle: bool) {
        if data.len() <= INLINE_STR_MAX_LEN {
            self.push_inline_str(data, run_length, try_rle);
        } else {
            self.push_str_buffer(data, run_length, try_rle);
        }
    }
    fn push_int(&mut self, data: i64, run_length: usize) {
        self.push_fixed_size_type(
            FieldValueKind::Integer,
            field_value_flags::DEFAULT,
            data.to_owned(),
            run_length,
            true,
        );
    }
    fn push_stream_value_id(&mut self, id: StreamValueId, run_length: usize) {
        self.push_fixed_size_type(
            FieldValueKind::StreamValueId,
            field_value_flags::DEFAULT,
            id,
            run_length,
            true,
        );
    }
    fn push_error(&mut self, err: OperatorApplicationError, run_length: usize) {
        self.push_fixed_size_type(
            FieldValueKind::Error,
            field_value_flags::DEFAULT,
            err,
            run_length,
            true,
        );
    }
    fn push_reference(&mut self, reference: FieldReference, run_length: usize) {
        self.push_fixed_size_type(
            FieldValueKind::Reference,
            field_value_flags::DEFAULT,
            reference,
            run_length,
            true,
        );
    }
    fn push_null(&mut self, run_length: usize) {
        self.push_zst(
            FieldValueKind::Null,
            field_value_flags::DEFAULT,
            run_length,
            true,
        );
    }
    fn push_unset(&mut self, run_length: usize) {
        self.push_zst(
            FieldValueKind::Unset,
            field_value_flags::DEFAULT,
            run_length,
            true,
        );
    }
}

impl<T: private_impl::FDUnsafePushInterface> FDPushInterface for T {}
