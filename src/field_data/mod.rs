// This module implements a run-length encoded, dynamically typed data structure (FieldData)
// the type information for each entry is stored in a separate vec from the main data
// the type information itself is run-length encoded even if the data for the two entries is different

// SAFETY: due to its nature, this datastructure requires a lot of unsafe code,
// some of which is very repetitive. So far, nobody could be bothered
// with annotating every little piece of it.

pub mod fd_iter;
pub mod fd_iter_hall;

use std::{
    collections::HashMap,
    iter,
    mem::{align_of, size_of, ManuallyDrop},
    ops::DerefMut,
    ptr, slice, u8,
};

use std::ops::Deref;

use crate::{
    field_data::{fd_iter::FDTypedValue, field_value_flags::TYPE_RELEVANT},
    operations::errors::OperatorApplicationError,
    stream_field_data::StreamValueId,
    utils::string_store::StringStoreEntry,
    worker_thread_session::FieldId,
};

use self::{
    fd_iter::{FDIter, FDIterMut, FDIterator, FDTypedSlice},
    field_value_flags::SHARED_VALUE,
};

//if the u32 overflows we just split into two values
pub type RunLength = u32;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum FieldValueKind {
    Unset,
    Null,
    EntryId,
    Integer, //TODO: bigint, float, decimal, ...
    StreamValueId,
    Reference,
    Error,
    Html,
    BytesInline,
    BytesBuffer,
    BytesFile,
    Object,
}

impl FieldValueKind {
    pub fn needs_drop(self) -> bool {
        use FieldValueKind::*;
        match self {
            Unset | Null | EntryId | Integer | Reference | StreamValueId => false,
            Error | Html | BytesInline | BytesBuffer | BytesFile | Object => true,
        }
    }
    pub fn needs_alignment(self) -> bool {
        use FieldValueKind::*;
        match self {
            Unset | Null | BytesInline => false,
            _ => true,
        }
    }
    pub fn needs_copy(self) -> bool {
        self.needs_drop()
    }
    #[inline(always)]
    pub fn align_size_up(self, size: usize) -> usize {
        if self.needs_alignment() {
            (size + FIELD_ALIGN_MASK) & FIELD_ALIGN_MASK
        } else {
            size
        }
    }
    #[inline(always)]
    pub fn align_size_down(self, size: usize) -> usize {
        if self.needs_alignment() {
            size & FIELD_ALIGN_MASK
        } else {
            size
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr_down(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            // doing this instead of the straight conversion to usize
            // to avoid loosing provenance
            ptr.sub(ptr as usize & MAX_FIELD_ALIGN)
        } else {
            ptr
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr_up(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            ptr.sub((ptr as usize + MAX_FIELD_ALIGN) & MAX_FIELD_ALIGN)
        } else {
            ptr
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)] //TODO
struct ObjectEntry {
    kind: FieldValueKind,
    data_offset: usize,
}
#[allow(dead_code)] //TODO
#[derive(Clone)]
pub struct Object {
    data: FieldData,
    table: HashMap<StringStoreEntry, ObjectEntry>,
}

#[derive(Copy, Clone, PartialEq)]
pub struct FieldReference {
    pub field: FieldId,
    pub begin: usize,
    pub end: usize,
}

#[derive(Clone)]
pub struct Html {
    //TODO
}

#[derive(Clone)]
pub struct BytesBufferFile {
    //TODO
}

//used to figure out the maximum alignment
#[repr(C)]
union FieldValueUnion {
    text: ManuallyDrop<String>,
    bytes: ManuallyDrop<Vec<u8>>,
    bytes_file: ManuallyDrop<BytesBufferFile>,
    object: ManuallyDrop<Object>,
}

pub const MAX_FIELD_ALIGN: usize = align_of::<FieldValueUnion>();
pub const FIELD_ALIGN_MASK: usize = !MAX_FIELD_ALIGN;

pub type FieldValueSize = u16;

pub mod field_value_flags {
    use super::MAX_FIELD_ALIGN;
    pub type FieldValueFlags = u8;
    //offset must be zero so we don't have to shift
    const_assert!(MAX_FIELD_ALIGN.is_power_of_two() && MAX_FIELD_ALIGN <= 16);
    pub const ALIGNMENT_AFTER: FieldValueFlags = 0xF; //consumes offsets 0 through 3
    pub const SHARED_VALUE_OFFSET: FieldValueFlags = 5;
    pub const BYTES_ARE_UTF8_OFFSET: FieldValueFlags = 6;
    pub const DELETED_OFFSET: FieldValueFlags = 7;

    pub const SHARED_VALUE: FieldValueFlags = 1 << SHARED_VALUE_OFFSET;
    pub const BYTES_ARE_UTF8: FieldValueFlags = 1 << BYTES_ARE_UTF8_OFFSET;
    pub const DELETED: FieldValueFlags = 1 << DELETED_OFFSET;

    pub const DEFAULT: FieldValueFlags = 0;

    pub const TYPE_RELEVANT: FieldValueFlags = BYTES_ARE_UTF8_OFFSET;
}
pub use field_value_flags::FieldValueFlags;

#[derive(Clone, Copy, PartialEq)]
pub struct FieldValueFormat {
    pub kind: FieldValueKind,
    pub flags: FieldValueFlags,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    pub size: FieldValueSize,
}

impl Default for FieldValueFormat {
    fn default() -> Self {
        Self {
            kind: FieldValueKind::Unset,
            flags: field_value_flags::DEFAULT,
            size: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct FieldValueHeader {
    pub fmt: FieldValueFormat,
    pub run_length: RunLength,
}

impl FieldValueFormat {
    pub fn shared_value(self) -> bool {
        self.flags & field_value_flags::SHARED_VALUE != 0
    }
    pub fn set_shared_value(&mut self, val: bool) {
        self.flags |= (val as FieldValueFlags) << field_value_flags::SHARED_VALUE_OFFSET;
    }
    pub fn bytes_are_utf8(self) -> bool {
        self.flags & field_value_flags::BYTES_ARE_UTF8 != 0
    }
    pub fn set_bytes_are_utf8(&mut self, val: bool) {
        self.flags |= (val as FieldValueFlags) << field_value_flags::BYTES_ARE_UTF8_OFFSET;
    }
    pub fn alignment_after(self) -> usize {
        (self.flags & field_value_flags::ALIGNMENT_AFTER) as usize
    }
    pub fn set_alignment_after(&mut self, val: usize) {
        debug_assert!(val & !(field_value_flags::ALIGNMENT_AFTER as usize) == 0);
        self.flags |= (val as u8) & field_value_flags::ALIGNMENT_AFTER;
    }
    pub fn deleted(self) -> bool {
        self.flags & field_value_flags::DELETED != 0
    }
    pub fn set_deleted(&mut self, val: bool) {
        self.flags |= (val as FieldValueFlags) << field_value_flags::DELETED_OFFSET;
    }
}

impl Deref for FieldValueHeader {
    type Target = FieldValueFormat;

    fn deref(&self) -> &Self::Target {
        &self.fmt
    }
}

impl DerefMut for FieldValueHeader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fmt
    }
}

impl FieldValueHeader {
    pub fn data_element_count(&self) -> usize {
        if self.shared_value() {
            1
        } else {
            self.run_length as usize
        }
    }
    pub fn data_size(&self) -> usize {
        if self.shared_value() {
            self.size as usize
        } else {
            self.size as usize * self.run_length as usize
        }
    }
    pub fn total_size(&self) -> usize {
        self.data_size() + self.alignment_after()
    }
}

#[derive(Default)]
pub struct FieldData {
    pub(in crate::field_data) data: Vec<u8>,
    pub(in crate::field_data) header: Vec<FieldValueHeader>,
}

impl Clone for FieldData {
    fn clone(&self) -> Self {
        let mut fd = Self {
            data: Vec::with_capacity(self.data.len()),
            header: self.header.clone(),
        };
        let fd_ref = &mut fd;
        self.copy_n(usize::MAX, move |f| f(fd_ref));
        fd
    }
}

pub const INLINE_STR_MAX_LEN: usize = 8192;

unsafe fn drop_slice<T>(slice: &[T]) {
    let droppable = slice::from_raw_parts_mut(slice.as_ptr() as *mut ManuallyDrop<T>, slice.len());
    for e in droppable.iter_mut() {
        ManuallyDrop::drop(e);
    }
}

impl FieldData {
    pub fn clear(&mut self) {
        let mut iter = self.iter();
        while let Some(range) = iter.typed_range_fwd(usize::MAX, 0) {
            unsafe {
                match range.data {
                    FDTypedSlice::Unset(_) => (),
                    FDTypedSlice::Null(_) => (),
                    FDTypedSlice::Integer(_) => (),
                    FDTypedSlice::BytesInline(_) => (),
                    FDTypedSlice::TextInline(_) => (),
                    FDTypedSlice::StreamValueId(s) => drop_slice(s),
                    FDTypedSlice::Reference(s) => drop_slice(s),
                    FDTypedSlice::Error(s) => drop_slice(s),
                    FDTypedSlice::Html(s) => drop_slice(s),
                    FDTypedSlice::Object(s) => drop_slice(s),
                }
            }
        }
        self.header.clear();
        self.data.clear();
    }
    fn adjust_deletion_header_bounds(
        &mut self,
        first_header_idx: usize,
        first_header_oversize: RunLength,
        last_header_idx: usize,
        last_header_oversize: RunLength,
    ) {
        let mut headers_to_insert = 0;
        if first_header_oversize > 0 {
            headers_to_insert += 1;
        }
        if last_header_oversize > 0 {
            headers_to_insert += 1;
        }
        if headers_to_insert == 0 {
            return;
        }
        if headers_to_insert == 1 {
            if first_header_oversize > 0 {
                let h = self.header[first_header_idx];
                self.header.insert(
                    first_header_idx + 1,
                    FieldValueHeader {
                        fmt: h.fmt,
                        run_length: h.run_length - first_header_oversize,
                    },
                );
                self.header[first_header_idx].run_length = first_header_oversize;
                self.header[first_header_idx].set_deleted(false);
                return;
            }
            let mut h = self.header[last_header_idx];
            h.set_deleted(false);
            self.header.insert(
                last_header_idx + 1,
                FieldValueHeader {
                    fmt: h.fmt,
                    run_length: last_header_oversize,
                },
            );
            self.header[last_header_idx].run_length -= last_header_oversize;
            return;
        }
        unsafe {
            self.header.reserve(2);
            let buf = self.header.as_mut_ptr();
            std::ptr::copy(
                buf.add(last_header_idx),
                buf.add(last_header_idx + 2),
                self.header.len() - last_header_idx,
            );
            std::ptr::copy(
                buf.add(first_header_idx),
                buf.add(first_header_idx + 1),
                last_header_idx - first_header_idx,
            );
            *buf.add(last_header_idx + 1) = *buf.add(last_header_idx + 2);
        }
        self.header[first_header_idx].set_deleted(false);
        self.header[first_header_idx].run_length = first_header_oversize;
        self.header[first_header_idx + 1].run_length -= first_header_oversize;

        self.header[last_header_idx + 1].run_length -= last_header_oversize;
        self.header[last_header_idx + 2].set_deleted(false);
        self.header[last_header_idx + 2].run_length = last_header_oversize;
    }
    pub fn delete_n_bwd<'a>(mut iter: FDIterMut<'a>, n: usize) -> FDIterMut<'a> {
        if n == 0 {
            return iter;
        }
        let mut drops_rem = n;
        let mut last = true;
        // we have to initialize these because rust can't know that we
        // eliminated the empty range through the check above
        let mut last_header_idx = 0;
        let mut first_header_oversize = 0;
        let mut last_header_oversize = 0;
        while let Some(range) = iter.typed_range_bwd(drops_rem, 0) {
            let header_idx = unsafe {
                (range.headers.last().unwrap_unchecked() as *const FieldValueHeader)
                    .offset_from(iter.fd.header.as_ptr()) as usize
            };
            if last {
                last_header_idx = header_idx;
                last_header_oversize = range.first_header_run_length_oversize;
                last = false;
            }
            drops_rem -= range.field_count;
            for i in 0..range.headers.len() {
                iter.fd.header[i].set_deleted(true);
            }
            first_header_oversize = range.first_header_run_length_oversize;
        }
        debug_assert!(drops_rem == 0);

        let first_header_idx = iter.get_next_header_index();
        iter.fd.adjust_deletion_header_bounds(
            first_header_idx,
            first_header_oversize,
            last_header_idx,
            last_header_oversize,
        );
        iter
    }
    pub fn dup_nth(&mut self, n: usize, mut added_run_len: RunLength) {
        let mut iter = FDIterMut::from_start(self);
        let skipped_fields = iter.next_n_fields(n);
        assert!(n == skipped_fields);
        let hi = iter.get_next_header_index();
        let h = &mut iter.fd.header[hi];

        if h.shared_value() {
            let free_rl = RunLength::MAX - h.run_length;
            if free_rl > added_run_len {
                h.run_length += free_rl;
                return;
            }
            h.run_length += free_rl;
            added_run_len -= free_rl;
            //TODO: we could check the next header here...
        } else if h.run_length == 1 && added_run_len != RunLength::MAX {
            h.run_length += added_run_len;
            h.set_shared_value(true);
            return;
        }
        let mut hn = *h;
        hn.run_length = added_run_len;
        iter.fd.header.insert(hi + 1, hn);
        let data_end = iter.data + hn.data_size();
        let data_end_padded = data_end + hn.alignment_after();
        let mut data_end_after_insert = data_end + hn.size as usize;
        let misalign_before = data_end_padded & FIELD_ALIGN_MASK;
        if (data_end_after_insert & FIELD_ALIGN_MASK) != misalign_before {
            data_end_after_insert =
                ((data_end_after_insert + FIELD_ALIGN_MASK) & FIELD_ALIGN_MASK) + misalign_before;
        }
        let target_loc;
        unsafe {
            let buf = iter.fd.data.as_mut_ptr();
            std::ptr::copy(
                buf.add(data_end_padded),
                buf.add(data_end_after_insert),
                iter.fd.data.len() - data_end_padded,
            );
            target_loc = buf.add(data_end);

            let fd = iter.get_next_typed_field();
            match fd.value {
                FDTypedValue::Unset(_) => (),
                FDTypedValue::Null(_) => (),
                FDTypedValue::Integer(v) => ptr::write(target_loc as *mut i64, v),
                FDTypedValue::StreamValueId(v) => ptr::write(target_loc as *mut usize, v),
                FDTypedValue::Reference(v) => ptr::write(target_loc as *mut FieldReference, *v),
                FDTypedValue::Error(v) => {
                    ptr::write(target_loc as *mut OperatorApplicationError, v.clone())
                }
                FDTypedValue::Html(v) => ptr::write(target_loc as *mut Html, v.clone()),
                FDTypedValue::Object(v) => ptr::write(target_loc as *mut Object, v.clone()),
                FDTypedValue::BytesInline(v) => {
                    ptr::copy(v.as_ptr(), target_loc as *mut u8, v.len())
                }
                FDTypedValue::TextInline(v) => {
                    ptr::copy(v.as_ptr(), target_loc as *mut u8, v.len())
                }
            }
        }
    }
    // can't make this pub as it can be used to break the type layout
    fn pad_to_align(&mut self) -> usize {
        let align = self.data.len() % MAX_FIELD_ALIGN;
        self.data.extend(iter::repeat(0).take(align));
        self.header.last_mut().map(|h| h.set_alignment_after(align));
        align
    }

    pub fn copy_n<'a, TargetApplicatorFn: FnMut(&dyn Fn(&mut FieldData))>(
        &self,
        n: usize,
        mut targets_applicator: TargetApplicatorFn,
    ) {
        let mut h;

        // search backwards to the first header that we need to copy
        let mut remaining_elements = n;
        let mut header_idx = self.header.len();
        debug_assert!(!self.header.is_empty());
        let headers_len = self.header.len();
        let data_size_total = self.data.len();
        let mut copy_data_size = 0;
        // this includes the align
        let mut copy_data_size_at_first_aligned_field = data_size_total;
        let mut first_aligned_field_idx = headers_len;
        let mut first_aligned_field_padding = 0;
        loop {
            header_idx -= 1;
            h = self.header[header_idx];
            if h.run_length as usize >= remaining_elements {
                h.run_length = remaining_elements as RunLength;
                break;
            }
            if header_idx == 0 {
                break;
            }
            remaining_elements -= h.run_length as usize;

            copy_data_size += h.data_size();
            if h.kind.needs_alignment() {
                copy_data_size_at_first_aligned_field = copy_data_size;
                first_aligned_field_idx = header_idx;
                first_aligned_field_padding = (data_size_total - copy_data_size) % MAX_FIELD_ALIGN;
                copy_data_size += first_aligned_field_padding;
            }
        }

        // copy the headers over to the targets

        targets_applicator(&|t: &mut FieldData| {
            if let Some(h_tgt) = t.header.last_mut() {
                if !h.shared_value() && !h_tgt.shared_value() && h_tgt.kind == h.kind {
                    if let Some(rl) = h_tgt.run_length.checked_add(h.run_length) {
                        h_tgt.run_length = rl;
                        if header_idx + 1 != self.header.len() {
                            t.header.extend(&self.header[header_idx + 1..]);
                        }
                        return;
                    }
                }
            }
            t.header.extend(&self.header[header_idx..]);
        });

        // copy over leading unaligned data
        let data_start = data_size_total - copy_data_size;
        let first_aligned_field_start = data_size_total - copy_data_size_at_first_aligned_field;

        if first_aligned_field_start != data_start {
            unsafe {
                append_data_memcpy(
                    self.data.as_ptr().add(data_start),
                    first_aligned_field_start - data_start - first_aligned_field_padding,
                    &mut targets_applicator,
                );
            }
            h = self.header[first_aligned_field_idx];
        }

        // if all memory was unaligned, we're done here
        if first_aligned_field_start == data_size_total {
            return;
        }

        // add padding so all targets are aligned
        targets_applicator(&|t: &mut FieldData| {
            t.pad_to_align();
        });

        // copy over the remaining data, using the fact that source and
        // all targets are aligned at this point
        let mut uncopied_data_start = first_aligned_field_start;
        let mut header_idx = first_aligned_field_idx;
        let mut data_pos = first_aligned_field_start;
        loop {
            let field_data_size = h.data_size();
            if h.kind.needs_copy() {
                unsafe {
                    let start = self.data.as_ptr().add(uncopied_data_start);
                    append_data_memcpy(
                        start,
                        data_pos - uncopied_data_start,
                        &mut targets_applicator,
                    );
                    append_data(h, self.data.as_ptr().add(data_pos), &mut targets_applicator)
                }
                data_pos += field_data_size;
                uncopied_data_start = data_pos;
            } else {
                data_pos += field_data_size;
            }
            header_idx += 1;
            if header_idx == headers_len {
                break;
            }
            h = self.header[header_idx];
            if h.kind.needs_alignment() {
                data_pos += data_pos % MAX_FIELD_ALIGN;
            }
        }
        unsafe {
            let start = self.data.as_ptr().add(uncopied_data_start);
            append_data_memcpy(
                start,
                data_pos - uncopied_data_start,
                &mut targets_applicator,
            );
        }
    }
    pub fn push_inline_str(&mut self, data: &str, run_length: usize, try_rle: bool) {
        let mut run_length = run_length;
        assert!(data.len() <= INLINE_STR_MAX_LEN);
        let fmt = FieldValueFormat {
            kind: FieldValueKind::BytesInline,
            flags: field_value_flags::BYTES_ARE_UTF8
                | if run_length > 1 { SHARED_VALUE } else { 0 },
            size: data.len() as FieldValueSize,
        };
        if try_rle {
            if let Some(h) = self.header.last_mut() {
                if h.fmt == fmt {
                    let len = h.size as usize;
                    let prev_data = unsafe {
                        std::str::from_utf8_unchecked(slice::from_raw_parts(
                            self.data.as_ptr().sub(len) as *const u8,
                            len,
                        ))
                    };
                    if prev_data == data {
                        let rem = (RunLength::MAX - h.run_length) as usize;
                        h.set_shared_value(true);
                        if rem > run_length {
                            h.run_length += run_length as RunLength;
                            return;
                        } else {
                            run_length -= rem;
                            h.run_length = RunLength::MAX;
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
            self.data.extend_from_slice(data.as_bytes());
        }
        self.header.push(FieldValueHeader {
            fmt,
            run_length: run_length as RunLength,
        });
        self.data.extend_from_slice(data.as_bytes());
    }
    unsafe fn push_fixed_size_type<T: PartialEq + Clone>(
        &mut self,
        kind: FieldValueKind,
        flags: FieldValueFlags,
        data: T,
        run_length: usize,
        try_rle: bool,
    ) {
        debug_assert!(flags & !TYPE_RELEVANT == 0);
        let mut run_length = run_length;
        if let Some(h) = self.header.last_mut() {
            let mut try_amend_header = false;
            let mut same_value = false;
            if kind.needs_alignment() && !h.kind.needs_alignment() {
                h.set_alignment_after(kind.align_size_up(self.data.len()) - self.data.len());
            } else if h.kind == kind && (h.flags & TYPE_RELEVANT) == flags {
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
                        if !h.shared_value() && h.run_length > 1 {
                            h.run_length -= 1;
                            run_length += 1;
                        } else {
                            h.set_shared_value(true);
                            try_amend_header = true;
                        }
                    }
                } else if !h.shared_value() {
                    try_amend_header = true;
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
        if kind.needs_alignment() {
            self.pad_to_align();
        }
        let fmt = FieldValueFormat {
            kind,
            flags,
            size: size_of::<T>() as FieldValueSize,
        };
        while run_length > RunLength::MAX as usize {
            self.header.push(FieldValueHeader {
                fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
            self.data
                .extend_from_slice(unsafe { as_u8_slice(&data.clone()) });
        }
        self.header.push(FieldValueHeader {
            fmt,
            run_length: run_length as RunLength,
        });
        let data = ManuallyDrop::new(data);
        self.data.extend_from_slice(unsafe { as_u8_slice(&data) });
    }
    pub fn push_str_buffer(&mut self, data: &str, run_length: usize, try_rle: bool) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::BytesBuffer,
                field_value_flags::BYTES_ARE_UTF8,
                data.to_owned(),
                run_length,
                try_rle,
            );
        }
    }
    pub fn push_str(&mut self, data: &str, run_length: usize, try_rle: bool) {
        if data.len() <= INLINE_STR_MAX_LEN {
            self.push_inline_str(data, run_length, try_rle);
        } else {
            self.push_str_buffer(data, run_length, try_rle);
        }
    }
    pub fn push_int(&mut self, data: i64, run_length: usize) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::Integer,
                field_value_flags::DEFAULT,
                data.to_owned(),
                run_length,
                true,
            );
        }
    }
    pub fn push_stream_value_id(&mut self, id: StreamValueId, run_length: usize) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::StreamValueId,
                field_value_flags::DEFAULT,
                id,
                run_length,
                true,
            );
        }
    }
    pub fn push_error(&mut self, err: OperatorApplicationError, run_length: usize) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::Error,
                field_value_flags::DEFAULT,
                err,
                run_length,
                true,
            );
        }
    }
    pub fn push_reference(&mut self, reference: FieldReference, run_length: usize) {
        unsafe {
            self.push_fixed_size_type(
                FieldValueKind::Reference,
                field_value_flags::DEFAULT,
                reference,
                run_length,
                true,
            );
        }
    }
    pub fn push_zst(&mut self, kind: FieldValueKind, run_length: usize) {
        assert!(kind == FieldValueKind::Null || kind == FieldValueKind::Unset);
        unsafe {
            self.push_fixed_size_type(kind, field_value_flags::DEFAULT, (), run_length, true);
        }
    }
    pub fn push_null(&mut self, run_length: usize) {
        self.push_zst(FieldValueKind::Null, run_length);
    }
    pub fn push_unset(&mut self, run_length: usize) {
        self.push_zst(FieldValueKind::Unset, run_length);
    }
    pub fn iter<'a>(&'a self) -> FDIter<'a> {
        FDIter::from_start(self)
    }
    pub fn iter_from_end<'a>(&'a self) -> FDIter<'a> {
        FDIter::from_end(self)
    }

    pub unsafe fn internals(&mut self) -> (&mut Vec<FieldValueHeader>, &mut Vec<u8>) {
        (&mut self.header, &mut self.data)
    }
}

pub type EntryId = usize;

unsafe fn append_data_memcpy<'a, TargetApplicatorFn: FnMut(&dyn Fn(&mut FieldData))>(
    ptr: *const u8,
    size: usize,
    target_applicator: &mut TargetApplicatorFn,
) {
    let source = slice::from_raw_parts(ptr, size);
    target_applicator(&|tgt| {
        tgt.data.extend_from_slice(source);
    });
}

unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    slice::from_raw_parts((p as *const T) as *const u8, size_of::<T>())
}

unsafe fn extend_with_clones<T: Clone>(tgt: &mut Vec<u8>, src: *const T, count: usize) {
    let source = slice::from_raw_parts(src as *const T, count);
    for v in source {
        tgt.extend_from_slice(as_u8_slice(&v.clone()));
    }
}

unsafe fn append_data<'a, TargetApplicatorFn: FnMut(&dyn Fn(&mut FieldData))>(
    h: FieldValueHeader,
    source: *const u8,
    target_applicator: &mut TargetApplicatorFn,
) {
    let count = if h.shared_value() {
        1
    } else {
        h.run_length as usize
    };
    use FieldValueKind::*;
    target_applicator(&|tgt| match h.kind {
        Unset | Null | EntryId | Integer | Reference | BytesInline | StreamValueId => {
            unreachable!()
        }
        BytesBuffer => extend_with_clones(&mut tgt.data, source as *const Vec<u8>, count),
        BytesFile => extend_with_clones(
            &mut tgt.data,
            source as *const crate::field_data::BytesBufferFile,
            count,
        ),
        Error => unsafe {
            extend_with_clones(
                &mut tgt.data,
                source as *const OperatorApplicationError,
                count,
            )
        },
        Html => extend_with_clones(
            &mut tgt.data,
            source as *const crate::field_data::Html,
            count,
        ),
        Object => extend_with_clones(
            &mut tgt.data,
            source as *const crate::field_data::Object,
            count,
        ),
    });
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
