pub mod field_data_iterator;

use std::{
    collections::HashMap,
    iter,
    mem::{align_of, size_of, ManuallyDrop},
    ops::DerefMut,
    ptr::drop_in_place,
    slice, u8,
};

use std::ops::Deref;

use crate::{
    field_data::field_value_flags::TYPE_RELEVANT, operations::errors::OperatorApplicationError,
    stream_field_data::StreamValueId, utils::string_store::StringStoreEntry,
    worker_thread_session::FieldId,
};

use self::{field_data_iterator::FDIter, field_value_flags::SHARED_VALUE};

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
            unsafe { ptr.sub(ptr as usize & MAX_FIELD_ALIGN) }
        } else {
            ptr
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr_up(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            unsafe { ptr.sub((ptr as usize + MAX_FIELD_ALIGN) & MAX_FIELD_ALIGN) }
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

const_assert!(MAX_FIELD_ALIGN.is_power_of_two() && MAX_FIELD_ALIGN <= 16);
pub const FIELD_ALIGN_MASK: usize = !MAX_FIELD_ALIGN;

pub type FieldValueSize = u16;
pub type FieldValueFlags = field_value_flags::Type;

pub mod field_value_flags {
    pub type Type = u8;
    //offset must be zero so we don't have to shift
    pub const ALIGNMENT_AFTER: Type = 0xF;

    pub const SHARED_VALUE_OFFSET: Type = 4;
    pub const BYTES_ARE_UTF8_OFFSET: Type = 5;

    pub const SHARED_VALUE: Type = 1 << SHARED_VALUE_OFFSET;
    pub const BYTES_ARE_UTF8: Type = 1 << BYTES_ARE_UTF8_OFFSET;

    pub const DEFAULT: Type = 0;

    pub const TYPE_RELEVANT: Type = BYTES_ARE_UTF8_OFFSET;
}

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
        self.flags |= (val as field_value_flags::Type) << field_value_flags::SHARED_VALUE_OFFSET;
    }
    pub fn bytes_are_utf8(self) -> bool {
        self.flags & field_value_flags::BYTES_ARE_UTF8 != 0
    }
    pub fn set_bytes_are_utf8(&mut self, val: bool) {
        self.flags |= (val as field_value_flags::Type) << field_value_flags::BYTES_ARE_UTF8_OFFSET;
    }
    pub fn alignment_after(self) -> usize {
        (self.flags & field_value_flags::ALIGNMENT_AFTER) as usize
    }
    pub fn set_alignment_after(&mut self, val: usize) {
        debug_assert!(val & !(field_value_flags::ALIGNMENT_AFTER as usize) == 0);
        self.flags |= (val as u8) & field_value_flags::ALIGNMENT_AFTER;
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
        let mut res = Self {
            data: Vec::with_capacity(self.data.len()),
            header: self.header.clone(),
        };
        self.copy_n(usize::MAX, &mut [&mut res]);
        res
    }
}

pub const INLINE_STR_MAX_LEN: usize = 8192;

impl FieldData {
    pub fn clear(&mut self) {
        self.drop_n(usize::MAX);
    }
    pub fn drop_n(&mut self, n: usize) {
        let mut remaining_size = self.data.len();
        let mut remaining_drops = n;
        let mut header_idx = self.header.len();
        while remaining_drops > 0 && remaining_size > 0 {
            header_idx -= 1;
            let h = &mut self.header[header_idx];
            if h.run_length as usize > remaining_drops {
                h.run_length -= remaining_drops as RunLength;
                break;
            }
            remaining_drops -= h.run_length as usize;
            let drop_count = if h.shared_value() { 1 } else { h.run_length };
            if h.kind.needs_drop() {
                for _ in 0..drop_count {
                    remaining_size -= h.size as usize;
                    unsafe {
                        drop_data(*h, self.data.as_mut_ptr().add(remaining_size));
                    }
                }
            } else {
                remaining_size -= drop_count as usize * h.size as usize;
            }
            if h.kind.needs_alignment() {
                remaining_size -= remaining_size % MAX_FIELD_ALIGN;
            }
        }
        self.header.truncate(header_idx);
        self.data.truncate(remaining_size);
    }
    pub fn dup_nth(&mut self, n: usize, new_run_len: RunLength) {
        let mut hi = self.header.len();
        let mut i = 0;
        while i < n {
            hi -= 1;
            i += self.header[hi].run_length as usize;
        }
        let h = &mut self.header[hi];
        let mut tgt = *h;
        tgt.run_length = new_run_len;
        debug_assert!(h.run_length > 0 && (h.shared_value() || h.run_length > 1));
        if h.shared_value() {
            tgt.run_length -= 1;
            if let Some(rl) = h.run_length.checked_add(tgt.run_length) {
                h.run_length = rl;
            } else {
                self.header.insert(hi, tgt);
            }
        } else if i == n + 1 {
            //nth is first of this header, insert it at the start
            h.run_length -= 1;
            if h.run_length == 1 {
                h.set_shared_value(true);
            }
            self.header.insert(hi + 1, tgt);
        } else if i == n + h.run_length as usize {
            //nth is last of this header, insert it as the start
            h.run_length -= 1;
            if h.run_length == 1 {
                h.set_shared_value(true);
            }
            self.header.insert(hi, tgt);
        } else {
            // nth is sandwiched in this header, insert two
            let rl_after = (i - n) as RunLength;
            h.run_length = rl_after;
            hi += 1;
            let mut tgt2 = tgt;
            tgt2.run_length = h.run_length - rl_after;
            self.header.splice(hi..hi, [tgt, tgt2]);
        }
    }
    // can't make this pub as it can be used to break the type layout
    fn pad_to_align(&mut self) -> usize {
        let align = self.data.len() % MAX_FIELD_ALIGN;
        self.data.extend(iter::repeat(0).take(align));
        self.header.last_mut().map(|h| h.set_alignment_after(align));
        align
    }
    pub fn copy_n<'a>(&self, n: usize, targets: &mut [&'a mut FieldData]) {
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
        for t in targets.iter_mut() {
            if let Some(h_tgt) = t.header.last_mut() {
                if !h.shared_value() && !h_tgt.shared_value() && h_tgt.kind == h.kind {
                    if let Some(rl) = h_tgt.run_length.checked_add(h.run_length) {
                        h_tgt.run_length = rl;
                        if header_idx + 1 != self.header.len() {
                            t.header.extend(&self.header[header_idx + 1..]);
                        }
                        continue;
                    }
                }
            }
            t.header.extend(&self.header[header_idx..]);
        }

        // copy over leading unaligned data
        let data_start = data_size_total - copy_data_size;
        let first_aligned_field_start = data_size_total - copy_data_size_at_first_aligned_field;

        if first_aligned_field_start != data_start {
            unsafe {
                append_data_memcpy(
                    self.data.as_ptr().add(data_start),
                    first_aligned_field_start - data_start - first_aligned_field_padding,
                    targets,
                );
            }
            h = self.header[first_aligned_field_idx];
        }

        // if all memory was unaligned, we're done here
        if first_aligned_field_start == data_size_total {
            return;
        }

        // add padding so all targets are aligned
        for t in targets.iter_mut() {
            t.pad_to_align();
        }

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
                    append_data_memcpy(start, data_pos - uncopied_data_start, targets);
                    append_data(h, self.data.as_ptr().add(data_pos), targets)
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
            append_data_memcpy(start, data_pos - uncopied_data_start, targets);
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
        flags: field_value_flags::Type,
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

unsafe fn drop_data(fmt: FieldValueHeader, ptr: *mut u8) {
    use FieldValueKind::*;
    unsafe {
        match fmt.kind {
            Unset | Null | EntryId | Integer | Reference | BytesInline | StreamValueId => {
                unreachable!()
            }
            BytesBuffer => drop_in_place(ptr as *mut Vec<u8>),
            BytesFile => drop_in_place(ptr as *mut BytesBufferFile),
            Error => drop_in_place(ptr as *mut OperatorApplicationError),
            Html => drop_in_place(ptr as *mut crate::field_data::Html),
            Object => drop_in_place(ptr as *mut crate::field_data::Object),
        }
    }
}

unsafe fn append_data_memcpy<'a>(ptr: *const u8, size: usize, targets: &mut [&'a mut FieldData]) {
    let source = slice::from_raw_parts(ptr, size);
    for tgt in targets {
        tgt.data.extend_from_slice(source);
    }
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

unsafe fn append_data<'a>(
    h: FieldValueHeader,
    source: *const u8,
    targets: &mut [&'a mut FieldData],
) {
    let count = if h.shared_value() {
        1
    } else {
        h.run_length as usize
    };
    use FieldValueKind::*;
    for tgt in targets {
        match h.kind {
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
        }
    }
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
