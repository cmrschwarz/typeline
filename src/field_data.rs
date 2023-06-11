use std::{
    collections::HashMap,
    iter,
    mem::{align_of, size_of, ManuallyDrop},
    ops::{DerefMut, Range},
    ptr::drop_in_place,
    slice, u8,
};

use std::ops::Deref;

use crate::{
    field_data_iter::{
        FieldDataIter, NoRleTypesFieldDataIter, RawFieldDataIter, SingleTypeFieldDataIter,
    },
    operations::OperatorApplicationError,
    string_store::StringStoreEntry,
    worker_thread_session::FieldId,
};

//if the u32 overflows we just split into two values
pub type RunLength = u32;

#[derive(Clone, Copy, PartialEq)]
pub enum StreamKind {
    Plain,
    Reference,
    BufferMem,
    BufferFile,
}

pub type FieldValueKindIntegralType = u8;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum FieldValueKind {
    Unset,
    Null,
    EntryId,
    Integer, //TODO: bigint, float, decimal, ...
    Reference,
    Error,
    Html,
    BytesInline,
    BytesBuffer,
    BytesFile,
    Object,
}
// because these enum values as generics is annoying
pub mod FieldValueKindValues {
    use super::FieldValueKind::*;
    use super::FieldValueKindIntegralType as KindType;
    pub const UNSET: KindType = Unset as KindType;
    pub const NULL: KindType = Null as KindType;
    pub const ENTRY_ID: KindType = EntryId as KindType;
    pub const INTEGER: KindType = Integer as KindType;
    pub const REFERENCE: KindType = Reference as KindType;
    pub const ERROR: KindType = Error as KindType;
    pub const HTML: KindType = Html as KindType;
    pub const BYTES_INLINE: KindType = BytesInline as KindType;
    pub const BYTES_BUFFER: KindType = BytesBuffer as KindType;
    pub const BYTES_FILE: KindType = BytesFile as KindType;
    pub const OBJECT: KindType = Object as KindType;
}
impl FieldValueKind {
    pub fn needs_drop(self) -> bool {
        use FieldValueKind::*;
        match self {
            Unset | Null | EntryId | Integer | Reference => false,
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
    pub fn align_size(self, size: usize) -> usize {
        if self.needs_alignment() {
            size & FIELD_ALIGN_MASK
        } else {
            size
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            // doing this instead of the straight conversion to usize
            // to avoid loosing provenance
            unsafe { ptr.sub(ptr as usize & MAX_FIELD_ALIGN) }
        } else {
            ptr
        }
    }
}

#[derive(Clone)]
struct ObjectEntry {
    kind: FieldValueKind,
    data_offset: usize,
}

#[derive(Clone)]
pub struct Object {
    data: FieldData,
    table: HashMap<StringStoreEntry, ObjectEntry>,
}

pub struct CrossFieldReference {
    field: FieldId,
    range: Range<usize>,
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

const_assert!(MAX_FIELD_ALIGN.is_power_of_two());
pub const FIELD_ALIGN_MASK: usize = !MAX_FIELD_ALIGN;

type FieldValueSize = u16;

pub mod FieldValueHeaderFlags {
    pub type Type = u8;
    pub const SHARED_VALUE: Type = 0;
    pub const END_OF_STREAM: Type = 1;
    pub const BYTES_ARE_UTF8: Type = 2;
    pub const DEFAULT_FLAGS: Type = END_OF_STREAM;
}

#[derive(Clone, Copy)]
pub struct FieldValueHeaderFormat {
    pub kind: FieldValueKind,
    pub flags: FieldValueHeaderFlags::Type,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    pub size: FieldValueSize,
}

impl Default for FieldValueHeaderFormat {
    fn default() -> Self {
        Self {
            kind: FieldValueKind::Unset,
            flags: FieldValueHeaderFlags::DEFAULT_FLAGS,
            size: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct FieldValueHeader {
    pub fmt: FieldValueHeaderFormat,
    pub run_length: RunLength,
}

impl FieldValueHeaderFormat {
    pub fn shared_value(self) -> bool {
        self.flags & FieldValueHeaderFlags::SHARED_VALUE != 0
    }
    pub fn set_shared_value(&mut self, val: bool) {
        self.flags |= (val as FieldValueHeaderFlags::Type) << FieldValueHeaderFlags::SHARED_VALUE;
    }
    pub fn end_of_stream(self) -> bool {
        self.flags & FieldValueHeaderFlags::END_OF_STREAM != 0
    }
    pub fn set_end_of_stream(&mut self, val: bool) {
        self.flags |= (val as FieldValueHeaderFlags::Type) << FieldValueHeaderFlags::END_OF_STREAM;
    }
    pub fn bytes_are_utf8(self) -> bool {
        self.flags & FieldValueHeaderFlags::BYTES_ARE_UTF8 != 0
    }
    pub fn set_bytes_are_utf8(&mut self, val: bool) {
        self.flags |= (val as FieldValueHeaderFlags::Type) << FieldValueHeaderFlags::BYTES_ARE_UTF8;
    }
}

impl Deref for FieldValueHeader {
    type Target = FieldValueHeaderFormat;

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
}

#[derive(Default)]
pub struct FieldData {
    pub(crate) data: Vec<u8>,
    pub(crate) header: Vec<FieldValueHeader>,
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

const INLINE_STR_MAX_LEN: usize = 8192;

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
    fn pad_to_align(&mut self) {
        self.data
            .extend(iter::repeat(0).take(self.data.len() % MAX_FIELD_ALIGN));
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
    pub fn push_str(&mut self, data: &str, run_length: RunLength) {
        let inline = data.len() < INLINE_STR_MAX_LEN;
        let tgt_kind = if inline {
            FieldValueKind::BytesInline
        } else {
            FieldValueKind::BytesBuffer
        };
        let mut could_amend_header = false;
        if let Some(h) = self.header.last_mut() {
            if !h.shared_value() && h.kind == tgt_kind && h.end_of_stream() == true {
                if let Some(rl) = h.run_length.checked_add(1) {
                    h.run_length = rl;
                    could_amend_header = true;
                }
            }
        }

        if !could_amend_header {
            self.header.push(FieldValueHeader {
                fmt: FieldValueHeaderFormat {
                    kind: tgt_kind,
                    flags: Default::default(),
                    size: if inline {
                        data.len()
                    } else {
                        size_of::<String>()
                    } as FieldValueSize,
                },
                run_length,
            })
        }
        if inline {
            self.data.extend_from_slice(data.as_bytes());
        } else {
            self.pad_to_align();
            let owned_str = ManuallyDrop::new(data.to_owned());
            self.data
                .extend_from_slice(unsafe { as_u8_slice(&owned_str) });
        }
    }
    pub fn iter<'a>(&'a self) -> FieldDataIter<'a> {
        FieldDataIter::new(RawFieldDataIter::new(self))
    }
    pub fn iter_no_rle_types<'a>(&'a self) -> NoRleTypesFieldDataIter<'a> {
        NoRleTypesFieldDataIter::new(RawFieldDataIter::new(self))
    }
}

pub type EntryId = usize;

unsafe fn load_slice<'a>(ptr: *const u8, size: FieldValueSize) -> &'a [u8] {
    slice::from_raw_parts(ptr, size as usize)
}

unsafe fn load_str_slice<'a>(ptr: *const u8, size: FieldValueSize) -> &'a str {
    std::str::from_utf8_unchecked(load_slice(ptr, size))
}

unsafe fn drop_data(fmt: FieldValueHeader, ptr: *mut u8) {
    use FieldValueKind::*;
    unsafe {
        match fmt.kind {
            Unset | Null | EntryId | Integer | Reference | BytesInline => unreachable!(),
            BytesBuffer => drop_in_place(ptr as *mut Vec<u8>),
            BytesFile => drop_in_place(ptr as *mut crate::field_data::BytesBufferFile),
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
            Unset | Null | EntryId | Integer | Reference | BytesInline => unreachable!(),
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
