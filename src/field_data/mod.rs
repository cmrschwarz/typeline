// This module implements a run-length encoded, dynamically typed data structure (FieldData)
// the type information for each entry is stored in a separate vec from the main data
// the type information itself is run-length encoded even if the data for the two entries is different

// SAFETY: due to its nature, this datastructure requires a lot of unsafe code,
// some of which is very repetitive. So far, nobody could be bothered
// with annotating every little piece of it.

pub mod command_buffer;
pub mod iter_hall;
pub mod iters;
pub mod push_interface;
pub mod record_set;
pub mod typed;
pub mod typed_iters;
use std::{
    collections::HashMap,
    iter,
    mem::{align_of, size_of, ManuallyDrop},
    ops::DerefMut,
    slice, u8,
};

use std::ops::Deref;

use crate::{
    operations::errors::OperatorApplicationError,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
    },
    stream_value::StreamValueId,
    utils::string_store::StringStoreEntry,
    worker_thread_session::{FieldId, MatchSetManager},
};

use self::{
    field_value_flags::{BYTES_ARE_UTF8, SHARED_VALUE},
    iters::{FieldIterator, Iter, IterMut},
    push_interface::PushInterface,
    typed::TypedSlice,
};

//if the u32 overflows we just split into two values
pub type RunLength = u32;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum FieldValueKind {
    Success,
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
            Success | Unset | Null | EntryId | Integer | Reference | StreamValueId => false,
            Error | Html | BytesInline | BytesBuffer | BytesFile | Object => true,
        }
    }
    pub fn needs_alignment(self) -> bool {
        use FieldValueKind::*;
        match self {
            Success | Unset | Null | BytesInline => false,
            _ => true,
        }
    }
    pub fn needs_copy(self) -> bool {
        self.needs_drop()
    }
    #[inline(always)]
    pub fn align_size_up(self, size: usize) -> usize {
        if self.needs_alignment() {
            (size + !FIELD_ALIGN_MASK) & FIELD_ALIGN_MASK
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
    pub fn size(self) -> usize {
        match self {
            FieldValueKind::Success => 0,
            FieldValueKind::Unset => 0,
            FieldValueKind::Null => 0,
            FieldValueKind::EntryId => std::mem::size_of::<EntryId>(),
            FieldValueKind::Integer => std::mem::size_of::<i64>(),
            FieldValueKind::StreamValueId => std::mem::size_of::<StreamValueId>(),
            FieldValueKind::Reference => std::mem::size_of::<FieldReference>(),
            FieldValueKind::Error => std::mem::size_of::<OperatorApplicationError>(),
            FieldValueKind::Html => std::mem::size_of::<Html>(),
            FieldValueKind::BytesBuffer => std::mem::size_of::<Vec<u8>>(),
            FieldValueKind::BytesFile => std::mem::size_of::<BytesBufferFile>(),
            FieldValueKind::Object => std::mem::size_of::<Object>(),
            // should not be used for size calculations
            // but is used for example in is_zst
            FieldValueKind::BytesInline => usize::MAX,
        }
    }
    pub fn is_variable_sized_type(self) -> bool {
        self == FieldValueKind::BytesInline
    }
    pub fn is_zst(self) -> bool {
        self.size() == 0
    }
    pub fn is_fixed_size_type(self) -> bool {
        !self.is_variable_sized_type() && !self.is_zst()
    }
}

pub struct Unset;
pub struct Null;
pub struct Success;

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
pub const FIELD_ALIGN_MASK: usize = !(MAX_FIELD_ALIGN - 1);

pub type FieldValueSize = u16;

pub mod field_value_flags {
    use super::MAX_FIELD_ALIGN;
    pub type FieldValueFlags = u8;
    //offset must be zero so we don't have to shift
    const_assert!(MAX_FIELD_ALIGN.is_power_of_two() && MAX_FIELD_ALIGN <= 16);
    pub const LEADING_PADDING: FieldValueFlags = 0xF; //consumes offsets 0 through 3
    pub const SAME_VALUE_AS_PREVIOUS_OFFSET: FieldValueFlags = 4;
    pub const SHARED_VALUE_OFFSET: FieldValueFlags = 5;
    pub const BYTES_ARE_UTF8_OFFSET: FieldValueFlags = 6;
    pub const DELETED_OFFSET: FieldValueFlags = 7;

    pub const SHARED_VALUE: FieldValueFlags = 1 << SHARED_VALUE_OFFSET;
    pub const BYTES_ARE_UTF8: FieldValueFlags = 1 << BYTES_ARE_UTF8_OFFSET;
    pub const DELETED: FieldValueFlags = 1 << DELETED_OFFSET;
    pub const SAME_VALUE_AS_PREVIOUS: FieldValueFlags = 1 << SAME_VALUE_AS_PREVIOUS_OFFSET;

    pub const DEFAULT: FieldValueFlags = 0;
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
        self.flags &= !field_value_flags::SHARED_VALUE;
        self.flags |= (val as FieldValueFlags) << field_value_flags::SHARED_VALUE_OFFSET;
    }
    pub fn bytes_are_utf8(self) -> bool {
        self.flags & field_value_flags::BYTES_ARE_UTF8 != 0
    }
    pub fn set_bytes_are_utf8(&mut self, val: bool) {
        self.flags &= !field_value_flags::BYTES_ARE_UTF8;
        self.flags |= (val as FieldValueFlags) << field_value_flags::BYTES_ARE_UTF8_OFFSET;
    }
    pub fn leading_padding(self) -> usize {
        (self.flags & field_value_flags::LEADING_PADDING) as usize
    }
    pub fn set_leading_padding(&mut self, val: usize) {
        debug_assert!(val & !(field_value_flags::LEADING_PADDING as usize) == 0);
        self.flags &= !field_value_flags::LEADING_PADDING;
        self.flags |= (val as u8) & field_value_flags::LEADING_PADDING;
    }
    pub fn deleted(self) -> bool {
        self.flags & field_value_flags::DELETED != 0
    }
    pub fn set_deleted(&mut self, val: bool) {
        self.flags &= !field_value_flags::DELETED;
        self.flags |= (val as FieldValueFlags) << field_value_flags::DELETED_OFFSET;
    }
    pub fn same_value_as_previous(self) -> bool {
        self.flags & field_value_flags::SAME_VALUE_AS_PREVIOUS != 0
    }
    pub fn set_same_value_as_previous(&mut self, val: bool) {
        self.flags &= !field_value_flags::SAME_VALUE_AS_PREVIOUS;
        self.flags |= (val as FieldValueFlags) << field_value_flags::SAME_VALUE_AS_PREVIOUS_OFFSET;
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
    pub fn unique_data_element_count(&self) -> RunLength {
        if self.shared_value() {
            if self.same_value_as_previous() {
                0
            } else {
                1
            }
        } else {
            self.run_length
        }
    }
    pub fn data_element_count(&self) -> RunLength {
        if self.shared_value() {
            1
        } else {
            self.run_length
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
        self.data_size() + self.leading_padding()
    }
    pub fn run_len_rem(&self) -> RunLength {
        RunLength::MAX - self.run_length
    }
}

pub const INLINE_STR_MAX_LEN: usize = 8192;

#[derive(Default)]
pub struct FieldData {
    pub(self) data: Vec<u8>,
    pub(self) header: Vec<FieldValueHeader>,
    pub(self) field_count: usize,
}

impl Clone for FieldData {
    fn clone(&self) -> Self {
        let mut fd = Self {
            data: Vec::with_capacity(self.data.len()),
            header: Vec::with_capacity(self.header.len()),
            field_count: 0,
        };
        let fd_ref = &mut fd;
        FieldData::copy(self.iter(), &mut |f| f(fd_ref));
        fd
    }
}

unsafe fn drop_slice<T>(slice: &[T]) {
    let droppable = slice::from_raw_parts_mut(slice.as_ptr() as *mut ManuallyDrop<T>, slice.len());
    for e in droppable.iter_mut() {
        ManuallyDrop::drop(e);
    }
}

pub struct FieldDataInternals<'a> {
    pub data: &'a mut Vec<u8>,
    pub header: &'a mut Vec<FieldValueHeader>,
    pub field_count: &'a mut usize,
}
impl FieldData {
    pub unsafe fn from_raw_parts(
        header: Vec<FieldValueHeader>,
        data: Vec<u8>,
        field_count: usize,
    ) -> Self {
        Self {
            header,
            data,
            field_count,
        }
    }
    pub fn clear(&mut self) {
        self.field_count = 0;
        FieldData::set_end(self.iter_mut());
    }
    pub fn set_end<'a>(iter: IterMut<'a>) {
        let fd = unsafe { &mut *(iter.fd as *mut FieldData) };
        let mut iter = iter.into_base_iter();
        let header_end;
        let data_end;
        if !iter.is_prev_valid() {
            header_end = 0;
            data_end = 0;
        } else {
            header_end = iter.get_prev_header_index() + 1;
            data_end = iter.get_prev_field_data_end();
        }
        while let Some(range) = iter.typed_range_fwd(usize::MAX, 0) {
            unsafe {
                match range.data {
                    TypedSlice::Unset(_) => (),
                    TypedSlice::Success(_) => (),
                    TypedSlice::Null(_) => (),
                    TypedSlice::Integer(_) => (),
                    TypedSlice::BytesInline(_) => (),
                    TypedSlice::TextInline(_) => (),
                    TypedSlice::StreamValueId(s) => drop_slice(s),
                    TypedSlice::Reference(s) => drop_slice(s),
                    TypedSlice::Error(s) => drop_slice(s),
                    TypedSlice::Html(s) => drop_slice(s),
                    TypedSlice::Object(s) => drop_slice(s),
                    TypedSlice::BytesBuffer(s) => drop_slice(s),
                }
            }
        }
        fd.header.truncate(header_end);
        fd.data.truncate(data_end);
    }

    unsafe fn pad_to_align(&mut self) -> usize {
        let mut align = self.data.len() % MAX_FIELD_ALIGN;
        if align == 0 {
            return 0;
        }
        align = MAX_FIELD_ALIGN - align;
        self.data.extend(iter::repeat(0).take(align));
        align
    }

    pub fn copy<'a>(
        mut iter: impl FieldIterator<'a> + Clone,
        mut targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        while let Some(tr) = iter.typed_range_fwd(usize::MAX, field_value_flags::DELETED) {
            copied_fields += tr.field_count;
            targets_applicator(&mut |fd| {
                let first_header_idx = fd.header.len();
                fd.header.extend_from_slice(tr.headers);
                if tr.headers[0].kind.needs_alignment() {
                    let align = unsafe { fd.pad_to_align() };
                    fd.header[first_header_idx].set_leading_padding(align);
                }
                fd.header[first_header_idx].run_length -= tr.first_header_run_length_oversize;
            });
            unsafe { append_data(tr.data, &mut targets_applicator) };
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }

    pub fn copy_resolve_refs<'a, I: FieldIterator<'a>>(
        match_set_mgr: &mut MatchSetManager,
        mut iter: AutoDerefIter<'a, I>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        while let Some(tr) = iter.typed_range_fwd(
            match_set_mgr,
            usize::MAX,
            field_value_flags::BYTES_ARE_UTF8 | field_value_flags::DELETED,
        ) {
            copied_fields += tr.base.field_count;
            if tr.refs.is_none() {
                targets_applicator(&mut |fd| {
                    let first_header_idx = fd.header.len();
                    fd.header.extend_from_slice(tr.base.headers);
                    if tr.base.headers[0].kind.needs_alignment() {
                        let align = unsafe { fd.pad_to_align() };
                        fd.header[first_header_idx].set_leading_padding(align);
                    }
                    fd.header[first_header_idx].run_length -=
                        tr.base.first_header_run_length_oversize;
                });
                unsafe { append_data(tr.base.data, targets_applicator) };
            } else {
                match tr.base.data {
                    TypedSlice::BytesInline(data) => {
                        for (v, rl, _offset) in RefAwareInlineBytesIter::from_range(&tr, data) {
                            targets_applicator(&mut |fd| {
                                //TODO: maybe do a little rle here?
                                fd.header.push(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        kind: FieldValueKind::BytesInline,
                                        flags: SHARED_VALUE,
                                        size: v.len() as FieldValueSize,
                                    },
                                    run_length: rl,
                                });
                                fd.data.extend_from_slice(v);
                            });
                        }
                    }
                    TypedSlice::TextInline(data) => {
                        for (v, rl, _offset) in RefAwareInlineTextIter::from_range(&tr, data) {
                            targets_applicator(&mut |fd| {
                                //TODO: maybe do a little rle here?
                                fd.header.push(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        kind: FieldValueKind::BytesInline,
                                        flags: SHARED_VALUE | BYTES_ARE_UTF8,
                                        size: v.len() as FieldValueSize,
                                    },
                                    run_length: rl,
                                });
                                fd.data.extend_from_slice(v.as_bytes());
                            });
                        }
                    }
                    TypedSlice::BytesBuffer(data) => {
                        for (v, rl, _offset) in RefAwareBytesBufferIter::from_range(&tr, data) {
                            targets_applicator(&mut |fd| {
                                fd.push_bytes(v, rl as usize, true, false);
                            });
                        }
                    }
                    TypedSlice::Success(_)
                    | TypedSlice::Unset(_)
                    | TypedSlice::Null(_)
                    | TypedSlice::Integer(_)
                    | TypedSlice::StreamValueId(_)
                    | TypedSlice::Reference(_)
                    | TypedSlice::Error(_)
                    | TypedSlice::Html(_)
                    | TypedSlice::Object(_) => unreachable!(),
                }
            }
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }

    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter::from_start(self, 0)
    }
    pub fn iter_mut<'a>(&'a mut self) -> IterMut<'a> {
        IterMut::from_start(self, 0)
    }
    pub unsafe fn internals(&mut self) -> FieldDataInternals {
        FieldDataInternals {
            data: &mut self.data,
            header: &mut self.header,
            field_count: &mut self.field_count,
        }
    }
    pub fn field_count(&self) -> usize {
        self.field_count
    }
}

pub type EntryId = usize;

unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    slice::from_raw_parts((p as *const T) as *const u8, size_of::<T>())
}

unsafe fn extend_with_clones<T: Clone>(
    target_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    src: &[T],
) {
    let src_size = src.len() * std::mem::size_of::<T>();
    target_applicator(&mut |fd| {
        fd.data.reserve(src_size);
        unsafe {
            let mut tgt = fd.data.as_mut_ptr_range().end as *mut T;
            for v in src {
                std::ptr::write(tgt, v.clone());
                tgt = tgt.add(1);
            }
            fd.data.set_len(fd.data.len() + src_size);
        }
    });
}
unsafe fn extend_raw<T: Sized>(
    target_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    src: &[T],
) {
    let src_bytes = std::slice::from_raw_parts(
        src.as_ptr() as *const u8,
        src.len() * std::mem::size_of::<T>(),
    );
    target_applicator(&mut |fd| fd.data.extend_from_slice(src_bytes));
}

unsafe fn append_data<'a>(
    ts: TypedSlice<'a>,
    target_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
) {
    match ts {
        TypedSlice::Unset(_) | TypedSlice::Null(_) | TypedSlice::Success(_) => (),
        TypedSlice::Integer(v) => extend_raw(target_applicator, v),
        TypedSlice::StreamValueId(v) => extend_raw(target_applicator, v),
        TypedSlice::Reference(v) => extend_raw(target_applicator, v),
        TypedSlice::BytesInline(v) => extend_raw(target_applicator, v),
        TypedSlice::TextInline(v) => extend_raw(target_applicator, v.as_bytes()),
        TypedSlice::BytesBuffer(v) => extend_with_clones(target_applicator, v),
        TypedSlice::Error(v) => extend_with_clones(target_applicator, v),
        TypedSlice::Html(v) => extend_with_clones(target_applicator, v),
        TypedSlice::Object(v) => extend_with_clones(target_applicator, v),
    };
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
