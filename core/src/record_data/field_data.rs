use std::{
    any::Any,
    collections::VecDeque,
    fmt::{Debug, Display},
    mem::{align_of, size_of, size_of_val, ManuallyDrop},
    ops::{Deref, DerefMut},
};

use super::{
    custom_data::CustomDataBox,
    field_value::{
        Array, FieldReference, FieldValueKind, Null, Object,
        SlicedFieldReference, Undefined,
    },
    field_value_ref::value_as_bytes,
    match_set::MatchSetManager,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareFieldValueSliceIter,
        RefAwareInlineBytesIter, RefAwareInlineTextIter,
        RefAwareTextBufferIter,
    },
};
use crate::{
    operators::errors::OperatorApplicationError, utils::ringbuf::RingBuf,
};

use self::field_value_flags::SHARED_VALUE;
pub use field_value_flags::FieldValueFlags;
use num::{BigInt, BigRational};

use super::{
    field_value_ref::FieldValueSlice,
    iters::{FieldIterator, Iter},
    push_interface::PushInterface,
    stream_value::StreamValueId,
};

// if the u32 overflows we just split into two values
pub type RunLength = u32;

// the kinds of data representations that are stored in a `FieldData` Object
// this includes StreamValueId, FieldReferences, ...
// which are not actual data types, but helper representations / indirections
// This also does not differentiate between the text and bytes type as they
// have the same data layout
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldValueRepr {
    #[default]
    Null,
    Undefined,
    Int,
    BigInt,
    Float,
    Rational,
    TextInline,
    TextBuffer,
    TextFile,
    BytesInline,
    BytesBuffer,
    BytesFile,
    Object,
    Array,
    Custom,
    Error,
    StreamValueId,
    FieldReference,
    SlicedFieldReference,
    // ENHANCE //PERF (maybe): CustomDynamicLength,
    // CustomDynamicLengthAligned (store some subtype index at the start
    // of the actual data)
}

#[derive(Clone)]
pub struct TextBufferFile {
    // TODO
}

#[derive(Clone)]
pub struct BytesBufferFile {
    // TODO
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FieldValueFormat {
    pub repr: FieldValueRepr,
    pub flags: FieldValueFlags,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    pub size: FieldValueSize,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct FieldValueHeader {
    pub fmt: FieldValueFormat,
    // this shal never be zero, **even for deleted fields**
    pub run_length: RunLength,
}

#[derive(Default)]
pub struct FieldData {
    pub(super) data: FieldDataBuffer,
    pub(super) headers: VecDeque<FieldValueHeader>,
    pub(super) field_count: usize,
}

pub struct FieldDataInternals<'a> {
    pub data: &'a FieldDataBuffer,
    pub header: &'a VecDeque<FieldValueHeader>,
    pub field_count: &'a usize,
}
pub struct FieldDataInternalsMut<'a> {
    pub data: &'a mut FieldDataBuffer,
    pub header: &'a mut VecDeque<FieldValueHeader>,
    pub field_count: &'a mut usize,
}

// only used to figure out the maximum alignment needed for fields
#[repr(C)]
union FieldValueAlignmentCheckUnion {
    int: i64,
    float: f64,
    bigint: ManuallyDrop<BigInt>,
    rational: ManuallyDrop<BigRational>,
    text: ManuallyDrop<String>,
    bytes: ManuallyDrop<Vec<u8>>,
    bytes_file: ManuallyDrop<BytesBufferFile>,
    field_ref: FieldReference,
    sliced_field_ref: SlicedFieldReference,
    object: ManuallyDrop<Object>,
    array: ManuallyDrop<Array>,
    custom: ManuallyDrop<CustomDataBox>,
}

pub const MAX_FIELD_ALIGN: usize = align_of::<FieldValueAlignmentCheckUnion>();
pub const FIELD_ALIGN_MASK: usize = !(MAX_FIELD_ALIGN - 1);
pub type FieldDataBuffer = RingBuf<MAX_FIELD_ALIGN>;

pub type FieldValueSize = u16;

pub mod field_value_flags {
    use super::MAX_FIELD_ALIGN;
    pub type FieldValueFlags = u8;
    // offset must be zero so we don't have to shift
    const_assert!(MAX_FIELD_ALIGN.is_power_of_two() && MAX_FIELD_ALIGN <= 16);
    // consumes offsets 0 through 3
    // Storing this (instead of computing it from the kind & position)
    // is necessary because otherwise we couldn't iterate backwards.
    // We wouldn't know by how much the type before an element had to be padded
    // to make the successor aligned.
    pub const LEADING_PADDING: FieldValueFlags = 0xF;
    pub const SAME_VALUE_AS_PREVIOUS_OFFSET: FieldValueFlags = 4;
    pub const SHARED_VALUE_OFFSET: FieldValueFlags = 5;
    pub const DELETED_OFFSET: FieldValueFlags = 7;
    // When the run_length is one, `SHARED_VALUE` **must** also be set
    pub const SHARED_VALUE: FieldValueFlags = 1 << SHARED_VALUE_OFFSET;
    pub const DELETED: FieldValueFlags = 1 << DELETED_OFFSET;
    // share data with previous header (potentially across ZSTs)
    // leading padding must be zero if this is set
    pub const SAME_VALUE_AS_PREVIOUS: FieldValueFlags =
        1 << SAME_VALUE_AS_PREVIOUS_OFFSET;

    pub const DEFAULT: FieldValueFlags = 0;
    pub const NONE: FieldValueFlags = 0;
}

#[derive(Clone, Copy)]
pub struct FieldValueFlagsDebugRepr {
    pub padding: u8,
    pub deleted: bool,
    pub shared_value: bool,
    pub same_as_prev: bool,
}

// used to constrain generic functions that accept data for field values
pub unsafe trait FieldValueType: PartialEq + Any {
    const REPR: FieldValueRepr;
    const DST: bool = true;
    const ZST: bool = false;
    const TRIVIALLY_COPYABLE: bool = Self::ZST;
    const SUPPORTS_REFS: bool = !Self::TRIVIALLY_COPYABLE;
}
unsafe impl FieldValueType for [u8] {
    const REPR: FieldValueRepr = FieldValueRepr::BytesInline;
}
unsafe impl FieldValueType for str {
    const REPR: FieldValueRepr = FieldValueRepr::TextInline;
}

pub unsafe trait FixedSizeFieldValueType:
    Clone + PartialEq + Any + Sized
{
    const REPR: FieldValueRepr;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    const ZST: bool = std::mem::size_of::<Self>() == 0;
    const TRIVIALLY_COPYABLE: bool = Self::ZST;
    const SUPPORTS_REFS: bool = !Self::TRIVIALLY_COPYABLE;
}
unsafe impl<T: FixedSizeFieldValueType> FieldValueType for T {
    const DST: bool = false;
    const REPR: FieldValueRepr = Self::REPR;
    const ZST: bool = Self::ZST;
    const TRIVIALLY_COPYABLE: bool = Self::TRIVIALLY_COPYABLE;
    const SUPPORTS_REFS: bool = Self::SUPPORTS_REFS;
}
unsafe impl FixedSizeFieldValueType for Undefined {
    const REPR: FieldValueRepr = FieldValueRepr::Null;
}
unsafe impl FixedSizeFieldValueType for Null {
    const REPR: FieldValueRepr = FieldValueRepr::Undefined;
}
unsafe impl FixedSizeFieldValueType for i64 {
    const REPR: FieldValueRepr = FieldValueRepr::Int;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for f64 {
    const REPR: FieldValueRepr = FieldValueRepr::Float;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for StreamValueId {
    const REPR: FieldValueRepr = FieldValueRepr::StreamValueId;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for FieldReference {
    const REPR: FieldValueRepr = FieldValueRepr::FieldReference;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for SlicedFieldReference {
    const REPR: FieldValueRepr = FieldValueRepr::SlicedFieldReference;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for OperatorApplicationError {
    const REPR: FieldValueRepr = FieldValueRepr::Error;
}
unsafe impl FixedSizeFieldValueType for Vec<u8> {
    const REPR: FieldValueRepr = FieldValueRepr::BytesBuffer;
}
unsafe impl FixedSizeFieldValueType for String {
    const REPR: FieldValueRepr = FieldValueRepr::TextBuffer;
}
unsafe impl FixedSizeFieldValueType for Object {
    const REPR: FieldValueRepr = FieldValueRepr::Object;
}
unsafe impl FixedSizeFieldValueType for Array {
    const REPR: FieldValueRepr = FieldValueRepr::Array;
}
unsafe impl FixedSizeFieldValueType for BigInt {
    const REPR: FieldValueRepr = FieldValueRepr::BigInt;
}
unsafe impl FixedSizeFieldValueType for BigRational {
    const REPR: FieldValueRepr = FieldValueRepr::Rational;
}
unsafe impl FixedSizeFieldValueType for CustomDataBox {
    const REPR: FieldValueRepr = FieldValueRepr::Custom;
}
pub const INLINE_STR_MAX_LEN: usize = 8192;

impl FieldValueRepr {
    pub fn needs_drop(self) -> bool {
        match self {
            FieldValueRepr::Undefined
            | FieldValueRepr::Null
            | FieldValueRepr::Int
            | FieldValueRepr::Float
            | FieldValueRepr::FieldReference
            | FieldValueRepr::SlicedFieldReference
            | FieldValueRepr::StreamValueId
            | FieldValueRepr::TextInline
            | FieldValueRepr::BytesInline => false,
            FieldValueRepr::BigInt
            | FieldValueRepr::Rational
            | FieldValueRepr::Error
            | FieldValueRepr::TextBuffer
            | FieldValueRepr::TextFile
            | FieldValueRepr::BytesBuffer
            | FieldValueRepr::BytesFile
            | FieldValueRepr::Object
            | FieldValueRepr::Array
            | FieldValueRepr::Custom => true,
        }
    }
    #[inline(always)]
    pub fn needs_alignment(self) -> bool {
        !self.is_zst() && !self.is_variable_sized_type()
    }
    #[inline]
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
            unsafe { ptr.sub(ptr as usize & MAX_FIELD_ALIGN) }
        } else {
            ptr
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr_up(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            unsafe {
                ptr.sub((ptr as usize + MAX_FIELD_ALIGN) & MAX_FIELD_ALIGN)
            }
        } else {
            ptr
        }
    }
    pub fn size(self) -> usize {
        match self {
            FieldValueRepr::Undefined => size_of::<Undefined>(),
            FieldValueRepr::Null => size_of::<Null>(),
            FieldValueRepr::Int => size_of::<i64>(),
            FieldValueRepr::BigInt => size_of::<BigInt>(),
            FieldValueRepr::Float => size_of::<f64>(),
            FieldValueRepr::Rational => size_of::<BigRational>(),
            FieldValueRepr::StreamValueId => size_of::<StreamValueId>(),
            FieldValueRepr::FieldReference => size_of::<FieldReference>(),
            FieldValueRepr::SlicedFieldReference => {
                size_of::<SlicedFieldReference>()
            }
            FieldValueRepr::Error => size_of::<OperatorApplicationError>(),
            FieldValueRepr::TextBuffer => size_of::<String>(),
            FieldValueRepr::TextFile => size_of::<TextBufferFile>(),
            FieldValueRepr::BytesBuffer => size_of::<Vec<u8>>(),
            FieldValueRepr::BytesFile => size_of::<BytesBufferFile>(),
            FieldValueRepr::Object => size_of::<Object>(),
            FieldValueRepr::Array => size_of::<Array>(),
            FieldValueRepr::Custom => size_of::<CustomDataBox>(),
            // should not be used for size calculations
            // but is used for example in is_zst
            FieldValueRepr::TextInline | FieldValueRepr::BytesInline => {
                usize::MAX
            }
        }
    }
    pub fn is_variable_sized_type(self) -> bool {
        self == FieldValueRepr::BytesInline
            || self == FieldValueRepr::TextInline
    }
    pub fn is_zst(self) -> bool {
        self.size() == 0
    }
    pub fn is_fixed_size_type(self) -> bool {
        !self.is_variable_sized_type() && !self.is_zst()
    }
    pub unsafe fn from_u8(v: u8) -> FieldValueRepr {
        unsafe { std::mem::transmute(v) }
    }
    pub const fn to_str(&self) -> &'static str {
        match self {
            FieldValueRepr::Undefined => "undefined",
            FieldValueRepr::Null => "null",
            FieldValueRepr::Int => "int",
            FieldValueRepr::BigInt => "integer",
            FieldValueRepr::Float => "float",
            FieldValueRepr::Rational => "rational",
            FieldValueRepr::StreamValueId => "stream_value_id",
            FieldValueRepr::FieldReference => "field_reference",
            FieldValueRepr::SlicedFieldReference => "sliced_field_reference",
            FieldValueRepr::Error => "error",
            FieldValueRepr::TextInline => "text_inline",
            FieldValueRepr::TextBuffer => "text_buffer",
            FieldValueRepr::TextFile => "text_file",
            FieldValueRepr::BytesInline => "bytes_inline",
            FieldValueRepr::BytesBuffer => "bytes_buffer",
            FieldValueRepr::BytesFile => "bytes_file",
            FieldValueRepr::Object => "object",
            FieldValueRepr::Array => "array",
            FieldValueRepr::Custom => "custom",
        }
    }
    pub const fn kind(&self) -> Option<FieldValueKind> {
        Some(match self {
            FieldValueRepr::Undefined => FieldValueKind::Undefined,
            FieldValueRepr::Null => FieldValueKind::Null,
            FieldValueRepr::Int => FieldValueKind::Int,
            FieldValueRepr::BigInt => FieldValueKind::BigInt,
            FieldValueRepr::Float => FieldValueKind::Float,
            FieldValueRepr::Rational => FieldValueKind::Rational,
            FieldValueRepr::StreamValueId => FieldValueKind::StreamValueId,
            FieldValueRepr::FieldReference => FieldValueKind::FieldReference,
            FieldValueRepr::SlicedFieldReference => {
                FieldValueKind::SlicedFieldReference
            }
            FieldValueRepr::Error => FieldValueKind::Error,
            FieldValueRepr::TextInline | FieldValueRepr::TextBuffer => {
                FieldValueKind::Text
            }
            FieldValueRepr::BytesInline | FieldValueRepr::BytesBuffer => {
                FieldValueKind::Bytes
            }
            FieldValueRepr::TextFile => FieldValueKind::Text,
            FieldValueRepr::BytesFile => FieldValueKind::Bytes,
            FieldValueRepr::Object => FieldValueKind::Object,
            FieldValueRepr::Array => FieldValueKind::Array,
            FieldValueRepr::Custom => FieldValueKind::Custom,
        })
    }
    pub fn to_format(&self) -> FieldValueFormat {
        FieldValueFormat {
            repr: *self,
            flags: field_value_flags::DEFAULT,
            size: if self.is_variable_sized_type() {
                0
            } else {
                self.size() as FieldValueSize
            },
        }
    }
}

impl Display for FieldValueRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl From<field_value_flags::FieldValueFlags> for FieldValueFlagsDebugRepr {
    fn from(value: field_value_flags::FieldValueFlags) -> Self {
        let h = FieldValueFormat {
            flags: value,
            ..Default::default()
        };
        Self {
            padding: h.leading_padding() as u8,
            deleted: h.deleted(),
            shared_value: h.shared_value(),
            same_as_prev: h.same_value_as_previous(),
        }
    }
}

impl std::fmt::Debug for FieldValueFlagsDebugRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flags")
            .field("pad", &usize::from(self.padding))
            .field("del", &usize::from(self.deleted))
            .field("shared_val", &usize::from(self.shared_value))
            .field("same_as_prev", &usize::from(self.same_as_prev))
            .finish()
    }
}

impl std::fmt::Debug for FieldValueFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldValueFormat")
            .field("repr", &self.repr)
            .field("size", &self.size)
            .field("flags", &FieldValueFlagsDebugRepr::from(self.flags))
            .finish()
    }
}

impl Default for FieldValueFormat {
    fn default() -> Self {
        Self {
            repr: FieldValueRepr::Null,
            flags: field_value_flags::DEFAULT,
            size: 0,
        }
    }
}

impl FieldValueFormat {
    #[inline(always)]
    pub fn shared_value(self) -> bool {
        self.flags & field_value_flags::SHARED_VALUE != 0
    }
    #[inline(always)]
    pub fn set_shared_value(&mut self, val: bool) {
        self.flags &= !field_value_flags::SHARED_VALUE;
        self.flags |= FieldValueFlags::from(val)
            << field_value_flags::SHARED_VALUE_OFFSET;
    }

    #[inline(always)]
    pub fn set_shared_value_if_rl_1(&mut self, rl: RunLength) {
        if rl == 1 {
            self.set_shared_value(true);
        }
    }
    #[inline(always)]
    pub fn leading_padding(self) -> usize {
        (self.flags & field_value_flags::LEADING_PADDING) as usize
    }
    pub fn set_leading_padding(&mut self, val: usize) {
        debug_assert!(val < MAX_FIELD_ALIGN);
        self.flags &= !field_value_flags::LEADING_PADDING;
        self.flags |= (val as u8) & field_value_flags::LEADING_PADDING;
    }
    #[inline(always)]
    pub fn deleted(self) -> bool {
        self.flags & field_value_flags::DELETED != 0
    }
    pub fn references_alive_data(self) -> bool {
        !self.deleted() && !self.repr.is_zst()
    }
    pub fn set_deleted(&mut self, val: bool) {
        self.flags &= !field_value_flags::DELETED;
        self.flags |=
            FieldValueFlags::from(val) << field_value_flags::DELETED_OFFSET;
    }
    #[inline(always)]
    pub fn same_value_as_previous(self) -> bool {
        self.flags & field_value_flags::SAME_VALUE_AS_PREVIOUS != 0
    }
    #[inline(always)]
    pub fn set_same_value_as_previous(&mut self, val: bool) {
        self.flags &= !field_value_flags::SAME_VALUE_AS_PREVIOUS;
        self.flags |= FieldValueFlags::from(val)
            << field_value_flags::SAME_VALUE_AS_PREVIOUS_OFFSET;
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
    pub fn set_shared_value_if_rl_1(&mut self) {
        self.fmt.set_shared_value_if_rl_1(self.run_length);
    }
    pub fn unique_data_element_count(&self) -> RunLength {
        if self.shared_value() {
            RunLength::from(!self.same_value_as_previous())
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
    pub fn data_size_unique(&self) -> usize {
        if self.same_value_as_previous() {
            0
        } else {
            self.data_size()
        }
    }
    pub fn total_size(&self) -> usize {
        self.data_size() + self.leading_padding()
    }
    pub fn total_size_unique(&self) -> usize {
        if self.same_value_as_previous() {
            0
        } else {
            self.total_size()
        }
    }
    pub fn run_len_rem(&self) -> RunLength {
        RunLength::MAX - self.run_length
    }
    pub fn effective_run_length(&self) -> RunLength {
        if self.deleted() {
            0
        } else {
            self.run_length
        }
    }
    pub fn is_compatible(&self, other: FieldValueFormat) -> bool {
        const RELEVANT_FLAGS: field_value_flags::FieldValueFlags =
            field_value_flags::DELETED;

        self.repr == other.repr
            && self.size == other.size
            && self.flags & RELEVANT_FLAGS == other.flags & RELEVANT_FLAGS
    }
}

impl Clone for FieldData {
    fn clone(&self) -> Self {
        let mut fd = Self {
            data: FieldDataBuffer::with_capacity(self.data.len()),
            headers: VecDeque::with_capacity(self.headers.len()),
            field_count: 0, // set by copy
        };
        let fd_ref = &mut fd;
        let mut iter = self.iter();
        FieldData::copy(&mut iter, &mut |f| f(fd_ref));
        fd
    }
}

impl FieldData {
    pub unsafe fn from_raw_parts(
        header: VecDeque<FieldValueHeader>,
        data: FieldDataBuffer,
        field_count: usize,
    ) -> Self {
        Self {
            headers: header,
            data,
            field_count,
        }
    }
    pub fn clear(&mut self) {
        let (d1, d2) = self.data.as_slices_mut();
        let l1 = d1.len();
        let (d1, d2) = (d1.as_mut_ptr(), d2.as_mut_ptr());
        let mut iter = self.iter();
        loop {
            let slice_start_pos = iter.get_next_field_data();
            let Some(range) = iter.typed_range_fwd(usize::MAX, 0) else {
                break;
            };
            unsafe {
                let kind = range.data.repr();
                let len = range.data.len();
                FieldValueSlice::drop_from_kind(
                    if slice_start_pos < l1 {
                        d1.add(slice_start_pos)
                    } else {
                        d2.add(slice_start_pos - l1)
                    },
                    len,
                    kind,
                );
            }
        }
        self.field_count = 0;
        self.headers.clear();
        self.data.clear();
    }

    pub unsafe fn pad_to_align(&mut self) -> usize {
        // TODO: we should really only align to the target align of the field
        // e.g. field references are 2 bytes, no need to align to 8
        let mut align = self.data.len() % MAX_FIELD_ALIGN;
        if align == 0 {
            return 0;
        }
        align = MAX_FIELD_ALIGN - align;
        self.data.resize(self.data.len() + align, 0);
        align
    }

    // this is technically safe, but will leak unless paired with a
    // header that matches the contained type ranges (which itself is not safe)
    pub fn append_data_to(&self, target: &mut FieldDataBuffer) {
        let mut iter = self.iter();
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DEFAULT)
        {
            unsafe { append_data(tr.data, &mut |f| f(target)) };
        }
    }

    pub unsafe fn copy_data<'a>(
        mut iter: impl FieldIterator<'a>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut fields_copied = 0;
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DEFAULT)
        {
            unsafe {
                append_data(tr.data, &mut |f: &mut dyn Fn(
                    &mut FieldDataBuffer,
                )| {
                    targets_applicator(&mut |fd| f(&mut fd.data))
                })
            };
            fields_copied += tr.field_count;
        }
        fields_copied
    }
    pub fn append_from_other(&mut self, other: &FieldData) -> usize {
        let mut iter = other.iter();
        Self::copy(&mut iter, &mut |f| f(self))
    }
    pub fn append_from_iter<'a>(
        &mut self,
        iter: &mut impl FieldIterator<'a>,
    ) -> usize {
        Self::copy(iter, &mut |f| f(self))
    }
    pub fn copy<'a>(
        iter: &mut impl FieldIterator<'a>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DELETED)
        {
            copied_fields += tr.field_count;
            targets_applicator(&mut |fd| {
                let first_header_idx = fd.headers.len();
                fd.headers.extend(tr.headers);
                if tr.headers[0].repr.needs_alignment() {
                    let align = unsafe { fd.pad_to_align() };
                    fd.headers[first_header_idx].set_leading_padding(align);
                }
                fd.headers[first_header_idx].run_length -=
                    tr.first_header_run_length_oversize;
            });
            unsafe {
                append_data(tr.data, &mut |f: &mut dyn Fn(
                    &mut FieldDataBuffer,
                )| {
                    targets_applicator(&mut |fd| f(&mut fd.data))
                })
            };
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }

    pub fn copy_resolve_refs<'a, I: FieldIterator<'a>>(
        match_set_mgr: &mut MatchSetManager,
        iter: &mut AutoDerefIter<'a, I>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        // by setting the deleted flag here, we can avoid copying deleted
        // records
        while let Some(tr) = iter.typed_range_fwd(
            match_set_mgr,
            usize::MAX,
            field_value_flags::DELETED,
        ) {
            copied_fields += tr.base.field_count;
            if tr.refs.is_none() {
                targets_applicator(&mut |fd| {
                    let first_header_idx = fd.headers.len();
                    fd.headers.extend(tr.base.headers);
                    if tr.base.headers[0].repr.needs_alignment() {
                        let align = unsafe { fd.pad_to_align() };
                        fd.headers[first_header_idx]
                            .set_leading_padding(align);
                    }
                    fd.headers[first_header_idx].run_length -=
                        tr.base.first_header_run_length_oversize;
                });
                unsafe {
                    append_data(tr.base.data, &mut |f: &mut dyn Fn(
                        &mut FieldDataBuffer,
                    )| {
                        targets_applicator(&mut |fd| f(&mut fd.data))
                    })
                };
            } else {
                match tr.base.data {
                    FieldValueSlice::BytesInline(data) => {
                        for (v, rl, _offset) in
                            RefAwareInlineBytesIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                // PERF: maybe do a little rle here?
                                fd.headers.push_back(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        repr: FieldValueRepr::BytesInline,
                                        flags: SHARED_VALUE,
                                        size: v.len() as FieldValueSize,
                                    },
                                    run_length: rl,
                                });
                                fd.data.extend_from_slice(v);
                            });
                        }
                    }
                    FieldValueSlice::TextInline(data) => {
                        for (v, rl, _offset) in
                            RefAwareInlineTextIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                // PERF: maybe do a little rle here?
                                fd.headers.push_back(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        repr: FieldValueRepr::TextInline,
                                        flags: SHARED_VALUE,
                                        size: v.len() as FieldValueSize,
                                    },
                                    run_length: rl,
                                });
                                fd.data.extend_from_slice(v.as_bytes());
                            });
                        }
                    }
                    FieldValueSlice::BytesBuffer(data) => {
                        for (v, rl, _offset) in
                            RefAwareBytesBufferIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_bytes(v, rl as usize, true, false);
                            });
                        }
                    }
                    FieldValueSlice::TextBuffer(data) => {
                        for (v, rl, _offset) in
                            RefAwareTextBufferIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_str(v, rl as usize, true, false);
                            });
                        }
                    }
                    FieldValueSlice::BigInt(data) => {
                        for (v, rl) in
                            RefAwareFieldValueSliceIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_big_int(
                                    v.clone(),
                                    rl as usize,
                                    true,
                                    false,
                                );
                            });
                        }
                    }
                    FieldValueSlice::Rational(data) => {
                        for (v, rl) in
                            RefAwareFieldValueSliceIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_rational(
                                    v.clone(),
                                    rl as usize,
                                    true,
                                    false,
                                );
                            });
                        }
                    }
                    // TODO: do we have to worry about internal field
                    // references?
                    FieldValueSlice::Object(data) => {
                        for (v, rl) in
                            RefAwareFieldValueSliceIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_object(
                                    v.clone(),
                                    rl as usize,
                                    true,
                                    false,
                                );
                            });
                        }
                    }
                    FieldValueSlice::Array(data) => {
                        for (v, rl) in
                            RefAwareFieldValueSliceIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_array(
                                    v.clone(),
                                    rl as usize,
                                    true,
                                    false,
                                );
                            });
                        }
                    }
                    FieldValueSlice::Custom(data) => {
                        for (v, rl) in
                            RefAwareFieldValueSliceIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_custom(
                                    v.clone_dyn(),
                                    rl as usize,
                                    true,
                                    false,
                                );
                            });
                        }
                    }
                    FieldValueSlice::Error(data) => {
                        for (v, rl) in
                            RefAwareFieldValueSliceIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_error(
                                    v.clone(),
                                    rl as usize,
                                    true,
                                    false,
                                );
                            });
                        }
                    }
                    // these types don't support field references
                    FieldValueSlice::Undefined(_)
                    | FieldValueSlice::Null(_)
                    | FieldValueSlice::Int(_)
                    | FieldValueSlice::Float(_)
                    | FieldValueSlice::StreamValueId(_)
                    | FieldValueSlice::FieldReference(_)
                    | FieldValueSlice::SlicedFieldReference(_) => {
                        unreachable!();
                    }
                }
            }
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }
    #[allow(clippy::iter_not_returning_iterator)]
    pub fn iter(&self) -> Iter<'_, &'_ FieldData> {
        Iter::from_start(self)
    }
    pub unsafe fn internals_mut(&mut self) -> FieldDataInternalsMut {
        FieldDataInternalsMut {
            data: &mut self.data,
            header: &mut self.headers,
            field_count: &mut self.field_count,
        }
    }
    pub fn internals(&self) -> FieldDataInternals {
        FieldDataInternals {
            data: &self.data,
            header: &self.headers,
            field_count: &self.field_count,
        }
    }
    pub fn field_count(&self) -> usize {
        self.field_count
    }
}

#[inline(always)]
unsafe fn extend_with_clones<T: Clone>(
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
    src: &[T],
) {
    let src_size = size_of_val(src);
    target_applicator(&mut |data| {
        data.reserve(src_size);
        for v in src {
            // PERF: this will do a `reserve_contiguous` on each iteration
            // we should be able to do better than this
            data.extend_from_slice(unsafe {
                value_as_bytes(&ManuallyDrop::new(v.clone()))
            });
        }
    });
}

#[inline(always)]
unsafe fn extend_raw<T: Sized + Copy>(
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
    src: &[T],
) {
    let src_bytes = unsafe {
        std::slice::from_raw_parts(src.as_ptr().cast(), size_of_val(src))
    };
    target_applicator(&mut |data| data.extend_from_slice(src_bytes));
}

#[inline(always)]
unsafe fn append_data(
    ts: FieldValueSlice<'_>,
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
) {
    unsafe {
        match ts {
            FieldValueSlice::Null(_) | FieldValueSlice::Undefined(_) => (),
            FieldValueSlice::Int(v) => extend_raw(target_applicator, v),
            FieldValueSlice::BigInt(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::Float(v) => extend_raw(target_applicator, v),
            FieldValueSlice::Rational(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::StreamValueId(v) => {
                extend_raw(target_applicator, v)
            }
            FieldValueSlice::FieldReference(v) => {
                extend_raw(target_applicator, v)
            }
            FieldValueSlice::SlicedFieldReference(v) => {
                extend_raw(target_applicator, v)
            }
            FieldValueSlice::BytesInline(v) => {
                extend_raw(target_applicator, v)
            }
            FieldValueSlice::TextInline(v) => {
                extend_raw(target_applicator, v.as_bytes())
            }
            FieldValueSlice::TextBuffer(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::BytesBuffer(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::Error(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::Object(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::Array(v) => {
                extend_with_clones(target_applicator, v)
            }
            FieldValueSlice::Custom(v) => {
                extend_with_clones(target_applicator, v)
            }
        };
    }
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Drop for FieldData {
    fn drop(&mut self) {
        self.clear()
    }
}
