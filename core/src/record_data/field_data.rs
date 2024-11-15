use super::{
    array::Array,
    custom_data::CustomDataBox,
    field_value::{
        FieldReference, FieldValueKind, Null, Object, SlicedFieldReference,
        Undefined,
    },
    field_value_ref::value_as_bytes,
    iter::{
        field_iterator::FieldIterOpts,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
        },
    },
    match_set::MatchSetManager,
    varying_type_inserter::VaryingTypeInserter,
};
use crate::{
    cli::call_expr::Argument,
    operators::{errors::OperatorApplicationError, macro_def::MacroRef},
    utils::ringbuf::RingBuf,
};
use metamatch::metamatch;
use std::{
    any::Any,
    collections::VecDeque,
    fmt::{Debug, Display},
    mem::{align_of, size_of_val, ManuallyDrop},
};

use self::field_value_flags::SHARED_VALUE;
pub use field_value_flags::FieldValueFlags;
use num::{BigInt, BigRational};

use super::{
    field_value_ref::FieldValueSlice,
    iter::{field_iter::FieldIter, field_iterator::FieldIterator},
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
    BigRational,
    TextInline,
    TextBuffer,
    BytesInline,
    BytesBuffer,
    Object,
    Array,
    Custom,
    Error,
    Macro,
    Argument,
    StreamValueId,
    FieldReference,
    SlicedFieldReference,
    // ENHANCE //PERF (maybe): CustomDynamicLength,
    // CustomDynamicLengthAligned (store some subtype index at the start
    // of the actual data)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FieldValueFormat {
    pub repr: FieldValueRepr,
    pub flags: FieldValueFlags,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    pub size: FieldValueSize,
}

#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::DerefMut,
)]
pub struct FieldValueHeader {
    #[deref]
    #[deref_mut]
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
    use static_assertions::const_assert;

    use super::MAX_FIELD_ALIGN;
    pub type FieldValueFlags = u8;
    // offset must be zero so we don't have to shift
    const_assert!(MAX_FIELD_ALIGN.is_power_of_two() && MAX_FIELD_ALIGN <= 16);
    // consumes offsets 0 through 3
    // Storing this (instead of computing it from the kind & position)
    // is necessary because otherwise we couldn't iterate backwards.
    // We wouldn't know by how much the type before an element had to be padded
    // to make the successor aligned.
    pub const LEADING_PADDING_BIT_COUNT: FieldValueFlags = 4;
    pub const LEADING_PADDING_MASK: FieldValueFlags = FieldValueFlags::MAX
        >> (FieldValueFlags::BITS - LEADING_PADDING_BIT_COUNT as u32);
    pub const LEADING_PADDING_OFFSET: FieldValueFlags = 0;
    pub const SAME_VALUE_AS_PREVIOUS_OFFSET: FieldValueFlags = 4;
    pub const SHARED_VALUE_OFFSET: FieldValueFlags = 5;
    pub const DELETED_OFFSET: FieldValueFlags = 7;
    // MUST be set for RL=1. MUST be set for ZSTs. // TODO: normalize this
    pub const SHARED_VALUE: FieldValueFlags = 1 << SHARED_VALUE_OFFSET;
    pub const DELETED: FieldValueFlags = 1 << DELETED_OFFSET;
    // share data with previous header (potentially across ZSTs)
    // leading padding must be zero if this is set
    pub const SAME_VALUE_AS_PREVIOUS: FieldValueFlags =
        1 << SAME_VALUE_AS_PREVIOUS_OFFSET;

    pub const DEFAULT: FieldValueFlags = 0;
    pub const NONE: FieldValueFlags = 0;

    pub fn padding(pad: usize) -> FieldValueFlags {
        (pad as u8) << LEADING_PADDING_OFFSET
    }
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
    const KIND: FieldValueKind;
    const FIELD_VALUE_BOXED: bool = false;
    const TRIVIALLY_COPYABLE: bool = false;
    const DST: bool = true;
    const ZST: bool = false;
    const SIZE: usize;
    const ALIGN: usize;
}
pub unsafe trait VariableSizedFieldValueType: FieldValueType {}
pub unsafe trait FixedSizeFieldValueType:
    Clone + PartialEq + Any + Sized
{
    const REPR: FieldValueRepr;
    const KIND: FieldValueKind;
    const FIELD_VALUE_BOXED: bool = false;
    const TRIVIALLY_COPYABLE: bool = false;
    const ZST: bool = std::mem::size_of::<Self>() == 0;
}
unsafe impl<T: FixedSizeFieldValueType> FieldValueType for T {
    const DST: bool = false;
    const REPR: FieldValueRepr = Self::REPR;
    const KIND: FieldValueKind = Self::KIND;
    const FIELD_VALUE_BOXED: bool = Self::FIELD_VALUE_BOXED;
    const TRIVIALLY_COPYABLE: bool = Self::TRIVIALLY_COPYABLE;
    const ZST: bool = Self::ZST;
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

unsafe impl FieldValueType for [u8] {
    const KIND: FieldValueKind = FieldValueKind::Bytes;
    const REPR: FieldValueRepr = FieldValueRepr::BytesInline;
    const TRIVIALLY_COPYABLE: bool = true;
    const SIZE: usize = usize::MAX;
    const ALIGN: usize = 0;
}
unsafe impl VariableSizedFieldValueType for [u8] {}

unsafe impl FieldValueType for str {
    const KIND: FieldValueKind = FieldValueKind::Text;
    const REPR: FieldValueRepr = FieldValueRepr::TextInline;
    const TRIVIALLY_COPYABLE: bool = true;
    const SIZE: usize = usize::MAX;
    const ALIGN: usize = 0;
}
unsafe impl VariableSizedFieldValueType for str {}

unsafe impl FixedSizeFieldValueType for Undefined {
    const REPR: FieldValueRepr = FieldValueRepr::Null;
    const KIND: FieldValueKind = FieldValueKind::Null;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for Null {
    const REPR: FieldValueRepr = FieldValueRepr::Undefined;
    const KIND: FieldValueKind = FieldValueKind::Undefined;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for i64 {
    const REPR: FieldValueRepr = FieldValueRepr::Int;
    const KIND: FieldValueKind = FieldValueKind::Int;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for f64 {
    const REPR: FieldValueRepr = FieldValueRepr::Float;
    const KIND: FieldValueKind = FieldValueKind::Float;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for StreamValueId {
    const REPR: FieldValueRepr = FieldValueRepr::StreamValueId;
    const KIND: FieldValueKind = FieldValueKind::StreamValueId;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for FieldReference {
    const REPR: FieldValueRepr = FieldValueRepr::FieldReference;
    const KIND: FieldValueKind = FieldValueKind::FieldReference;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for SlicedFieldReference {
    const REPR: FieldValueRepr = FieldValueRepr::SlicedFieldReference;
    const KIND: FieldValueKind = FieldValueKind::SlicedFieldReference;
    const TRIVIALLY_COPYABLE: bool = true;
}
unsafe impl FixedSizeFieldValueType for OperatorApplicationError {
    const REPR: FieldValueRepr = FieldValueRepr::Error;
    const KIND: FieldValueKind = FieldValueKind::Error;
}
unsafe impl FixedSizeFieldValueType for Vec<u8> {
    const REPR: FieldValueRepr = FieldValueRepr::BytesBuffer;
    const KIND: FieldValueKind = FieldValueKind::Bytes;
}
unsafe impl FixedSizeFieldValueType for String {
    const REPR: FieldValueRepr = FieldValueRepr::TextBuffer;
    const KIND: FieldValueKind = FieldValueKind::Text;
}
unsafe impl FixedSizeFieldValueType for Object {
    const REPR: FieldValueRepr = FieldValueRepr::Object;
    const KIND: FieldValueKind = FieldValueKind::Object;
    const FIELD_VALUE_BOXED: bool = true;
}
unsafe impl FixedSizeFieldValueType for Array {
    const REPR: FieldValueRepr = FieldValueRepr::Array;
    const KIND: FieldValueKind = FieldValueKind::Array;
}
unsafe impl FixedSizeFieldValueType for BigInt {
    const REPR: FieldValueRepr = FieldValueRepr::BigInt;
    const KIND: FieldValueKind = FieldValueKind::Int;
    const FIELD_VALUE_BOXED: bool = true;
}
unsafe impl FixedSizeFieldValueType for BigRational {
    const REPR: FieldValueRepr = FieldValueRepr::BigRational;
    const KIND: FieldValueKind = FieldValueKind::Float;
    const FIELD_VALUE_BOXED: bool = true;
}
unsafe impl FixedSizeFieldValueType for Argument {
    const REPR: FieldValueRepr = FieldValueRepr::Argument;
    const KIND: FieldValueKind = FieldValueKind::Argument;
    const FIELD_VALUE_BOXED: bool = true;
}
unsafe impl FixedSizeFieldValueType for MacroRef {
    const REPR: FieldValueRepr = FieldValueRepr::Macro;
    const KIND: FieldValueKind = FieldValueKind::Macro;
}
unsafe impl FixedSizeFieldValueType for CustomDataBox {
    const REPR: FieldValueRepr = FieldValueRepr::Custom;
    const KIND: FieldValueKind = FieldValueKind::Custom;
    // this does intentionally **not** set `FIELD_VALUE_BOXED`, because the
    // type itself is already the box
}
pub const INLINE_STR_MAX_LEN: usize = 8192;

pub trait WithFieldValueType<R> {
    fn call<T: FieldValueType + ?Sized>(&mut self) -> R;
}

impl FieldValueRepr {
    pub fn with_repr_t<R>(
        &self,
        mut callable: impl WithFieldValueType<R>,
    ) -> R {
        metamatch!(match self {
            #[expand((REP, T) in [
                (Null, Null),
                (Undefined, Undefined),
                (Int, i64),
                (BigInt, BigInt),
                (Float, f64),
                (BigRational, BigRational),
                (TextInline, str),
                (TextBuffer, String),
                (BytesInline, [u8]),
                (BytesBuffer, Vec<u8>),
                (Object, Object),
                (Array, Array),
                (Custom, CustomDataBox),
                (Error, OperatorApplicationError),
                (StreamValueId, StreamValueId),
                (FieldReference, FieldReference),
                (SlicedFieldReference, SlicedFieldReference),
                (Macro, MacroRef),
                (Argument, Argument),
            ])]
            FieldValueRepr::REP => callable.call::<T>(),
        })
    }

    pub fn is_trivially_copyable(self) -> bool {
        struct TriviallyCopyable;
        impl WithFieldValueType<bool> for TriviallyCopyable {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> bool {
                T::TRIVIALLY_COPYABLE
            }
        }
        self.with_repr_t(TriviallyCopyable)
    }
    pub fn needs_drop(self) -> bool {
        !self.is_trivially_copyable()
    }
    pub fn size(self) -> usize {
        struct Align;
        impl WithFieldValueType<usize> for Align {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> usize {
                T::SIZE
            }
        }
        self.with_repr_t(Align)
    }
    pub fn required_alignment(&self) -> usize {
        struct Align;
        impl WithFieldValueType<usize> for Align {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> usize {
                T::ALIGN
            }
        }
        self.with_repr_t(Align)
    }
    pub fn required_padding(&self, size_before: usize) -> usize {
        let align = self.required_alignment();
        if align == 0 {
            return 0;
        };
        let rem = size_before % align;
        if rem == 0 {
            return 0;
        }
        align - rem
    }
    pub fn align_size_up(&self, size_before: usize) -> usize {
        size_before + self.required_padding(size_before)
    }
    #[inline(always)]
    pub fn needs_alignment(self) -> bool {
        self.required_alignment() != 0
    }

    pub fn is_dst(self) -> bool {
        struct Dst;
        impl WithFieldValueType<bool> for Dst {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> bool {
                T::DST
            }
        }
        self.with_repr_t(Dst)
    }
    pub fn is_zst(self) -> bool {
        struct Zst;
        impl WithFieldValueType<bool> for Zst {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> bool {
                T::ZST
            }
        }
        self.with_repr_t(Zst)
    }
    pub fn is_fixed_size_type(self) -> bool {
        !self.is_dst() && !self.is_zst()
    }
    pub fn is_field_value_boxed(self) -> bool {
        struct FieldValueBoxed;
        impl WithFieldValueType<bool> for FieldValueBoxed {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> bool {
                T::FIELD_VALUE_BOXED
            }
        }
        self.with_repr_t(FieldValueBoxed)
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
            FieldValueRepr::BigRational => "rational",
            FieldValueRepr::StreamValueId => "stream_value_id",
            FieldValueRepr::FieldReference => "field_reference",
            FieldValueRepr::SlicedFieldReference => "sliced_field_reference",
            FieldValueRepr::Error => "error",
            FieldValueRepr::TextInline => "text_inline",
            FieldValueRepr::TextBuffer => "text_buffer",
            FieldValueRepr::BytesInline => "bytes_inline",
            FieldValueRepr::BytesBuffer => "bytes_buffer",
            FieldValueRepr::Object => "object",
            FieldValueRepr::Array => "array",
            FieldValueRepr::Argument => "argument",
            FieldValueRepr::Macro => "macro",
            FieldValueRepr::Custom => "custom",
        }
    }
    pub fn kind(&self) -> FieldValueKind {
        struct Kind;
        impl WithFieldValueType<FieldValueKind> for Kind {
            fn call<T: FieldValueType + ?Sized>(&mut self) -> FieldValueKind {
                T::KIND
            }
        }
        self.with_repr_t(Kind)
    }
    pub fn to_format(&self) -> FieldValueFormat {
        FieldValueFormat {
            repr: *self,
            flags: field_value_flags::DEFAULT,
            size: if self.is_dst() {
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
    pub fn set_shared_value_if_zst(&mut self) {
        if self.repr.is_zst() {
            self.set_shared_value(true);
        }
    }
    pub fn normalize_shared_value(&mut self, rl: RunLength) {
        if rl == 1 || self.repr.is_zst() {
            self.set_shared_value(true);
        }
    }

    #[inline(always)]
    pub fn leading_padding(self) -> usize {
        (self.flags & field_value_flags::LEADING_PADDING_MASK) as usize
    }
    pub fn set_leading_padding(&mut self, val: usize) {
        self.flags &= !field_value_flags::LEADING_PADDING_MASK;
        self.flags |= (val as u8) & field_value_flags::LEADING_PADDING_MASK;
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

impl FieldValueHeader {
    pub fn shared_value_or_rl_one(self) -> bool {
        self.run_length == 1 || self.shared_value()
    }
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
    pub fn is_type_compatible(&self, other: FieldValueFormat) -> bool {
        self.repr == other.repr && self.size == other.size
    }
    // Chec whether we can run length append a new value
    // (run length 1, ignoring shared flag, and assumed to not be the same
    // value as this header) to this header.
    pub fn is_value_appendable(&self, other: FieldValueFormat) -> bool {
        let type_compatible = self.is_type_compatible(other);

        let same_deleted_status = self.flags & field_value_flags::DELETED
            == other.flags & field_value_flags::DELETED;

        let new_value_appending_to_same_as_prev =
            self.same_value_as_previous() && !other.same_value_as_previous();

        type_compatible
            && same_deleted_status
            && !new_value_appending_to_same_as_prev
            && !self.shared_value_and_rl_not_one()
    }

    pub fn shared_value_and_rl_not_one(&self) -> bool {
        self.shared_value() && self.run_length != 1
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
            let Some(range) =
                iter.typed_range_fwd(usize::MAX, FieldIterOpts::default())
            else {
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

    pub unsafe fn pad_to_align(&mut self, repr: FieldValueRepr) -> usize {
        let field_align = repr.required_alignment();
        if field_align == 0 {
            return 0;
        }
        let mut curr_align = self.data.len() % field_align;
        if curr_align == 0 {
            return 0;
        }
        curr_align = field_align - curr_align;
        self.data.resize(self.data.len() + curr_align, 0);
        curr_align
    }

    // this is technically safe, but will leak unless paired with a
    // header that matches the contained type ranges (which itself is not safe)
    pub fn append_data_to(&self, target: &mut FieldDataBuffer) {
        let mut iter = self.iter();
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, FieldIterOpts::default())
        {
            unsafe { append_data(tr.data, &mut |f| f(target)) };
        }
    }

    pub unsafe fn copy_data(
        mut iter: impl FieldIterator,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut fields_copied = 0;
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, FieldIterOpts::default())
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
    pub fn append_from_iter(
        &mut self,
        iter: &mut impl FieldIterator,
    ) -> usize {
        Self::copy(iter, &mut |f| f(self))
    }
    pub fn copy(
        iter: &mut impl FieldIterator,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, FieldIterOpts::default())
        {
            copied_fields += tr.field_count;
            targets_applicator(&mut |fd| {
                let first_header_idx = fd.headers.len();
                fd.headers.extend(tr.headers);
                let align = unsafe { fd.pad_to_align(tr.headers[0].repr) };
                fd.headers[first_header_idx].set_leading_padding(align);
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

    pub fn copy_resolve_refs<I: FieldIterator>(
        match_set_mgr: &mut MatchSetManager,
        iter: &mut AutoDerefIter<I>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        // by setting the deleted flag here, we can avoid copying deleted
        // records
        while let Some(tr) = iter.typed_range_fwd(
            match_set_mgr,
            usize::MAX,
            FieldIterOpts::default(),
        ) {
            copied_fields += tr.base.field_count;
            if tr.refs.is_none() {
                targets_applicator(&mut |fd| {
                    let first_header_idx = fd.headers.len();
                    fd.headers.extend(tr.base.headers);
                    let align =
                        unsafe { fd.pad_to_align(tr.base.headers[0].repr) };
                    fd.headers[first_header_idx].set_leading_padding(align);
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
                continue;
            }
            metamatch!(match tr.base.data {
                #[expand(REP in [Undefined, Null])]
                FieldValueSlice::REP(count) => {
                    targets_applicator(&mut |fd| {
                        fd.push_zst(
                            <REP as FixedSizeFieldValueType>::REPR,
                            count,
                            true,
                        )
                    });
                }

                #[expand((REP, ITER, VAL) in [
                    (TextInline, RefAwareInlineTextIter, v.as_bytes()),
                    (BytesInline, RefAwareInlineBytesIter, v),
                ])]
                FieldValueSlice::REP(data) => {
                    for (v, rl, _offset) in ITER::from_range(&tr, data) {
                        targets_applicator(&mut |fd| {
                            // PERF: maybe do a little rle here?
                            fd.headers.push_back(FieldValueHeader {
                                fmt: FieldValueFormat {
                                    repr: FieldValueRepr::REP,
                                    flags: SHARED_VALUE,
                                    size: v.len() as FieldValueSize,
                                },
                                run_length: rl,
                            });
                            fd.data.extend_from_slice(VAL);
                        });
                    }
                }

                #[expand((REP, ITER, PUSH_FN) in [
                    (TextBuffer, RefAwareTextBufferIter, push_str),
                    (BytesBuffer, RefAwareBytesBufferIter, push_bytes),
                ])]
                FieldValueSlice::REP(data) => {
                    for (v, rl, _offset) in ITER::from_range(&tr, data) {
                        targets_applicator(&mut |fd| {
                            fd.PUSH_FN(v, rl as usize, true, false);
                        });
                    }
                }

                #[expand(REP in [
                    Int, Float, StreamValueId, FieldReference,
                    SlicedFieldReference
                ])]
                FieldValueSlice::REP(data) => {
                    for (v, rl) in
                        RefAwareFieldValueRangeIter::from_range(&tr, data)
                    {
                        targets_applicator(&mut |fd| {
                            fd.push_fixed_size_type(
                                *v,
                                rl as usize,
                                true,
                                false,
                            );
                        });
                    }
                }

                #[expand(REP in [
                    BigInt, BigRational, Error, Object, Array,
                    Argument, Macro, Custom
                ])]
                FieldValueSlice::REP(data) => {
                    for (v, rl) in
                        RefAwareFieldValueRangeIter::from_range(&tr, data)
                    {
                        targets_applicator(&mut |fd| {
                            fd.push_fixed_size_type(
                                v.clone(),
                                rl as usize,
                                true,
                                false,
                            );
                        });
                    }
                }
            })
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }
    #[allow(clippy::iter_not_returning_iterator)]
    pub fn iter(&self) -> FieldIter<&'_ FieldData> {
        FieldIter::from_start(self)
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

    pub fn varying_type_inserter(
        &mut self,
    ) -> VaryingTypeInserter<&mut FieldData> {
        VaryingTypeInserter::new(self)
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
        metamatch!(match ts {
            FieldValueSlice::Null(_) | FieldValueSlice::Undefined(_) => (),
            FieldValueSlice::TextInline(v) =>
                extend_raw(target_applicator, v.as_bytes()),

            #[expand(REP in [
                Int, Float, BytesInline,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            FieldValueSlice::REP(v) => {
                debug_assert!(FieldValueRepr::REP.is_trivially_copyable());
                extend_raw(target_applicator, v)
            }

            #[expand(REP in [
                BigInt, BigRational, TextBuffer, BytesBuffer,
                Error, Object, Array, Argument, Custom, Macro
            ])]
            FieldValueSlice::REP(v) => {
                debug_assert!(!FieldValueRepr::REP.is_trivially_copyable());
                extend_with_clones(target_applicator, v)
            }
        });
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
