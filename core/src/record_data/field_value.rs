use std::{any::Any, fmt::Display, mem::ManuallyDrop};

use indexmap::IndexMap;
use metamatch::metamatch;
use num::{bigint::Sign, BigInt, BigRational};

use crate::{
    cli::call_expr::Argument,
    operators::errors::OperatorApplicationError,
    record_data::field_value_ref::FieldValueSlice,
    utils::{
        force_cast,
        maybe_text::{MaybeText, MaybeTextRef},
        string_store::StringStoreEntry,
    },
};

use super::{
    array::Array,
    custom_data::CustomDataBox,
    field::FieldRefOffset,
    field_data::{FieldValueRepr, FieldValueType, FixedSizeFieldValueType},
    field_value_ref::{FieldValueRef, FieldValueRefMut},
    stream_value::StreamValueId,
};

// the different logical data types
// irrespective of representation in memory, see FieldDataRepr for that

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum FieldValueKind {
    Undefined,
    Null,
    Int,
    Float,
    Error,
    Bytes,
    Text,
    Object,
    Array,
    Argument,
    FieldReference,
    SlicedFieldReference,
    StreamValueId,
    Custom,
}

impl Display for FieldValueKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

#[derive(Debug, Clone, Default)]
pub enum FieldValue {
    #[default]
    Undefined,
    Null,
    Int(i64),
    BigInt(Box<BigInt>),
    Float(f64),
    BigRational(Box<BigRational>),
    // PERF: better to use a custom version of Arc<String> for this with only
    // one indirection (and no weak refs) that still stores the capacity
    // (unlike `Arc<str>`). Maybe that type stores the meta info at
    // the end so we can convert to `String` without realloc or memcpy
    Text(String),
    Bytes(Vec<u8>), // TODO: same as for `Text`
    // this is the only field that's allowed to be 32 bytes large
    // this still keeps FieldValue at 32 bytes due to Rust's
    // cool enum layout optimizations
    Array(Array),
    Object(Box<Object>),
    Custom(CustomDataBox),
    Error(OperatorApplicationError),
    Argument(Box<Argument>),
    StreamValueId(StreamValueId),
    FieldReference(FieldReference),
    SlicedFieldReference(SlicedFieldReference),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Null;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Undefined;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupSeparator;

pub type ObjectKeysStored = IndexMap<String, FieldValue>;
pub type ObjectKeysInterned = IndexMap<StringStoreEntry, FieldValue>;

#[derive(Debug, Clone, PartialEq)]
pub enum Object {
    KeysStored(ObjectKeysStored),
    KeysInterned(ObjectKeysInterned),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FieldReference {
    pub field_ref_offset: FieldRefOffset,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SlicedFieldReference {
    pub field_ref_offset: FieldRefOffset,
    pub begin: usize,
    pub end: usize,
}

impl SlicedFieldReference {
    pub fn new(
        field_ref_offset: FieldRefOffset,
        begin: usize,
        end: usize,
    ) -> Self {
        Self {
            field_ref_offset,
            begin,
            end,
        }
    }
}
impl FieldReference {
    pub fn new(field_ref_offset: FieldRefOffset) -> Self {
        Self { field_ref_offset }
    }
}

impl Object {
    pub fn new_keys_stored() -> Object {
        Object::KeysStored(IndexMap::default())
    }
    pub fn len(&self) -> usize {
        match self {
            Object::KeysStored(d) => d.len(),
            Object::KeysInterned(d) => d.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        match self {
            Object::KeysStored(o) => o.clear(),
            Object::KeysInterned(o) => o.clear(),
        }
    }
    pub fn push_stored_key(&mut self, key: String, value: FieldValue) {
        if let Object::KeysStored(o) = self {
            o.insert(key, value);
        } else {
            unreachable!()
        }
    }
}

impl FromIterator<(String, FieldValue)> for Object {
    fn from_iter<I: IntoIterator<Item = (String, FieldValue)>>(
        iter: I,
    ) -> Self {
        Object::KeysStored(IndexMap::from_iter(iter))
    }
}

impl FieldValueKind {
    pub fn repr(&self, inline: bool) -> FieldValueRepr {
        metamatch!(match self {
            #[expand((KIND, INLINE, BUFFERED) in [
                (Text, TextInline, TextBuffer),
                (Bytes, BytesInline, BytesBuffer),
                (Int, Int, BigInt),
                (Float, Float, BigRational)
            ])]
            FieldValueKind::KIND => {
                if inline {
                    FieldValueRepr::INLINE
                } else {
                    FieldValueRepr::BUFFERED
                }
            }

            #[expand(T in [
                Undefined, Null, Error, Object, Array,
                Argument, FieldReference,
                SlicedFieldReference, StreamValueId, Custom
            ])]
            FieldValueKind::T => FieldValueRepr::T,
        })
    }
    pub fn to_str(self) -> &'static str {
        match self {
            FieldValueKind::Undefined => "undefined",
            FieldValueKind::Null => "null",
            FieldValueKind::Int => "int",
            FieldValueKind::Float => "float",
            FieldValueKind::Error => "error",
            FieldValueKind::Text => "str",
            FieldValueKind::Bytes => "bytes",
            FieldValueKind::Custom => "custom",
            FieldValueKind::Object => "object",
            FieldValueKind::Array => "array",
            FieldValueKind::FieldReference => "field_reference",
            FieldValueKind::SlicedFieldReference => "sliced_field_reference",
            FieldValueKind::StreamValueId => "stream_value_id",
            FieldValueKind::Argument => "argument",
        }
    }
    pub fn is_valid_utf8(self) -> bool {
        match self {
            FieldValueKind::Undefined
            | FieldValueKind::Null
            | FieldValueKind::Int
            | FieldValueKind::Float
            | FieldValueKind::Error
            | FieldValueKind::Text
            | FieldValueKind::Object
            | FieldValueKind::Array => true,
            FieldValueKind::Bytes
            | FieldValueKind::FieldReference
            | FieldValueKind::SlicedFieldReference
            | FieldValueKind::StreamValueId
            | FieldValueKind::Custom
            | FieldValueKind::Argument => false,
        }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            Self::REP => matches!(other, Self::REP),

            #[expand(REP in [
                Int, Error, Array, Object, Bytes, Text,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(l) => {
                matches!(other, FieldValue::REP(r) if r == l)
            }
        })
    }
}

pub fn bigint_to_int(bi: &BigInt) -> Option<i64> {
    let mut iter = bi.iter_u64_digits();
    let first = iter.next().unwrap_or(0);
    if iter.next().is_some() {
        None
    } else {
        let mut first = TryInto::<i64>::try_into(first).ok()?;
        if bi.sign() == Sign::Minus {
            first = -first;
        }
        Some(first)
    }
}

impl FieldValue {
    pub fn repr(&self) -> FieldValueRepr {
        metamatch!(match self {
            FieldValue::Null => FieldValueRepr::Null,
            FieldValue::Undefined => FieldValueRepr::Undefined,

            FieldValue::Text(_) => FieldValueRepr::TextBuffer,
            FieldValue::Bytes(_) => FieldValueRepr::BytesBuffer,

            #[expand(REP in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(_) => FieldValueRepr::REP,
        })
    }
    pub fn downcast_ref<R: FieldValueType>(&self) -> Option<&R> {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            FieldValue::REP => <dyn Any>::downcast_ref(&REP),

            #[expand(REP in [
                Int, Error, Array, Object, Text, Bytes,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(v) => {
                if R::FIELD_VALUE_BOXED {
                    <dyn Any>::downcast_ref::<Box<R>>(v).map(|v| &**v)
                } else {
                    <dyn Any>::downcast_ref(v)
                }
            }
        })
    }
    pub fn downcast_mut<R: FieldValueType>(&mut self) -> Option<&mut R> {
        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            v @ FieldValue::T => <dyn Any>::downcast_mut(v),

            #[expand(REP in [
                Int, Error, Array, Object, Text, Bytes,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(v) => {
                if R::FIELD_VALUE_BOXED {
                    <dyn Any>::downcast_mut::<Box<R>>(v).map(|v| &mut **v)
                } else {
                    <dyn Any>::downcast_mut(v)
                }
            }
        })
    }
    pub fn as_maybe_text_ref(&self) -> Option<MaybeTextRef> {
        match self {
            FieldValue::Text(v) => Some(MaybeTextRef::Text(v)),
            FieldValue::Bytes(v) => Some(MaybeTextRef::Bytes(v)),
            FieldValue::Argument(v) => v.value.as_maybe_text_ref(),
            FieldValue::Undefined
            | FieldValue::Null
            | FieldValue::Int(_)
            | FieldValue::BigInt(_)
            | FieldValue::Float(_)
            | FieldValue::BigRational(_)
            | FieldValue::Array(_)
            | FieldValue::Object(_)
            | FieldValue::Custom(_)
            | FieldValue::Error(_)
            | FieldValue::StreamValueId(_)
            | FieldValue::FieldReference(_)
            | FieldValue::SlicedFieldReference(_) => None,
        }
    }
    pub fn text_or_bytes(&self) -> Option<&[u8]> {
        self.as_maybe_text_ref().map(|t| t.as_bytes())
    }
    pub fn downcast<R: FixedSizeFieldValueType>(self) -> Option<R> {
        let mut this = ManuallyDrop::new(self);
        this.downcast_mut().map(|v| unsafe { std::ptr::read(v) })
    }
    pub fn downcast_allowing_text_as_bytes<R: FixedSizeFieldValueType>(
        self,
    ) -> Option<R> {
        if let FieldValue::Text(text) = self {
            if R::REPR != FieldValueRepr::TextBuffer {
                return FieldValue::Bytes(text.into_bytes()).downcast();
            }
            return FieldValue::Text(text).downcast();
        }
        let mut this = ManuallyDrop::new(self);
        this.downcast_mut().map(|v| unsafe { std::ptr::read(v) })
    }
    pub fn as_ref(&self) -> FieldValueRef {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            FieldValue::REP => FieldValueRef::REP,

            #[expand(REP in [
                Int, Error, Array, Object, Text, Bytes,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(v) => FieldValueRef::REP(v),
        })
    }
    // This is different from `.as_ref().as_slice()` as it is able to use
    // `TextBuffer`/`BytesBuffer`, which get lost in translation when using
    // `as_ref()`
    pub fn as_slice(&self) -> FieldValueSlice {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            FieldValue::REP => FieldValueSlice::REP(1),

            #[expand(REP in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(v) =>
                FieldValueSlice::REP(std::slice::from_ref(v)),

            #[expand((REP, TGT) in [
                (Text, TextBuffer),
                (Bytes, BytesBuffer)])
            ]
            FieldValue::REP(v) =>
                FieldValueSlice::TGT(std::slice::from_ref(v)),
        })
    }
    pub fn as_ref_mut(&mut self) -> FieldValueRefMut {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            FieldValue::REP => FieldValueRefMut::REP,

            #[expand(REP in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValue::REP(v) => FieldValueRefMut::REP(v),

            #[expand((VALUE_T, REF_T) in [
                (Text, TextBuffer),
                (Bytes, BytesBuffer)
            ])]
            FieldValue::VALUE_T(v) => FieldValueRefMut::REF_T(v),
        })
    }
    pub fn from_maybe_text(t: MaybeText) -> Self {
        match t {
            MaybeText::Text(t) => FieldValue::Text(t),
            MaybeText::Bytes(b) => FieldValue::Bytes(b),
        }
    }
    pub fn into_maybe_text(self) -> Option<MaybeText> {
        match self {
            FieldValue::Text(t) => Some(MaybeText::Text(t)),
            FieldValue::Bytes(b) => Some(MaybeText::Bytes(b)),
            FieldValue::Argument(arg) => arg.value.into_maybe_text(),
            _ => None,
        }
    }
    pub fn from_fixed_sized_type<T: FixedSizeFieldValueType>(v: T) -> Self {
        // SAFETY: We *know* that `T` and `Q` will be *identical* because of
        // the check on `T::REP`. `FixedSizeFieldValueType` is an
        // unsafe trait, so assuming that nobody gave us an incorrect
        // `REP` is sound.
        metamatch!(match T::REPR {
            #[expand(REP in [Null, Undefined])]
            FieldValueRepr::REP => FieldValue::REP,

            #[expand(REP in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            FieldValueRepr::REP => {
                if T::FIELD_VALUE_BOXED {
                    FieldValue::REP(unsafe { force_cast(Box::new(v)) })
                } else {
                    FieldValue::REP(unsafe { force_cast(v) })
                }
            }

            #[expand((REP_T, VALUE_T) in [
                (TextBuffer, Text),
                (BytesBuffer, Bytes)
            ])]
            FieldValueRepr::REP_T => {
                FieldValue::VALUE_T(unsafe { force_cast(v) })
            }

            // not fixed size types
            FieldValueRepr::TextInline | FieldValueRepr::BytesInline => {
                unreachable!()
            }
        })
    }
    pub fn is_error(&self) -> bool {
        matches!(self, FieldValue::Error(_))
    }
    pub fn kind(&self) -> FieldValueKind {
        self.repr().kind()
    }
    pub fn is_valid_utf8(&self) -> bool {
        self.kind().is_valid_utf8()
    }
    pub fn try_cast_int(&self) -> Option<i64> {
        match self {
            &FieldValue::Int(v) => Some(v),
            FieldValue::BigInt(v) => bigint_to_int(v),
            #[allow(clippy::cast_precision_loss, clippy::float_cmp)]
            &FieldValue::Float(f) => {
                let int = f as i64;
                if int as f64 == f {
                    return Some(int);
                }
                None
            }
            FieldValue::Text(v) => v.parse().ok(),
            FieldValue::Bytes(v) => std::str::from_utf8(v).ok()?.parse().ok(),
            FieldValue::BigRational(v) => {
                if !v.is_integer() {
                    return None;
                }
                bigint_to_int(v.numer())
            }
            FieldValue::Argument(v) => v.value.try_cast_int(),
            FieldValue::Undefined
            | FieldValue::Null
            | FieldValue::Array(_)
            | FieldValue::Object(_)
            | FieldValue::Custom(_)
            | FieldValue::Error(_)
            | FieldValue::StreamValueId(_)
            | FieldValue::FieldReference(_)
            | FieldValue::SlicedFieldReference(_) => None,
        }
    }
}
