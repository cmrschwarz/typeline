use std::{any::Any, fmt::Display, mem::ManuallyDrop};

use indexmap::IndexMap;
use num::{BigInt, BigRational};

use crate::{
    operators::errors::OperatorApplicationError,
    utils::{maybe_text::MaybeText, string_store::StringStoreEntry},
};

use super::{
    array::Array,
    custom_data::CustomDataBox,
    field::FieldRefOffset,
    field_data::{FieldValueRepr, FieldValueType, FixedSizeFieldValueType},
    field_value_ref::FieldValueRef,
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
    Rational,
    Error,
    Bytes,
    Text,
    Object,
    Array,
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
    Rational(Box<BigRational>),
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
    Object(Object),
    Custom(CustomDataBox),
    Error(OperatorApplicationError),
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

pub type ObjectKeysStored = Box<IndexMap<String, FieldValue>>;
pub type ObjectKeysInterned = Box<IndexMap<StringStoreEntry, FieldValue>>;

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
        Object::KeysStored(Box::default())
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
        Object::KeysStored(Box::new(IndexMap::from_iter(iter)))
    }
}

impl FieldValueKind {
    pub fn to_str(self) -> &'static str {
        match self {
            FieldValueKind::Undefined => "undefined",
            FieldValueKind::Null => "null",
            FieldValueKind::Int => "int",
            FieldValueKind::Float => "float",
            FieldValueKind::Rational => "rational",
            FieldValueKind::Error => "error",
            FieldValueKind::Text => "str",
            FieldValueKind::Bytes => "bytes",
            FieldValueKind::Custom => "custom",
            FieldValueKind::Object => "object",
            FieldValueKind::Array => "array",
            FieldValueKind::FieldReference => "field_reference",
            FieldValueKind::SlicedFieldReference => "sliced_field_reference",
            FieldValueKind::StreamValueId => "stream_value_id",
        }
    }
    pub fn is_valid_utf8(self) -> bool {
        match self {
            FieldValueKind::Undefined
            | FieldValueKind::Null
            | FieldValueKind::Int
            | FieldValueKind::Float
            | FieldValueKind::Rational
            | FieldValueKind::Error
            | FieldValueKind::Text
            | FieldValueKind::Object
            | FieldValueKind::Array => true,
            FieldValueKind::Bytes
            | FieldValueKind::FieldReference
            | FieldValueKind::SlicedFieldReference
            | FieldValueKind::StreamValueId
            | FieldValueKind::Custom => false,
        }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Int(l) => matches!(other, Self::Int(r) if r == l),
            Self::BigInt(l) => matches!(other, Self::BigInt(r) if r == l),
            Self::Float(l) => matches!(other, Self::Float(r) if r == l),
            Self::Rational(l) => {
                matches!(other, Self::Rational(r) if r == l)
            }
            Self::Bytes(l) => matches!(other, Self::Bytes(r) if r == l),
            Self::Text(l) => matches!(other, Self::Text(r) if r == l),
            Self::Error(l) => matches!(other, Self::Error(r) if r == l),
            Self::Array(l) => matches!(other, Self::Array(r) if r == l),
            Self::Object(l) => matches!(other, Self::Object(r) if r == l),
            Self::FieldReference(l) => {
                matches!(other, Self::FieldReference(r) if r == l)
            }
            Self::SlicedFieldReference(l) => {
                matches!(other, Self::SlicedFieldReference(r) if r == l)
            }
            Self::StreamValueId(l) => {
                matches!(other, Self::StreamValueId(r) if l == r)
            }
            Self::Custom(l) => matches!(other, Self::Custom(r) if r == l),
            Self::Null => matches!(other, Self::Null),
            Self::Undefined => matches!(other, Self::Undefined),
        }
    }
}

impl FieldValue {
    pub fn repr(&self) -> FieldValueRepr {
        match self {
            FieldValue::Null => FieldValueRepr::Null,
            FieldValue::Undefined => FieldValueRepr::Undefined,
            FieldValue::Int(_) => FieldValueRepr::Int,
            FieldValue::BigInt(_) => FieldValueRepr::BigInt,
            FieldValue::Float(_) => FieldValueRepr::Float,
            FieldValue::Rational(_) => FieldValueRepr::Rational,
            FieldValue::Bytes(_) => FieldValueRepr::BytesBuffer,
            FieldValue::Text(_) => FieldValueRepr::TextBuffer,
            FieldValue::Error(_) => FieldValueRepr::Error,
            FieldValue::Array(_) => FieldValueRepr::Array,
            FieldValue::Object(_) => FieldValueRepr::Object,
            FieldValue::FieldReference(_) => FieldValueRepr::FieldReference,
            FieldValue::SlicedFieldReference(_) => {
                FieldValueRepr::SlicedFieldReference
            }
            FieldValue::Custom(_) => FieldValueRepr::Custom,
            FieldValue::StreamValueId(_) => FieldValueRepr::StreamValueId,
        }
    }
    pub fn downcast_ref<R: FieldValueType>(&self) -> Option<&R> {
        match self {
            FieldValue::Null => <dyn Any>::downcast_ref(&Null),
            FieldValue::Undefined => <dyn Any>::downcast_ref(&Undefined),
            FieldValue::Int(v) => <dyn Any>::downcast_ref(v),
            FieldValue::BigInt(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Float(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Rational(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Text(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Bytes(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Array(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Object(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Custom(v) => <dyn Any>::downcast_ref(v),
            FieldValue::Error(v) => <dyn Any>::downcast_ref(v),
            FieldValue::FieldReference(v) => <dyn Any>::downcast_ref(v),
            FieldValue::SlicedFieldReference(v) => <dyn Any>::downcast_ref(v),
            FieldValue::StreamValueId(v) => <dyn Any>::downcast_ref(v),
        }
    }
    pub fn downcast_mut<R: FieldValueType>(&mut self) -> Option<&mut R> {
        match self {
            v @ FieldValue::Null => <dyn Any>::downcast_mut(v),
            v @ FieldValue::Undefined => <dyn Any>::downcast_mut(v),
            FieldValue::Int(v) => <dyn Any>::downcast_mut(v),
            FieldValue::BigInt(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Float(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Rational(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Text(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Bytes(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Array(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Object(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Custom(v) => <dyn Any>::downcast_mut(v),
            FieldValue::Error(v) => <dyn Any>::downcast_mut(v),
            FieldValue::FieldReference(v) => <dyn Any>::downcast_mut(v),
            FieldValue::SlicedFieldReference(v) => <dyn Any>::downcast_mut(v),
            FieldValue::StreamValueId(v) => <dyn Any>::downcast_mut(v),
        }
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
        match self {
            FieldValue::Undefined => FieldValueRef::Undefined,
            FieldValue::Null => FieldValueRef::Null,
            FieldValue::Int(v) => FieldValueRef::Int(v),
            FieldValue::BigInt(v) => FieldValueRef::BigInt(v),
            FieldValue::Float(v) => FieldValueRef::Float(v),
            FieldValue::Rational(v) => FieldValueRef::Rational(v),
            FieldValue::Text(v) => FieldValueRef::Text(v),
            FieldValue::Bytes(v) => FieldValueRef::Bytes(v),
            FieldValue::Array(v) => FieldValueRef::Array(v),
            FieldValue::Object(v) => FieldValueRef::Object(v),
            FieldValue::Custom(v) => FieldValueRef::Custom(v),
            FieldValue::Error(v) => FieldValueRef::Error(v),
            FieldValue::StreamValueId(v) => FieldValueRef::StreamValueId(v),
            FieldValue::FieldReference(v) => FieldValueRef::FieldReference(v),
            FieldValue::SlicedFieldReference(v) => {
                FieldValueRef::SlicedFieldReference(v)
            }
        }
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
            _ => None,
        }
    }
    pub fn from_fixed_sized_type<T: FixedSizeFieldValueType>(v: T) -> Self {
        // SAFETY: We *know* that `T` and `Q` will be *identical* because of
        // the check on `T::REPR`. `FixedSizeFieldValueType` is an
        // unsafe trait, so assuming that nobody gave us an incorrect
        // `REPR` is sound.
        use crate::utils::force_cast as xx;
        unsafe {
            match T::REPR {
                FieldValueRepr::Null => FieldValue::Null,
                FieldValueRepr::Undefined => FieldValue::Undefined,
                FieldValueRepr::Int => FieldValue::Int(xx(v)),
                FieldValueRepr::BigInt => xx(v),
                FieldValueRepr::Float => FieldValue::Float(xx(v)),
                FieldValueRepr::Rational => {
                    FieldValue::Rational(Box::new(xx::<T, BigRational>(v)))
                }
                FieldValueRepr::TextBuffer => FieldValue::Text(xx(v)),
                FieldValueRepr::BytesBuffer => FieldValue::Bytes(xx(v)),
                FieldValueRepr::Object => FieldValue::Object(xx(v)),
                FieldValueRepr::Array => FieldValue::Array(xx(v)),
                FieldValueRepr::Custom => FieldValue::Custom(xx(v)),
                FieldValueRepr::Error => FieldValue::Error(xx(v)),
                FieldValueRepr::StreamValueId => {
                    FieldValue::StreamValueId(xx(v))
                }
                FieldValueRepr::FieldReference => {
                    FieldValue::FieldReference(xx(v))
                }
                FieldValueRepr::SlicedFieldReference => {
                    FieldValue::SlicedFieldReference(xx(v))
                }
                // not fixed size types
                FieldValueRepr::TextInline | FieldValueRepr::BytesInline => {
                    unreachable!()
                }
            }
        }
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
}
