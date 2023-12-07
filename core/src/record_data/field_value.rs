use indexmap::IndexMap;
use num_bigint::BigInt;
use num_rational::BigRational;

use crate::{
    operators::errors::OperatorApplicationError,
    utils::string_store::StringStoreEntry,
};

use super::{
    custom_data::CustomDataBox, field::FieldIdOffset,
    field_data::FieldDataRepr,
};

// the different logical data types
// irrespective of representation in memory, see FieldDataRepr for that
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum FieldValueKind {
    Undefined,
    Null,
    Int,
    BigInt,
    Float,
    Rational,
    Error,
    Bytes,
    Text,
    Object,
    Array,
    Reference,
    Custom,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    Null,
    Undefined,
    Int(i64),
    // this is the only field that's allowed to be 32 bytes large
    // this still keeps FieldValue at 32 bytes due to Rust's
    // cool enum layout optimizations
    BigInt(BigInt),
    Float(f64),
    Rational(Box<BigRational>),
    Bytes(Vec<u8>),
    String(String),
    Error(OperatorApplicationError),
    Array(Array),
    Object(Object),
    FieldReference(FieldReference),
    Custom(CustomDataBox),
}

pub struct Null;
pub struct Undefined;

#[derive(Debug, Clone, PartialEq)]
pub enum Object {
    KeysStored(Box<IndexMap<Box<str>, FieldValue>>),
    KeysInterned(Box<IndexMap<StringStoreEntry, FieldValue>>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Array {
    Null(usize),
    Undefined(usize),
    Int(Box<[i64]>),
    Bytes(Box<Box<[u8]>>),
    String(Box<Box<str>>),
    Error(Box<[OperatorApplicationError]>),
    Array(Box<[Array]>),
    Object(Box<[Object]>),
    FieldReference(Box<[FieldReference]>),
    Custom(Box<[CustomDataBox]>),
    Mixed(Box<[FieldValue]>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FieldReference {
    pub field_id_offset: FieldIdOffset,
    pub begin: usize,
    pub end: usize,
}

impl FieldValueKind {
    pub fn to_preferred_data_repr(self) -> FieldDataRepr {
        match self {
            FieldValueKind::Undefined => FieldDataRepr::Undefined,
            FieldValueKind::Null => FieldDataRepr::Null,
            FieldValueKind::Int => FieldDataRepr::Int,
            FieldValueKind::BigInt => FieldDataRepr::BigInt,
            FieldValueKind::Float => FieldDataRepr::Float,
            FieldValueKind::Rational => FieldDataRepr::Rational,
            FieldValueKind::Error => FieldDataRepr::Error,
            FieldValueKind::Bytes => FieldDataRepr::BytesInline,
            FieldValueKind::Text => FieldDataRepr::BytesInline,
            FieldValueKind::Object => FieldDataRepr::Object,
            FieldValueKind::Custom => FieldDataRepr::Custom,
            FieldValueKind::Array => FieldDataRepr::Array,
            FieldValueKind::Reference => FieldDataRepr::Reference,
        }
    }
    pub fn to_guaranteed_data_repr(self) -> FieldDataRepr {
        match self {
            FieldValueKind::Undefined => FieldDataRepr::Undefined,
            FieldValueKind::Null => FieldDataRepr::Null,
            FieldValueKind::Int => FieldDataRepr::Int,
            FieldValueKind::BigInt => FieldDataRepr::BigInt,
            FieldValueKind::Float => FieldDataRepr::Float,
            FieldValueKind::Rational => FieldDataRepr::Rational,
            FieldValueKind::Error => FieldDataRepr::Error,
            FieldValueKind::Bytes => FieldDataRepr::BytesBuffer,
            FieldValueKind::Text => FieldDataRepr::BytesBuffer,
            FieldValueKind::Object => FieldDataRepr::Object,
            FieldValueKind::Custom => FieldDataRepr::Custom,
            FieldValueKind::Array => FieldDataRepr::Array,
            FieldValueKind::Reference => FieldDataRepr::Reference,
        }
    }
    pub fn to_str(self) -> &'static str {
        match self {
            FieldValueKind::Undefined => "undefined",
            FieldValueKind::Null => "null",
            FieldValueKind::Int => "int",
            FieldValueKind::BigInt => "integer",
            FieldValueKind::Float => "float",
            FieldValueKind::Rational => "rational",
            FieldValueKind::Error => "error",
            FieldValueKind::Text => "str",
            FieldValueKind::Bytes => "bytes",
            FieldValueKind::Custom => "custom",
            FieldValueKind::Object => "object",
            FieldValueKind::Array => "array",
            FieldValueKind::Reference => "reference",
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
            Self::String(l) => matches!(other, Self::String(r) if r == l),
            Self::Error(l) => matches!(other, Self::Error(r) if r == l),
            Self::Array(l) => matches!(other, Self::Array(r) if r == l),
            Self::Object(l) => matches!(other, Self::Object(r) if r == l),
            Self::FieldReference(l) => {
                matches!(other, Self::FieldReference(r) if r == l)
            }
            Self::Custom(l) => matches!(other, Self::Custom(r) if r == l),
            Self::Null => other == &Self::Null,
            Self::Undefined => other == &Self::Undefined,
        }
    }
}

impl FieldValue {
    pub fn kind(&self) -> FieldValueKind {
        match self {
            FieldValue::Null => FieldValueKind::Null,
            FieldValue::Undefined => FieldValueKind::Undefined,
            FieldValue::Int(_) => FieldValueKind::Int,
            FieldValue::BigInt(_) => FieldValueKind::BigInt,
            FieldValue::Float(_) => FieldValueKind::Float,
            FieldValue::Rational(_) => FieldValueKind::Rational,
            FieldValue::Bytes(_) => FieldValueKind::Bytes,
            FieldValue::String(_) => FieldValueKind::Text,
            FieldValue::Error(_) => FieldValueKind::Error,
            FieldValue::Array(_) => FieldValueKind::Array,
            FieldValue::Object(_) => FieldValueKind::Object,
            FieldValue::FieldReference(_) => FieldValueKind::Reference,
            FieldValue::Custom(_) => FieldValueKind::Custom,
        }
    }
}
