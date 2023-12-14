use indexmap::IndexMap;
use num_bigint::BigInt;
use num_rational::BigRational;

use crate::{
    operators::{
        errors::OperatorApplicationError,
        utils::{NULL_STR, UNDEFINED_STR},
    },
    utils::string_store::{StringStore, StringStoreEntry},
};

use super::{
    custom_data::CustomDataBox,
    field::{FieldIdOffset, FieldManager},
    field_data::FieldValueRepr,
    match_set::MatchSetManager,
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
    FieldReference,
    SlicedFieldReference,
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
    Text(String),
    Bytes(Vec<u8>),
    Array(Array),
    Object(Object),
    Custom(CustomDataBox),
    Error(OperatorApplicationError),
    FieldReference(FieldReference),
    SlicedFieldReference(SlicedFieldReference),
}

#[derive(Clone, Copy, Debug)]
pub struct Null;
#[derive(Clone, Copy, Debug)]
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
    Bytes(Box<[Box<[u8]>]>),
    String(Box<[Box<str>]>),
    Error(Box<[OperatorApplicationError]>),
    Array(Box<[Array]>),
    Object(Box<[Object]>),
    FieldReference(Box<[FieldReference]>),
    SlicedFieldReference(Box<[SlicedFieldReference]>),
    Custom(Box<[CustomDataBox]>),
    Mixed(Box<[FieldValue]>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FieldReference {
    pub field_id_offset: FieldIdOffset,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SlicedFieldReference {
    pub field_id_offset: FieldIdOffset,
    pub begin: usize,
    pub end: usize,
}

impl FieldValueKind {
    pub fn to_preferred_data_repr(self) -> FieldValueRepr {
        match self {
            FieldValueKind::Undefined => FieldValueRepr::Undefined,
            FieldValueKind::Null => FieldValueRepr::Null,
            FieldValueKind::Int => FieldValueRepr::Int,
            FieldValueKind::BigInt => FieldValueRepr::BigInt,
            FieldValueKind::Float => FieldValueRepr::Float,
            FieldValueKind::Rational => FieldValueRepr::Rational,
            FieldValueKind::Error => FieldValueRepr::Error,
            FieldValueKind::Bytes => FieldValueRepr::BytesInline,
            FieldValueKind::Text => FieldValueRepr::BytesInline,
            FieldValueKind::Object => FieldValueRepr::Object,
            FieldValueKind::Custom => FieldValueRepr::Custom,
            FieldValueKind::Array => FieldValueRepr::Array,
            FieldValueKind::FieldReference => FieldValueRepr::FieldReference,
            FieldValueKind::SlicedFieldReference => {
                FieldValueRepr::SlicedFieldReference
            }
        }
    }
    pub fn to_guaranteed_data_repr(self) -> FieldValueRepr {
        match self {
            FieldValueKind::Undefined => FieldValueRepr::Undefined,
            FieldValueKind::Null => FieldValueRepr::Null,
            FieldValueKind::Int => FieldValueRepr::Int,
            FieldValueKind::BigInt => FieldValueRepr::BigInt,
            FieldValueKind::Float => FieldValueRepr::Float,
            FieldValueKind::Rational => FieldValueRepr::Rational,
            FieldValueKind::Error => FieldValueRepr::Error,
            FieldValueKind::Bytes => FieldValueRepr::BytesBuffer,
            FieldValueKind::Text => FieldValueRepr::BytesBuffer,
            FieldValueKind::Object => FieldValueRepr::Object,
            FieldValueKind::Custom => FieldValueRepr::Custom,
            FieldValueKind::Array => FieldValueRepr::Array,
            FieldValueKind::FieldReference => FieldValueRepr::FieldReference,
            FieldValueKind::SlicedFieldReference => {
                FieldValueRepr::SlicedFieldReference
            }
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
            FieldValueKind::FieldReference => "field_reference",
            FieldValueKind::SlicedFieldReference => "sliced_field_reference",
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
            Self::Custom(l) => matches!(other, Self::Custom(r) if r == l),
            Self::Null => matches!(other, Self::Null),
            Self::Undefined => matches!(other, Self::Undefined),
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
            FieldValue::Text(_) => FieldValueKind::Text,
            FieldValue::Error(_) => FieldValueKind::Error,
            FieldValue::Array(_) => FieldValueKind::Array,
            FieldValue::Object(_) => FieldValueKind::Object,
            FieldValue::FieldReference(_) => FieldValueKind::FieldReference,
            FieldValue::SlicedFieldReference(_) => {
                FieldValueKind::SlicedFieldReference
            }
            FieldValue::Custom(_) => FieldValueKind::Custom,
        }
    }
}

pub fn format_bytes(
    _fmt: &mut impl std::io::Write,
    _v: &[u8],
) -> std::io::Result<()> {
    todo!()
}

pub fn format_quoted_string(
    fmt: &mut impl std::io::Write,
    v: &str,
) -> std::io::Result<()> {
    // TODO: escape properly
    fmt.write_fmt(format_args!("{:?}", v))
}

pub fn format_error(
    w: &mut impl std::io::Write,
    v: &OperatorApplicationError,
) -> std::io::Result<()> {
    w.write_fmt(format_args!("(\"error\")\"{}\"", v))
}

pub struct FormattingContext<'a> {
    pub ss: &'a StringStore,
    pub fm: &'a FieldManager,
    pub msm: &'a MatchSetManager,
}

impl FieldValue {
    pub fn format(
        &self,
        w: &mut impl std::io::Write,
        fc: &mut FormattingContext<'_>,
    ) -> std::io::Result<()> {
        match self {
            FieldValue::Null => w.write(NULL_STR.as_bytes()).map(|_| ()),
            FieldValue::Undefined => {
                w.write(UNDEFINED_STR.as_bytes()).map(|_| ())
            }
            FieldValue::Int(v) => w.write_fmt(format_args!("{}", v)),
            FieldValue::BigInt(v) => w.write_fmt(format_args!("{}", v)),
            FieldValue::Float(v) => w.write_fmt(format_args!("{}", v)),
            FieldValue::Rational(v) => w.write_fmt(format_args!("{}", v)),
            FieldValue::Bytes(v) => format_bytes(w, v),
            FieldValue::Text(v) => format_quoted_string(w, v),
            FieldValue::Error(e) => format_error(w, e),
            FieldValue::Array(a) => a.format(w, fc),
            FieldValue::Object(o) => o.format(w, fc),
            FieldValue::FieldReference(_) => todo!(),
            FieldValue::SlicedFieldReference(_) => todo!(),
            FieldValue::Custom(v) => v.stringify(w).map(|_| ()),
        }
    }
}

impl Object {
    pub fn format(
        &self,
        w: &mut impl std::io::Write,
        fc: &mut FormattingContext<'_>,
    ) -> std::io::Result<()> {
        w.write_all(b"{")?;
        // TODO: escape keys
        let mut first = true;
        match self {
            Object::KeysStored(m) => {
                for (k, v) in m.iter() {
                    if first {
                        first = false;
                    } else {
                        w.write_all(b", ")?;
                    }
                    format_quoted_string(w, k)?;
                    w.write_all(b": ")?;
                    v.format(w, fc)?;
                }
            }
            Object::KeysInterned(m) => {
                for (&k, v) in m.iter() {
                    if first {
                        first = false;
                    } else {
                        w.write_all(b", ")?;
                    }
                    format_quoted_string(w, fc.ss.lookup(k))?;
                    w.write_all(b": ")?;
                    v.format(w, fc)?;
                }
            }
        }
        w.write_all(b"}")?;
        Ok(())
    }
}

impl Array {
    pub fn format(
        &self,
        w: &mut impl std::io::Write,
        fc: &mut FormattingContext<'_>,
    ) -> std::io::Result<()> {
        fn format_array<
            'a,
            T: 'a,
            I: Iterator<Item = &'a T>,
            W: std::io::Write,
        >(
            w: &mut W,
            fc: &mut FormattingContext<'_>,
            iter: I,
            mut f: impl FnMut(
                &mut W,
                &mut FormattingContext<'_>,
                &T,
            ) -> std::io::Result<()>,
        ) -> std::io::Result<()> {
            w.write_all(b"[")?;
            let mut first = true;
            for i in iter {
                if first {
                    first = false;
                } else {
                    w.write_all(b", ")?;
                }
                f(w, fc, i)?;
            }
            w.write_all(b"]")
        }
        match self {
            Array::Null(v) => {
                format_array(w, fc, (0..*v).map(|_| &()), |f, _, _| {
                    f.write_all(NULL_STR.as_bytes())
                })
            }
            Array::Undefined(v) => {
                format_array(w, fc, (0..*v).map(|_| &()), |f, _, _| {
                    f.write_all(UNDEFINED_STR.as_bytes())
                })
            }
            Array::Int(v) => format_array(w, fc, v.iter(), |f, _, v| {
                f.write_fmt(format_args!("{v}"))
            }),
            Array::Bytes(v) => {
                format_array(w, fc, v.iter(), |f, _, v| format_bytes(f, v))
            }
            Array::String(v) => format_array(w, fc, v.iter(), |f, _, v| {
                format_quoted_string(f, v)
            }),
            Array::Error(v) => {
                format_array(w, fc, v.iter(), |f, _, v| format_error(f, v))
            }
            Array::Array(v) => {
                format_array(w, fc, v.iter(), |f, fc, v| v.format(f, fc))
            }
            Array::Object(v) => {
                format_array(w, fc, v.iter(), |f, fc, v| v.format(f, fc))
            }
            Array::FieldReference(v) => {
                format_array(w, fc, v.iter(), |_f, _, _v| todo!())
            }
            Array::SlicedFieldReference(v) => {
                format_array(w, fc, v.iter(), |_f, _, _v| todo!())
            }
            Array::Custom(v) => format_array(w, fc, v.iter(), |f, _, v| {
                v.stringify(f).map(|_| ())
            }),
            Array::Mixed(v) => {
                format_array(w, fc, v.iter(), |f, fc, v| v.format(f, fc))
            }
        }
    }
}
