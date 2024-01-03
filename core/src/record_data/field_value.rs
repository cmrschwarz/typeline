use std::{
    any::Any,
    fmt::Display,
    io::Write,
    mem::ManuallyDrop,
    ops::{Add, AddAssign, Div, MulAssign, Rem, Sub},
};

use indexmap::IndexMap;
use num::{BigInt, BigRational, FromPrimitive, One, Signed, Zero};

use crate::{
    operators::{errors::OperatorApplicationError, format::RealizedFormatKey},
    utils::{
        escaped_writer::EscapedWriter,
        string_store::{StringStore, StringStoreEntry},
    },
    NULL_STR, UNDEFINED_STR,
};

use super::{
    custom_data::CustomDataBox,
    field::{FieldManager, FieldRefOffset},
    field_data::{FieldValueRepr, FieldValueType, FixedSizeFieldValueType},
    match_set::MatchSetManager,
    stream_value::StreamValueId,
    typed::FieldValueRef,
};

// the different logical data types
// irrespective of representation in memory, see FieldDataRepr for that

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
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
    StreamValueId(StreamValueId),
    FieldReference(FieldReference),
    SlicedFieldReference(SlicedFieldReference),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Null;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl Array {
    pub fn len(&self) -> usize {
        match self {
            Array::Null(len) => *len,
            Array::Undefined(len) => *len,
            Array::Int(a) => a.len(),
            Array::Bytes(a) => a.len(),
            Array::String(a) => a.len(),
            Array::Error(a) => a.len(),
            Array::Array(a) => a.len(),
            Array::Object(a) => a.len(),
            Array::FieldReference(a) => a.len(),
            Array::SlicedFieldReference(a) => a.len(),
            Array::Custom(a) => a.len(),
            Array::Mixed(a) => a.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Object {
    pub fn len(&self) -> usize {
        match self {
            Object::KeysStored(d) => d.len(),
            Object::KeysInterned(d) => d.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
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
            FieldValueKind::Text => FieldValueRepr::TextInline,
            FieldValueKind::Object => FieldValueRepr::Object,
            FieldValueKind::Custom => FieldValueRepr::Custom,
            FieldValueKind::Array => FieldValueRepr::Array,
            FieldValueKind::FieldReference => FieldValueRepr::FieldReference,
            FieldValueKind::SlicedFieldReference => {
                FieldValueRepr::SlicedFieldReference
            }
            FieldValueKind::StreamValueId => FieldValueRepr::StreamValueId,
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
            FieldValueKind::Text => FieldValueRepr::TextBuffer,
            FieldValueKind::Object => FieldValueRepr::Object,
            FieldValueKind::Custom => FieldValueRepr::Custom,
            FieldValueKind::Array => FieldValueRepr::Array,
            FieldValueKind::FieldReference => FieldValueRepr::FieldReference,
            FieldValueKind::SlicedFieldReference => {
                FieldValueRepr::SlicedFieldReference
            }
            FieldValueKind::StreamValueId => FieldValueRepr::StreamValueId,
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
            FieldValueKind::StreamValueId => "stream_value_id",
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
            FieldValue::StreamValueId(_) => FieldValueKind::StreamValueId,
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
    pub fn format(
        &self,
        w: &mut impl std::io::Write,
        fc: &FormattingContext<'_>,
    ) -> std::io::Result<()> {
        match self {
            FieldValue::Null => w.write(NULL_STR.as_bytes()).map(|_| ()),
            FieldValue::Undefined => {
                w.write(UNDEFINED_STR.as_bytes()).map(|_| ())
            }
            FieldValue::Int(v) => w.write_fmt(format_args!("{v}")),
            FieldValue::BigInt(v) => w.write_fmt(format_args!("{v}")),
            FieldValue::Float(v) => w.write_fmt(format_args!("{v}")),
            FieldValue::Rational(v) => {
                if fc.print_rationals_raw {
                    w.write_fmt(format_args!("{v}"))
                } else {
                    format_rational(w, v, RATIONAL_DIGITS)
                }
            }
            FieldValue::Bytes(v) => format_bytes(w, v),
            FieldValue::Text(v) => format_quoted_string(w, v),
            FieldValue::Error(e) => format_error(w, e),
            FieldValue::Array(a) => a.format(w, fc),
            FieldValue::Object(o) => o.format(w, fc),
            FieldValue::FieldReference(_) => todo!(),
            FieldValue::SlicedFieldReference(_) => todo!(),
            FieldValue::StreamValueId(_) => todo!(),
            FieldValue::Custom(v) => v.stringify(w, &fc.rfk).map(|_| ()),
        }
    }
}

pub fn format_bytes(
    w: &mut impl std::io::Write,
    v: &[u8],
) -> std::io::Result<()> {
    w.write_all(b"b'")?;
    let mut w = EscapedWriter::new(w, b'"');
    w.write_all(v)?;
    w.into_inner().unwrap().write_all(b"'")?;
    Ok(())
}

pub fn format_quoted_string(
    w: &mut impl std::io::Write,
    v: &str,
) -> std::io::Result<()> {
    w.write_all(b"\"")?;
    let mut w = EscapedWriter::new(w, b'"');
    w.write_all(v.as_bytes())?;
    w.into_inner().unwrap().write_all(b"\"")?;
    Ok(())
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
    pub print_rationals_raw: bool,
    pub rfk: RealizedFormatKey,
}

pub const RATIONAL_DIGITS: u32 = 40; // TODO: make this configurable

pub fn format_rational(
    w: &mut impl std::io::Write,
    v: &BigRational,
    decimals: u32,
) -> std::io::Result<()> {
    // PERF: this function is stupid
    if v.is_integer() {
        w.write_fmt(format_args!("{}", v))?;
        return Ok(());
    }
    let negative = v.is_negative();
    let mut whole_number = v.to_integer();
    let mut v = v.sub(&whole_number).abs();
    let one_half =
        BigRational::new(BigInt::one(), BigInt::from_u8(2).unwrap());
    if decimals == 0 {
        if v >= one_half {
            whole_number.add_assign(if negative {
                -BigInt::one()
            } else {
                BigInt::one()
            });
        }
        w.write_fmt(format_args!("{}", whole_number))?;
        return Ok(());
    }
    w.write_fmt(format_args!("{}.", &whole_number))?;

    v.mul_assign(BigInt::from_u64(10).unwrap().pow(decimals));
    let mut decimal_part = v.to_integer();
    v = v.sub(&decimal_part).abs();
    if v >= one_half {
        decimal_part.add_assign(BigInt::one());
    }
    if !v.is_zero() {
        w.write_fmt(format_args!("{}", &decimal_part))?;
        return Ok(());
    }
    let ten = BigInt::from_u8(10).unwrap();
    // PERF: really bad
    loop {
        let rem = decimal_part.clone().rem(&ten);
        if !rem.is_zero() {
            break;
        }
        decimal_part = decimal_part.div(&ten).add(rem);
    }
    w.write_fmt(format_args!("{}", &decimal_part))?;
    Ok(())
}

impl Object {
    pub fn format(
        &self,
        w: &mut impl std::io::Write,
        fc: &FormattingContext<'_>,
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
        fc: &FormattingContext<'_>,
    ) -> std::io::Result<()> {
        fn format_array<
            'a,
            T: 'a,
            I: Iterator<Item = &'a T>,
            W: std::io::Write,
        >(
            w: &mut W,
            fc: &FormattingContext<'_>,
            iter: I,
            mut f: impl FnMut(
                &mut W,
                &FormattingContext<'_>,
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
            Array::Custom(v) => format_array(w, fc, v.iter(), |f, fc, v| {
                v.stringify(f, &fc.rfk).map(|_| ())
            }),
            Array::Mixed(v) => {
                format_array(w, fc, v.iter(), |f, fc, v| v.format(f, fc))
            }
        }
    }
}
