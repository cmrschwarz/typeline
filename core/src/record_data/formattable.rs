use std::io::Write;

use num::{BigInt, BigRational};

use crate::{
    operators::errors::OperatorApplicationError,
    record_data::field_value::{
        format_rational, Array, FormattingContext, Object, Undefined,
        RATIONAL_DIGITS,
    },
    utils::{
        counting_writer::{
            CharLimitedLengthAndCharsCountingWriter,
            LengthAndCharsCountingWriter, LengthCountingWriter,
        },
        escaped_writer::EscapedWriter,
        int_string_conversions::i64_digits,
        text_write::TextWriteIoAdapter,
        MAX_UTF8_CHAR_LEN,
    },
    NULL_STR, UNDEFINED_STR,
};

use super::{
    custom_data::CustomData, field_value::Null, field_value_ref::FieldValueRef,
};

// format string grammar:
#[rustfmt::skip]
/*
format_string = ( [<brace_escaped_text>] [format_key] )*
format_key = '{' [key] [ ':' format_spec ] '}'
format_spec = [[fill]align]['+']['#'|'##']['0'][width]['.'precision][debug_repr[number_format]]
fill = <any character>
align = '<' | '^' | '>'
debug_repr = ['?' | '??'] ['%'] | '%'
number_format = 'x' | 'X' | '0x' | '0X' | 'o' | '0o' | 'b' | '0b' | 'e' | 'E'
key = identifier
width = identifier | number
precision = identifier | number
identifier = basic_identifier | escaped_identifier
basic_identifier = '\p{XID_Start}' '\p{XID_Continue}'
escaped_identifier = '@{' <brace_escaped_text> '}'
*/

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum NumberFormat {
    #[default]
    Default, // the default value representation, e.g. '42' for 42
    Binary,        // base 2, e.g '101010' for 42
    BinaryZeroB,   // binary, but with leading 0b, e.g. '0b101010' for 42
    Octal,         // base 8, e.g '52' for 42
    OctalZeroO,    // ocatl, but with leading 0o, e.g. '0o52' for 42
    LowerHex,      // lower case hexadecimal, e.g '2a' for 42
    UpperHex,      // upper case hexadecimal, e.g '2A' for 42
    LowerHexZeroX, // like LowerHex, but with leading 0x, e.g. '0x2a' for 42
    UpperHexZeroX, // like UpperHex, but with leading 0x, e.g. '0x2A' for 42
    LowerExp,      // lower case scientific notation, e.g. '4.2e1' for 42
    UpperExp,      // upper case scientific notation, e.g. '4.2E1' for 42
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TypeReprFormat {
    #[default]
    Regular, // regular representation, without string escaping etc.
    Typed, // typed + escaped, e.g. "foo\n" insead of foo<newline>, no errors
    Debug, // like typed, but prefix ~ for stream values
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PrettyPrintFormat {
    #[default]
    Regular, // default. objects & arrays get spaces, but stay on one line
    Pretty,  /* add newlines and indentation to objects. adds 0x to hex
              * numbers */
    Compact, // no spaces at all
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FormatFillSpec {
    pub fill_char: Option<char>,
    pub alignment: FormatFillAlignment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FormatFillAlignment {
    #[default]
    Right,
    Left,
    Center,
}

impl FormatFillSpec {
    pub fn new(
        fill_char: Option<char>,
        alignment: FormatFillAlignment,
    ) -> Self {
        Self {
            fill_char,
            alignment,
        }
    }
    pub fn get_fill_char(&self) -> char {
        self.fill_char.unwrap_or(' ')
    }
    pub fn distribute_padding(&self, padding: usize) -> (usize, usize) {
        match self.alignment {
            FormatFillAlignment::Left => (padding, 0),
            FormatFillAlignment::Center => ((padding + 1) / 2, padding / 2),
            FormatFillAlignment::Right => (0, padding),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FormatOptions {
    pub fill: Option<FormatFillSpec>,
    pub add_plus_sign: bool,

    pub number_format: NumberFormat,

    pub type_repr: TypeReprFormat,

    pub pretty_print: PrettyPrintFormat,

    pub zero_pad_numbers: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RealizedFormatKey {
    pub min_char_count: usize,
    pub float_precision: Option<usize>,
    pub opts: FormatOptions,
}

pub struct TextLayout {
    pub truncated_text_len: usize,
    pub padding: usize,
}

pub struct TextBounds {
    pub len: usize,
    pub char_count: usize,
}

#[derive(Clone, Copy)]
pub struct ValueFormattingOpts {
    pub is_stream_value: bool,
    pub type_repr_format: TypeReprFormat,
}

impl TextBounds {
    fn new(len: usize, char_count: usize) -> Self {
        Self { len, char_count }
    }
}

impl TextLayout {
    pub fn new(truncated_text_len: usize, padding: usize) -> Self {
        Self {
            truncated_text_len,
            padding,
        }
    }
    pub fn total_len(&self, opts: &FormatOptions) -> usize {
        self.truncated_text_len + self.padding * opts.fill_char_width()
    }
}

impl FormatOptions {
    fn fill_char_width(&self) -> usize {
        if let Some(f) = self.fill {
            return f.fill_char.map_or(1, char::len_utf8);
        }
        1
    }
}

pub trait Formatable<'a> {
    type FormattingContext: 'a + Copy;
    fn format<W: std::io::Write>(
        &self,
        ctx: Self::FormattingContext,
        w: &mut W,
    );
    fn refuses_truncation(&self, _ctx: Self::FormattingContext) -> bool {
        true
    }
    fn total_length_cheap(&self, _ctx: Self::FormattingContext) -> bool {
        false
    }
    fn length_total(&self, ctx: Self::FormattingContext) -> usize {
        let mut w = LengthCountingWriter::default();
        self.format(ctx, &mut w);
        w.len
    }
    fn text_bounds_total(&self, ctx: Self::FormattingContext) -> TextBounds {
        let mut w = LengthAndCharsCountingWriter::default();
        self.format(ctx, &mut w);
        TextBounds::new(w.len, w.char_count)
    }
    fn char_bound_text_bounds(
        &self,
        ctx: Self::FormattingContext,
        max_chars: usize,
    ) -> TextBounds {
        if self.refuses_truncation(ctx) {
            return self.text_bounds_total(ctx);
        }
        let mut w = CharLimitedLengthAndCharsCountingWriter::new(max_chars);
        self.format(ctx, &mut w);
        TextBounds::new(w.len, w.char_count)
    }
}

impl Formatable<'_> for [u8] {
    type FormattingContext = ValueFormattingOpts;
    fn refuses_truncation(&self, opts: Self::FormattingContext) -> bool {
        opts.type_repr_format == TypeReprFormat::Regular
    }
    fn total_length_cheap(&self, opts: Self::FormattingContext) -> bool {
        opts.type_repr_format != TypeReprFormat::Regular
    }
    fn format<W: std::io::Write>(
        &self,
        opts: Self::FormattingContext,
        w: &mut W,
    ) {
        if opts.type_repr_format == TypeReprFormat::Regular {
            if let Err(e) = w.write_all(self) {
                assert!(e.kind() == std::io::ErrorKind::WriteZero);
            }
            return;
        }
        if opts.is_stream_value
            && opts.type_repr_format == TypeReprFormat::Debug
        {
            w.write_all(b"~b\"").unwrap();
        } else {
            w.write_all(b"b\"").unwrap();
        }
        let mut ew = EscapedWriter::new(TextWriteIoAdapter(w), b'"');
        ew.write_all(self).unwrap();
        ew.into_inner().unwrap().0.write_all(b"\"").unwrap();
    }
    fn length_total(&self, opts: Self::FormattingContext) -> usize {
        if opts.type_repr_format == TypeReprFormat::Regular {
            return self.len();
        }
        let mut w = LengthCountingWriter::default();
        self.format(opts, &mut w);
        w.len
    }
}
impl<'a> Formatable<'a> for str {
    type FormattingContext = ValueFormattingOpts;
    fn refuses_truncation(&self, opts: Self::FormattingContext) -> bool {
        opts.type_repr_format == TypeReprFormat::Regular
    }
    fn total_length_cheap(&self, opts: Self::FormattingContext) -> bool {
        opts.type_repr_format != TypeReprFormat::Regular
    }
    fn format<W: std::io::Write>(
        &self,
        opts: Self::FormattingContext,
        w: &mut W,
    ) {
        if opts.type_repr_format == TypeReprFormat::Regular {
            if let Err(e) = w.write_all(self.as_bytes()) {
                assert!(e.kind() == std::io::ErrorKind::WriteZero);
            }
            return;
        }
        if opts.is_stream_value
            && opts.type_repr_format == TypeReprFormat::Debug
        {
            w.write_all(b"~\"").unwrap();
        } else {
            w.write_all(b"\"").unwrap();
        }
        let mut ew = EscapedWriter::new(TextWriteIoAdapter(w), b'"');
        ew.write_all(self.as_bytes()).unwrap();
        ew.into_inner().unwrap().0.write_all(b"\"").unwrap();
    }
    fn length_total(&self, opts: Self::FormattingContext) -> usize {
        if opts.type_repr_format == TypeReprFormat::Regular {
            return self.len();
        }
        let mut w = LengthCountingWriter::default();
        self.format(opts, &mut w);
        w.len
    }
}
impl<'a> Formatable<'a> for i64 {
    type FormattingContext = &'a RealizedFormatKey;
    fn total_length_cheap(&self, _ctx: &'a RealizedFormatKey) -> bool {
        true
    }
    fn format<W: std::io::Write>(
        &self,
        ctx: &'a RealizedFormatKey,
        w: &mut W,
    ) {
        let char_count = ctx.min_char_count;
        if ctx.opts.add_plus_sign {
            if ctx.opts.zero_pad_numbers {
                w.write_fmt(format_args!("{self:+0char_count$}")).unwrap();
                return;
            }
            w.write_fmt(format_args!("{self:+char_count$}")).unwrap();
            return;
        }
        if ctx.opts.zero_pad_numbers {
            w.write_fmt(format_args!("{self:0char_count$}")).unwrap();
            return;
        }
        w.write_fmt(format_args!("{self}")).unwrap();
    }
    fn length_total(&self, ctx: Self::FormattingContext) -> usize {
        let digits = i64_digits(ctx.opts.add_plus_sign, *self);
        if !ctx.opts.zero_pad_numbers {
            return digits;
        }
        digits.max(ctx.min_char_count)
    }
    fn text_bounds_total(&self, ctx: Self::FormattingContext) -> TextBounds {
        let len = self.length_total(ctx);
        TextBounds::new(len, len)
    }
}
impl<'a> Formatable<'a> for Object {
    type FormattingContext = &'a FormattingContext<'a>;

    fn format<W: std::io::Write>(
        &self,
        fc: &'a FormattingContext<'a>,
        w: &mut W,
    ) {
        self.format(&mut TextWriteIoAdapter(w), fc).unwrap();
    }
}
impl<'a> Formatable<'a> for Array {
    type FormattingContext = &'a FormattingContext<'a>;

    fn format<W: std::io::Write>(
        &self,
        fc: &'a FormattingContext<'a>,
        w: &mut W,
    ) {
        Array::format(self, &mut TextWriteIoAdapter(w), fc).unwrap();
    }
}
impl<'a> Formatable<'a> for BigRational {
    type FormattingContext = &'a RealizedFormatKey;

    fn format<W: std::io::Write>(
        &self,
        _fc: &'a RealizedFormatKey,
        w: &mut W,
    ) {
        // TODO: we dont support zero pad etc. for now
        format_rational(&mut TextWriteIoAdapter(w), self, RATIONAL_DIGITS)
            .unwrap();
    }
}
impl<'a> Formatable<'a> for BigInt {
    type FormattingContext = &'a RealizedFormatKey;

    fn format<W: std::io::Write>(
        &self,
        _fc: &'a RealizedFormatKey,
        w: &mut W,
    ) {
        // TODO: we dont support zero pad etc. for now
        w.write_fmt(format_args!("{self}")).unwrap();
    }
}
impl<'a> Formatable<'a> for f64 {
    type FormattingContext = &'a RealizedFormatKey;

    fn format<W: std::io::Write>(
        &self,
        ctx: &'a RealizedFormatKey,
        w: &mut W,
    ) {
        let char_count = ctx.min_char_count;
        if let Some(float_prec) = ctx.float_precision {
            if ctx.opts.add_plus_sign {
                if ctx.opts.zero_pad_numbers {
                    w.write_fmt(format_args!(
                        "{self:+0char_count$.float_prec$}"
                    ))
                    .unwrap();
                    return;
                }
                w.write_fmt(format_args!("{self:+char_count$.float_prec$}"))
                    .unwrap();
                return;
            }
            if ctx.opts.zero_pad_numbers {
                w.write_fmt(format_args!("{self:0char_count$.float_prec$}"))
                    .unwrap();
                return;
            }
            w.write_fmt(format_args!("{self:.float_prec$}")).unwrap();
            return;
        }
        if ctx.opts.add_plus_sign {
            if ctx.opts.zero_pad_numbers {
                w.write_fmt(format_args!("{self:+0char_count$}")).unwrap();
                return;
            }
            w.write_fmt(format_args!("{self:+char_count$}")).unwrap();
            return;
        }
        if ctx.opts.zero_pad_numbers {
            w.write_fmt(format_args!("{self:0char_count$}")).unwrap();
        }
    }
}
impl<'a> Formatable<'a> for Null {
    type FormattingContext = ();
    fn total_length_cheap(&self, _ctx: ()) -> bool {
        true
    }
    fn format<W: std::io::Write>(&self, _ctx: (), w: &mut W) {
        w.write_all(NULL_STR.as_bytes()).unwrap();
    }
    fn length_total(&self, _ctx: Self::FormattingContext) -> usize {
        NULL_STR.len()
    }
    fn text_bounds_total(&self, _ctx: Self::FormattingContext) -> TextBounds {
        let len = NULL_STR.len();
        TextBounds::new(len, len)
    }
}
impl<'a> Formatable<'a> for Undefined {
    type FormattingContext = ();
    fn total_length_cheap(&self, _ctx: ()) -> bool {
        true
    }
    fn format<W: std::io::Write>(&self, _ctx: (), w: &mut W) {
        w.write_all(UNDEFINED_STR.as_bytes()).unwrap();
    }
    fn length_total(&self, _ctx: Self::FormattingContext) -> usize {
        UNDEFINED_STR.len()
    }
    fn text_bounds_total(&self, _ctx: Self::FormattingContext) -> TextBounds {
        let len = UNDEFINED_STR.len();
        TextBounds::new(len, len)
    }
}
impl<'a> Formatable<'a> for OperatorApplicationError {
    type FormattingContext = ValueFormattingOpts; // is_stream_value
    fn format<W: std::io::Write>(
        &self,
        opts: Self::FormattingContext,
        w: &mut W,
    ) {
        let sv = match opts.type_repr_format {
            TypeReprFormat::Regular => unreachable!(),
            TypeReprFormat::Typed => "",
            TypeReprFormat::Debug => {
                if opts.is_stream_value {
                    "~"
                } else {
                    ""
                }
            }
        };
        w.write_fmt(format_args!("{sv}(error)\"")).unwrap();
        let mut ew = EscapedWriter::new(TextWriteIoAdapter(w), b'"');
        std::io::Write::write_all(&mut ew, self.message().as_bytes()).unwrap();
        ew.into_inner().unwrap().0.write_all(b"\"").unwrap();
    }
}

impl<'a> Formatable<'a> for dyn CustomData {
    type FormattingContext = &'a RealizedFormatKey;

    fn format<W: std::io::Write>(
        &self,
        ctx: Self::FormattingContext,
        w: &mut W,
    ) {
        CustomData::format_raw(self, &mut TextWriteIoAdapter(w), ctx).unwrap();
    }
}

impl<'a> Formatable<'a> for FieldValueRef<'a> {
    type FormattingContext = &'a FormattingContext<'a>;
    fn format<W: std::io::Write>(
        &self,
        opts: Self::FormattingContext,
        w: &mut W,
    ) {
        match *self {
            FieldValueRef::Null => Formatable::format(&Null, (), w),
            FieldValueRef::Undefined => Formatable::format(&Undefined, (), w),
            FieldValueRef::Int(v) => Formatable::format(v, &opts.rfk, w),
            FieldValueRef::BigInt(v) => Formatable::format(v, &opts.rfk, w),
            FieldValueRef::Float(v) => Formatable::format(v, &opts.rfk, w),
            FieldValueRef::Rational(v) => Formatable::format(v, &opts.rfk, w),
            FieldValueRef::Text(v) => {
                Formatable::format(v, opts.value_formatting_opts(), w)
            }
            FieldValueRef::Bytes(v) => {
                Formatable::format(v, opts.value_formatting_opts(), w)
            }
            FieldValueRef::Array(v) => Formatable::format(v, opts, w),
            FieldValueRef::Object(v) => Formatable::format(v, opts, w),
            FieldValueRef::Custom(v) => Formatable::format(&**v, &opts.rfk, w),
            FieldValueRef::Error(v) => {
                Formatable::format(v, opts.value_formatting_opts(), w)
            }
            FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                FieldValueRef::format(self, &mut TextWriteIoAdapter(w), opts)
                    .unwrap()
            }
        }
    }

    fn refuses_truncation(&self, opts: Self::FormattingContext) -> bool {
        match *self {
            FieldValueRef::Null => Formatable::refuses_truncation(&Null, ()),
            FieldValueRef::Undefined => {
                Formatable::refuses_truncation(&Undefined, ())
            }
            FieldValueRef::Int(v) => {
                Formatable::refuses_truncation(v, &opts.rfk)
            }
            FieldValueRef::BigInt(v) => {
                Formatable::refuses_truncation(v, &opts.rfk)
            }
            FieldValueRef::Float(v) => {
                Formatable::refuses_truncation(v, &opts.rfk)
            }
            FieldValueRef::Rational(v) => {
                Formatable::refuses_truncation(v, &opts.rfk)
            }
            FieldValueRef::Text(v) => {
                Formatable::refuses_truncation(v, opts.value_formatting_opts())
            }
            FieldValueRef::Bytes(v) => {
                Formatable::refuses_truncation(v, opts.value_formatting_opts())
            }
            FieldValueRef::Array(v) => Formatable::refuses_truncation(v, opts),
            FieldValueRef::Object(v) => {
                Formatable::refuses_truncation(v, opts)
            }
            FieldValueRef::Custom(v) => {
                Formatable::refuses_truncation(&**v, &opts.rfk)
            }
            FieldValueRef::Error(v) => {
                Formatable::refuses_truncation(v, opts.value_formatting_opts())
            }
            FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }

    fn total_length_cheap(&self, opts: Self::FormattingContext) -> bool {
        match *self {
            FieldValueRef::Null => Formatable::total_length_cheap(&Null, ()),
            FieldValueRef::Undefined => {
                Formatable::total_length_cheap(&Undefined, ())
            }
            FieldValueRef::Int(v) => {
                Formatable::total_length_cheap(v, &opts.rfk)
            }
            FieldValueRef::BigInt(v) => {
                Formatable::total_length_cheap(v, &opts.rfk)
            }
            FieldValueRef::Float(v) => {
                Formatable::total_length_cheap(v, &opts.rfk)
            }
            FieldValueRef::Rational(v) => {
                Formatable::total_length_cheap(v, &opts.rfk)
            }
            FieldValueRef::Text(v) => {
                Formatable::total_length_cheap(v, opts.value_formatting_opts())
            }
            FieldValueRef::Bytes(v) => {
                Formatable::total_length_cheap(v, opts.value_formatting_opts())
            }
            FieldValueRef::Array(v) => Formatable::total_length_cheap(v, opts),
            FieldValueRef::Object(v) => {
                Formatable::total_length_cheap(v, opts)
            }
            FieldValueRef::Custom(v) => {
                Formatable::total_length_cheap(&**v, &opts.rfk)
            }
            FieldValueRef::Error(v) => {
                Formatable::total_length_cheap(v, opts.value_formatting_opts())
            }
            FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }

    fn length_total(&self, opts: Self::FormattingContext) -> usize {
        match *self {
            FieldValueRef::Null => Formatable::length_total(&Null, ()),
            FieldValueRef::Undefined => {
                Formatable::length_total(&Undefined, ())
            }
            FieldValueRef::Int(v) => Formatable::length_total(v, &opts.rfk),
            FieldValueRef::BigInt(v) => Formatable::length_total(v, &opts.rfk),
            FieldValueRef::Float(v) => Formatable::length_total(v, &opts.rfk),
            FieldValueRef::Rational(v) => {
                Formatable::length_total(v, &opts.rfk)
            }
            FieldValueRef::Text(v) => {
                Formatable::length_total(v, opts.value_formatting_opts())
            }
            FieldValueRef::Bytes(v) => {
                Formatable::length_total(v, opts.value_formatting_opts())
            }
            FieldValueRef::Array(v) => Formatable::length_total(v, opts),
            FieldValueRef::Object(v) => Formatable::length_total(v, opts),
            FieldValueRef::Custom(v) => {
                Formatable::length_total(&**v, &opts.rfk)
            }
            FieldValueRef::Error(v) => {
                Formatable::length_total(v, opts.value_formatting_opts())
            }
            FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }

    fn text_bounds_total(&self, opts: Self::FormattingContext) -> TextBounds {
        match *self {
            FieldValueRef::Null => Formatable::text_bounds_total(&Null, ()),
            FieldValueRef::Undefined => {
                Formatable::text_bounds_total(&Undefined, ())
            }
            FieldValueRef::Int(v) => {
                Formatable::text_bounds_total(v, &opts.rfk)
            }
            FieldValueRef::BigInt(v) => {
                Formatable::text_bounds_total(v, &opts.rfk)
            }
            FieldValueRef::Float(v) => {
                Formatable::text_bounds_total(v, &opts.rfk)
            }
            FieldValueRef::Rational(v) => {
                Formatable::text_bounds_total(v, &opts.rfk)
            }
            FieldValueRef::Text(v) => {
                Formatable::text_bounds_total(v, opts.value_formatting_opts())
            }
            FieldValueRef::Bytes(v) => {
                Formatable::text_bounds_total(v, opts.value_formatting_opts())
            }
            FieldValueRef::Array(v) => Formatable::text_bounds_total(v, opts),
            FieldValueRef::Object(v) => Formatable::text_bounds_total(v, opts),
            FieldValueRef::Custom(v) => {
                Formatable::text_bounds_total(&**v, &opts.rfk)
            }
            FieldValueRef::Error(v) => {
                Formatable::text_bounds_total(v, opts.value_formatting_opts())
            }
            FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }

    fn char_bound_text_bounds(
        &self,
        opts: Self::FormattingContext,
        max_chars: usize,
    ) -> TextBounds {
        match *self {
            FieldValueRef::Null => {
                Formatable::char_bound_text_bounds(&Null, (), max_chars)
            }
            FieldValueRef::Undefined => {
                Formatable::char_bound_text_bounds(&Undefined, (), max_chars)
            }
            FieldValueRef::Int(v) => {
                Formatable::char_bound_text_bounds(v, &opts.rfk, max_chars)
            }
            FieldValueRef::BigInt(v) => {
                Formatable::char_bound_text_bounds(v, &opts.rfk, max_chars)
            }
            FieldValueRef::Float(v) => {
                Formatable::char_bound_text_bounds(v, &opts.rfk, max_chars)
            }
            FieldValueRef::Rational(v) => {
                Formatable::char_bound_text_bounds(v, &opts.rfk, max_chars)
            }
            FieldValueRef::Text(v) => Formatable::char_bound_text_bounds(
                v,
                opts.value_formatting_opts(),
                max_chars,
            ),
            FieldValueRef::Bytes(v) => Formatable::char_bound_text_bounds(
                v,
                opts.value_formatting_opts(),
                max_chars,
            ),
            FieldValueRef::Array(v) => {
                Formatable::char_bound_text_bounds(v, opts, max_chars)
            }
            FieldValueRef::Object(v) => {
                Formatable::char_bound_text_bounds(v, opts, max_chars)
            }
            FieldValueRef::Custom(v) => {
                Formatable::char_bound_text_bounds(&**v, &opts.rfk, max_chars)
            }
            FieldValueRef::Error(v) => Formatable::char_bound_text_bounds(
                v,
                opts.value_formatting_opts(),
                max_chars,
            ),
            FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }
}

pub fn calc_fmt_layout<'a, F: Formatable<'a> + ?Sized>(
    ctx: F::FormattingContext,
    min_chars: usize,
    max_chars: usize,
    formatable: &F,
) -> TextLayout {
    if max_chars == usize::MAX || formatable.refuses_truncation(ctx) {
        if formatable.total_length_cheap(ctx) {
            let text_len = formatable.length_total(ctx);
            if (text_len / MAX_UTF8_CHAR_LEN) >= min_chars {
                return TextLayout::new(text_len, 0);
            }
        }
        let tb = formatable.text_bounds_total(ctx);
        if tb.char_count >= min_chars {
            return TextLayout::new(tb.len, 0);
        }
        return TextLayout::new(tb.len, min_chars - tb.char_count);
    }
    if min_chars > max_chars {
        let tb = formatable.char_bound_text_bounds(ctx, max_chars);
        // we might have min_chars = 1, max_chars = 0
        // so we need the saturating sub
        return TextLayout::new(
            tb.len,
            min_chars.saturating_sub(tb.char_count),
        );
    }
    let tb = formatable.char_bound_text_bounds(ctx, max_chars);
    if tb.char_count >= max_chars {
        return TextLayout::new(tb.len, 0);
    }
    TextLayout::new(tb.len, min_chars.saturating_sub(tb.char_count))
}
