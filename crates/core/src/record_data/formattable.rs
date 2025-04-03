use std::{
    borrow::Borrow,
    io::ErrorKind,
    ops::{DivAssign, Rem, SubAssign},
};

use metamatch::metamatch;
use num::{
    BigInt, BigRational, FromPrimitive, One, Signed, ToPrimitive, Zero,
};

use crate::{
    cli::call_expr::Argument,
    operators::errors::OperatorApplicationError,
    options::chain_settings::RationalsPrintMode,
    record_data::{
        array::Array, field_value::Undefined, object::Object,
        scope_manager::OpDeclRef, stream_value::StreamValueData,
    },
    utils::{
        counting_writer::{
            CharLimitedLengthAndCharsCountingWriter,
            LengthAndCharsCountingWriter, LengthCountingWriter,
        },
        escaped_writer::{
            escape_to_maybe_text_write, EscapedWriter, ESCAPE_DOUBLE_QUOTES,
        },
        int_string_conversions::{bool_to_str, i64_digits},
        lazy_lock_guard::LazyRwLockGuard,
        maybe_text::MaybeTextRef,
        string_store::StringStore,
        text_write::{MaybeTextWrite, TextWrite, TextWriteIoAdapter},
        MAX_UTF8_CHAR_LEN,
    },
    NULL_STR, UNDEFINED_STR,
};
use std::ops::{AddAssign, MulAssign, Sub};

use super::{
    custom_data::CustomData,
    field::FieldManager,
    field_value::Null,
    field_value_ref::FieldValueRef,
    match_set::MatchSetManager,
    stream_value::{StreamValue, StreamValueDataType},
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
            FormatFillAlignment::Center => (padding.div_ceil(2), padding / 2),
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

#[derive(Default)]
pub struct FormattingContext<'a, 'b> {
    pub ss: Option<&'a mut LazyRwLockGuard<'b, StringStore>>,
    pub fm: Option<&'a FieldManager>,
    pub msm: Option<&'a MatchSetManager>,
    pub rationals_print_mode: RationalsPrintMode,
    pub is_stream_value: bool,
    pub rfk: RealizedFormatKey,
}

impl ValueFormattingOpts {
    pub fn for_nested_value() -> Self {
        ValueFormattingOpts {
            is_stream_value: false,
            type_repr_format: TypeReprFormat::Typed,
        }
    }
}

impl TypeReprFormat {
    pub fn is_typed(&self) -> bool {
        match self {
            TypeReprFormat::Regular => false,
            TypeReprFormat::Typed | TypeReprFormat::Debug => true,
        }
    }
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

impl RealizedFormatKey {
    pub fn with_type_repr(repr: TypeReprFormat) -> Self {
        Self {
            opts: FormatOptions {
                type_repr: repr,
                ..FormatOptions::default()
            },
            ..Self::default()
        }
    }
    pub fn must_buffer_stream(&self, sv: &StreamValue) -> bool {
        match self.opts.type_repr {
            TypeReprFormat::Regular => {}
            TypeReprFormat::Typed | TypeReprFormat::Debug => {
                if sv.data_type.is_none()
                    || sv.data_type == Some(StreamValueDataType::MaybeText)
                {
                    return true;
                }
            }
        }
        if self.min_char_count > 0
            && sv.data_len_present() < self.min_char_count
        {
            return true;
        }
        false
    }
}

pub trait Formattable<'a, 'b> {
    type Context;
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()>;
    fn refuses_truncation(&self, _ctx: &mut Self::Context) -> bool {
        true
    }
    fn total_length_cheap(&self, _ctx: &mut Self::Context) -> bool {
        false
    }
    fn length_total(&self, ctx: &mut Self::Context) -> usize {
        let mut w = LengthCountingWriter::default();
        self.format(ctx, &mut w).unwrap();
        w.len
    }
    fn text_bounds_total(&self, ctx: &mut Self::Context) -> TextBounds {
        let mut w = LengthAndCharsCountingWriter::default();
        self.format(ctx, &mut w).unwrap();
        TextBounds::new(w.len, w.char_count)
    }
    fn char_bound_text_bounds(
        &self,
        ctx: &mut Self::Context,
        max_chars: usize,
    ) -> TextBounds {
        if self.refuses_truncation(ctx) {
            return self.text_bounds_total(ctx);
        }
        let mut w = CharLimitedLengthAndCharsCountingWriter::new(max_chars);
        if let Err(e) = self.format(ctx, &mut w) {
            assert!(std::io::ErrorKind::WriteZero == e.kind());
        }
        TextBounds::new(w.len, w.char_count)
    }
}

impl Formattable<'_, '_> for [u8] {
    type Context = ValueFormattingOpts;
    fn refuses_truncation(&self, opts: &mut Self::Context) -> bool {
        opts.type_repr_format == TypeReprFormat::Regular
    }
    fn total_length_cheap(&self, opts: &mut Self::Context) -> bool {
        opts.type_repr_format != TypeReprFormat::Regular
    }
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        opts: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        if opts.type_repr_format == TypeReprFormat::Regular {
            return w.write_all(self);
        }
        if opts.is_stream_value
            && opts.type_repr_format == TypeReprFormat::Debug
        {
            w.write_all_text("~b\"")?;
        } else {
            w.write_all_text("b\"")?;
        }
        escape_to_maybe_text_write(w, self, &ESCAPE_DOUBLE_QUOTES)?;

        w.write_all_text("\"")
    }
    fn length_total(&self, opts: &mut Self::Context) -> usize {
        if opts.type_repr_format == TypeReprFormat::Regular {
            return self.len();
        }
        let mut w = LengthCountingWriter::default();
        self.format(opts, &mut w).unwrap();
        w.len
    }
}

impl Formattable<'_, '_> for str {
    type Context = ValueFormattingOpts;
    fn refuses_truncation(&self, opts: &mut Self::Context) -> bool {
        opts.type_repr_format == TypeReprFormat::Regular
    }
    fn total_length_cheap(&self, opts: &mut Self::Context) -> bool {
        opts.type_repr_format != TypeReprFormat::Regular
    }
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        opts: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        if opts.type_repr_format == TypeReprFormat::Regular {
            return w.write_all_text(self);
        }
        if opts.is_stream_value
            && opts.type_repr_format == TypeReprFormat::Debug
        {
            w.write_all_text("~\"")?;
        } else {
            w.write_all_text("\"")?;
        }
        let mut ew = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
        TextWrite::write_all_text(&mut ew, self)?;
        ew.into_inner()?.write_all_text("\"")
    }
    fn length_total(&self, opts: &mut Self::Context) -> usize {
        if opts.type_repr_format == TypeReprFormat::Regular {
            return self.len();
        }
        let mut w = LengthCountingWriter::default();
        self.format(opts, &mut w).unwrap();
        w.len
    }
}
impl Formattable<'_, '_> for i64 {
    type Context = RealizedFormatKey;
    fn total_length_cheap(&self, _ctx: &mut Self::Context) -> bool {
        true
    }
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        let char_count = ctx.min_char_count;
        if ctx.opts.add_plus_sign {
            if ctx.opts.zero_pad_numbers {
                return w.write_text_fmt(format_args!("{self:+0char_count$}"));
            }
            return w.write_text_fmt(format_args!("{self:+char_count$}"));
        }
        if ctx.opts.zero_pad_numbers {
            return w.write_text_fmt(format_args!("{self:0char_count$}"));
        }
        w.write_text_fmt(format_args!("{self}"))
    }
    fn length_total(&self, ctx: &mut Self::Context) -> usize {
        let digits = i64_digits(ctx.opts.add_plus_sign, *self);
        if !ctx.opts.zero_pad_numbers {
            return digits;
        }
        digits.max(ctx.min_char_count)
    }
    fn text_bounds_total(&self, ctx: &mut Self::Context) -> TextBounds {
        let len = self.length_total(ctx);
        TextBounds::new(len, len)
    }
}
impl Formattable<'_, '_> for bool {
    type Context = RealizedFormatKey;
    fn total_length_cheap(&self, _ctx: &mut Self::Context) -> bool {
        true
    }
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        _ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        // TODO: padding?
        w.write_text_fmt(format_args!("{self}"))
    }
    fn length_total(&self, _ctx: &mut Self::Context) -> usize {
        // TODO: padding?
        bool_to_str(*self).len()
    }
    fn text_bounds_total(&self, ctx: &mut Self::Context) -> TextBounds {
        let len = self.length_total(ctx);
        TextBounds::new(len, len)
    }
}
impl<'a, 'b: 'a> Formattable<'a, 'b> for Object {
    type Context = FormattingContext<'a, 'b>;

    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        fc: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        w.write_all_text("{")?;
        // TODO: escape keys
        let mut first = true;
        match self {
            Object::KeysStored(m) => {
                for (k, v) in m {
                    if first {
                        first = false;
                    } else {
                        w.write_all_text(", ")?;
                    }
                    format_quoted_string_raw(w, k)?;
                    w.write_all_text(": ")?;
                    Formattable::format(&v.as_ref(), fc, w)?;
                }
            }
            Object::KeysInterned(m) => {
                for (&k, v) in m {
                    if first {
                        first = false;
                    } else {
                        w.write_all_text(", ")?;
                    }
                    let Some(ss) = &mut fc.ss else {
                        return Err(std::io::Error::new(
                            ErrorKind::InvalidInput,
                            "string store needed but not supplied",
                        ));
                    };
                    format_quoted_string_raw(w, ss.get().lookup(k))?;
                    w.write_all_text(": ")?;
                    Formattable::format(&v.as_ref(), fc, w)?;
                }
            }
        }
        w.write_all_text("}")
    }
}
impl<'a, 'b: 'a> Formattable<'a, 'b> for Array {
    type Context = FormattingContext<'a, 'b>;

    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        fc: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        fn format_array<
            'a,
            'b,
            T: Formattable<'a, 'b> + ?Sized,
            I: IntoIterator<Item = impl Borrow<T>>,
            W: MaybeTextWrite + ?Sized,
        >(
            iter: I,
            fc: &mut T::Context,
            w: &mut W,
        ) -> std::io::Result<()> {
            w.write_all_text("[")?;
            let mut first = true;
            for x in iter {
                if first {
                    first = false;
                } else {
                    w.write_all_text(", ")?;
                }
                x.borrow().format(fc, w)?;
            }
            w.write_all_text("]")?;
            Ok(())
        }
        let repr_before = fc.rfk.opts.type_repr;
        if repr_before != TypeReprFormat::Debug {
            fc.rfk.opts.type_repr = TypeReprFormat::Typed;
        }
        let res = metamatch!(match self {
            #[expand(for REP in [Null, Undefined])]
            Array::REP(count) => {
                format_array(std::iter::repeat(REP).take(*count), &mut (), w)
            }

            #[expand(for (REP, T, FC) in [
                (Bool,          bool,                       raw!(&mut fc.rfk)),
                (Int,           i64,                        raw!(&mut fc.rfk)),
                (Float,         f64,                        raw!(&mut fc.rfk)),
                (Array,         Array,                      raw!(fc)),
                (Object,        Object,                     raw!(fc)),
                (BigInt,        BigInt,                     raw!(&mut fc.rfk)),
                (BigRational,   BigRational,                raw!(fc)),
                (Argument,      Argument,                   raw!(fc)),
                (OpDecl,        OpDeclRef,                  raw!(fc)),
                (Error,         OperatorApplicationError,   raw!(&mut fc.value_formatting_opts())),
            ])]
            Array::REP(v) => {
                format_array::<T, _, _>(&**v, FC, w)
            }

            #[expand(for (REP, T, FC) in [
                (Text, str,     raw!(&mut fc.value_formatting_opts())),
                (Bytes, [u8],   raw!(&mut fc.value_formatting_opts())),
                (Custom, raw!(dyn CustomData), raw!(&mut fc.rfk)),
            ])]
            Array::REP(v) => {
                format_array::<T, _, _>(v.iter().map(|v| &**v), FC, w)
            }

            Array::FieldReference(_) | Array::SlicedFieldReference(_) => {
                todo!()
            }

            Array::Mixed(v) => fc.for_nested_values(|fc| {
                format_array(v.iter().map(|v| v.as_ref()), fc, w)
            }),

            Array::StreamValueId(_) => todo!(),
        });
        fc.rfk.opts.type_repr = repr_before;
        res
    }
}
impl<'a, 'b: 'a> Formattable<'a, 'b> for BigRational {
    type Context = FormattingContext<'a, 'b>;
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        fc: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        // TODO: we dont support zero pad etc. for now
        format_rational(w, self, fc.rationals_print_mode)
    }
}
impl Formattable<'_, '_> for BigInt {
    type Context = RealizedFormatKey;
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        _fc: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        // TODO: we dont support zero pad etc. for now
        w.write_text_fmt(format_args!("{self}"))
    }
}
impl Formattable<'_, '_> for f64 {
    type Context = RealizedFormatKey;
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        let char_count = ctx.min_char_count;
        if let Some(float_prec) = ctx.float_precision {
            if ctx.opts.add_plus_sign {
                if ctx.opts.zero_pad_numbers {
                    return w.write_text_fmt(format_args!(
                        "{self:+0char_count$.float_prec$}"
                    ));
                }
                return w.write_text_fmt(format_args!(
                    "{self:+char_count$.float_prec$}"
                ));
            }
            if ctx.opts.zero_pad_numbers {
                return w.write_text_fmt(format_args!(
                    "{self:0char_count$.float_prec$}"
                ));
            }
            return w.write_text_fmt(format_args!("{self:.float_prec$}"));
        }
        if self.fract() == 0.0 {
            if ctx.opts.add_plus_sign {
                if ctx.opts.zero_pad_numbers {
                    return w.write_text_fmt(format_args!(
                        "{self:+0char_count$.1}"
                    ));
                }
                return w
                    .write_text_fmt(format_args!("{self:+char_count$.1}"));
            }
            if ctx.opts.zero_pad_numbers {
                return w
                    .write_text_fmt(format_args!("{self:0char_count$.1}"));
            }
            w.write_text_fmt(format_args!("{self:.1}"))
        } else {
            if ctx.opts.add_plus_sign {
                if ctx.opts.zero_pad_numbers {
                    return w
                        .write_text_fmt(format_args!("{self:+0char_count$}"));
                }
                return w.write_text_fmt(format_args!("{self:+char_count$}"));
            }
            if ctx.opts.zero_pad_numbers {
                return w.write_text_fmt(format_args!("{self:0char_count$}"));
            }
            w.write_text_fmt(format_args!("{self}"))
        }
    }
}
impl Formattable<'_, '_> for Null {
    type Context = ();
    fn total_length_cheap(&self, _ctx: &mut Self::Context) -> bool {
        true
    }
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        _ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        w.write_all_text(NULL_STR)
    }
    fn length_total(&self, _ctx: &mut Self::Context) -> usize {
        NULL_STR.len()
    }
    fn text_bounds_total(&self, _ctx: &mut Self::Context) -> TextBounds {
        let len = NULL_STR.len();
        TextBounds::new(len, len)
    }
}
impl Formattable<'_, '_> for Undefined {
    type Context = ();
    fn total_length_cheap(&self, _ctx: &mut Self::Context) -> bool {
        true
    }
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        _ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        w.write_all_text(UNDEFINED_STR)
    }
    fn length_total(&self, _ctx: &mut Self::Context) -> usize {
        UNDEFINED_STR.len()
    }
    fn text_bounds_total(&self, _ctx: &mut Self::Context) -> TextBounds {
        let len = UNDEFINED_STR.len();
        TextBounds::new(len, len)
    }
}
impl Formattable<'_, '_> for OperatorApplicationError {
    type Context = ValueFormattingOpts; // is_stream_value
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        opts: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
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
        w.write_text_fmt(format_args!("{sv}(error)\"")).unwrap();
        let mut ew = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
        TextWrite::write_all_text(&mut ew, self.message())?;
        ew.into_inner().unwrap().write_all_text("\"")
    }
}

impl Formattable<'_, '_> for dyn CustomData {
    type Context = RealizedFormatKey;

    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        CustomData::format_raw(self, &mut TextWriteIoAdapter(w), ctx)
    }

    fn refuses_truncation(&self, _ctx: &mut Self::Context) -> bool {
        true // TODO
    }

    fn total_length_cheap(&self, _ctx: &mut Self::Context) -> bool {
        false // TODO
    }
}

pub trait WithFormattable {
    type Result;
    fn call<'a, 'b, F: Formattable<'a, 'b> + ?Sized>(
        &mut self,
        v: &F,
        ctx: &mut F::Context,
    ) -> Self::Result;
}

pub fn with_formattable<'a, 'b: 'a, R>(
    fc: &mut FormattingContext<'a, 'b>,
    v: FieldValueRef<'_>,
    mut with_fmt: impl WithFormattable<Result = R>,
) -> R {
    metamatch!(match v {
        #[expand(for T in [Null, Undefined])]
        FieldValueRef::T => with_fmt.call(&T, &mut ()),

        #[expand(for (REP, CTX) in [
            (Bool,         raw!(&mut fc.rfk)),
            (Int,          raw!(&mut fc.rfk)),
            (Float,        raw!(&mut fc.rfk)),
            (Array,        raw!(fc)),
            (Object,       raw!(fc)),
            (BigInt,       raw!(&mut fc.rfk)),
            (BigRational,  raw!(fc)),
            (Argument,     raw!(fc)),
            (OpDecl,       raw!(fc)),
            (Text,         raw!(&mut fc.value_formatting_opts())),
            (Bytes,        raw!(&mut fc.value_formatting_opts())),
            (Error,        raw!(&mut fc.value_formatting_opts())),
        ])]
        FieldValueRef::REP(v) => with_fmt.call(v, CTX),

        FieldValueRef::Custom(v) => with_fmt.call(&**v, &mut fc.rfk),

        #[expand(for REP in [
            StreamValueId, FieldReference, SlicedFieldReference
        ])]
        FieldValueRef::REP(_) => {
            todo!()
        }
    })
}

impl<'a, 'b: 'a> Formattable<'a, 'b> for FieldValueRef<'_> {
    type Context = FormattingContext<'a, 'b>;
    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        opts: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        struct Format<'a, W: ?Sized>(&'a mut W);
        impl<'a, W: MaybeTextWrite + ?Sized> WithFormattable for Format<'a, W> {
            type Result = std::io::Result<()>;
            fn call<'x, 'y, F: Formattable<'x, 'y> + ?Sized>(
                &mut self,
                v: &F,
                ctx: &mut F::Context,
            ) -> Self::Result {
                v.format(ctx, self.0)
            }
        }
        with_formattable(opts, *self, Format(w))
    }

    fn refuses_truncation(&self, opts: &mut Self::Context) -> bool {
        struct RefusesTruncation;
        impl WithFormattable for RefusesTruncation {
            type Result = bool;
            fn call<'x, 'y, F: Formattable<'x, 'y> + ?Sized>(
                &mut self,
                v: &F,
                ctx: &mut F::Context,
            ) -> Self::Result {
                v.refuses_truncation(ctx)
            }
        }
        with_formattable(opts, *self, RefusesTruncation)
    }

    fn total_length_cheap(&self, opts: &mut Self::Context) -> bool {
        struct TotalLengthCheap;
        impl WithFormattable for TotalLengthCheap {
            type Result = bool;
            fn call<'x, 'y, F: Formattable<'x, 'y> + ?Sized>(
                &mut self,
                v: &F,
                ctx: &mut F::Context,
            ) -> Self::Result {
                v.total_length_cheap(ctx)
            }
        }
        with_formattable(opts, *self, TotalLengthCheap)
    }

    fn length_total(&self, opts: &mut Self::Context) -> usize {
        struct LengthTotal;
        impl WithFormattable for LengthTotal {
            type Result = usize;
            fn call<'x, 'y, F: Formattable<'x, 'y> + ?Sized>(
                &mut self,
                v: &F,
                ctx: &mut F::Context,
            ) -> Self::Result {
                v.length_total(ctx)
            }
        }
        with_formattable(opts, *self, LengthTotal)
    }

    fn text_bounds_total(&self, opts: &mut Self::Context) -> TextBounds {
        struct TextBoundsTotal;
        impl WithFormattable for TextBoundsTotal {
            type Result = TextBounds;
            fn call<'x, 'y, F: Formattable<'x, 'y> + ?Sized>(
                &mut self,
                v: &F,
                ctx: &mut F::Context,
            ) -> Self::Result {
                v.text_bounds_total(ctx)
            }
        }
        with_formattable(opts, *self, TextBoundsTotal)
    }

    fn char_bound_text_bounds(
        &self,
        opts: &mut Self::Context,
        max_chars: usize,
    ) -> TextBounds {
        struct CharBoundTextBounds {
            max_chars: usize,
        }
        impl WithFormattable for CharBoundTextBounds {
            type Result = TextBounds;
            fn call<'x, 'y, F: Formattable<'x, 'y> + ?Sized>(
                &mut self,
                v: &F,
                ctx: &mut F::Context,
            ) -> Self::Result {
                v.char_bound_text_bounds(ctx, self.max_chars)
            }
        }
        with_formattable(opts, *self, CharBoundTextBounds { max_chars })
    }
}

impl<'a, 'b: 'a> Formattable<'a, 'b> for Argument {
    type Context = FormattingContext<'a, 'b>;

    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        self.value.as_ref().format(ctx, w)
    }

    fn refuses_truncation(&self, ctx: &mut Self::Context) -> bool {
        self.value.as_ref().refuses_truncation(ctx)
    }

    fn total_length_cheap(&self, ctx: &mut Self::Context) -> bool {
        self.value.as_ref().total_length_cheap(ctx)
    }

    fn length_total(&self, ctx: &mut Self::Context) -> usize {
        self.value.as_ref().length_total(ctx)
    }

    fn text_bounds_total(&self, ctx: &mut Self::Context) -> TextBounds {
        self.value.as_ref().text_bounds_total(ctx)
    }

    fn char_bound_text_bounds(
        &self,
        ctx: &mut Self::Context,
        max_chars: usize,
    ) -> TextBounds {
        self.value.as_ref().char_bound_text_bounds(ctx, max_chars)
    }
}

impl<'a, 'b> Formattable<'a, 'b> for StreamValue<'_> {
    type Context = ValueFormattingOpts;

    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        mut w: &mut W,
    ) -> std::io::Result<()> {
        if let Some(e) = &self.error {
            return e.format(ctx, w);
        }

        let typed = ctx.type_repr_format.is_typed();
        if ctx.type_repr_format == TypeReprFormat::Debug {
            w.write_all_text("~")?;
        }
        if typed {
            match self.data_type.unwrap() {
                StreamValueDataType::Text | StreamValueDataType::MaybeText => {
                    w.write_all_text("\"")?
                }
                StreamValueDataType::Bytes => w.write_all_text("b\"")?,
                StreamValueDataType::VariableTypeArray
                | StreamValueDataType::FixedTypeArray(_)
                | StreamValueDataType::SingleValue(_) => {
                    todo!()
                }
            }
        }
        fn write_parts(
            this: &StreamValue,
            w: &mut (impl MaybeTextWrite + ?Sized),
        ) -> std::io::Result<()> {
            for part in &this.data {
                match part {
                    StreamValueData::StaticText(t) => {
                        w.write_all_text(t)?;
                    }
                    StreamValueData::StaticBytes(b) => w.write_all(b)?,
                    StreamValueData::Text { data, range } => {
                        w.write_all_text(&data[range.clone()])?
                    }
                    StreamValueData::Bytes { data, range } => {
                        w.write_all(&data[range.clone()])?
                    }
                    StreamValueData::Single(_) => todo!(),
                }
            }
            Ok(())
        }
        if typed {
            let mut w_esc = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
            write_parts(self, &mut w_esc)?;
            w = w_esc.into_inner()?;
        } else {
            write_parts(self, w)?;
        }

        if typed && self.done {
            w.write_all_text("\"")?;
        }
        Ok(())
    }
}

pub fn calc_fmt_layout<'a, 'b, F: Formattable<'a, 'b> + ?Sized>(
    ctx: &mut F::Context,
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

pub fn format_bytes(w: &mut impl TextWrite, v: &[u8]) -> std::io::Result<()> {
    w.write_all_text("b\"")?;
    let mut w = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
    std::io::Write::write_all(&mut w, v)?;
    w.into_inner().unwrap().write_all_text("\"")?;
    Ok(())
}
pub fn format_bytes_raw(
    w: &mut impl std::io::Write,
    v: &[u8],
) -> std::io::Result<()> {
    // TODO: proper raw encoding
    format_bytes(&mut TextWriteIoAdapter(w), v)
}
pub fn format_maybe_text_raw(
    w: &mut dyn MaybeTextWrite,
    fmt: PrettyPrintFormat,
    data: MaybeTextRef,
) -> std::io::Result<()> {
    match &data {
        MaybeTextRef::Text(t) => {
            if fmt == PrettyPrintFormat::Regular {
                return w.write_all_text(t);
            }
            w.write_all_text("\"")?;
            let mut ew = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
            TextWrite::write_all_text(&mut ew, t)?;
            ew.into_inner().unwrap().write_all_text("\"")
        }
        MaybeTextRef::Bytes(b) => {
            if fmt == PrettyPrintFormat::Regular {
                return std::io::Write::write_all(w, b);
            }
            w.write_all_text("\"")?;
            let mut ew = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
            std::io::Write::write_all(&mut ew, b)?;
            ew.into_inner().unwrap().write_all_text("\"")
        }
    }
}

pub fn format_maybe_text(
    w: &mut dyn TextWrite,
    fmt: PrettyPrintFormat,
    data: MaybeTextRef,
) -> std::io::Result<()> {
    match data {
        MaybeTextRef::Text(t) => {
            if fmt == PrettyPrintFormat::Regular {
                return w.write_all_text(t);
            }
            w.write_all_text("\"")?;
            let mut ew = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
            TextWrite::write_all_text(&mut ew, t)?;
            ew.into_inner().unwrap().write_all_text("\"")
        }
        MaybeTextRef::Bytes(b) => {
            if fmt == PrettyPrintFormat::Regular {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "byte sequence isn't valid utf-8",
                ));
            }
            w.write_all_text("\"")?;
            let mut ew = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
            std::io::Write::write_all(&mut ew, b)?;
            ew.into_inner().unwrap().write_all_text("\"")
        }
    }
}

pub fn format_quoted_string_raw(
    w: &mut (impl TextWrite + ?Sized),
    v: &str,
) -> std::io::Result<()> {
    w.write_all_text("\"")?;
    let mut w = EscapedWriter::new(w, &ESCAPE_DOUBLE_QUOTES);
    std::io::Write::write_all(&mut w, v.as_bytes())?;
    w.into_inner().unwrap().write_all_text("\"")?;
    Ok(())
}

pub fn format_error_raw(
    w: &mut impl TextWrite,
    v: &OperatorApplicationError,
) -> std::io::Result<()> {
    w.write_text_fmt(format_args!("(\"error\")\"{v}\""))
}

impl<'a, 'b> FormattingContext<'a, 'b> {
    pub fn value_formatting_opts(&self) -> ValueFormattingOpts {
        ValueFormattingOpts {
            is_stream_value: self.is_stream_value,
            type_repr_format: self.rfk.opts.type_repr,
        }
    }
    pub fn nested_value_formatting_opts(&self) -> ValueFormattingOpts {
        ValueFormattingOpts {
            is_stream_value: false,
            type_repr_format: TypeReprFormat::Typed,
        }
    }
    pub fn for_nested_values<T>(
        &mut self,
        f: impl FnOnce(&mut FormattingContext) -> T,
    ) -> T {
        let sv = self.is_stream_value;
        self.is_stream_value = false;
        let tr = self.rfk.opts.type_repr;
        self.rfk.opts.type_repr = TypeReprFormat::Typed;
        let res = f(self);
        self.rfk.opts.type_repr = tr;
        self.is_stream_value = sv;
        res
    }
}

pub fn format_rational(
    w: &mut (impl TextWrite + ?Sized),
    v: &BigRational,
    mode: RationalsPrintMode,
) -> std::io::Result<()> {
    match mode {
        RationalsPrintMode::Cutoff(decimals) => {
            format_rational_as_decimals_raw(w, v, decimals)
        }
        RationalsPrintMode::Raw => w.write_text_fmt(format_args!("{}", v)),
        RationalsPrintMode::Dynamic => {
            let v = v.reduced();
            let (_, mut denom) = v.clone().into_raw();
            let five = BigInt::from_u32(5).unwrap();
            let two = BigInt::from_u32(2).unwrap();
            // rational is printable iff it's reduced denominator
            // only contains the prime factors two and five
            // PERF: :(
            while !denom.is_one()
                && !denom.is_zero()
                && denom.clone().rem(&five).is_zero()
            {
                denom.div_assign(&five);
            }
            while !denom.is_one()
                && !denom.is_zero()
                && denom.clone().rem(&two).is_zero()
            {
                denom.div_assign(&two);
            }
            if denom.is_one() {
                format_rational_as_decimals_raw(w, &v, u32::MAX)
            } else {
                w.write_text_fmt(format_args!("{}", &v))
            }
        }
    }
}

pub fn format_rational_as_decimals_raw(
    w: &mut (impl TextWrite + ?Sized),
    v: &BigRational,
    mut decimals: u32,
) -> std::io::Result<()> {
    // PERF: this function is stupid
    if v.is_integer() {
        w.write_text_fmt(format_args!("{v}"))?;
        return Ok(());
    }
    let negative = v.is_negative();
    let mut whole_number = v.to_integer();
    let mut v = v.sub(&whole_number).abs();
    let one = BigInt::one();
    let one_half = BigRational::new(one.clone(), BigInt::from_u8(2).unwrap());
    if decimals == 0 {
        if v >= one_half {
            whole_number.add_assign(if negative { -one } else { one });
        }
        w.write_text_fmt(format_args!("{whole_number}"))?;
        return Ok(());
    }
    w.write_text_fmt(format_args!("{}.", &whole_number))?;

    // PERF: really bad
    let ten = BigInt::from_u64(10).unwrap();
    while decimals > 1 && !v.is_zero() {
        v.mul_assign(&ten);
        let int_part = v.to_integer();
        w.write_text_fmt(format_args!("{}", v.to_u8().unwrap()))?;
        v.sub_assign(int_part);
        decimals -= 1;
    }

    if !v.is_zero() {
        let round_up = v > one_half;
        v.mul_assign(&ten);
        if round_up {
            v.add_assign(&one);
        }
        let int_part = v.to_u8().unwrap();
        w.write_text_fmt(format_args!("{int_part}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use num::{BigInt, BigRational};
    use rstest::rstest;

    use crate::options::chain_settings::RationalsPrintMode;

    use super::format_rational;

    #[rstest]
    #[case(1, 3, "1/3")]
    #[case(1, 2, "0.5")]
    #[case(1, 7, "1/7")]
    #[case(1, 42, "1/42")]
    #[case(1234, 1000, "1.234")]
    #[case(1234, -1000, "-1.234")]
    fn print_dynamic_fraction(
        #[case] num: i64,
        #[case] denom: i64,
        #[case] output: &str,
    ) {
        let mut res = String::new();
        format_rational(
            &mut res,
            &BigRational::new(BigInt::from(num), BigInt::from(denom)),
            RationalsPrintMode::Dynamic,
        )
        .unwrap();
        assert_eq!(res, output)
    }
}
