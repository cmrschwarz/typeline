use std::{io::BufRead, ops::MulAssign};

use arrayvec::{ArrayString, ArrayVec};
use bstr::ByteSlice;
use indexmap::IndexMap;
use metamatch::metamatch;
use num::{pow::Pow, BigInt, BigRational, FromPrimitive};
use once_cell::sync::Lazy;
use smallstr::SmallString;
use thiserror::Error;

use crate::{
    extension::ExtensionRegistry,
    record_data::{
        array::Array, field_value::FieldValueUnboxed, object::Object,
    },
    utils::{
        int_string_conversions::I64_MAX_DECIMAL_DIGITS,
        io::{
            read_char, read_until_unescape2, ReadCharError,
            ReadUntilUnescapeError, ReplacementError, ReplacementState,
        },
        MAX_UTF8_CHAR_LEN,
    },
};

// ENHANCE: improve these error messages by handrolling the impl
// e.g. special case for stray `\`s indicating that the user
// probably wanted a string literal
#[derive(Debug, PartialEq, Eq, Error)]
pub enum TysonParseErrorKind {
    #[error("{0}")]
    Other(Box<str>),
    #[error("invalid utf-8")]
    InvalidUtf8(ArrayVec<u8, MAX_UTF8_CHAR_LEN>),
    #[error("invalid unicode escape")]
    InvalidUnicodeEscape(ArrayString<4>),
    #[error("invalid unicode escape")]
    InvalidExtendedUnicodeEscape(ArrayString<6>),
    #[error("unicode escape is too long")]
    ExtendedUnicodeEscapeTooLong,
    #[error("character '{0}' cannot be escaped")]
    NonEscapbleCharacter(char),
    #[error(
        "\"\\x\" is not allowed in \"unicode\" strings. Use \"\\u\" or a 'binary' string."
    )]
    ByteEscapeInUnicodeString,
    #[error("invalid binary escape")]
    InvalidByteEscape(ArrayString<4>),
    #[error("expected \" or \' after leading b for parsing a binary string")]
    ExpectedQuoteAfterB,
    #[error("invalid number")]
    InvalidNumber,
    #[error("stray character '{0}'")]
    StrayToken(char),
    #[error("trailing character '{0}'")]
    TrailingCharacters(char),
    #[error("unexpected end of stream")]
    UnexpectedEof,
}

#[derive(Error, Debug)]
pub enum TysonParseError {
    #[error("{0}")]
    Io(std::io::Error),
    #[error("(line {line}, col {col}): {kind}")]
    InvalidSyntax {
        line: usize,
        col: usize,
        kind: TysonParseErrorKind,
    },
}

pub struct TysonParser<'a, S: BufRead> {
    stream: S,
    line: usize,
    col: usize,
    #[allow(unused)] // TODO
    extension_registry: Option<&'a ExtensionRegistry>,
    use_floating_point: bool,
}

impl PartialEq for TysonParseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(e1), Self::Io(e2)) => e1.kind() == e2.kind(),
            (
                Self::InvalidSyntax {
                    line: l_line,
                    col: l_col,
                    kind: l_kind,
                },
                Self::InvalidSyntax {
                    line: r_line,
                    col: r_col,
                    kind: r_kind,
                },
            ) => l_line == r_line && l_col == r_col && l_kind == r_kind,
            _ => false,
        }
    }
}
impl Eq for TysonParseError {}

impl<'a, S: BufRead> TysonParser<'a, S> {
    pub fn new(
        stream: S,
        use_floating_point: bool,
        exts: Option<&'a ExtensionRegistry>,
    ) -> Self {
        Self::new_with_position(stream, use_floating_point, 1, 1, exts)
    }
    pub fn new_with_position(
        stream: S,
        use_floating_point: bool,
        line: usize,
        col: usize,
        exts: Option<&'a ExtensionRegistry>,
    ) -> Self {
        Self {
            stream,
            line,
            col,
            use_floating_point,
            extension_registry: exts,
        }
    }
    fn peek_byte_raw(&mut self) -> Result<Option<u8>, std::io::Error> {
        Ok(self.stream.fill_buf()?.first().copied())
    }
    fn peek_byte(&mut self) -> Result<Option<u8>, TysonParseError> {
        self.peek_byte_raw().map_err(TysonParseError::Io)
    }
    pub fn void_byte_char(&mut self) {
        self.stream.consume(1);
        self.col += 1;
    }
    fn read_char(&mut self) -> Result<char, TysonParseError> {
        match read_char(&mut self.stream) {
            Ok(c) => Ok(c),
            Err(e) => match e {
                ReadCharError::Io(e) => Err(TysonParseError::Io(e)),
                ReadCharError::InvalidUtf8 { len, sequence } => {
                    self.err(TysonParseErrorKind::InvalidUtf8(
                        sequence[..len as usize].iter().copied().collect(),
                    ))
                }
                ReadCharError::Eof => {
                    self.err(TysonParseErrorKind::UnexpectedEof)
                }
            },
        }
    }
    fn read_char_eat_whitespace(&mut self) -> Result<char, TysonParseError> {
        let c = self.read_char()?;
        self.consume_until_non_whitespace_char(c)
    }
    fn consume_until_non_whitespace_char(
        &mut self,
        mut c: char,
    ) -> Result<char, TysonParseError> {
        loop {
            match c {
                '\n' => {
                    self.line += 1;
                    self.col = 1;
                }
                c if c.is_whitespace() => {
                    self.col += 1;
                }
                other => return Ok(other),
            }
            c = self.read_char()?;
        }
    }
    fn consume_char_eat_whitespace(
        &mut self,
    ) -> Result<char, TysonParseError> {
        let res = self.read_char_eat_whitespace()?;
        self.col += 1;
        Ok(res)
    }
    fn parse_identifier_after_first_char(
        &mut self,
        fist_char: char,
    ) -> Result<(String, char), TysonParseError> {
        // PERF: this will perform badly but is good enough for now
        // consider using a byte buffer and maybe even using a regex
        // to do this?
        let mut res = String::new();
        res.push(fist_char);
        loop {
            let c = self.read_char()?;
            if !unicode_ident::is_xid_continue(c) {
                return Ok((res, c));
            }
            res.push(c);
            self.col += 1;
        }
    }
    fn parse_binary_string_after_b(
        &mut self,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        let quote = self.read_char_eat_whitespace()?;
        if !['\'', '"'].contains(&quote) {
            return self.err(TysonParseErrorKind::ExpectedQuoteAfterB);
        }
        self.parse_string_after_quote(quote as u8, true)
    }
    fn parse_string_token_after_quote(
        &mut self,
        quote_kind: u8,
    ) -> Result<String, TysonParseError> {
        let FieldValueUnboxed::Text(v) =
            self.parse_string_after_quote(quote_kind, false)?
        else {
            unreachable!()
        };
        Ok(v)
    }
    pub fn parse_string_after_quote(
        &mut self,
        quote_kind: u8,
        binary: bool,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        let mut buf = Vec::new();
        let escape_sequences = [
            b'a', // audible bell
            b'b', // backspace
            b'f', // formfeed
            b'v', // vertical tab
            b'r', b'n', b't', // whitespace
            b'\'', b'\"', b'\\', // self escapes
        ];
        let replacements = [
            b'\x07', b'\x08', b'\x0C', b'\x0B', // (AB, BS, FF, VT)
            b'\r', b'\n', b'\t', // whitespace
            b'\'', b'\"', b'\\', // self escapes
        ];
        let mut curr_line_begin = 0;
        let parse_extended_unicode_escape = |s: ReplacementState| {
            let Some(esc_end) = s.find_limited(2, b'}', 2 + 6 + 1)? else {
                return Err(ReplacementError::Error(
                    TysonParseErrorKind::ExtendedUnicodeEscapeTooLong,
                ));
            };
            let buf_offset = s.buf_offset();
            let buf = s.pull_into_buf(esc_end)?;
            let esc_seq = &buf[buf_offset + 3..buf_offset + esc_end];

            let esc_str = std::str::from_utf8(esc_seq).map_err(|e| {
                let err_start = e.valid_up_to();
                let err_end = e
                    .error_len()
                    .map(|l| err_start + l)
                    .unwrap_or(esc_seq.len());
                ReplacementError::Error(TysonParseErrorKind::InvalidUtf8(
                    esc_seq[err_start..err_end].iter().copied().collect(),
                ))
            })?;

            let esc_value = u32::from_str_radix(esc_str, 16)
                .ok()
                .and_then(char::from_u32)
                .ok_or_else(|| {
                    ReplacementError::Error(
                        TysonParseErrorKind::InvalidExtendedUnicodeEscape(
                            ArrayString::from(esc_str).unwrap(),
                        ),
                    )
                })?;
            buf.truncate(buf_offset);
            buf.extend([0u8; 4].iter().take(esc_value.len_utf8()));
            esc_value.encode_utf8(&mut buf[buf_offset..]);
            Ok(esc_end + 1)
        };
        let parse_unicode_escape = |_s: ReplacementState| {
            // tricky because maybe two required because of surrogate pairs
            todo!("\\uFFFF escape sequences");
        };
        let parse_byte_escape = |s: ReplacementState| {
            let buf_offset = s.buf_offset();
            let buf = s.pull_into_buf(4)?;
            let esc_seq = &buf[buf_offset + 2..buf_offset + 4];
            let esc_str = std::str::from_utf8(esc_seq).map_err(|e| {
                let err_start = e.valid_up_to();
                let err_end = e
                    .error_len()
                    .map(|l| err_start + l)
                    .unwrap_or(esc_seq.len());
                ReplacementError::Error(TysonParseErrorKind::InvalidUtf8(
                    esc_seq[err_start..err_end].iter().copied().collect(),
                ))
            })?;
            let esc_value =
                u8::from_str_radix(esc_str, 16).ok().ok_or_else(|| {
                    ReplacementError::Error(
                        TysonParseErrorKind::InvalidByteEscape(
                            ArrayString::from(esc_str).unwrap(),
                        ),
                    )
                })?;

            buf.truncate(buf_offset);
            buf.push(esc_value);
            Ok(4)
        };
        let escape_handler = |s: ReplacementState| {
            if s[0] == b'\n' {
                debug_assert!(s.seq_len == 1);
                if !binary {
                    let line = &s.buffer()[curr_line_begin..];
                    if let Err(e) = line.to_str() {
                        let err_start = e.valid_up_to();
                        let err_end = e
                            .error_len()
                            .map(|l| err_start + l)
                            .unwrap_or(line.len());
                        return Err(ReplacementError::Error(
                            TysonParseErrorKind::InvalidUtf8(
                                line[err_start..err_end]
                                    .iter()
                                    .copied()
                                    .collect(),
                            ),
                        ));
                    }
                }
                self.line += 1;
                self.col = 1;
                curr_line_begin = s.buffer().len();
                return Ok(1);
            }
            debug_assert!(s[0] == b'\\');
            let esc_kind = s.get(1)?;
            if esc_kind == b'u' {
                let unicode_esc_kind = s.get(2)?;
                if unicode_esc_kind == b'{' {
                    return parse_extended_unicode_escape(s);
                }
                return parse_unicode_escape(s);
            }
            if esc_kind == b'x' {
                if !binary {
                    return Err(ReplacementError::Error(
                        TysonParseErrorKind::ByteEscapeInUnicodeString,
                    ));
                }
                return parse_byte_escape(s);
            }
            if let Some(i) = escape_sequences.find_byte(esc_kind) {
                let seq_len = s.seq_len;
                let buf = s.into_buffer();
                buf.truncate(buf.len() - seq_len);
                buf.push(replacements[i]);
                return Ok(2);
            }
            let esc_kind = s.get_char(1)?.map_err(|err_seq| {
                ReplacementError::Error(TysonParseErrorKind::InvalidUtf8(
                    err_seq,
                ))
            })?;
            Err(ReplacementError::Error(
                // TODO: fix for non ascii
                TysonParseErrorKind::NonEscapbleCharacter(esc_kind),
            ))
        };
        if let Err(e) = read_until_unescape2(
            &mut self.stream,
            &mut buf,
            quote_kind,
            b'\\',
            b'\n',
            escape_handler,
        ) {
            return match e {
                ReadUntilUnescapeError::Io(e) => Err(TysonParseError::Io(e)),
                ReadUntilUnescapeError::UnescapeFailed(e) => match e.base {
                    TysonParseErrorKind::InvalidUtf8(seq) => {
                        let end = buf.len() - e.escape_seq_len - seq.len();
                        Err(TysonParseError::InvalidSyntax {
                            line: self.line,
                            col: buf[curr_line_begin..end].chars().count(),
                            kind: TysonParseErrorKind::InvalidUtf8(seq),
                        })
                    }
                    other => Err(TysonParseError::InvalidSyntax {
                        line: self.line,
                        col: self.col
                            + buf[curr_line_begin
                                ..buf.len() - e.escape_seq_len]
                                .chars()
                                .count(),
                        kind: other,
                    }),
                },
            };
        }
        if binary {
            return Ok(FieldValueUnboxed::Bytes(buf));
        }
        // verify the last, uncompleted line of the string for utf8-compliance
        match buf[curr_line_begin..].to_str() {
            Err(e) => {
                return Err(TysonParseError::InvalidSyntax {
                    line: self.line,
                    col: buf[curr_line_begin..e.valid_up_to()].chars().count(),
                    kind: TysonParseErrorKind::InvalidUtf8(
                        buf[e.valid_up_to()..].iter().copied().collect(),
                    ),
                });
            }
            Ok(s) => {
                self.col += s.chars().count() + 1; // +1 for the delimiter
            }
        }
        // SAFETY: we already verified each line separately
        Ok(FieldValueUnboxed::Text(unsafe {
            String::from_utf8_unchecked(buf)
        }))
    }
    fn err<T>(&self, kind: TysonParseErrorKind) -> Result<T, TysonParseError> {
        Err(self.error(kind))
    }
    fn error(&self, kind: TysonParseErrorKind) -> TysonParseError {
        TysonParseError::InvalidSyntax {
            line: self.line,
            col: self.col,
            kind,
        }
    }
    pub fn parse_number(
        &mut self,
        first: u8,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        let mut buf = SmallString::<[u8; I64_MAX_DECIMAL_DIGITS]>::new();
        let mut sign = None;
        let mut floating_point = None;
        let mut exponent = None;
        let mut exponent_sign = None;
        let mut c = first;
        let mut first_byte = true;
        loop {
            match c {
                b'i' | b'I' => {
                    if !first_byte {
                        if buf.len() != 1 || sign.is_none() {
                            break;
                        }
                        self.void_byte_char();
                    }
                    if !self.read_char()?.eq_ignore_ascii_case(&'n')
                        || !self.read_char()?.eq_ignore_ascii_case(&'f')
                    {
                        return Err(TysonParseError::InvalidSyntax {
                            line: self.line,
                            col: self.col - 1,
                            kind: TysonParseErrorKind::InvalidNumber,
                        });
                    }
                    self.col += 2;
                    if let Some(idx) = sign {
                        debug_assert!(idx == 0);
                        if buf.as_bytes()[idx] == b'-' {
                            return Ok(FieldValueUnboxed::Float(
                                f64::NEG_INFINITY,
                            ));
                        }
                        debug_assert!(buf.as_bytes()[idx] == b'+');
                    }
                    return Ok(FieldValueUnboxed::Float(f64::INFINITY));
                }
                b'n' | b'N' => {
                    if !first_byte {
                        if buf.len() != 1 || sign.is_none() {
                            break;
                        }
                        self.void_byte_char();
                    }
                    if !self.read_char()?.eq_ignore_ascii_case(&'a')
                        || !self.read_char()?.eq_ignore_ascii_case(&'n')
                    {
                        return Err(TysonParseError::InvalidSyntax {
                            line: self.line,
                            col: self.col - 1,
                            kind: TysonParseErrorKind::InvalidNumber,
                        });
                    }
                    self.col += 2;
                    return Ok(FieldValueUnboxed::Float(f64::NAN));
                }
                b'+' | b'-' => {
                    if buf.is_empty() {
                        sign = Some(0);
                    } else if exponent == Some(buf.len() - 1) {
                        exponent_sign = Some(buf.len());
                    } else {
                        break;
                    }
                    buf.push(c as char);
                }
                b'.' => {
                    if floating_point.is_some() || exponent.is_some() {
                        break;
                    }
                    floating_point = Some(buf.len());
                    buf.push('.');
                }
                b'0'..=b'9' => buf.push(c as char),
                b'_' => (),
                b'e' | b'E' => {
                    if exponent.is_some() {
                        break;
                    }
                    exponent = Some(buf.len());
                    buf.push('e');
                }
                _ => break,
            }
            if first_byte {
                first_byte = false;
            } else {
                self.void_byte_char();
            }
            if let Some(b) = self.peek_byte()? {
                c = b;
            } else {
                break;
            }
        }
        let exponent_digit_count =
            buf.len() - exponent.map(|e| e + 1).unwrap_or(buf.len());
        let mut digit_count = buf.len()
            - [sign, floating_point, exponent, exponent_sign]
                .iter()
                .map(|o| o.map(|_| 1).unwrap_or(0))
                .sum::<usize>()
            - exponent_digit_count;

        if digit_count == 0 {
            return Err(TysonParseError::InvalidSyntax {
                line: self.line,
                col: self.col - buf.len(),
                kind: TysonParseErrorKind::InvalidNumber,
            });
        }
        let whole_number_start = sign.map(|_| 1).unwrap_or(0);
        let whole_numer_end = floating_point.or(exponent).unwrap_or(buf.len());
        for &c in buf[whole_number_start..whole_numer_end].as_bytes() {
            if c != b'0' {
                break;
            }
            digit_count -= 1;
        }
        if floating_point.is_none() && exponent.is_none() {
            if digit_count <= I64_MAX_DECIMAL_DIGITS + 1 {
                if let Ok(v) = buf.parse::<i64>() {
                    return Ok(FieldValueUnboxed::Int(v));
                }
            }
            return Ok(FieldValueUnboxed::BigInt(
                BigInt::parse_bytes(buf.as_bytes(), 10).unwrap(),
            ));
        }
        if self.use_floating_point {
            if let Ok(v) = buf.parse::<f64>() {
                // if !v.is_nan() && !v.is_infinite() {
                return Ok(FieldValueUnboxed::Float(v));
                // }
            };
        }
        let mut rational = if let Some(fp) = floating_point {
            let decimals_start = fp + 1;
            let decimals_end = exponent.unwrap_or(buf.len());
            buf.remove(fp);
            exponent = exponent.map(|e| e - 1);
            BigRational::new_raw(
                BigInt::parse_bytes(buf[..decimals_end - 1].as_bytes(), 10)
                    .unwrap(),
                BigInt::from_u64(10)
                    .unwrap()
                    .pow(decimals_end - decimals_start),
            )
        } else {
            BigRational::new_raw(
                BigInt::parse_bytes(buf[..whole_numer_end].as_bytes(), 10)
                    .unwrap(),
                BigInt::from_u64(1).unwrap(),
            )
        };

        if let Some(e) = exponent {
            rational.mul_assign(BigRational::from_u64(10).unwrap().pow(
                BigInt::parse_bytes(buf[e + 1..].as_bytes(), 10).unwrap(),
            ));
        }
        Ok(FieldValueUnboxed::BigRational(rational))
    }
    fn parse_array_after_bracket(
        &mut self,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        let mut arr = Array::default();
        loop {
            let value = match self.parse_value() {
                Ok(v) => v,
                Err(TysonParseError::InvalidSyntax {
                    kind: TysonParseErrorKind::StrayToken(']'),
                    ..
                }) => return Ok(FieldValueUnboxed::Array(arr)),
                Err(e) => return Err(e),
            };
            arr.push_unboxed(value);
            let c = self.read_char_eat_whitespace()?;
            if c == ']' {
                return Ok(FieldValueUnboxed::Array(arr));
            }
            if c != ',' {
                return self.err(TysonParseErrorKind::StrayToken(c));
            }
        }
    }
    fn parse_object_after_brace(
        &mut self,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        let mut map = IndexMap::new();
        let mut c = self.consume_char_eat_whitespace()?;
        loop {
            if c == '}' {
                return Ok(FieldValueUnboxed::Object(Object::KeysStored(map)));
            }
            let key = if c == '"' || c == '\'' {
                let key = self.parse_string_token_after_quote(c as u8)?;
                c = self.consume_char_eat_whitespace()?;
                key
            } else {
                if !unicode_ident::is_xid_start(c) {
                    self.col -= 1;
                    return self.err(TysonParseErrorKind::StrayToken(c));
                }
                let (ident, cont) =
                    self.parse_identifier_after_first_char(c)?;
                c = self.consume_until_non_whitespace_char(cont)?;
                ident
            };
            if c != ':' {
                self.col -= 1;
                return self.err(TysonParseErrorKind::StrayToken(c));
            }
            let value = self.parse_value()?;
            map.insert(key, value.into());
            c = self.consume_char_eat_whitespace()?;
            if c == ',' {
                c = self.consume_char_eat_whitespace()?;
                continue;
            }
            if c != '}' {
                self.col -= 1;
                return self.err(TysonParseErrorKind::StrayToken(c));
            }
        }
    }
    fn parse_type_after_parenthesis(
        &mut self,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        todo!();
    }
    pub fn is_number_start(first: u8) -> bool {
        match first {
            // int | float | Inf | NaN
            b'0'..=b'9' | b'+' | b'-' | b'.' | b'i' | b'I' | b'N' => true,
            _ => false,
        }
    }
    pub fn parse_value(
        &mut self,
    ) -> Result<FieldValueUnboxed, TysonParseError> {
        let c = self.consume_char_eat_whitespace()?;
        metamatch!(match c {
            '{' => self.parse_object_after_brace(),
            '"' => self.parse_string_after_quote(b'"', false),
            '\'' => self.parse_string_after_quote(b'\'', false),
            'b' => self.parse_binary_string_after_b(),
            '(' => self.parse_type_after_parenthesis(),
            '[' => self.parse_array_after_bracket(),
            'n' => {
                let b = self.peek_byte()?;
                if b == Some(b'a') {
                    return self.parse_number(c as u8);
                }
                for expected in "ull".chars() {
                    let c = self.read_char()?;
                    if c != expected {
                        return Err(TysonParseError::InvalidSyntax {
                            line: self.line,
                            col: self.col,
                            kind: TysonParseErrorKind::StrayToken(c),
                        });
                    }
                    self.col += 1;
                }
                Ok(FieldValueUnboxed::Null)
            }
            #[expand((FIRST, REST, VAL) in [
                ('t', "rue", FieldValueUnboxed::Bool(true)),
                ('f', "alse", FieldValueUnboxed::Bool(false)),
                ('u', "ndefined", FieldValueUnboxed::Undefined),
            ])]
            FIRST => {
                for expected in REST.chars() {
                    let c = self.read_char()?;
                    if c != expected {
                        return Err(TysonParseError::InvalidSyntax {
                            line: self.line,
                            col: self.col,
                            kind: TysonParseErrorKind::StrayToken(c),
                        });
                    }
                    self.col += 1;
                }
                Ok(VAL)
            }

            c if c.is_ascii() && Self::is_number_start(c as u8) => {
                self.parse_number(c as u8)
            }
            other => {
                self.col -= 1;
                self.err(TysonParseErrorKind::StrayToken(other))
            }
        })
    }
    pub fn reject_further_input(&mut self) -> Result<(), TysonParseError> {
        match self.read_char_eat_whitespace() {
            Ok(c) => self.err(TysonParseErrorKind::TrailingCharacters(c)),
            Err(TysonParseError::InvalidSyntax {
                kind: TysonParseErrorKind::UnexpectedEof,
                ..
            }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn end_of_input(&mut self) -> Result<bool, std::io::Error> {
        Ok(self.peek_byte_raw()?.is_none())
    }
    pub fn get_line(&self) -> usize {
        self.line
    }
    pub fn get_col(&self) -> usize {
        self.col
    }
}

pub fn parse_tyson(
    input: impl BufRead,
    use_floating_point: bool,
    exts: Option<&ExtensionRegistry>,
) -> Result<FieldValueUnboxed, TysonParseError> {
    let mut tp = TysonParser::new(input, use_floating_point, exts);
    let res = tp.parse_value()?;
    tp.reject_further_input()?;
    Ok(res)
}

pub fn parse_tyson_str(
    input: &str,
    use_floating_point: bool,
    exts: Option<&ExtensionRegistry>,
) -> Result<FieldValueUnboxed, TysonParseError> {
    // PERF: we could skip a lot of utf8 checking
    // if we already know that this is a string
    parse_tyson(input.as_bytes(), use_floating_point, exts)
}

pub fn is_valid_identifier(str: &[u8]) -> bool {
    static REGEX: Lazy<regex::bytes::Regex> = Lazy::new(|| {
        regex::bytes::Regex::new(r"^\p{XID_Start}\p{XID_Continue}*$").unwrap()
    });
    REGEX.is_match(str)
}

#[cfg(test)]
mod test {
    use indexmap::indexmap;
    use num::BigInt;
    use rstest::rstest;

    use crate::record_data::{
        array::Array,
        field_value::{FieldValue, FieldValueUnboxed},
    };

    use super::{parse_tyson_str, TysonParseError, TysonParseErrorKind};

    fn parse(s: &str) -> Result<FieldValueUnboxed, TysonParseError> {
        parse_tyson_str(s, false, None)
    }

    #[test]
    fn empty_string() {
        assert_eq!(
            parse(""),
            Err(TysonParseError::InvalidSyntax {
                line: 1,
                col: 1,
                kind: TysonParseErrorKind::UnexpectedEof,
            })
        );
    }
    #[test]
    fn whitespace_only() {
        assert_eq!(
            parse("\n\n "),
            Err(TysonParseError::InvalidSyntax {
                line: 3,
                col: 2,
                kind: TysonParseErrorKind::UnexpectedEof,
            })
        );
    }

    #[test]
    fn string() {
        assert_eq!(
            parse(r#""foo""#),
            Ok(FieldValueUnboxed::Text("foo".into()))
        );
    }

    #[test]
    fn single_quoted_string() {
        assert_eq!(parse("'foo'"), Ok(FieldValueUnboxed::Text("foo".into())));
    }

    #[rstest]
    #[case("b'1'", b"1")]
    #[case("b'\\xFF'", b"\xFF")]
    fn byte_escapes(#[case] v: &str, #[case] res: &[u8]) {
        assert_eq!(parse(v), Ok(FieldValueUnboxed::Bytes(res.to_vec())));
    }

    #[test]
    fn invalid_escape_char() {
        assert_eq!(
            parse("'\\q'"),
            Err(TysonParseError::InvalidSyntax {
                line: 1,
                col: 2,
                kind: TysonParseErrorKind::NonEscapbleCharacter('q')
            })
        );
    }

    #[test]
    fn invalid_escape_char_non_ascii() {
        assert_eq!(
            parse("'\\\u{1F480}'"),
            Err(TysonParseError::InvalidSyntax {
                line: 1,
                col: 2,
                kind: TysonParseErrorKind::NonEscapbleCharacter('\u{1F480}')
            })
        );
    }

    #[test]
    fn illegal_byte_escape_in_string() {
        assert_eq!(
            parse("\"\\xFF\""),
            Err(TysonParseError::InvalidSyntax {
                line: 1,
                col: 2,
                kind: TysonParseErrorKind::ByteEscapeInUnicodeString
            })
        );
    }

    #[test]
    fn unicode_escape() {
        assert_eq!(
            parse(r#""foo\u{1F4A9}bar""#),
            Ok(FieldValueUnboxed::Text("foo\u{1F4A9}bar".into()))
        );
    }

    #[test]
    fn array() {
        assert_eq!(
            parse(r#" [1,2,3,"4"] "#),
            Ok(FieldValueUnboxed::Array(Array::Mixed(vec![
                FieldValue::Int(1),
                FieldValue::Int(2),
                FieldValue::Int(3),
                FieldValue::Text("4".into())
            ])))
        );
    }
    #[test]
    fn null() {
        assert_eq!(parse("null"), Ok(FieldValueUnboxed::Null));
    }
    #[test]
    fn undefined() {
        assert_eq!(parse("undefined"), Ok(FieldValueUnboxed::Undefined));
    }
    #[rstest]
    #[case("1", 1)]
    #[case("-1", -1)]
    #[case("+1", 1)]
    #[case("-0", 0)]
    #[case("+00000000000000000000000", 0)]
    #[case(&i64::MAX.to_string(), i64::MAX)]
    #[case(&i64::MIN.to_string(), i64::MIN)]
    fn int(#[case] v: &str, #[case] res: i64) {
        assert_eq!(parse(v), Ok(FieldValueUnboxed::Int(res)));
    }
    #[rstest]
    #[case("9223372036854775808")]
    #[case("-9223372036854775809")]
    fn big_int(#[case] v: &str) {
        assert_eq!(
            parse(v),
            Ok(FieldValueUnboxed::BigInt(
                BigInt::parse_bytes(v.as_bytes(), 10).unwrap()
            ))
        );
    }
    #[rstest]
    #[case("Inf", f64::INFINITY)]
    #[case("+inf", f64::INFINITY)]
    #[case("-inf", f64::NEG_INFINITY)]
    fn inf(#[case] v: &str, #[case] res: f64) {
        assert_eq!(parse(v), Ok(FieldValueUnboxed::Float(res)));
    }
    #[rstest]
    #[case("nan")]
    #[case("NAn")]
    #[case("+nAn")]
    #[case("-NaN")]
    fn nan(#[case] input: &str) {
        assert!(
            matches!(parse(input), Ok(FieldValueUnboxed::Float(v)) if v.is_nan())
        );
    }
    #[rstest]
    #[case("{foo: 3}", "foo")]
    #[case("{'bar': 3}", "bar")]
    #[case("{\"baz\": 3}", "baz")]
    #[case("{\u{d8}: 3}", "\u{d8}")]
    fn object_keys(#[case] input: &str, #[case] key_name: &str) {
        use crate::record_data::object::Object;

        assert_eq!(
            parse(input),
            Ok(FieldValueUnboxed::Object(Object::KeysStored(indexmap![
                key_name.to_string() => FieldValue::Int(3)
            ]))),
        );
    }
}
