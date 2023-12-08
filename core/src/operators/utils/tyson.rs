use std::io::BufRead;

use arrayvec::{ArrayString, ArrayVec};
use bstr::ByteSlice;
use indexmap::IndexMap;
use num_bigint::BigInt;
use smallstr::SmallString;
use thiserror::Error;

use crate::{
    extension::ExtensionRegistry,
    record_data::field_value::{Array, FieldValue, Object},
    utils::{
        int_string_conversions::I64_MAX_DECIMAL_DIGITS,
        io::{
            read_char, read_until_unescape2, ReadCharError,
            ReadUntilUnescapeError, ReplacementError, ReplacementState,
        },
        MAX_UTF8_CHAR_LEN,
    },
};

#[derive(Debug, PartialEq, Eq, Error)]
pub enum TysonParseErrorKind {
    #[error("{0}")]
    Other(Box<str>),
    #[error("invalid utf-8")]
    InvalidUtf8(ArrayVec<u8, MAX_UTF8_CHAR_LEN>),
    #[error("invalid unicode escape")]
    InvalidUnicodeEscape([u8; 4]),
    #[error("invalid unicode escape")]
    InvalidExtendedUnicodeEscape(ArrayString<6>),
    #[error("unicode escape is too long")]
    ExtendedUnicodeEscapeTooLong,
    #[error("character '{0}' cannot be escaped")]
    NonEscapbleCharacter(u8),
    #[error("invalid number")]
    InvalidNumber,
    #[error("stray character '{0}'")]
    StrayToken(char),
    #[error("trailing character '{0}'")]
    TrailingCharacters(char),
    #[error("unexpected end of stream")]
    UnexpectedEof,
}

#[derive(Debug)]
pub enum TysonParseError {
    Io(std::io::Error),
    InvalidSequence {
        line: usize,
        col: usize,
        kind: TysonParseErrorKind,
    },
}

struct TysonParser<'a, S: BufRead> {
    stream: S,
    #[allow(unused)] // TODO
    extension_registry: &'a ExtensionRegistry,
    line: usize,
    col: usize,
}

impl PartialEq for TysonParseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(e1), Self::Io(e2)) => e1.kind() == e2.kind(),
            (
                Self::InvalidSequence {
                    line: l_line,
                    col: l_col,
                    kind: l_kind,
                },
                Self::InvalidSequence {
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
    fn new(stream: S, exts: &'a ExtensionRegistry) -> Self {
        Self {
            stream,
            line: 1,
            col: 1,
            extension_registry: exts,
        }
    }
    fn peek_byte(&mut self) -> Result<Option<u8>, TysonParseError> {
        Ok(self
            .stream
            .fill_buf()
            .map_err(TysonParseError::Io)?
            .first()
            .copied())
    }
    fn void_byte_char(&mut self) {
        self.stream.consume(1);
        self.col += 1;
    }
    fn read_char(&mut self) -> Result<char, TysonParseError> {
        match read_char(&mut self.stream) {
            Ok(c) => Ok(c),
            Err(e) => match e {
                ReadCharError::Io(e) => Err(TysonParseError::Io(e)),
                ReadCharError::InvalidUtf8 { len, sequence } => self.err(
                    TysonParseErrorKind::InvalidUtf8(ArrayVec::from_iter(
                        sequence[..len as usize].iter().copied(),
                    )),
                ),
                ReadCharError::Eof => {
                    self.err(TysonParseErrorKind::UnexpectedEof)
                }
            },
        }
    }
    fn read_char_eat_whitespace(&mut self) -> Result<char, TysonParseError> {
        loop {
            match self.read_char()? {
                '\n' => {
                    self.line += 1;
                    self.col = 1;
                }
                ' ' | '\t' | '\r' => self.col += 1,
                other => return Ok(other),
            }
        }
    }
    fn consume_char_eat_whitespace(
        &mut self,
    ) -> Result<char, TysonParseError> {
        let res = self.read_char_eat_whitespace()?;
        self.col += 1;
        Ok(res)
    }
    fn reject_further_input(&mut self) -> Result<(), TysonParseError> {
        match self.read_char_eat_whitespace() {
            Ok(c) => self.err(TysonParseErrorKind::TrailingCharacters(c)),
            Err(TysonParseError::InvalidSequence {
                kind: TysonParseErrorKind::UnexpectedEof,
                ..
            }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn parse_string_token_after_quote(
        &mut self,
        quote_kind: u8,
    ) -> Result<String, TysonParseError> {
        let mut buf = Vec::new();
        debug_assert!([b'"', b'\''].contains(&quote_kind));
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
                    ArrayVec::from_iter(
                        esc_seq[err_start..err_end].iter().copied(),
                    ),
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
        let parse_unicode_escape = |_s: ReplacementState| todo!();
        let escape_handler = |s: ReplacementState| {
            if s[0] == b'\n' {
                debug_assert!(s.seq_len == 1);
                let line = &s.buffer()[curr_line_begin..];
                if let Err(e) = line.to_str() {
                    let err_start = e.valid_up_to();
                    let err_end = e
                        .error_len()
                        .map(|l| err_start + l)
                        .unwrap_or(line.len());
                    return Err(ReplacementError::Error(
                        TysonParseErrorKind::InvalidUtf8(ArrayVec::from_iter(
                            line[err_start..err_end].iter().copied(),
                        )),
                    ));
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
            if let Some(i) = escape_sequences.find_byte(esc_kind) {
                let seq_len = s.seq_len;
                let buf = s.into_buffer();
                buf.truncate(buf.len() - seq_len);
                buf.push(replacements[i]);
                return Ok(2);
            }
            Err(ReplacementError::Error(
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
                        Err(TysonParseError::InvalidSequence {
                            line: self.line,
                            col: buf[curr_line_begin..end].chars().count(),
                            kind: TysonParseErrorKind::InvalidUtf8(seq),
                        })
                    }
                    other => Err(TysonParseError::InvalidSequence {
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
        match buf[curr_line_begin..].to_str() {
            Err(e) => {
                return Err(TysonParseError::InvalidSequence {
                    line: self.line,
                    col: buf[curr_line_begin..e.valid_up_to()].chars().count(),
                    kind: TysonParseErrorKind::InvalidUtf8(
                        ArrayVec::from_iter(
                            buf[e.valid_up_to()..].iter().copied(),
                        ),
                    ),
                });
            }
            Ok(s) => {
                self.col = s.chars().count() + 1; // +1 for the delimiter
            }
        }
        // SAFETY: we already verified each line separately
        Ok(unsafe { String::from_utf8_unchecked(buf) })
    }
    fn err<T>(&self, kind: TysonParseErrorKind) -> Result<T, TysonParseError> {
        Err(self.error(kind))
    }
    fn error(&self, kind: TysonParseErrorKind) -> TysonParseError {
        TysonParseError::InvalidSequence {
            line: self.line,
            col: self.col,
            kind,
        }
    }
    fn parse_string_after_quote(
        &mut self,
        quote_kind: u8,
    ) -> Result<FieldValue, TysonParseError> {
        Ok(FieldValue::String(
            self.parse_string_token_after_quote(quote_kind)?,
        ))
    }
    fn parse_number(
        &mut self,
        first: u8,
    ) -> Result<FieldValue, TysonParseError> {
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
                    if self.read_char()?.to_ascii_lowercase() != 'n'
                        || self.read_char()?.to_ascii_lowercase() != 'f'
                    {
                        return Err(TysonParseError::InvalidSequence {
                            line: self.line,
                            col: self.col - 1,
                            kind: TysonParseErrorKind::InvalidNumber,
                        });
                    }
                    self.col += 2;
                    if let Some(idx) = sign {
                        debug_assert!(idx == 0);
                        if buf.as_bytes()[idx] == b'-' {
                            return Ok(FieldValue::Float(f64::NEG_INFINITY));
                        }
                        debug_assert!(buf.as_bytes()[idx] == b'+');
                    }
                    return Ok(FieldValue::Float(f64::INFINITY));
                }
                b'n' | b'N' => {
                    if !first_byte {
                        if buf.len() != 1 || sign.is_none() {
                            break;
                        }
                        self.void_byte_char();
                    }
                    if self.read_char()?.to_ascii_lowercase() != 'a'
                        || self.read_char()?.to_ascii_lowercase() != 'n'
                    {
                        return Err(TysonParseError::InvalidSequence {
                            line: self.line,
                            col: self.col - 1,
                            kind: TysonParseErrorKind::InvalidNumber,
                        });
                    }
                    self.col += 2;
                    return Ok(FieldValue::Float(f64::NAN));
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
        let exponent_digit_count = buf.len() - exponent.unwrap_or(buf.len());
        let mut digit_count = buf.len()
            - [sign, floating_point, exponent, exponent_sign]
                .iter()
                .map(|o| o.map(|_| 1).unwrap_or(0))
                .sum::<usize>()
            - exponent_digit_count;

        if digit_count == 0 {
            return Err(TysonParseError::InvalidSequence {
                line: self.line,
                col: self.col - buf.len(),
                kind: TysonParseErrorKind::InvalidNumber,
            });
        }
        for &c in buf[sign.map(|_| 1).unwrap_or(0)
            ..floating_point.or(exponent).unwrap_or(buf.len())]
            .as_bytes()
        {
            if c != b'0' {
                break;
            }
            digit_count -= 1;
        }
        if floating_point.is_none() && exponent.is_none() {
            if digit_count <= I64_MAX_DECIMAL_DIGITS + 1 {
                if let Ok(v) = buf.parse::<i64>() {
                    return Ok(FieldValue::Int(v));
                }
            }
            return Ok(FieldValue::BigInt(
                BigInt::parse_bytes(buf.as_bytes(), 10).unwrap(),
            ));
        }
        if digit_count <= f64::DIGITS as usize
            && exponent_digit_count <= f64::MAX_10_EXP as usize
        {
            if let Ok(v) = buf.parse::<f64>() {
                return Ok(FieldValue::Float(v));
            };
        }
        // TODO: rational
        Err(TysonParseError::InvalidSequence {
            line: self.line,
            col: self.col - buf.len(),
            kind: TysonParseErrorKind::InvalidNumber,
        })
    }
    fn parse_array_after_bracket(
        &mut self,
    ) -> Result<FieldValue, TysonParseError> {
        // PERF: todo: maybe optimize for specific types
        let mut arr = Vec::new();
        loop {
            let value = match self.parse_value() {
                Ok(v) => v,
                Err(TysonParseError::InvalidSequence {
                    kind: TysonParseErrorKind::StrayToken(']'),
                    ..
                }) => return Ok(FieldValue::Array(Array::Mixed(arr.into()))),
                Err(e) => return Err(e),
            };
            arr.push(value);
            let c = self.read_char_eat_whitespace()?;
            if c == ']' {
                return Ok(FieldValue::Array(Array::Mixed(arr.into())));
            }
            if c != ',' {
                return self.err(TysonParseErrorKind::StrayToken(c));
            }
        }
    }
    fn parse_object_after_brace(
        &mut self,
    ) -> Result<FieldValue, TysonParseError> {
        let mut map = IndexMap::new();
        let mut c = self.consume_char_eat_whitespace()?;
        loop {
            if c == '}' {
                return Ok(FieldValue::Object(Object::KeysStored(Box::new(
                    map,
                ))));
            }
            if c != '"' && c == '\'' {
                self.col -= 1;
                return self.err(TysonParseErrorKind::StrayToken(c));
            }
            let key = self.parse_string_token_after_quote(c as u8)?;
            c = self.consume_char_eat_whitespace()?;
            if c != ':' {
                self.col -= 1;
                return self.err(TysonParseErrorKind::StrayToken(c));
            }
            let value = self.parse_value()?;
            map.insert(key.into(), value);
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
    ) -> Result<FieldValue, TysonParseError> {
        todo!();
    }
    fn parse_value(&mut self) -> Result<FieldValue, TysonParseError> {
        let c = self.consume_char_eat_whitespace()?;
        match c {
            '{' => self.parse_object_after_brace(),
            '0'..='9' | '+' | '-' | '.' => self.parse_number(c as u8),
            '"' => self.parse_string_after_quote(b'"'),
            '\'' => self.parse_string_after_quote(b'\''),
            '(' => self.parse_type_after_parenthesis(),
            '[' => self.parse_array_after_bracket(),
            'i' | 'I' | 'N' => self.parse_number(c as u8), // inf / NaN
            'n' => {
                let b = self.peek_byte()?;
                if b == Some(b'a') {
                    return self.parse_number(c as u8);
                }
                for expected in "ull".chars() {
                    let c = self.read_char()?;
                    if c != expected {
                        return Err(TysonParseError::InvalidSequence {
                            line: self.line,
                            col: self.col,
                            kind: TysonParseErrorKind::StrayToken(c),
                        });
                    }
                    self.col += 1;
                }
                Ok(FieldValue::Null)
            }
            'u' => {
                for expected in "ndefined".chars() {
                    let c = self.read_char()?;
                    if c != expected {
                        return Err(TysonParseError::InvalidSequence {
                            line: self.line,
                            col: self.col,
                            kind: TysonParseErrorKind::StrayToken(c),
                        });
                    }
                    self.col += 1;
                }
                Ok(FieldValue::Undefined)
            }
            other => {
                self.col -= 1;
                self.err(TysonParseErrorKind::StrayToken(other))
            }
        }
    }
}

pub fn parse_tyson(
    input: impl BufRead,
    exts: &ExtensionRegistry,
) -> Result<FieldValue, TysonParseError> {
    let mut tp = TysonParser::new(input, exts);
    let res = tp.parse_value()?;
    tp.reject_further_input()?;
    Ok(res)
}

pub fn parse_tyson_str(
    input: &str,
    exts: &ExtensionRegistry,
) -> Result<FieldValue, TysonParseError> {
    // PERF: we could skip a lot of utf8 checking
    // if we already know that this is a string
    parse_tyson(input.as_bytes(), exts)
}

#[cfg(test)]
mod test {
    use num_bigint::BigInt;
    use rstest::rstest;

    use crate::{
        extension::ExtensionRegistry,
        record_data::field_value::{Array, FieldValue},
    };

    use super::{parse_tyson_str, TysonParseError, TysonParseErrorKind};

    fn parse(s: &str) -> Result<FieldValue, TysonParseError> {
        parse_tyson_str(s, &ExtensionRegistry::default())
    }

    #[test]
    fn empty_string() {
        assert_eq!(
            parse(""),
            Err(TysonParseError::InvalidSequence {
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
            Err(TysonParseError::InvalidSequence {
                line: 3,
                col: 2,
                kind: TysonParseErrorKind::UnexpectedEof,
            })
        );
    }

    #[test]
    fn string() {
        assert_eq!(parse(r#""foo""#), Ok(FieldValue::String("foo".into())));
    }

    #[test]
    fn single_quoted_string() {
        assert_eq!(parse("'foo'"), Ok(FieldValue::String("foo".into())));
    }

    #[test]
    fn unicode_escape() {
        assert_eq!(
            parse(r#""foo\u{1F4A9}bar""#),
            Ok(FieldValue::String("foo\u{1F4A9}bar".into()))
        );
    }

    #[test]
    fn array() {
        assert_eq!(
            parse(r#" [1,2,3,"4"] "#),
            Ok(FieldValue::Array(Array::Mixed(
                vec![
                    FieldValue::Int(1),
                    FieldValue::Int(2),
                    FieldValue::Int(3),
                    FieldValue::String("4".into())
                ]
                .into()
            )))
        );
    }
    #[test]
    fn null() {
        assert_eq!(parse("null"), Ok(FieldValue::Null));
    }
    #[test]
    fn undefined() {
        assert_eq!(parse("undefined"), Ok(FieldValue::Undefined));
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
        assert_eq!(parse(v), Ok(FieldValue::Int(res)));
    }
    #[rstest]
    #[case("9223372036854775808")]
    #[case("-9223372036854775809")]
    fn big_int(#[case] v: &str) {
        assert_eq!(
            parse(v),
            Ok(FieldValue::BigInt(
                BigInt::parse_bytes(v.as_bytes(), 10).unwrap()
            ))
        );
    }
    #[test]
    fn inf() {
        assert_eq!(parse(r#"Inf"#), Ok(FieldValue::Float(f64::INFINITY)));
        assert_eq!(parse(r#"+inf"#), Ok(FieldValue::Float(f64::INFINITY)));
        assert_eq!(parse(r#"-inf"#), Ok(FieldValue::Float(f64::NEG_INFINITY)));
    }
    #[test]
    fn nan() {
        assert!(
            matches!(parse(r#"nan"#), Ok(FieldValue::Float(v)) if v.is_nan())
        );
        assert!(
            matches!(parse(r#"NAn"#), Ok(FieldValue::Float(v)) if v.is_nan())
        );
        assert!(
            matches!(parse(r#"+nAn"#), Ok(FieldValue::Float(v)) if v.is_nan())
        );
        assert!(
            matches!(parse(r#"-NaN"#), Ok(FieldValue::Float(v)) if v.is_nan())
        );
    }
}
