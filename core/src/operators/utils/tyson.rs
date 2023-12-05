use std::io::BufRead;

use arrayvec::{ArrayString, ArrayVec};
use bstr::{ByteSlice, ByteVec};

use crate::{
    extension::ExtensionRegistry,
    record_data::field_data::FieldValue,
    utils::{
        io::{
            read_char, read_until_unescape2, ReadCharError,
            ReadUntilUnescapeError, ReplacementError, ReplacementState,
        },
        MAX_UTF8_CHAR_LEN,
    },
};

pub enum TysonParseErrorKind {
    Other(Box<str>),
    InvalidUtf8(ArrayVec<u8, MAX_UTF8_CHAR_LEN>),
    InvalidUnicodeEscape([u8; 4]),
    InvalidExtendedUnicodeEscape(ArrayString<6>),
    ExtendedUnicodeEscapeTooLong,
    NonEscapbleCharacter(u8),
    StrayToken(char),
    UnescapedBackslash,
    TrailingCharacters(char),
    UnexpectedEof,
}

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

impl<'a, S: BufRead> TysonParser<'a, S> {
    fn new(stream: S, exts: &'a ExtensionRegistry) -> Self {
        Self {
            stream,
            line: 0,
            col: 0,
            extension_registry: exts,
        }
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
    fn reject_further_input(&mut self) -> Result<(), TysonParseError> {
        match self.read_char() {
            Ok(c) => self.err(TysonParseErrorKind::TrailingCharacters(c)),
            Err(TysonParseError::InvalidSequence {
                kind: TysonParseErrorKind::UnexpectedEof,
                ..
            }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn parse_string_token_after_double_quote(
        &mut self,
    ) -> Result<String, TysonParseError> {
        let mut buf = Vec::new();
        let escape_sequences =
            [b'"', b'\\', b'/', b'b', b'f', b'n', b'r', b'h', b't', b'u'];
        let replacements = [
            b'"', b'\\', b'/', b'\x08', b'\x12', b'\n', b'\r', b'\x09',
            b'\x11',
        ];
        let mut curr_line_begin = 0;
        let parse_extended_unicode_escape = |s: ReplacementState| {
            let Some(esc_end) = s.find_limited(2, b'}', 2 + 6 + 1)? else {
                return Err(ReplacementError::Error(
                    TysonParseErrorKind::ExtendedUnicodeEscapeTooLong,
                ));
            };
            let buf_offset = s.buf_offset();
            let buf = s.pull_into_buf(esc_end - 1)?;
            let esc_seq = &buf[buf_offset + 3..buf_offset + esc_end - 1];

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
            buf.push_char(esc_value);
            Ok(esc_end)
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
                self.col = 0;
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
            b'"',
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
        // SAFETY: we verified each line separately in the code above
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
    fn parse_string_after_double_quote(
        &mut self,
    ) -> Result<FieldValue, TysonParseError> {
        Ok(FieldValue::String(
            self.parse_string_token_after_double_quote()?,
        ))
    }
    fn parse_number(
        &mut self,
        _first: char,
    ) -> Result<FieldValue, TysonParseError> {
        todo!()
    }
    fn parse_object_after_brace(
        &mut self,
    ) -> Result<FieldValue, TysonParseError> {
        todo!()
    }
    fn parse_type_after_parenthesis(
        &mut self,
    ) -> Result<FieldValue, TysonParseError> {
        todo!()
    }
    fn parse_value(&mut self) -> Result<FieldValue, TysonParseError> {
        loop {
            let c = self.read_char()?;
            match c {
                '{' => return self.parse_object_after_brace(),
                '0'..='9' | '+' | '-' | '.' => return self.parse_number(c),
                '"' => return self.parse_string_after_double_quote(),
                '(' => return self.parse_type_after_parenthesis(),
                '\n' => {
                    self.col = 0;
                    self.line += 1;
                }
                other => {
                    if !other.is_whitespace() {
                        return self.err(TysonParseErrorKind::StrayToken(c));
                    }
                    self.col += 1;
                }
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
