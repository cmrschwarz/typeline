use std::io::Read;

use crate::{
    extension::ExtensionRegistry,
    record_data::field_data::FieldValue,
    utils::{
        read_char::{read_char, ReadCharError},
        MAX_UTF8_CHAR_LEN,
    },
};

enum TysonParseErrorKind {
    Io(std::io::Error),
    Other(Box<str>),
    InvalidUtf8 {
        len: u8,
        sequence: [u8; MAX_UTF8_CHAR_LEN],
    },
    StrayToken(char),
    TrailingCharacters(char),
    UnexpectedEof,
}

pub struct TysonParseError {
    line: usize,
    col: usize,
    kind: TysonParseErrorKind,
}

impl TysonParseError {
    pub fn new(line: usize, col: usize, kind: TysonParseErrorKind) -> Self {
        Self { line, col, kind }
    }
}

struct TysonParser<'a, S: Read> {
    stream: S,
    extension_registry: &'a ExtensionRegistry,
    line: usize,
    col: usize,
}

impl<'a, S: Read> TysonParser<'a, S> {
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
                ReadCharError::Io(e) => self.error(TysonParseErrorKind::Io(e)),
                ReadCharError::InvalidUtf8 { len, sequence } => self
                    .error(TysonParseErrorKind::InvalidUtf8 { len, sequence }),
                ReadCharError::Eof => {
                    self.error(TysonParseErrorKind::UnexpectedEof)
                }
            },
        }
    }
    fn reject_further_input(&mut self) -> Result<(), TysonParseError> {
        match read_char(&mut self.stream) {
            Ok(c) => {
                return self.error(TysonParseErrorKind::TrailingCharacters(c))
            }
            Err(e) => match e {
                ReadCharError::Io(e) => self.error(TysonParseErrorKind::Io(e)),
                ReadCharError::InvalidUtf8 { len, sequence } => self
                    .error(TysonParseErrorKind::InvalidUtf8 { len, sequence }),
                ReadCharError::Eof => Ok(()),
            },
        }
    }
    fn error<T>(
        &self,
        kind: TysonParseErrorKind,
    ) -> Result<T, TysonParseError> {
        Err(TysonParseError::new(self.line, self.col, kind))
    }
    fn parse_string_after_double_quote(
        &mut self,
    ) -> Result<FieldValue, TysonParseError> {
        todo!()
    }
    fn parse_number(
        &mut self,
        first: char,
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
                        return self.error(TysonParseErrorKind::StrayToken(c));
                    }
                    self.col += 1;
                }
            }
        }
    }
}

pub fn parse_tyson(
    input: impl Read,
    exts: &ExtensionRegistry,
) -> Result<FieldValue, TysonParseError> {
    TysonParser::new(input, exts).parse_value()
}
