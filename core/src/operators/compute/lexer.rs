use std::char::REPLACEMENT_CHARACTER;

use crate::{
    record_data::field_value::FieldValue,
    tyson::{TysonParseError, TysonParser},
    utils::counting_writer::CountingReader,
};

use super::parser::{ComputeExprParseError, ParseErrorKind};
use bstr::ByteSlice;
use metamatch::metamatch;
use unicode_ident::{is_xid_continue, is_xid_start};

pub struct ComputeExprLexer<'a> {
    input: &'a [u8],
    offset: usize,
    lookahead: Option<ComputeExprToken<'a>>,
}

#[derive(Clone, PartialEq)]
pub enum TokenKind<'a> {
    Literal(FieldValue),
    Identifier(&'a str),

    Let,
    If,
    Else,

    LParen,
    RParen,

    LBrace,
    RBrace,

    LBracket,
    RBracket,

    LAngleBracket,
    LShift,
    LessThanEquals,
    LShiftEquals,

    RAngleBracket,
    RShift,
    GreaterThanEquals,
    RShiftEquals,

    Plus,
    PlusEquals,
    Minus,
    MinusEquals,
    Star,
    StarEquals,
    Slash,
    SlashEquals,
    Percent,
    PercentEquals,

    Tilde,
    TildeEquals,

    Exclamation,
    ExclamationEquals,

    Pipe,
    PipeEquals,
    DoublePipe,
    DoublePipeEquals,

    Caret,
    CaretEquals,
    DoubleCaret,
    DoubleCaretEquals,

    Ampersand,
    AmpersandEquals,
    DoubleAmpersand,
    DoubleAmpersandEquals,

    Equals,
    DoubleEquals,

    Colon,
    Comma,
    Semicolon,
}

#[derive(Clone)]
pub struct ComputeExprToken<'a> {
    pub span: ComputeExprSpan,
    pub kind: TokenKind<'a>,
}

#[derive(Clone, Copy)]
pub struct ComputeExprSpan {
    pub begin: u32,
    pub end: u32,
}

impl<'a> ComputeExprLexer<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            offset: 0,
            lookahead: None,
        }
    }
    pub fn next_token_start(&self) -> ComputeExprSpan {
        ComputeExprSpan {
            begin: self.offset as u32,
            end: self.offset as u32,
        }
    }
    pub fn empty_error(&self) -> ComputeExprParseError<'static> {
        ComputeExprParseError {
            span: self.next_token_start(),
            kind: ParseErrorKind::Empty,
        }
    }
    pub fn eof_error(
        &self,
        expected: &'static str,
    ) -> ComputeExprParseError<'static> {
        ComputeExprParseError {
            span: self.next_token_start(),
            kind: ParseErrorKind::EndOfInputWhileExpectingToken { expected },
        }
    }

    pub fn munch_or_eof_err(
        &mut self,
        expected: &'static str,
    ) -> Result<ComputeExprToken<'a>, ComputeExprParseError<'a>> {
        let Some(tok) = self.munch_token()? else {
            return Err(self.eof_error(expected));
        };
        Ok(tok)
    }
    pub fn peek_token(
        &mut self,
    ) -> Result<Option<&ComputeExprToken<'a>>, ComputeExprParseError<'a>> {
        if self.lookahead.is_some() {
            return Ok(self.lookahead.as_ref());
        }
        self.lookahead = self.munch_token()?;
        Ok(self.lookahead.as_ref())
    }
    pub fn peek_token_mut(
        &mut self,
    ) -> Result<&mut Option<ComputeExprToken<'a>>, ComputeExprParseError<'a>>
    {
        if self.lookahead.is_some() {
            return Ok(&mut self.lookahead);
        }
        self.lookahead = self.munch_token()?;
        Ok(&mut self.lookahead)
    }
    pub fn munch_token(
        &mut self,
    ) -> Result<Option<ComputeExprToken<'a>>, ComputeExprParseError<'a>> {
        if self.lookahead.is_some() {
            return Ok(self.lookahead.take());
        }

        let mut res = None;
        for (begin, end, c) in self.input[self.offset..].char_indices() {
            if c.is_ascii_whitespace() {
                self.offset = end;
                continue;
            }

            res = Some((c, begin, end));
            break;
        }

        let Some((mut c, begin, end)) = res else {
            return Ok(None);
        };

        let mut span = ComputeExprSpan {
            begin: begin as u32,
            end: end as u32,
        };

        let simple_kind = metamatch!(match c {
            '{' => Some(TokenKind::LBrace),
            '}' => Some(TokenKind::RBrace),
            '(' => Some(TokenKind::LParen),
            ')' => Some(TokenKind::RParen),
            '[' => Some(TokenKind::LBracket),
            ']' => Some(TokenKind::RBracket),

            #[expand((C, BC, C_PLAIN, C_EQ, C_DOUBLE, C_DOUBLE_EQ) in [
                ('<', b'<', LAngleBracket, LessThanEquals, LShift, LShiftEquals),
                ('>', b'>', RAngleBracket, GreaterThanEquals, RShift, RShiftEquals),
                ('&', b'&', Ampersand, AmpersandEquals, DoubleAmpersand, DoubleAmpersandEquals),
                ('|', b'|', Pipe, PipeEquals, DoublePipe,DoublePipeEquals),
                ('^', b'^', Caret, CaretEquals, DoubleCaret, DoubleCaretEquals)
            ])]
            C => {
                match &self.input[begin..(begin + 2).min(self.input.len())] {
                    [BC, BC, b'='] => {
                        self.offset += 2;
                        Some(TokenKind::C_DOUBLE_EQ)
                    }
                    [BC, BC] | [BC, BC, _] => {
                        self.offset += 1;
                        Some(TokenKind::C_DOUBLE)
                    }
                    [BC, b'='] | [BC, b'=', _] => {
                        self.offset += 1;
                        Some(TokenKind::C_EQ)
                    }
                    _ => Some(TokenKind::C_PLAIN),
                }
            }

            #[expand((C, CNEQ, CEQ) in [
                ('+', Plus, PlusEquals),
                ('-', Minus, MinusEquals),
                ('*', Star, StarEquals),
                ('/', Slash, SlashEquals),
                ('~', Tilde, TildeEquals),
                ('!', Exclamation, ExclamationEquals),
                ('=', Equals, DoubleEquals),
            ])]
            C => {
                if self.input.get(self.offset) == Some(&b'=') {
                    self.offset += 1;
                    Some(TokenKind::CEQ)
                } else {
                    Some(TokenKind::CNEQ)
                }
            }
            _ => None,
        });

        if let Some(kind) = simple_kind {
            return Ok(Some(ComputeExprToken { span, kind }));
        }

        let mut binary = false;
        let mut is_string_start = false;
        let mut is_digit = false;
        if c == 'b' {
            if let Some(&next) = self.input.get(self.offset) {
                if [b'\'', b'\"'].contains(&next) {
                    binary = true;
                    c = next as char;
                    is_string_start = true;
                }
            }
        }
        if !is_string_start {
            is_string_start = c == '"' || c == '\'';
        }

        if !is_string_start {
            is_digit = c.is_ascii_digit();
        }

        if is_string_start || is_digit {
            let mut r = CountingReader::new(&self.input[self.offset..]);
            let mut p = TysonParser::new(&mut r, true, None);

            let res = if is_string_start {
                p.parse_string_after_quote(c as u8, binary)
            } else {
                debug_assert!(is_digit);
                p.parse_number(c as u8)
            };
            span.end = r.offset as u32;
            return match res {
                Ok(v) => Ok(Some(ComputeExprToken {
                    span,
                    kind: TokenKind::Literal(v),
                })),
                Err(e) => match e {
                    TysonParseError::Io(_) => unreachable!(),
                    TysonParseError::InvalidSyntax { kind, .. } => {
                        Err(ComputeExprParseError {
                            span,
                            kind: ParseErrorKind::LiteralError(kind),
                        })
                    }
                },
            };
        }

        if is_xid_start(c) {
            let mut ident_end = self.offset;
            for (_, end, c) in self.input[self.offset..].char_indices() {
                if !is_xid_continue(c) {
                    break;
                }
                ident_end = end;
            }
            self.offset = ident_end;
            span.end = ident_end as u32;

            let ident = &self.input[begin..ident_end];

            let kind = match ident {
                b"let" => TokenKind::Let,
                b"if" => TokenKind::If,
                b"else" => TokenKind::Else,
                b"null" => TokenKind::Literal(FieldValue::Null),
                b"undefined" => TokenKind::Literal(FieldValue::Undefined),
                _ => TokenKind::Identifier(ident.to_str().unwrap()),
            };

            return Ok(Some(ComputeExprToken { span, kind }));
        }

        if c == REPLACEMENT_CHARACTER {
            let bytes = self.input[begin..end].try_into().unwrap();
            return Err(ComputeExprParseError {
                span,
                kind: ParseErrorKind::InvalidUTF8(bytes),
            });
        }

        Err(ComputeExprParseError {
            span,
            kind: ParseErrorKind::UnexpectedCharacter(c),
        })
    }
}
