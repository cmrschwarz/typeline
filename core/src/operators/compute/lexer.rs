use std::{char::REPLACEMENT_CHARACTER, fmt::Display};

use crate::{
    record_data::{
        field_value::FieldValue,
        formattable::{
            FormatOptions, FormattingContext, RealizedFormatKey,
            TypeReprFormat,
        },
    },
    tyson::{TysonParseError, TysonParser},
    utils::{
        counting_writer::CountingReader,
        text_write::{MaybeTextWritePanicAdapter, TextWriteFormatAdapter},
    },
};

use super::parser::{ComputeExprParseError, ParseErrorKind};
use bstr::ByteSlice;
use metamatch::metamatch;
use unicode_ident::{is_xid_continue, is_xid_start};

pub struct ComputeExprLexer<'a> {
    input: &'a [u8],
    offset: usize,
    line: u32,
    col: u16,
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
    pub begin_line: u32,
    pub end_line: u32,
    pub begin_col: u16,
    pub end_col: u16,
}

impl<'a> ComputeExprLexer<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            offset: 0,
            line: 0,
            col: 0,
            lookahead: None,
        }
    }
    pub fn next_token_start(&self) -> ComputeExprSpan {
        ComputeExprSpan {
            begin_line: self.line,
            end_line: self.line,
            begin_col: self.col,
            end_col: self.col,
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

        let mut span = self.next_token_start();

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
                        self.offset += 3;
                        Some(TokenKind::C_DOUBLE_EQ)
                    }
                    [BC, BC] | [BC, BC, _] => {
                        self.offset += 2;
                        Some(TokenKind::C_DOUBLE)
                    }
                    [BC, b'='] | [BC, b'=', _] => {
                        self.offset += 2;
                        Some(TokenKind::C_EQ)
                    }
                    _ => {
                        self.offset += 1;
                        Some(TokenKind::C_PLAIN)
                    }
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
                    self.offset += 2;
                    Some(TokenKind::CEQ)
                } else {
                    self.offset += 1;
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
            let mut cr =
                CountingReader::new_with_offset(self.input, self.offset);
            let mut p = TysonParser::new_with_position(
                &mut cr,
                true,
                self.line as usize,
                self.col as usize,
                None,
            );

            let res = if is_string_start {
                p.parse_string_after_quote(c as u8, binary)
            } else {
                debug_assert!(is_digit);
                p.parse_number(c as u8)
            };
            self.line = p.get_line().try_into().unwrap_or(u32::MAX);
            self.col = p.get_col().try_into().unwrap_or(u16::MAX);
            self.offset = cr.offset;

            span.end_line = self.line;
            span.end_col = self.col;
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
            self.col +=
                (ident_end - self.offset).try_into().unwrap_or(u16::MAX);
            self.offset = ident_end;

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

impl<'a> TokenKind<'a> {
    fn to_static_str(&self) -> Option<&'static str> {
        Some(match self {
            TokenKind::Literal(_) | TokenKind::Identifier(_) => return None,
            TokenKind::Let => "let",
            TokenKind::If => "if",
            TokenKind::Else => "else",
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::LBrace => "{",
            TokenKind::RBrace => "}",
            TokenKind::LBracket => "[",
            TokenKind::RBracket => "]",
            TokenKind::LAngleBracket => "<",
            TokenKind::LShift => "<<",
            TokenKind::LessThanEquals => "<=",
            TokenKind::LShiftEquals => "<<=",
            TokenKind::RAngleBracket => ">",
            TokenKind::RShift => ">>",
            TokenKind::GreaterThanEquals => ">=",
            TokenKind::RShiftEquals => ">>=",
            TokenKind::Plus => "+",
            TokenKind::PlusEquals => "+=",
            TokenKind::Minus => "-",
            TokenKind::MinusEquals => "-=",
            TokenKind::Star => "*",
            TokenKind::StarEquals => "*=",
            TokenKind::Slash => "/",
            TokenKind::SlashEquals => "/=",
            TokenKind::Percent => "%",
            TokenKind::PercentEquals => "%=",
            TokenKind::Tilde => "~",
            TokenKind::TildeEquals => "~=",
            TokenKind::Exclamation => "!",
            TokenKind::ExclamationEquals => "!=",
            TokenKind::Pipe => "|",
            TokenKind::PipeEquals => "|=",
            TokenKind::DoublePipe => "||",
            TokenKind::DoublePipeEquals => "||=",
            TokenKind::Caret => "^",
            TokenKind::CaretEquals => "^=",
            TokenKind::DoubleCaret => "^^",
            TokenKind::DoubleCaretEquals => "^^=",
            TokenKind::Ampersand => "&",
            TokenKind::AmpersandEquals => "&=",
            TokenKind::DoubleAmpersand => "&&",
            TokenKind::DoubleAmpersandEquals => "&&=",
            TokenKind::Equals => "=",
            TokenKind::DoubleEquals => "==",
            TokenKind::Colon => ":",
            TokenKind::Comma => ",",
            TokenKind::Semicolon => ";",
        })
    }
}

impl<'a> Display for TokenKind<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ss) = self.to_static_str() {
            f.write_fmt(format_args!("`{ss}`"))
        } else {
            match self {
                TokenKind::Literal(v) => {
                    let mut ctx = FormattingContext {
                        rfk: RealizedFormatKey {
                            opts: FormatOptions {
                                type_repr: TypeReprFormat::Typed,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ..Default::default()
                    };
                    v.format(
                        &mut ctx,
                        &mut MaybeTextWritePanicAdapter(
                            TextWriteFormatAdapter(f),
                        ),
                    )
                    .unwrap();
                    Ok(())
                }
                TokenKind::Identifier(v) => f.write_fmt(format_args!("`{v}`")),
                _ => unreachable!(),
            }
        }
    }
}
