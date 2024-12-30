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

#[derive(Clone, PartialEq, Debug)]
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
    DoubleStar,
    DoubleStarEquals,
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

#[derive(Clone, Debug, PartialEq)]
pub struct ComputeExprToken<'a> {
    pub span: ComputeExprSpan,
    pub kind: TokenKind<'a>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ComputeExprSpan {
    pub begin_line: u32,
    pub end_line: u32,
    pub begin_col: u16,
    pub end_col: u16,
}

impl ComputeExprSpan {
    pub fn new(
        begin_line: u32,
        end_line: u32,
        begin_col: u16,
        end_col: u16,
    ) -> Self {
        Self {
            begin_line,
            end_line,
            begin_col,
            end_col,
        }
    }
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
    pub fn peek_token_kind(
        &mut self,
    ) -> Result<Option<&TokenKind<'a>>, ComputeExprParseError<'a>> {
        if self.lookahead.is_none() {
            self.lookahead = self.munch_token()?;
        }
        Ok(self.lookahead.as_ref().map(|t| &t.kind))
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
    pub fn drop_token(&mut self) {
        debug_assert!(self.lookahead.is_some());
        self.lookahead.take();
    }
    pub fn munch_token(
        &mut self,
    ) -> Result<Option<ComputeExprToken<'a>>, ComputeExprParseError<'a>> {
        if self.lookahead.is_some() {
            return Ok(self.lookahead.take());
        }
        let offset_prev = self.offset;
        let mut res = None;
        for (begin, end, c) in self.input[self.offset..].char_indices() {
            if c == ' ' {
                self.offset += 1;
                self.col += 1;
                continue;
            }
            if c == '\n' {
                self.offset += 1;
                self.col = 0;
                self.line += 1;
                continue;
            }

            res = Some((c, offset_prev + begin, offset_prev + end));
            break;
        }

        let Some((mut c, c_begin, c_end)) = res else {
            return Ok(None);
        };

        let mut span = self.next_token_start();

        let simple_kind = metamatch!(match c {
            #[expand((CHAR, TOK_KIND) in [
                ('{', LBrace),
                ('}', RBrace),
                ('(', LParen),
                (')', RParen),
                ('[', LBracket),
                (']', RBracket),
            ])]
            CHAR => {
                self.offset += 1;
                Some(TokenKind::TOK_KIND)
            }

            #[expand((C, BC, C_PLAIN, C_EQ, C_DOUBLE, C_DOUBLE_EQ) in [
                ('<', b'<', LAngleBracket, LessThanEquals, LShift, LShiftEquals),
                ('>', b'>', RAngleBracket, GreaterThanEquals, RShift, RShiftEquals),
                ('&', b'&', Ampersand, AmpersandEquals, DoubleAmpersand, DoubleAmpersandEquals),
                ('|', b'|', Pipe, PipeEquals, DoublePipe, DoublePipeEquals),
                ('^', b'^', Caret, CaretEquals, DoubleCaret, DoubleCaretEquals),
                ('*', b'*', Star, StarEquals, DoubleStar, DoubleStarEquals),
            ])]
            C => {
                match &self.input[c_begin..(c_begin + 2).min(self.input.len())]
                {
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
            self.col = self.col.saturating_add((self.offset - c_begin) as u16);
            span.end_col = self.col;
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
                CountingReader::new_with_offset(&self.input[c_end..], c_end);
            let mut p = TysonParser::new_with_position(
                &mut cr,
                true,
                self.line as usize,
                self.col as usize + (c_end - c_begin),
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

        if is_xid_start(c) || c == '_' {
            let mut ident_end = c_end;
            for (_, end, c) in self.input[c_end..].char_indices() {
                if !is_xid_continue(c) {
                    break;
                }
                ident_end = c_end + end;
            }
            self.col +=
                (ident_end - self.offset).try_into().unwrap_or(u16::MAX);
            self.offset = ident_end;
            span.end_col = self.col;

            let ident = &self.input[c_begin..ident_end];

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
            let bytes = self.input[c_begin..c_end].try_into().unwrap();
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
            TokenKind::DoubleStar => "**",
            TokenKind::DoubleStarEquals => "**=",
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

#[cfg(test)]
mod test {
    use crate::record_data::field_value::FieldValue;

    use super::{
        ComputeExprLexer, ComputeExprSpan, ComputeExprToken, TokenKind,
    };

    #[track_caller]
    #[allow(clippy::needless_pass_by_value)]
    fn test_lexer<'a>(
        input: &str,
        output: impl IntoIterator<Item = ComputeExprToken<'a>>,
    ) {
        let mut lx = ComputeExprLexer::new(input.as_bytes());
        let mut tokens = Vec::new();
        while let Some(tok) = lx.munch_token().unwrap() {
            tokens.push(tok);
        }
        assert_eq!(tokens, output.into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn test_underscore_is_identifier() {
        test_lexer(
            "_",
            [ComputeExprToken {
                span: ComputeExprSpan::new(0, 0, 0, 1),
                kind: TokenKind::Identifier("_"),
            }],
        )
    }

    #[test]
    fn test_numbers_in_identifier() {
        test_lexer(
            "_foo123",
            [ComputeExprToken {
                span: ComputeExprSpan::new(0, 0, 0, 7),
                kind: TokenKind::Identifier("_foo123"),
            }],
        )
    }

    #[test]
    fn test_compute_expr_lex_number() {
        test_lexer(
            "1",
            [ComputeExprToken {
                span: ComputeExprSpan::new(0, 0, 0, 1),
                kind: TokenKind::Literal(FieldValue::Int(1)),
            }],
        )
    }
    #[test]
    fn test_compute_expr_lex_add() {
        test_lexer(
            " 1 + \n1",
            [
                ComputeExprToken {
                    span: ComputeExprSpan::new(0, 0, 1, 2),
                    kind: TokenKind::Literal(FieldValue::Int(1)),
                },
                ComputeExprToken {
                    span: ComputeExprSpan::new(0, 0, 3, 4),
                    kind: TokenKind::Plus,
                },
                ComputeExprToken {
                    span: ComputeExprSpan::new(1, 1, 0, 1),
                    kind: TokenKind::Literal(FieldValue::Int(1)),
                },
            ],
        )
    }
}
