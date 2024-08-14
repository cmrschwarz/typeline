use std::collections::{hash_map::Entry, HashMap};

use crate::{
    index_newtype,
    operators::compute::ast::BinaryOpKind,
    tyson::TysonParseErrorKind,
    utils::{
        index_vec::IndexVec, indexing_type::IndexingType, MAX_UTF8_CHAR_LEN,
    },
};
use arrayvec::ArrayVec;
use smallvec::SmallVec;

use super::{
    ast::{
        ComputeTemporaryRefData, ComputeValueRefType, IdentRefId, IfExpr,
        TemporaryRefId, UnaryOpKind,
    },
    lexer::{ComputeExprLexer, ComputeExprSpan, ComputeExprToken, TokenKind},
    ComputeIdentRefData, Expr, UnboundRefId,
};

pub enum ParenthesisKind {
    Brace,
    Bracket,
    Parenthesis,
}

pub enum DoubleCommaContext {
    Object,
    Array,
}

pub struct ComputeExprParseError<'a> {
    pub span: ComputeExprSpan,
    pub kind: ParseErrorKind<'a>,
}
pub enum ParseErrorKind<'a> {
    Empty,
    UnexpectedCharacter(char),
    InvalidUTF8(ArrayVec<u8, MAX_UTF8_CHAR_LEN>),
    LiteralError(TysonParseErrorKind),
    UnmatchedParenthesis {
        kind: ParenthesisKind,
        expected_closing_paren: Option<ComputeExprSpan>,
    },
    DoubleComma {
        context: DoubleCommaContext,
        first_comma: ComputeExprSpan,
    },
    UnexpectedToken {
        got: TokenKind<'a>,
        expected: &'static str,
    },
    EndOfInputWhileExpectingToken {
        expected: &'static str,
    },
    TrailingToken(TokenKind<'a>),
}

pub struct ComputeExprParser<'a> {
    scope_stack: SmallVec<[HashMap<&'a str, IdentRefId>; 1]>,
    lexer: ComputeExprLexer<'a>,
    unbound_idents: &'a mut IndexVec<UnboundRefId, ComputeIdentRefData>,
    temporaries: &'a mut IndexVec<TemporaryRefId, ComputeTemporaryRefData>,
}

index_newtype! {
    pub struct Precedence(u8);
}

impl<'a> ComputeExprParser<'a> {
    pub fn new(
        lexer: ComputeExprLexer<'a>,
        unbound_idents: &'a mut IndexVec<UnboundRefId, ComputeIdentRefData>,
        temporaries: &'a mut IndexVec<TemporaryRefId, ComputeTemporaryRefData>,
    ) -> Self {
        Self {
            lexer,
            unbound_idents,
            temporaries,
            scope_stack: SmallVec::from_iter([HashMap::new()]),
        }
    }

    fn parse_expression(
        &mut self,
        min_prec: Precedence,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let lhs = self.parse_value()?;
        self.parse_expression_after_value(lhs, min_prec)
    }

    fn parse_expression_after_value(
        &mut self,
        lhs: Expr,
        min_prec: Precedence,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let Some(tok) = self.lexer.peek_token()? else {
            return Ok(lhs);
        };

        type Tok<'a> = TokenKind<'a>;
        type Op = BinaryOpKind;
        let binary_op = match &tok.kind {
            Tok::LAngleBracket => Op::LessThan,
            Tok::LShift => Op::LShift,
            Tok::LessThanEquals => Op::LessThanEquals,
            Tok::LShiftEquals => Op::LShiftAssign,
            Tok::RAngleBracket => Op::GreaterThan,
            Tok::RShift => Op::RShift,
            Tok::GreaterThanEquals => Op::GreaterThanEquals,
            Tok::RShiftEquals => Op::RShiftAssign,
            Tok::Plus => Op::Add,
            Tok::PlusEquals => Op::AddAssign,
            Tok::Minus => Op::Subtract,
            Tok::MinusEquals => Op::SubtractAssign,
            Tok::Star => Op::Multiply,
            Tok::StarEquals => Op::MultiplyAssign,
            Tok::Slash => Op::Divide,
            Tok::SlashEquals => Op::DivideAssign,
            Tok::Percent => Op::Modulus,
            Tok::PercentEquals => Op::ModulusAssign,
            Tok::TildeEquals => Op::BitwiseNotAssign,
            Tok::ExclamationEquals => Op::NotEquals,
            Tok::Pipe => Op::BitwiseOr,
            Tok::PipeEquals => Op::BitwiseOrAssign,
            Tok::DoublePipe => Op::LogicalOr,
            Tok::DoublePipeEquals => Op::LogicalOrAssign,
            Tok::Caret => Op::BitwiseXor,
            Tok::CaretEquals => Op::BitwiseXorAssign,
            Tok::DoubleCaret => Op::LogicalXor,
            Tok::DoubleCaretEquals => Op::LogicalXorAssign,
            Tok::Ampersand => Op::BitwiseAnd,
            Tok::AmpersandEquals => Op::BitwiseAndAssign,
            Tok::DoubleAmpersand => Op::LogicalAnd,
            Tok::DoubleAmpersandEquals => Op::LogicalAndAssign,

            TokenKind::Literal(_)
            | TokenKind::Identifier(_)
            | TokenKind::Let
            | TokenKind::LParen
            | TokenKind::RParen
            | TokenKind::LBrace
            | TokenKind::RBrace
            | TokenKind::LBracket
            | TokenKind::RBracket
            | TokenKind::Tilde
            | TokenKind::Exclamation
            | TokenKind::Colon
            | TokenKind::Semicolon
            | TokenKind::Comma
            | TokenKind::If
            | TokenKind::Else
            | TokenKind::Equals
            | TokenKind::DoubleEquals => return Ok(lhs),
        };

        Ok(Expr::OpBinary(
            binary_op,
            Box::new([
                lhs,
                self.parse_expression(binary_op.prec().max(min_prec))?,
            ]),
        ))
    }

    fn parse_parenthesized_expr(
        &mut self,
        open_paren: ComputeExprSpan,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let res = self.parse_expression(Precedence::ZERO)?;
        let trailing_token = self.lexer.munch_token()?;
        let trailing_token_span = if let Some(trailing) = trailing_token {
            if matches!(trailing.kind, TokenKind::RParen) {
                return Ok(res);
            }
            Some(trailing.span)
        } else {
            None
        };
        Err(ComputeExprParseError {
            span: open_paren,
            kind: ParseErrorKind::UnmatchedParenthesis {
                kind: ParenthesisKind::Parenthesis,
                expected_closing_paren: trailing_token_span,
            },
        })
    }
    fn expect_to_munch_token_kind(
        &mut self,
        matching: fn(&TokenKind) -> bool,
        expected_err_msg: &'static str,
    ) -> Result<ComputeExprToken<'a>, ComputeExprParseError<'a>> {
        let Some(tok) = self.lexer.munch_token()? else {
            return Err(self.lexer.eof_error(expected_err_msg));
        };
        if !matching(&tok.kind) {
            return Err(ComputeExprParseError {
                span: tok.span,
                kind: ParseErrorKind::UnexpectedToken {
                    got: tok.kind,
                    expected: expected_err_msg,
                },
            });
        }
        Ok(tok)
    }
    fn expect_to_parse_block(
        &mut self,
        expected_err_msg: &'static str,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let open_brace_tok = self.expect_to_munch_token_kind(
            |k| matches!(k, TokenKind::LBrace),
            expected_err_msg,
        )?;
        self.parse_block(open_brace_tok.span, None)
    }
    fn parse_block(
        &mut self,
        open_brace_span: ComputeExprSpan,
        first_expr: Option<Expr>,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let mut block_stmts = Vec::new();
        if let Some(first_expr) = first_expr {
            block_stmts.push(first_expr);
        }
        let mut trailing_semicolon = true;
        loop {
            let Some(next_tok) = self.lexer.peek_token()? else {
                return Err(ComputeExprParseError {
                    span: open_brace_span,
                    kind: ParseErrorKind::UnmatchedParenthesis {
                        kind: ParenthesisKind::Brace,
                        expected_closing_paren: None,
                    },
                });
            };
            match &next_tok.kind {
                TokenKind::Semicolon => {
                    trailing_semicolon = true;
                    continue;
                }
                TokenKind::RBrace => {
                    let _ = self.lexer.munch_token();
                    return Ok(Expr::Block {
                        stmts: block_stmts.into_boxed_slice(),
                        trailing_semicolon,
                    });
                }
                _ => (),
            }
            block_stmts.push(self.parse_expression(Precedence::ZERO)?);
            trailing_semicolon = false;
        }
    }

    fn parse_object(
        &mut self,
        open_brace_span: ComputeExprSpan,
        first_key_expr: Expr,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let mut kv_pairs = Vec::new();

        let mut trailing_comma_observed = None;
        let mut key_expr = first_key_expr;
        loop {
            let Some(token_after_key_expr) = self.lexer.munch_token()? else {
                return Err(ComputeExprParseError {
                    span: open_brace_span,
                    kind: ParseErrorKind::UnmatchedParenthesis {
                        kind: ParenthesisKind::Brace,
                        expected_closing_paren: None,
                    },
                });
            };
            match token_after_key_expr.kind {
                TokenKind::Colon => {
                    let value = self.parse_expression(Precedence::ZERO)?;
                    kv_pairs.push((key_expr, Some(value)));
                    trailing_comma_observed = None;
                }
                TokenKind::Comma => {
                    kv_pairs.push((key_expr, None));
                    if let Some(span) = trailing_comma_observed {
                        return Err(ComputeExprParseError {
                            span: token_after_key_expr.span,
                            kind: ParseErrorKind::DoubleComma {
                                context: DoubleCommaContext::Object,
                                first_comma: span,
                            },
                        });
                    }
                    trailing_comma_observed = Some(token_after_key_expr.span);
                }
                TokenKind::RBrace => {
                    kv_pairs.push((key_expr, None));
                    return Ok(Expr::Object(kv_pairs.into_boxed_slice()));
                }
                _ => {
                    return Err(ComputeExprParseError {
                        span: token_after_key_expr.span,
                        kind: ParseErrorKind::UnexpectedToken {
                            got: token_after_key_expr.kind,
                            expected: "`:`, `,` or `}`",
                        },
                    })
                }
            }

            key_expr = self.parse_expression(Precedence::ZERO)?;
        }
    }

    fn parse_braced_expr(
        &mut self,
        open_brace_span: ComputeExprSpan,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let first_expr = self.parse_expression(Precedence::ZERO)?;
        let Some(next_tok) = self.lexer.peek_token()? else {
            return Err(ComputeExprParseError {
                span: open_brace_span,
                kind: ParseErrorKind::UnmatchedParenthesis {
                    kind: ParenthesisKind::Brace,
                    expected_closing_paren: None,
                },
            });
        };
        match next_tok.kind {
            TokenKind::RBrace => {
                let _ = self.lexer.munch_token();
                Ok(Expr::Block {
                    stmts: vec![first_expr].into_boxed_slice(),
                    trailing_semicolon: false,
                })
            }
            TokenKind::Colon | TokenKind::Comma => {
                self.parse_object(open_brace_span, first_expr)
            }
            TokenKind::Semicolon => {
                let _ = self.lexer.munch_token();
                self.parse_block(open_brace_span, Some(first_expr))
            }
            _ => {
                let next_tok = self.lexer.munch_token()?.unwrap();
                Err(ComputeExprParseError {
                    span: open_brace_span,
                    kind: ParseErrorKind::UnexpectedToken {
                        got: next_tok.kind,
                        expected: "`,`, `;` or `;`",
                    },
                })
            }
        }
    }

    fn parse_bracketed_expr(
        &mut self,
        open_brace_span: ComputeExprSpan,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let mut array = Vec::new();
        let mut trailing_comma_observed: Option<ComputeExprSpan> = None;
        loop {
            let Some(next_tok) = self.lexer.peek_token()? else {
                return Err(ComputeExprParseError {
                    span: open_brace_span,
                    kind: ParseErrorKind::UnmatchedParenthesis {
                        kind: ParenthesisKind::Bracket,
                        expected_closing_paren: None,
                    },
                });
            };
            match &next_tok.kind {
                TokenKind::Comma => {
                    if let Some(span) = trailing_comma_observed {
                        return Err(ComputeExprParseError {
                            span: next_tok.span,
                            kind: ParseErrorKind::DoubleComma {
                                context: DoubleCommaContext::Array,
                                first_comma: span,
                            },
                        });
                    }
                    trailing_comma_observed = Some(next_tok.span);
                    continue;
                }
                TokenKind::RBracket => {
                    let _ = self.lexer.munch_token();
                    return Ok(Expr::Array(array));
                }
                _ => (),
            }
            array.push(self.parse_expression(Precedence::ZERO)?);
            trailing_comma_observed = None;
        }
    }

    fn parse_if_expr(
        &mut self,
        _if_span: ComputeExprSpan,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let cond = self.parse_expression(Precedence::ZERO)?;
        let then_expr = self.expect_to_parse_block("`{` to start if block")?;
        self.expect_to_munch_token_kind(
            |k| matches!(k, TokenKind::Else),
            "`else`",
        )?;
        let else_expr =
            self.expect_to_parse_block("`{` to start else block")?;
        Ok(Expr::IfExpr(Box::new(IfExpr {
            cond,
            then_expr,
            else_expr,
        })))
    }

    fn parse_let_expr(
        &mut self,
        _let_kw_span: ComputeExprSpan,
    ) -> Result<Expr, ComputeExprParseError<'a>> {
        let ident_tok =
            self.lexer.munch_or_eof_err("identifier after `let`")?;
        let TokenKind::Identifier(ident) = ident_tok.kind else {
            return Err(ComputeExprParseError {
                span: ident_tok.span,
                kind: ParseErrorKind::UnexpectedToken {
                    got: ident_tok.kind,
                    expected: "identifier after `let`",
                },
            });
        };
        self.expect_to_munch_token_kind(
            |k| matches!(k, TokenKind::Equals),
            "`=` after let identifier",
        )?;
        let value_expr = self.parse_expression(Precedence::ZERO)?;
        let temp_id = self.temporaries.push_get_id(ComputeTemporaryRefData {
            name: ident.to_owned(),
            name_interned: None,
        });
        Ok(Expr::LetExpression(temp_id, Box::new(value_expr)))
    }

    fn parse_value(&mut self) -> Result<Expr, ComputeExprParseError<'a>> {
        let Some(tok) = self.lexer.munch_token()? else {
            return Err(self.lexer.empty_error());
        };
        let unary_op = match tok.kind {
            TokenKind::Minus => UnaryOpKind::UnaryMinus,
            TokenKind::Plus => UnaryOpKind::UnaryPlus,
            TokenKind::Exclamation => UnaryOpKind::LogicalNot,
            TokenKind::Tilde => UnaryOpKind::BitwiseNot,
            TokenKind::Identifier(ident) => {
                match self.scope_stack.last_mut().unwrap().entry(ident) {
                    Entry::Occupied(e) => {
                        return Ok(Expr::Reference(*e.get()));
                    }
                    Entry::Vacant(e) => {
                        let id = self.unbound_idents.push_get_id(
                            ComputeIdentRefData {
                                ref_type: ComputeValueRefType::Field,
                                name: ident.to_owned(),
                                name_interned: None,
                            },
                        );
                        let cref = IdentRefId::Unbound(id);
                        e.insert(cref);
                        return Ok(Expr::Reference(cref));
                    }
                }
            }
            TokenKind::Literal(v) => return Ok(Expr::Literal(v)),

            TokenKind::If => return self.parse_if_expr(tok.span),

            TokenKind::Let => return self.parse_let_expr(tok.span),

            TokenKind::LParen => {
                return self.parse_parenthesized_expr(tok.span);
            }
            TokenKind::LBrace => return self.parse_braced_expr(tok.span),
            TokenKind::LBracket => return self.parse_bracketed_expr(tok.span),
            _ => {
                return Err(ComputeExprParseError {
                    span: tok.span,
                    kind: ParseErrorKind::UnexpectedToken {
                        got: tok.kind,
                        expected: "expression",
                    },
                })
            }
        };
        Ok(Expr::OpUnary(
            unary_op,
            Box::new(self.parse_expression(unary_op.prec())?),
        ))
    }

    pub fn parse(&mut self) -> Result<Expr, ComputeExprParseError<'a>> {
        let res = self.parse_expression(Precedence::ZERO)?;
        if let Some(tok) = self.lexer.munch_token()? {
            return Err(ComputeExprParseError {
                span: tok.span,
                kind: ParseErrorKind::TrailingToken(tok.kind),
            });
        }
        Ok(res)
    }
}
