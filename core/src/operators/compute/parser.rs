use std::collections::{hash_map::Entry, HashMap};

use crate::{
    index_newtype,
    operators::compute::ast::BinaryOpKind,
    tyson::TysonParseErrorKind,
    utils::{
        index_vec::IndexVec, indexing_type::IndexingType,
        string_store::INVALID_STRING_STORE_ENTRY, MAX_UTF8_CHAR_LEN,
    },
};
use arrayvec::ArrayVec;

use super::{
    ast::{
        AccessIdx, Block, Expr, IdentId, IfExpr, LetBindingData, LetBindingId,
        UnaryOpKind,
    },
    lexer::{ComputeExprLexer, ComputeExprSpan, ComputeExprToken, TokenKind},
    ExternIdentId, UnboundIdentData,
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

impl<'a> std::fmt::Display for ParseErrorKind<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseErrorKind::Empty => {
                f.write_str("expected expression, found empty input")
            }
            ParseErrorKind::UnexpectedCharacter(c) => {
                f.write_fmt(format_args!("unexpected character `{c}`"))
            }
            ParseErrorKind::InvalidUTF8(_) => {
                // TODO: display character bytes
                f.write_str("invalid utf-8 ")
            }
            ParseErrorKind::LiteralError(e) => e.fmt(f),
            ParseErrorKind::UnmatchedParenthesis {
                kind,
                expected_closing_paren: _,
            } => {
                let kind = match kind {
                    ParenthesisKind::Brace => "brace",
                    ParenthesisKind::Bracket => "bracket",
                    ParenthesisKind::Parenthesis => "parenthesis",
                };
                f.write_fmt(format_args!("unmatched {kind}"))
            }
            ParseErrorKind::DoubleComma {
                context,
                first_comma: _,
            } => {
                let kind = match context {
                    DoubleCommaContext::Object => "object",
                    DoubleCommaContext::Array => "array",
                };
                f.write_fmt(format_args!("doubled comma in {kind}"))
            }
            ParseErrorKind::UnexpectedToken { got, expected } => f.write_fmt(
                format_args!("unexpected token {got}, expected {expected}"),
            ),
            ParseErrorKind::EndOfInputWhileExpectingToken { expected } => f
                .write_fmt(format_args!(
                    "unexpected end of input, expected {expected}"
                )),
            ParseErrorKind::TrailingToken(tok) => f.write_fmt(format_args!(
                "unexpected token {tok} after end of expression"
            )),
        }
    }
}

impl ComputeExprParseError<'_> {
    pub fn stringify_error(&self, file_path: &str) -> String {
        format!(
            "{file_path}:{}:{}: {}",
            self.span.begin_line + 1,
            self.span.begin_col + 1,
            self.kind
        )
    }
}

impl std::fmt::Debug for ComputeExprParseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "<unknown>:{}:{}: {}",
            self.span.begin_line + 1,
            self.span.begin_col + 1,
            self.kind
        ))
    }
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

pub struct ComputeExprParser<'a, 't> {
    symbol_table: HashMap<&'a str, IdentId>,
    lexer: ComputeExprLexer<'a>,
    unbound_idents: &'t mut IndexVec<ExternIdentId, UnboundIdentData>,
    let_bindings: &'t mut IndexVec<LetBindingId, LetBindingData>,
}

index_newtype! {
    pub struct Precedence(u8);
}

impl<'i, 't> ComputeExprParser<'i, 't> {
    pub fn new(
        lexer: ComputeExprLexer<'i>,
        unbound_idents: &'t mut IndexVec<ExternIdentId, UnboundIdentData>,
        let_bindings: &'t mut IndexVec<LetBindingId, LetBindingData>,
    ) -> Self {
        Self {
            lexer,
            unbound_idents,
            let_bindings,
            symbol_table: HashMap::new(),
        }
    }

    fn parse_expression(
        &mut self,
        min_prec: Precedence,
    ) -> Result<Expr, ComputeExprParseError<'i>> {
        let lhs = self.parse_value()?;
        self.parse_expression_after_value(lhs, min_prec)
    }

    fn parse_expression_after_value(
        &mut self,
        lhs: Expr,
        min_prec: Precedence,
    ) -> Result<Expr, ComputeExprParseError<'i>> {
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
        self.lexer.drop_token();
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
    ) -> Result<Expr, ComputeExprParseError<'i>> {
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
    ) -> Result<ComputeExprToken<'i>, ComputeExprParseError<'i>> {
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
    ) -> Result<Block, ComputeExprParseError<'i>> {
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
    ) -> Result<Block, ComputeExprParseError<'i>> {
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
                    self.lexer.drop_token();
                    return Ok(Block {
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
    ) -> Result<Expr, ComputeExprParseError<'i>> {
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
    ) -> Result<Expr, ComputeExprParseError<'i>> {
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
                self.lexer.drop_token();
                Ok(Expr::Block(Block {
                    stmts: vec![first_expr].into_boxed_slice(),
                    trailing_semicolon: false,
                }))
            }
            TokenKind::Colon | TokenKind::Comma => {
                self.parse_object(open_brace_span, first_expr)
            }
            TokenKind::Semicolon => {
                self.lexer.drop_token();
                Ok(Expr::Block(
                    self.parse_block(open_brace_span, Some(first_expr))?,
                ))
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
    ) -> Result<Expr, ComputeExprParseError<'i>> {
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
                    self.lexer.drop_token();
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
    ) -> Result<Expr, ComputeExprParseError<'i>> {
        let cond = self.parse_expression(Precedence::ZERO)?;
        let then_block =
            self.expect_to_parse_block("`{` to start if block")?;

        let mut else_block = None;
        if self.lexer.peek_token_kind()? == Some(&TokenKind::Else) {
            self.lexer.drop_token();
            else_block =
                Some(self.expect_to_parse_block("`{` to start else block")?);
        };

        Ok(Expr::IfExpr(Box::new(IfExpr {
            cond,
            then_block,
            else_block,
        })))
    }

    fn parse_let_expr(
        &mut self,
        _let_kw_span: ComputeExprSpan,
    ) -> Result<Expr, ComputeExprParseError<'i>> {
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
        let temp_id = self.let_bindings.push_get_id(LetBindingData {
            name: ident.to_owned(),
            access_count: AccessIdx::ZERO,
        });
        Ok(Expr::LetExpression(temp_id, Box::new(value_expr)))
    }

    fn parse_value(&mut self) -> Result<Expr, ComputeExprParseError<'i>> {
        let Some(tok) = self.lexer.munch_token()? else {
            return Err(self.lexer.empty_error());
        };
        let unary_op = match tok.kind {
            TokenKind::Minus => UnaryOpKind::UnaryMinus,
            TokenKind::Plus => UnaryOpKind::UnaryPlus,
            TokenKind::Exclamation => UnaryOpKind::LogicalNot,
            TokenKind::Tilde => UnaryOpKind::BitwiseNot,
            TokenKind::Identifier(ident) => {
                match self.symbol_table.entry(ident) {
                    Entry::Occupied(e) => {
                        let ident_id = *e.get();
                        let access_count = match ident_id {
                            IdentId::LetBinding(lb_id) => {
                                &mut self.let_bindings[lb_id].access_count
                            }
                            IdentId::Unbound(ubi) => {
                                &mut self.unbound_idents[ubi].access_count
                            }
                        };
                        let access_index = *access_count;
                        *access_count += AccessIdx::one();
                        return Ok(Expr::Reference {
                            ident_id,
                            access_idx: access_index,
                        });
                    }
                    Entry::Vacant(e) => {
                        let id = self.unbound_idents.push_get_id(
                            UnboundIdentData {
                                name: ident.to_owned(),
                                name_interned: INVALID_STRING_STORE_ENTRY,
                                access_count: AccessIdx::one(),
                            },
                        );
                        let cref = IdentId::Unbound(id);
                        e.insert(cref);
                        return Ok(Expr::Reference {
                            ident_id: cref,
                            access_idx: AccessIdx::ZERO,
                        });
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

    pub fn parse(&mut self) -> Result<Expr, ComputeExprParseError<'i>> {
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

#[cfg(test)]
mod test {
    use crate::{
        operators::compute::{
            ast::{BinaryOpKind, Expr, UnaryOpKind},
            lexer::ComputeExprLexer,
        },
        record_data::field_value::FieldValue,
        utils::index_vec::IndexVec,
    };

    use super::ComputeExprParser;

    #[track_caller]
    #[allow(clippy::needless_pass_by_value)]
    fn test_parse(
        input: &str,
        unbound: impl IntoIterator<Item = String>,
        let_bindings: impl IntoIterator<Item = String>,
        output: Expr,
    ) {
        let mut unbound_out = IndexVec::new();
        let mut let_bindings_out = IndexVec::new();
        let mut p = ComputeExprParser::new(
            ComputeExprLexer::new(input.as_bytes()),
            &mut unbound_out,
            &mut let_bindings_out,
        );
        let res = p.parse();
        let expr = res
            .map_err(|e| e.stringify_error("<expr>"))
            .expect("input parses successfully");
        assert_eq!(expr, output);
        assert_eq!(
            unbound_out.into_iter().map(|i| i.name).collect::<Vec<_>>(),
            unbound.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            let_bindings_out
                .into_iter()
                .map(|i| i.name)
                .collect::<Vec<_>>(),
            let_bindings.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_compue_expr_parse_add() {
        let one = Expr::Literal(FieldValue::Int(1));
        test_parse(
            "1+1",
            [],
            [],
            Expr::OpBinary(BinaryOpKind::Add, Box::new([one.clone(), one])),
        )
    }

    #[test]
    fn test_compue_expr_parse_add_unary_minus() {
        test_parse(
            "1+-2",
            [],
            [],
            Expr::OpBinary(
                BinaryOpKind::Add,
                Box::new([
                    Expr::Literal(FieldValue::Int(1)),
                    Expr::OpUnary(
                        UnaryOpKind::UnaryMinus,
                        Box::new(Expr::Literal(FieldValue::Int(2))),
                    ),
                ]),
            ),
        )
    }

    #[test]
    fn test_compue_expr_parse_add_mul_prec() {
        test_parse(
            "1+-2*3",
            [],
            [],
            Expr::OpBinary(
                BinaryOpKind::Add,
                Box::new([
                    Expr::Literal(FieldValue::Int(1)),
                    Expr::OpUnary(
                        UnaryOpKind::UnaryMinus,
                        Box::new(Expr::OpBinary(
                            BinaryOpKind::Multiply,
                            Box::new([
                                Expr::Literal(FieldValue::Int(2)),
                                Expr::Literal(FieldValue::Int(3)),
                            ]),
                        )),
                    ),
                ]),
            ),
        )
    }
}
