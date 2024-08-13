use crate::{
    index_newtype,
    operators::compute::ast::BinaryOpKind,
    tyson::TysonParseErrorKind,
    utils::{
        index_vec::IndexVec, indexing_type::IndexingType, MAX_UTF8_CHAR_LEN,
    },
};
use arrayvec::ArrayVec;

use super::{
    ast::{
        ComputeExprValue, ComputeTemporaryRefData, TemporaryRefId, UnaryOpKind,
    },
    lexer::{
        ComputeExprLexer, ComputeExprToken, ComputeExprTokenKind,
        ComputeExpressionSpan,
    },
    ComputeExpr, ComputeIdentRefData, IdentRefId,
};

pub struct ComputeExprParseError<'a> {
    pub span: ComputeExpressionSpan,
    pub kind: ComputeExprParseErrorKind<'a>,
}
pub enum ComputeExprParseErrorKind<'a> {
    Empty,
    UnexpectedCharacter(char),
    InvalidUTF8(ArrayVec<u8, MAX_UTF8_CHAR_LEN>),
    LiteralError(TysonParseErrorKind),
    UnmatchedParenthesis {
        expected_closing_paren: Option<ComputeExpressionSpan>,
    },
    UnexpectedToken {
        got: ComputeExprTokenKind<'a>,
        expected: &'static str,
    },
    EndOfInputWhileExpectingToken {
        expected: ArrayVec<&'static str, 4>,
    },
    TrailingToken(ComputeExprTokenKind<'a>),
}

pub struct ComputeExprParser<'a> {
    lexer: ComputeExprLexer<'a>,
    idents: &'a mut IndexVec<IdentRefId, ComputeIdentRefData>,
    temporaries: &'a mut IndexVec<TemporaryRefId, ComputeTemporaryRefData>,
}

index_newtype! {
    pub struct Precedence(u8);
}

impl<'a> ComputeExprParser<'a> {
    pub fn new(
        lexer: ComputeExprLexer<'a>,
        idents: &'a mut IndexVec<IdentRefId, ComputeIdentRefData>,
        temporaries: &'a mut IndexVec<TemporaryRefId, ComputeTemporaryRefData>,
    ) -> Self {
        Self {
            lexer,
            idents,
            temporaries,
        }
    }

    fn parse_expression(
        &mut self,
        min_prec: Precedence,
    ) -> Result<ComputeExpr, ComputeExprParseError<'a>> {
        let lhs = self.parse_value()?;
        self.parse_expression_after_value(lhs, min_prec)
    }

    fn parse_expression_after_value(
        &mut self,
        lhs: ComputeExpr,
        min_prec: Precedence,
    ) -> Result<ComputeExpr, ComputeExprParseError<'a>> {
        let Some(tok) = self.lexer.peek_token()? else {
            return Ok(lhs);
        };

        type Tok<'a> = ComputeExprTokenKind<'a>;
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
            Tok::TildeEquals => Op::NotEquals,
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

            ComputeExprTokenKind::Literal(_)
            | ComputeExprTokenKind::Identifier(_)
            | ComputeExprTokenKind::Let
            | ComputeExprTokenKind::LParen
            | ComputeExprTokenKind::RParen
            | ComputeExprTokenKind::LBrace
            | ComputeExprTokenKind::RBrace
            | ComputeExprTokenKind::LBracket
            | ComputeExprTokenKind::RBracket
            | ComputeExprTokenKind::Tilde
            | ComputeExprTokenKind::Exclamation
            | ComputeExprTokenKind::Colon => return Ok(lhs),
        };

        return Ok(ComputeExpr::OpBinary(
            binary_op,
            Box::new([
                lhs,
                self.parse_expression(binary_op.prec().max(min_prec))?,
            ]),
        ));
    }

    fn parse_parenthesized_expr(
        &mut self,
        tok: ComputeExprToken<'a>,
    ) -> Result<ComputeExpr, ComputeExprParseError<'a>> {
        debug_assert!(matches!(tok.kind, ComputeExprTokenKind::LParen));
        let start = tok.span;
        let res = self.parse_expression(Precedence::ZERO)?;
        let trailing_token = self.lexer.munch_token()?;
        let trailing_token_span = if let Some(trailing) = trailing_token {
            if matches!(trailing.kind, ComputeExprTokenKind::RParen) {
                return Ok(res);
            }
            Some(trailing.span)
        } else {
            None
        };
        Err(ComputeExprParseError {
            span: start,
            kind: ComputeExprParseErrorKind::UnmatchedParenthesis {
                expected_closing_paren: trailing_token_span,
            },
        })
    }

    fn parse_value(
        &mut self,
    ) -> Result<ComputeExpr, ComputeExprParseError<'a>> {
        let Some(tok) = self.lexer.munch_token()? else {
            return Err(self.lexer.empty_error());
        };
        let unary_op = match tok.kind {
            ComputeExprTokenKind::Minus => UnaryOpKind::UnaryMinus,
            ComputeExprTokenKind::Plus => UnaryOpKind::UnaryPlus,
            ComputeExprTokenKind::Exclamation => UnaryOpKind::LogicalNot,
            ComputeExprTokenKind::Tilde => UnaryOpKind::BitwiseNot,

            ComputeExprTokenKind::Literal(v) => {
                return Ok(ComputeExpr::Value(ComputeExprValue::Literal(v)))
            }

            ComputeExprTokenKind::LParen => {
                return self.parse_parenthesized_expr(tok);
            }
            ComputeExprTokenKind::LBrace => {
                todo!()
            }
            ComputeExprTokenKind::LBracket => {
                todo!()
            }

            _ => {
                return Err(ComputeExprParseError {
                    span: tok.span,
                    kind: ComputeExprParseErrorKind::UnexpectedToken {
                        got: tok.kind,
                        expected: "expression",
                    },
                })
            }
        };
        return Ok(ComputeExpr::OpUnary(
            unary_op,
            Box::new(self.parse_expression(unary_op.prec())?),
        ));
    }

    pub fn parse(&mut self) -> Result<ComputeExpr, ComputeExprParseError<'a>> {
        let res = self.parse_expression(Precedence::ZERO)?;
        if let Some(tok) = self.lexer.munch_token()? {
            return Err(ComputeExprParseError {
                span: tok.span,
                kind: ComputeExprParseErrorKind::TrailingToken(tok.kind),
            });
        }
        Ok(res)
    }
}
