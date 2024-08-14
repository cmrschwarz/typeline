use crate::{
    index_newtype,
    record_data::field_value::FieldValue,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::parser::Precedence;

index_newtype! {
    pub struct UnboundRefId(u32);
    pub struct TemporaryRefId(u32);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComputeValueRefType {
    Atom,
    Field,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeIdentRefData {
    pub ref_type: ComputeValueRefType,
    pub name: String,
    pub name_interned: Option<StringStoreEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeTemporaryRefData {
    pub name: String,
    pub name_interned: Option<StringStoreEntry>,
}

#[derive(Clone, Copy)]
pub enum UnaryOpKind {
    LogicalNot,
    BitwiseNot,
    UnaryPlus,
    UnaryMinus,
}

#[derive(Clone, Copy)]
pub enum BinaryOpKind {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessThanEquals,
    GreaterThanEquals,

    Add,
    AddAssign,

    Subtract,
    SubtractAssign,

    Multiply,
    MultiplyAssign,

    Divide,
    DivideAssign,

    Modulus,
    ModulusAssign,

    LShift,
    LShiftAssign,

    RShift,
    RShiftAssign,

    LogicalAnd,
    LogicalOr,
    LogicalXor,

    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,

    BitwiseAndAssign,
    BitwiseOrAssign,
    BitwiseXorAssign,
    BitwiseNotAssign,

    LogicalAndAssign,
    LogicalOrAssign,
    LogicalXorAssign,

    Access,
    Assign,
}

#[derive(Clone, Copy)]
pub enum IdentRefId {
    Temporary(TemporaryRefId),
    Unbound(UnboundRefId),
}

pub struct IfExpr {
    pub cond: Expr,
    pub then_expr: Expr,
    pub else_expr: Expr,
}

pub enum Expr {
    Literal(FieldValue),
    Reference(IdentRefId),
    OpUnary(UnaryOpKind, Box<Expr>),
    OpBinary(BinaryOpKind, Box<[Expr; 2]>),
    IfExpr(Box<IfExpr>),
    Block {
        stmts: Box<[Expr]>,
        trailing_semicolon: bool,
    },
    Object(Box<[(Expr, Option<Expr>)]>),
    Array(Vec<Expr>),
    FunctionCall(UnboundRefId, Box<[Expr]>),
    LetExpression(TemporaryRefId, Box<Expr>),
}

impl UnaryOpKind {
    pub fn prec(self) -> super::parser::Precedence {
        #[allow(clippy::match_same_arms)]
        let v = match self {
            UnaryOpKind::LogicalNot => 14,
            UnaryOpKind::BitwiseNot => 14,
            UnaryOpKind::UnaryPlus => 14,
            UnaryOpKind::UnaryMinus => 14,
        };
        Precedence::from_usize(v)
    }
}

impl BinaryOpKind {
    pub fn prec(self) -> super::parser::Precedence {
        #[allow(clippy::match_same_arms)]
        let v = match self {
            BinaryOpKind::Access => 16,

            BinaryOpKind::Multiply => 12,
            BinaryOpKind::Divide => 12,
            BinaryOpKind::Modulus => 12,

            BinaryOpKind::Add => 11,
            BinaryOpKind::Subtract => 11,

            BinaryOpKind::LShift => 10,
            BinaryOpKind::RShift => 10,

            BinaryOpKind::BitwiseAnd => 9,
            BinaryOpKind::BitwiseXor => 8,
            BinaryOpKind::BitwiseOr => 7,

            BinaryOpKind::Equals => 6,
            BinaryOpKind::NotEquals => 6,
            BinaryOpKind::LessThan => 6,
            BinaryOpKind::GreaterThan => 6,
            BinaryOpKind::LessThanEquals => 6,
            BinaryOpKind::GreaterThanEquals => 6,

            BinaryOpKind::LogicalAnd => 5,
            BinaryOpKind::LogicalXor => 4,
            BinaryOpKind::LogicalOr => 3,

            BinaryOpKind::Assign => 2,

            BinaryOpKind::AddAssign => 2,
            BinaryOpKind::SubtractAssign => 2,
            BinaryOpKind::MultiplyAssign => 2,
            BinaryOpKind::DivideAssign => 2,
            BinaryOpKind::ModulusAssign => 2,
            BinaryOpKind::LShiftAssign => 2,
            BinaryOpKind::RShiftAssign => 2,
            BinaryOpKind::LogicalAndAssign => 2,
            BinaryOpKind::LogicalXorAssign => 2,
            BinaryOpKind::LogicalOrAssign => 2,

            BinaryOpKind::BitwiseAndAssign => 2,
            BinaryOpKind::BitwiseOrAssign => 2,
            BinaryOpKind::BitwiseXorAssign => 2,
            BinaryOpKind::BitwiseNotAssign => 2,
        };
        Precedence::from_usize(v)
    }
}
