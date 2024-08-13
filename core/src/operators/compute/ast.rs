use crate::{
    index_newtype,
    record_data::field_value::FieldValue,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::parser::Precedence;

index_newtype! {
    pub struct IdentRefId(u32);
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
    pub name: Option<String>,
    pub name_interned: Option<StringStoreEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeTemporaryRefData {
    pub name: Option<String>,
    pub name_interned: Option<StringStoreEntry>,
}

pub enum ComputeExprValue {
    Literal(FieldValue),
    Reference(IdentRefId),
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

pub enum ComputeExpr {
    Value(ComputeExprValue),
    OpUnary(UnaryOpKind, Box<ComputeExpr>),
    OpBinary(BinaryOpKind, Box<[ComputeExpr; 2]>),
    FunctionCall(IdentRefId, Box<[ComputeExpr]>),
    LetExpression(TemporaryRefId, Box<ComputeExpr>),
}

impl UnaryOpKind {
    pub fn prec(self) -> super::parser::Precedence {
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
