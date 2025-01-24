use std::fmt::Display;

use crate::{
    index_newtype,
    record_data::field_value::{FieldValue, FieldValueKind},
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::parser::Precedence;

index_newtype! {
    pub struct ExternIdentId(u32);
    pub struct LetBindingId(u32);
    pub struct AccessIdx(u32);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnboundIdentData {
    pub name: String,
    pub name_interned: StringStoreEntry,
    pub access_count: AccessIdx,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LetBindingData {
    pub name: String,
    pub access_count: AccessIdx,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum UnaryOpKind {
    LogicalNot,
    BitwiseNot,
    UnaryPlus,
    UnaryMinus,
}

#[derive(Clone, Copy, PartialEq, Debug)]
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

    PowerOf,
    PowerOfAssign,

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

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct LetBindingRef {
    pub index: LetBindingId,
    pub access_idx: AccessIdx,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum IdentId {
    LetBinding(LetBindingId),
    Unbound(ExternIdentId),
}

#[derive(Clone, PartialEq, Debug)]
pub struct Block {
    pub stmts: Box<[Expr]>,
    pub trailing_semicolon: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub struct IfExpr {
    pub cond: Expr,
    pub then_block: Block,
    pub else_block: Option<Block>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum BuiltinFunction {
    Cast(FieldValueKind),
    Trim,
    Upper,
    Lower,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Expr {
    Literal(FieldValue),
    Parentheses(Box<Expr>),
    Reference {
        ident_id: IdentId,
        access_idx: AccessIdx,
    },
    OpUnary {
        kind: UnaryOpKind,
        child: Box<Expr>,
    },
    OpBinary {
        kind: BinaryOpKind,
        children: Box<[Expr; 2]>,
    },
    IfExpr(Box<IfExpr>),
    Block(Block),
    Object(Vec<(Expr, Option<Expr>)>),
    Array(Vec<Expr>),
    FunctionCall {
        lhs: Box<Expr>,
        args: Box<[Expr]>,
    },
    LetExpression(LetBindingId, Box<Expr>),
    BuiltinFunction(BuiltinFunction),
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
    pub fn to_str(self) -> &'static str {
        match self {
            UnaryOpKind::LogicalNot => "!",
            UnaryOpKind::BitwiseNot => "~",
            UnaryOpKind::UnaryPlus => "+",
            UnaryOpKind::UnaryMinus => "-",
        }
    }
}

impl BinaryOpKind {
    pub fn prec(self) -> super::parser::Precedence {
        #[allow(clippy::match_same_arms)]
        let v = match self {
            BinaryOpKind::Access => 16,

            BinaryOpKind::PowerOf => 13,

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
            BinaryOpKind::PowerOfAssign => 2,
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
    pub fn to_str(self) -> &'static str {
        match self {
            BinaryOpKind::Access => "[]",

            BinaryOpKind::PowerOf => "**",

            BinaryOpKind::Multiply => "*",
            BinaryOpKind::Divide => "/",
            BinaryOpKind::Modulus => "%",

            BinaryOpKind::Add => "+",
            BinaryOpKind::Subtract => "-",

            BinaryOpKind::LShift => "<<",
            BinaryOpKind::RShift => ">>",

            BinaryOpKind::BitwiseAnd => "&",
            BinaryOpKind::BitwiseXor => "^",
            BinaryOpKind::BitwiseOr => "|",

            BinaryOpKind::Equals => "==",
            BinaryOpKind::NotEquals => "!=",
            BinaryOpKind::LessThan => "<",
            BinaryOpKind::GreaterThan => ">",
            BinaryOpKind::LessThanEquals => "<=",
            BinaryOpKind::GreaterThanEquals => ">=",

            BinaryOpKind::LogicalAnd => "&&",
            BinaryOpKind::LogicalXor => "^^",
            BinaryOpKind::LogicalOr => "||",

            BinaryOpKind::Assign => "=",

            BinaryOpKind::AddAssign => "+=",
            BinaryOpKind::SubtractAssign => "-=",
            BinaryOpKind::MultiplyAssign => "*=",
            BinaryOpKind::PowerOfAssign => "**=",
            BinaryOpKind::DivideAssign => "/=",
            BinaryOpKind::ModulusAssign => "%=",
            BinaryOpKind::LShiftAssign => "<<=",
            BinaryOpKind::RShiftAssign => ">>=",
            BinaryOpKind::LogicalAndAssign => "&&=",
            BinaryOpKind::LogicalXorAssign => "^^=",
            BinaryOpKind::LogicalOrAssign => "||=",

            BinaryOpKind::BitwiseAndAssign => "&=",
            BinaryOpKind::BitwiseOrAssign => "|=",
            BinaryOpKind::BitwiseXorAssign => "^=",
            BinaryOpKind::BitwiseNotAssign => "~=",
        }
    }
}

impl Display for BinaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl Display for UnaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl BuiltinFunction {
    pub fn to_str(self) -> &'static str {
        match self {
            BuiltinFunction::Cast(kind) => kind.to_str(),
            BuiltinFunction::Trim => "trim",
            BuiltinFunction::Upper => "upper",
            BuiltinFunction::Lower => "lower",
        }
    }
}
