pub mod ast;
mod lexer;
pub mod parser;

use crate::utils::index_vec::IndexVec;
use ast::{ComputeIdentRefData, Expr, UnboundRefId};

pub struct OpCompute {
    expr: Expr,
    ident_refs: IndexVec<UnboundRefId, ComputeIdentRefData>,
}
