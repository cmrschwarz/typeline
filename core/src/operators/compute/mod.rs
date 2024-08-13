pub mod ast;
mod lexer;
pub mod parser;

use crate::utils::index_vec::IndexVec;
use ast::{ComputeExpr, ComputeIdentRefData, IdentRefId};

pub struct OpCompute {
    expr: ComputeExpr,
    ident_refs: IndexVec<IdentRefId, ComputeIdentRefData>,
}

#[cfg(test)]
mod test {}
