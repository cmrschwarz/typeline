use crate::cli::call_expr::CallExpr;

use super::{errors::OperatorCreationError, operator::OperatorData};

#[derive(Clone)]
pub struct OpNext {}

pub fn parse_op_next(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(OperatorData::Next(OpNext {}))
}
pub fn create_op_next() -> OperatorData {
    OperatorData::Next(OpNext {})
}
