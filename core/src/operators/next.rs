use crate::cli::call_expr::OperatorCallExpr;

use super::{errors::OperatorCreationError, operator::OperatorData};

#[derive(Clone)]
pub struct OpNext {}

pub fn parse_op_next(
    expr: &OperatorCallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_params()?;
    Ok(OperatorData::Next(OpNext {}))
}
pub fn create_op_next() -> OperatorData {
    OperatorData::Next(OpNext {})
}
