use crate::{cli::reject_operator_params, options::argument::CliArgIdx};

use super::{errors::OperatorCreationError, operator::OperatorData};

#[derive(Clone)]
pub struct OpNext {}

pub fn parse_op_next(
    params: &[&[u8]],
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_params("next", params, arg_idx)?;
    Ok(OperatorData::Next(OpNext {}))
}
pub fn create_op_next() -> OperatorData {
    OperatorData::Next(OpNext {})
}
