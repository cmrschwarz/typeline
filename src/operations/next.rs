use crate::options::argument::CliArgIdx;

use super::{errors::OperatorCreationError, operator::OperatorData};

#[derive(Clone)]
pub struct OpNext {}

pub fn parse_op_next(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(OperatorData::Next(OpNext {}))
}
pub fn create_op_next() -> OperatorData {
    OperatorData::Next(OpNext {})
}
