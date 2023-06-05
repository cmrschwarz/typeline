use crate::context::SessionData;

use self::operator_base::{OperatorId, OperatorSetupError};

pub mod control_flow_ops;
pub mod format;
pub mod operator_base;
pub mod operator_data;
pub mod regex;

pub fn setup_operator(_sd: &mut SessionData, _op_id: OperatorId) -> Result<(), OperatorSetupError> {
    todo!();
}
