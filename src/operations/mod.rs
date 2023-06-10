use crate::context::SessionData;

use self::operator_base::{OperatorId, OperatorSetupError};

#[allow(dead_code)] //TODO
pub mod format;
pub mod operator_base;
pub mod operator_data;
pub mod print;
pub mod regex;
pub mod split;
pub mod transform_state;

pub fn setup_operator(_sd: &mut SessionData, _op_id: OperatorId) -> Result<(), OperatorSetupError> {
    todo!();
}
