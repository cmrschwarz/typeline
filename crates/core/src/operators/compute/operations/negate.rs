use std::convert::Infallible;

use crate::operators::errors::OperatorApplicationError;

use super::{ErrorToOperatorApplicationError, UnaryOp};

#[derive(Debug)]
pub struct NetationOverflow;

impl ErrorToOperatorApplicationError for NetationOverflow {
    fn to_operator_application_error(
        self,
        op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::errors::OperatorApplicationError {
        OperatorApplicationError::new(
            "integer overflow during unary minus",
            op_id,
        )
    }
}

pub struct UnaryOpNegateI64;
unsafe impl UnaryOp for UnaryOpNegateI64 {
    type Value = i64;
    type Output = i64;
    type Error = NetationOverflow;

    fn try_calc_single(
        lhs: &Self::Value,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_neg().ok_or(NetationOverflow)
    }
}

pub struct UnaryOpNegateF64;
unsafe impl UnaryOp for UnaryOpNegateF64 {
    type Value = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Value,
    ) -> Result<Self::Output, Self::Error> {
        Ok(-lhs)
    }
}
