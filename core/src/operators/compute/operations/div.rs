use std::convert::Infallible;

use crate::operators::errors::OperatorApplicationError;
use num::{BigInt, FromPrimitive};

use super::{BinaryOp, ErrorToOperatorApplicationError};

#[derive(Debug)]
pub struct DivByZeroError;

impl ErrorToOperatorApplicationError for DivByZeroError {
    fn to_operator_application_error(
        self,
        op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::errors::OperatorApplicationError {
        OperatorApplicationError::new("Division by Zero", op_id)
    }
}

pub struct BinaryOpDivI64I64;
unsafe impl BinaryOp for BinaryOpDivI64I64 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = i64;
    type Error = DivByZeroError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_div(*rhs).ok_or(DivByZeroError)
    }
}

pub struct BinaryOpDivBigIntI64;
unsafe impl BinaryOp for BinaryOpDivBigIntI64 {
    type Lhs = BigInt;
    type Rhs = i64;
    type Output = BigInt;
    type Error = DivByZeroError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_div(&BigInt::from_i64(*rhs).unwrap())
            .ok_or(DivByZeroError)
    }
}

pub struct BinaryOpDivI64BigInt;
unsafe impl BinaryOp for BinaryOpDivI64BigInt {
    type Lhs = i64;
    type Rhs = BigInt;
    type Output = BigInt;
    type Error = DivByZeroError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        BigInt::from_i64(*lhs)
            .unwrap()
            .checked_div(rhs)
            .ok_or(DivByZeroError)
    }
}

pub struct BinaryOpDivF64F64;
unsafe impl BinaryOp for BinaryOpDivF64F64 {
    type Lhs = f64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(*lhs / *rhs)
    }
}

pub struct BinaryOpDivI64F64;
unsafe impl BinaryOp for BinaryOpDivI64F64 {
    type Lhs = i64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        // TODO: handle overflow
        #[allow(clippy::float_cmp, clippy::cast_precision_loss)]
        {
            Ok((*lhs as f64) / *rhs)
        }
    }
}

pub struct BinaryOpDivF64I64;
unsafe impl BinaryOp for BinaryOpDivF64I64 {
    type Lhs = f64;
    type Rhs = i64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        // TODO: handle overflow
        #[allow(clippy::float_cmp, clippy::cast_precision_loss)]
        {
            Ok(*lhs / (*rhs as f64))
        }
    }
}
