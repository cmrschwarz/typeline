use std::convert::Infallible;

use num::BigInt;

use crate::operators::errors::OperatorApplicationError;

use super::{BinaryOp, ErrorToOperatorApplicationError};

#[derive(Debug)]
pub struct MulOverflowError;

impl ErrorToOperatorApplicationError for MulOverflowError {
    fn to_operator_application_error(
        self,
        op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::errors::OperatorApplicationError {
        OperatorApplicationError::new(
            "integer overflow during multiplication",
            op_id,
        )
    }
}

pub struct BinaryOpMulI64I64;
unsafe impl BinaryOp for BinaryOpMulI64I64 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = i64;
    type Error = MulOverflowError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_mul(*rhs).ok_or(MulOverflowError)
    }
}

pub struct BinaryOpMulI64BigInt;
unsafe impl BinaryOp for BinaryOpMulI64BigInt {
    type Lhs = i64;
    type Rhs = BigInt;
    type Output = BigInt;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs * rhs)
    }
}

pub struct BinaryOpMulBigIntI64;
unsafe impl BinaryOp for BinaryOpMulBigIntI64 {
    type Lhs = BigInt;
    type Rhs = i64;
    type Output = BigInt;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs * rhs)
    }
}

pub struct BinaryOpMulF64F64;
unsafe impl BinaryOp for BinaryOpMulF64F64 {
    type Lhs = f64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs * rhs)
    }
}

pub struct BinaryOpMulF64I64;
unsafe impl BinaryOp for BinaryOpMulF64I64 {
    type Lhs = f64;
    type Rhs = i64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        #[allow(clippy::cast_precision_loss)]
        Ok(*lhs * (*rhs as f64))
    }
}

pub struct BinaryOpMulI64F64;
unsafe impl BinaryOp for BinaryOpMulI64F64 {
    type Lhs = i64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        #[allow(clippy::cast_precision_loss)]
        Ok((*lhs as f64) * *rhs)
    }
}
