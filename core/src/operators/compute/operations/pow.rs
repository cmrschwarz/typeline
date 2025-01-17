use std::convert::Infallible;

use num::BigInt;

use crate::operators::errors::OperatorApplicationError;

use super::{BinaryOp, ErrorToOperatorApplicationError};

#[derive(Debug)]
pub struct PowOverflowError;

impl ErrorToOperatorApplicationError for PowOverflowError {
    fn to_operator_application_error(
        self,
        op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::errors::OperatorApplicationError {
        OperatorApplicationError::new(
            "integer underflow during exponentiation",
            op_id,
        )
    }
}

pub struct BinaryOpPowI64I64;
unsafe impl BinaryOp for BinaryOpPowI64I64 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = i64;
    type Error = PowOverflowError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_pow(u32::try_from(*rhs).ok().ok_or(PowOverflowError)?)
            .ok_or(PowOverflowError)
    }
}

pub struct BinaryOpPowBigIntI64;
unsafe impl BinaryOp for BinaryOpPowBigIntI64 {
    type Lhs = BigInt;
    type Rhs = i64;
    type Output = BigInt;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        // TODO: handle larger cases
        Ok(lhs.pow(u32::try_from(*rhs).expect("todo handle overflow")))
    }
}

pub struct BinaryOpPowF64F64;
unsafe impl BinaryOp for BinaryOpPowF64F64 {
    type Lhs = f64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs.powf(*rhs))
    }
}

pub struct BinaryOpPowF64I64;
unsafe impl BinaryOp for BinaryOpPowF64I64 {
    type Lhs = f64;
    type Rhs = i64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        #[allow(clippy::cast_precision_loss)]
        Ok(lhs.powf(*rhs as f64))
    }
}

pub struct BinaryOpPowI64F64;
unsafe impl BinaryOp for BinaryOpPowI64F64 {
    type Lhs = i64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        #[allow(clippy::cast_precision_loss)]
        Ok((*lhs as f64).powf(*rhs))
    }
}
