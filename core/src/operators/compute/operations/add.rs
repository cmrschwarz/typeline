use num::BigInt;

use crate::operators::errors::OperatorApplicationError;

use super::{
    avx2::{
        calc_until_error_avx2, calc_until_error_avx2_lhs_immediate,
        calc_until_error_avx2_rhs_immediate,
    },
    BinaryOp, BinaryOpAvx2Adapter, BinaryOpAvx2Aware,
    ErrorToOperatorApplicationError,
};
use std::{
    arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_and_si256, _mm256_castsi256_pd,
        _mm256_movemask_pd, _mm256_xor_si256,
    },
    convert::Infallible,
    mem::MaybeUninit,
};

#[derive(Debug)]
pub struct AddOverflowError;

impl ErrorToOperatorApplicationError for AddOverflowError {
    fn to_operator_application_error(
        self,
        op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::errors::OperatorApplicationError {
        OperatorApplicationError::new(
            "integer overflow during addition",
            op_id,
        )
    }
}

fn i64_add_overflow_check(
    lhs_v: __m256i,
    rhs_v: __m256i,
    res_v: __m256i,
) -> Result<(), (usize, AddOverflowError)> {
    let of_mask = unsafe {
        // We have an overflow iff the sign of sum is not the same
        // as the sign of either of the inputs.
        // - mixed sign inputs never overflow
        // - if both inputs are positive but the sum is negative -> of
        // - if both inputs are negative but the sum is positive -> of
        // We can therefore calculate the overflow in the sign bit using
        // (a ^ res) & (b ^ res)
        let sign_diff_a_v = _mm256_xor_si256(res_v, lhs_v);
        let sign_diff_b_v = _mm256_xor_si256(res_v, rhs_v);
        let of_v = _mm256_and_si256(sign_diff_a_v, sign_diff_b_v);
        _mm256_movemask_pd(_mm256_castsi256_pd(of_v))
    };
    if of_mask == 0 {
        return Ok(());
    }
    Err((of_mask.leading_zeros() as usize, AddOverflowError))
}

pub type BinaryOpAddI64I64 = BinaryOpAvx2Adapter<BinaryOpAddI64I64Avx2>;
pub struct BinaryOpAddI64I64Avx2;
unsafe impl BinaryOpAvx2Aware for BinaryOpAddI64I64Avx2 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = i64;
    type Error = AddOverflowError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_add(*rhs).ok_or(AddOverflowError)
    }
    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_avx2(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_add_epi64(lhs, rhs) },
            i64_add_overflow_check,
            Self::try_calc_single,
        )
    }
    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_avx2_lhs_immediate(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_add_epi64(lhs, rhs) },
            i64_add_overflow_check,
            Self::try_calc_single,
        )
    }
    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_avx2_rhs_immediate(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_add_epi64(lhs, rhs) },
            i64_add_overflow_check,
            Self::try_calc_single,
        )
    }
}

pub struct BinaryOpAddI64BigInt;
unsafe impl BinaryOp for BinaryOpAddI64BigInt {
    type Lhs = i64;
    type Rhs = BigInt;
    type Output = BigInt;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs + rhs)
    }
}

pub struct BinaryOpAddBigIntI64;
unsafe impl BinaryOp for BinaryOpAddBigIntI64 {
    type Lhs = BigInt;
    type Rhs = i64;
    type Output = BigInt;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs + rhs)
    }
}

pub struct BinaryOpAddF64F64;
unsafe impl BinaryOp for BinaryOpAddF64F64 {
    type Lhs = f64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs + rhs)
    }
}
