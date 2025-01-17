use std::{
    arch::x86_64::{
        __m256i, _mm256_and_si256, _mm256_castsi256_pd, _mm256_movemask_pd,
        _mm256_sub_epi64, _mm256_xor_si256,
    },
    convert::Infallible,
    mem::MaybeUninit,
};

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

#[derive(Debug)]
pub struct UnderflowError;

impl ErrorToOperatorApplicationError for UnderflowError {
    fn to_operator_application_error(
        self,
        op_id: crate::operators::operator::OperatorId,
    ) -> crate::operators::errors::OperatorApplicationError {
        OperatorApplicationError::new(
            "integer underflow during subtraction",
            op_id,
        )
    }
}

fn i64_sub_overflow_check(
    lhs_v: __m256i,
    rhs_v: __m256i,
    res_v: __m256i,
) -> Result<(), (usize, UnderflowError)> {
    let of_mask = unsafe {
        // We have an overflow iff we have different signed inputs and the
        // sign of the first input does not equal the sign of the output
        // - same sign inputs never overflows
        // - of if a is positive, b is negative and the result is negative
        // - of if a is negative, b is positive and the result is positive
        // We can therefore calculate the overflow in the sign bit using
        // (a ^ b) & (a ^ res)
        let sign_diff_inputs = _mm256_xor_si256(lhs_v, rhs_v);
        let sign_diff_a_res = _mm256_xor_si256(lhs_v, res_v);
        let of_v = _mm256_and_si256(sign_diff_inputs, sign_diff_a_res);
        _mm256_movemask_pd(_mm256_castsi256_pd(of_v))
    };
    if of_mask == 0 {
        return Ok(());
    }
    Err((of_mask.leading_zeros() as usize, UnderflowError))
}

pub type BinaryOpSubI64I64 = BinaryOpAvx2Adapter<BinaryOpSubI64I64Avx2>;
pub struct BinaryOpSubI64I64Avx2;
unsafe impl BinaryOpAvx2Aware for BinaryOpSubI64I64Avx2 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = i64;
    type Error = UnderflowError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        lhs.checked_add(*rhs).ok_or(UnderflowError)
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
            |lhs, rhs| unsafe { _mm256_sub_epi64(lhs, rhs) },
            i64_sub_overflow_check,
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
            |lhs, rhs| unsafe { _mm256_sub_epi64(lhs, rhs) },
            i64_sub_overflow_check,
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
            |lhs, rhs| unsafe { _mm256_sub_epi64(lhs, rhs) },
            i64_sub_overflow_check,
            Self::try_calc_single,
        )
    }
}

pub struct BinaryOpSubI64BigInt;
unsafe impl BinaryOp for BinaryOpSubI64BigInt {
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

pub struct BinaryOpSubBigIntI64;
unsafe impl BinaryOp for BinaryOpSubBigIntI64 {
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

pub struct BinaryOpSubF64F64;
unsafe impl BinaryOp for BinaryOpSubF64F64 {
    type Lhs = f64;
    type Rhs = f64;
    type Output = f64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs - rhs)
    }
}
