use std::{
    arch::x86_64::{
        __m256i, _mm256_and_si256, _mm256_cmpeq_epi64, _mm256_loadu_si256,
        _mm256_mul_epi32, _mm256_or_si256, _mm256_set1_epi64x,
        _mm256_setzero_si256, _mm256_srli_epi64, _mm256_storeu_si256,
        _mm256_testc_si256,
    },
    convert::Infallible,
    mem::MaybeUninit,
};

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

    fn calc_until_error<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        unsafe { multiply_arrays_hybrid(lhs, rhs, res) }
    }

    fn calc_until_error_lhs_immediate<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        unsafe { multiply_arrays_hybrid_lhs_immediate(*lhs, rhs, res) }
    }

    fn calc_until_error_rhs_immediate<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        // mul is commutative
        unsafe { multiply_arrays_hybrid_lhs_immediate(*rhs, lhs, res) }
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

#[inline(always)]
unsafe fn check_32bit(
    vec: __m256i,
    zero: __m256i,
    sign_mask: __m256i,
) -> __m256i {
    unsafe {
        // Check if high 32 bits are all 0 (unsigned) or all 1 (sign extended)
        let hi_bits = _mm256_srli_epi64(vec, 32);
        let is_zero = _mm256_cmpeq_epi64(hi_bits, zero);
        let is_sign = _mm256_cmpeq_epi64(hi_bits, sign_mask);
        _mm256_or_si256(is_zero, is_sign)
    }
}

fn mul_i64_baseline(
    lhs: &[i64],
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
    start: usize,
    end: usize,
) -> (usize, Option<MulOverflowError>) {
    for i in start..end {
        if let Some(product) = lhs[i].checked_mul(rhs[i]) {
            res[i] = MaybeUninit::new(product);
        } else {
            return (i, Some(MulOverflowError));
        }
    }
    (end, None)
}

pub unsafe fn multiply_arrays_hybrid(
    lhs: &[i64],
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
) -> (usize, Option<MulOverflowError>) {
    let len = lhs.len().min(rhs.len()).min(res.len());
    let mut i = 0;

    unsafe {
        let zero = _mm256_setzero_si256();
        let sign_mask = _mm256_set1_epi64x(0xFFFF_FFFF);
        let all_ones = _mm256_set1_epi64x(-1);

        while i + 3 < len {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs.as_ptr().add(i).cast());
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs.as_ptr().add(i).cast());

            // Check if both operands are 32-bit-safe for entire chunk
            let lhs_valid = check_32bit(lhs_v, zero, sign_mask);
            let rhs_valid = check_32bit(rhs_v, zero, sign_mask);
            let valid_mask = _mm256_and_si256(lhs_valid, rhs_valid);

            // Fast path: all elements can use SIMD multiplication
            if _mm256_testc_si256(valid_mask, all_ones) != 0 {
                let product_v = _mm256_mul_epi32(lhs_v, rhs_v);
                #[allow(clippy::cast_ptr_alignment)]
                _mm256_storeu_si256(res.as_mut_ptr().add(i).cast(), product_v);
                i += 4;
                continue;
            }

            // Slow path: check elements individually
            if let (i, Some(e)) = mul_i64_baseline(lhs, rhs, res, i, i + 4) {
                return (i, Some(e));
            }
            i += 4;
        }
    }

    // Process remaining elements
    mul_i64_baseline(lhs, rhs, res, i, len)
}

fn mul_i64_baseline_lhs_immediate(
    lhs: i64,
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
    start: usize,
    end: usize,
) -> (usize, Option<MulOverflowError>) {
    for i in start..end {
        if let Some(product) = lhs.checked_mul(rhs[i]) {
            res[i] = MaybeUninit::new(product);
        } else {
            return (i, Some(MulOverflowError));
        }
    }
    (end, None)
}

pub unsafe fn multiply_arrays_hybrid_lhs_immediate(
    lhs: i64,
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
) -> (usize, Option<MulOverflowError>) {
    let len = rhs.len().min(res.len());
    if lhs > i64::from(i32::MAX) || lhs < i64::from(i32::MIN) {
        return mul_i64_baseline_lhs_immediate(lhs, rhs, res, 0, len);
    }

    let mut i = 0;

    unsafe {
        #[allow(clippy::cast_ptr_alignment)]
        let lhs_v = _mm256_set1_epi64x(lhs);
        let zero = _mm256_setzero_si256();
        let sign_mask = _mm256_set1_epi64x(0xFFFF_FFFF);
        let all_ones = _mm256_set1_epi64x(-1);

        while i + 3 < len {
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs.as_ptr().add(i).cast());
            let rhs_valid = check_32bit(rhs_v, zero, sign_mask);

            // Fast path: all elements can use SIMD multiplication
            if _mm256_testc_si256(rhs_valid, all_ones) != 0 {
                let product_v = _mm256_mul_epi32(lhs_v, rhs_v);
                #[allow(clippy::cast_ptr_alignment)]
                _mm256_storeu_si256(res.as_mut_ptr().add(i).cast(), product_v);
                i += 4;
                continue;
            }

            // Slow path: check elements individually
            if let (i, Some(e)) =
                mul_i64_baseline_lhs_immediate(lhs, rhs, res, i, i + 4)
            {
                return (i, Some(e));
            }
            i += 4;
        }
    }
    mul_i64_baseline_lhs_immediate(lhs, rhs, res, i, len)
}
