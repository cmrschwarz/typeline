use std::{
    arch::x86_64::{
        _mm256_cmp_pd, _mm256_cmpeq_epi64, _mm256_cmpgt_epi64,
        _mm256_or_si256, _CMP_GE_OQ,
    },
    convert::Infallible,
    marker::PhantomData,
};

use crate::record_data::field_data::FixedSizeFieldValueType;

use super::{
    avx2::{
        mm256d_to_bool_array, mm256i_to_bool_array, BinaryOpCmpF64Avx2Adapter,
        BinaryOpCmpF64F64Avx2Aware, BinaryOpCmpI64Avx2Adapter,
        BinaryOpCmpI64I64Avx2Aware,
    },
    BinaryOp,
};
use num_order::NumOrd;

pub type BinaryOpGeI64I64 = BinaryOpCmpI64Avx2Adapter<BinaryOpGeI64I64Avx2>;
pub struct BinaryOpGeI64I64Avx2;
impl BinaryOpCmpI64I64Avx2Aware for BinaryOpGeI64I64Avx2 {
    fn cmp_single(lhs: &i64, rhs: &i64) -> bool {
        lhs >= rhs
    }

    fn cmp_avx2(
        lhs: std::arch::x86_64::__m256i,
        rhs: std::arch::x86_64::__m256i,
    ) -> [bool; super::avx2::AVX2_I64_ELEM_COUNT] {
        mm256i_to_bool_array(unsafe {
            _mm256_or_si256(
                _mm256_cmpgt_epi64(lhs, rhs),
                _mm256_cmpeq_epi64(lhs, rhs),
            )
        })
    }
}

pub type BinaryOpGeF64F64 = BinaryOpCmpF64Avx2Adapter<BinaryOpGeF64F64Avx2>;
pub struct BinaryOpGeF64F64Avx2;
impl BinaryOpCmpF64F64Avx2Aware for BinaryOpGeF64F64Avx2 {
    fn cmp_single(lhs: &f64, rhs: &f64) -> bool {
        lhs >= rhs
    }

    fn cmp_avx2(
        lhs: std::arch::x86_64::__m256d,
        rhs: std::arch::x86_64::__m256d,
    ) -> [bool; super::avx2::AVX2_I64_ELEM_COUNT] {
        mm256d_to_bool_array(unsafe { _mm256_cmp_pd(lhs, rhs, _CMP_GE_OQ) })
    }
}

pub struct BasicBinaryOpGe<Lhs, Rhs>(PhantomData<(Lhs, Rhs)>);

unsafe impl<
        Lhs: NumOrd<Rhs> + FixedSizeFieldValueType,
        Rhs: FixedSizeFieldValueType,
    > BinaryOp for BasicBinaryOpGe<Lhs, Rhs>
{
    type Lhs = Lhs;
    type Rhs = Rhs;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs.num_ge(rhs))
    }
}
