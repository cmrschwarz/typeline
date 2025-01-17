use std::{
    arch::x86_64::{
        __m256d, __m256i, _mm256_cmp_pd, _mm256_cmpeq_epi64, _CMP_NEQ_OQ,
    },
    convert::Infallible,
    marker::PhantomData,
};

use crate::record_data::field_data::FixedSizeFieldValueType;

use super::{
    avx2::{
        mm256d_to_bool_array, mm256i_to_bool_array_neg,
        BinaryOpCmpF64Avx2Adapter, BinaryOpCmpF64Avx2Aware,
        BinaryOpCmpI64Avx2Adapter, BinaryOpCmpI64Avx2Aware,
        AVX2_I64_ELEM_COUNT,
    },
    BinaryOp,
};
use num_order::NumOrd;

pub type BinaryOpNeI64I64 = BinaryOpCmpI64Avx2Adapter<BinaryOpNeI64I64Avx2>;
pub struct BinaryOpNeI64I64Avx2;
impl BinaryOpCmpI64Avx2Aware for BinaryOpNeI64I64Avx2 {
    fn cmp_single(lhs: &i64, rhs: &i64) -> bool {
        *lhs != *rhs
    }
    fn cmp_avx2(lhs: __m256i, rhs: __m256i) -> [bool; AVX2_I64_ELEM_COUNT] {
        mm256i_to_bool_array_neg(unsafe { _mm256_cmpeq_epi64(lhs, rhs) })
    }
}

pub type BinaryOpNeF64F64 = BinaryOpCmpF64Avx2Adapter<BinaryOpNeF64F64Avx2>;
pub struct BinaryOpNeF64F64Avx2;
impl BinaryOpCmpF64Avx2Aware for BinaryOpNeF64F64Avx2 {
    fn cmp_single(lhs: &f64, rhs: &f64) -> bool {
        #[allow(clippy::float_cmp)]
        {
            *lhs != *rhs
        }
    }
    fn cmp_avx2(lhs: __m256d, rhs: __m256d) -> [bool; AVX2_I64_ELEM_COUNT] {
        mm256d_to_bool_array(unsafe { _mm256_cmp_pd(lhs, rhs, _CMP_NEQ_OQ) })
    }
}

pub struct BasicBinaryOpNe<Lhs, Rhs>(PhantomData<(Lhs, Rhs)>);
unsafe impl<
        Lhs: NumOrd<Rhs> + FixedSizeFieldValueType,
        Rhs: FixedSizeFieldValueType,
    > BinaryOp for BasicBinaryOpNe<Lhs, Rhs>
{
    type Lhs = Lhs;
    type Rhs = Rhs;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs.num_ne(rhs))
    }
}
