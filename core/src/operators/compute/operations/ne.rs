use std::{
    arch::x86_64::{_mm256_cmp_pd, _mm256_cmpeq_epi64, _CMP_NEQ_OQ},
    convert::Infallible,
    marker::PhantomData,
    mem::MaybeUninit,
};

use crate::record_data::field_data::FixedSizeFieldValueType;

use super::{
    avx2::{
        calc_cmp_avx2, calc_cmp_avx2_lhs_immediate,
        calc_cmp_avx2_rhs_immediate, calc_cmp_f64_avx2,
        calc_cmp_f64_avx2_lhs_immediate, calc_cmp_f64_avx2_rhs_immediate,
        mm256d_to_bool_array, mm256i_to_bool_array_neg,
    },
    BinaryOp, BinaryOpAvx2Adapter, BinaryOpAvx2Aware,
};
use num_order::NumOrd;

pub type BinaryOpNeI64I64 = BinaryOpAvx2Adapter<BinaryOpNeI64I64Avx2>;
pub struct BinaryOpNeI64I64Avx2;
unsafe impl BinaryOpAvx2Aware for BinaryOpNeI64I64Avx2 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(*lhs == *rhs)
    }
    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_avx2(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_cmpeq_epi64(lhs, rhs) },
            mm256i_to_bool_array_neg,
            |lhs, rhs| *lhs == *rhs,
        );
        (len, None)
    }
    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_avx2_lhs_immediate(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_cmpeq_epi64(lhs, rhs) },
            mm256i_to_bool_array_neg,
            |lhs, rhs| *lhs == *rhs,
        );
        (len, None)
    }
    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_avx2_rhs_immediate(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_cmpeq_epi64(lhs, rhs) },
            mm256i_to_bool_array_neg,
            |lhs, rhs| *lhs == *rhs,
        );
        (len, None)
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn f64_ne(lhs: &f64, rhs: &f64) -> bool {
    #[allow(clippy::float_cmp)]
    {
        *lhs == *rhs
    }
}
pub type BinaryOpNeF64F64 = BinaryOpAvx2Adapter<BinaryOpNeF64F64Avx2>;
pub struct BinaryOpNeF64F64Avx2;
unsafe impl BinaryOpAvx2Aware for BinaryOpNeF64F64Avx2 {
    type Lhs = f64;
    type Rhs = f64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(f64_ne(lhs, rhs))
    }
    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_f64_avx2(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_cmp_pd::<_CMP_NEQ_OQ>(lhs, rhs) },
            mm256d_to_bool_array,
            f64_ne,
        );
        (len, None)
    }
    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_f64_avx2_lhs_immediate(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_cmp_pd::<_CMP_NEQ_OQ>(lhs, rhs) },
            mm256d_to_bool_array,
            f64_ne,
        );
        (len, None)
    }
    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_f64_avx2_rhs_immediate(
            lhs,
            rhs,
            res,
            |lhs, rhs| unsafe { _mm256_cmp_pd::<_CMP_NEQ_OQ>(lhs, rhs) },
            mm256d_to_bool_array,
            f64_ne,
        );
        (len, None)
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
