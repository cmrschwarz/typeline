use std::{
    arch::x86_64::_mm256_cmpgt_epi64, convert::Infallible,
    marker::PhantomData, mem::MaybeUninit,
};

use crate::record_data::field_data::FixedSizeFieldValueType;

use super::{
    avx2::{
        calc_cmp_avx2, calc_cmp_avx2_lhs_immediate,
        calc_cmp_avx2_rhs_immediate, mm256i_to_bool_array_neg,
    },
    BinaryOp, BinaryOpAvx2Adapter, BinaryOpAvx2Aware,
};
use num_order::NumOrd;

pub type BinaryOpLeI64I64 = BinaryOpAvx2Adapter<BinaryOpLeI64I64Avx2>;
pub struct BinaryOpLeI64I64Avx2;
unsafe impl BinaryOpAvx2Aware for BinaryOpLeI64I64Avx2 {
    type Lhs = i64;
    type Rhs = i64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(*lhs <= *rhs)
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
            |lhs, rhs| unsafe { _mm256_cmpgt_epi64(lhs, rhs) },
            mm256i_to_bool_array_neg,
            |lhs, rhs| *lhs <= *rhs,
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
            |lhs, rhs| unsafe { _mm256_cmpgt_epi64(lhs, rhs) },
            mm256i_to_bool_array_neg,
            |lhs, rhs| *lhs <= *rhs,
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
            |lhs, rhs| unsafe { _mm256_cmpgt_epi64(lhs, rhs) },
            mm256i_to_bool_array_neg,
            |lhs, rhs| *lhs <= *rhs,
        );
        (len, None)
    }
}

pub struct BasicBinaryOpLe<Lhs, Rhs>(PhantomData<(Lhs, Rhs)>);

unsafe impl<
        Lhs: NumOrd<Rhs> + FixedSizeFieldValueType,
        Rhs: FixedSizeFieldValueType,
    > BinaryOp for BasicBinaryOpLe<Lhs, Rhs>
{
    type Lhs = Lhs;
    type Rhs = Rhs;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs.num_le(rhs))
    }
}
