use std::mem::MaybeUninit;

use num::{BigInt, FromPrimitive};

#[cfg(target_feature = "avx2")]
use super::binary_ops_avx2::{
    integer_add_immediate_stop_on_overflow_avx2,
    integer_add_stop_on_overflow_avx2,
    integer_sub_from_immediate_stop_on_overflow_avx2,
    integer_sub_immediate_stop_on_overflow_avx2,
    integer_sub_stop_on_overflow_avx2,
};

#[cfg(target_feature = "avx2")]
pub const AVX2_I64_ELEM_COUNT: usize = 4;

pub trait OverflowingBinOp {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64>;
    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt;

    fn calc_until_overflow_baseline(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        let len_min = lhs.len().min(rhs.len()).min(res.len());
        for i in 0..len_min {
            match Self::try_calc_single(lhs[i], rhs[i]) {
                Some(v) => res[i] = MaybeUninit::new(v),
                None => return i,
            }
        }
        len_min
    }
    fn calc_until_overflow_lhs_immediate_baseline(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        let len_min = rhs.len().min(res.len());
        for i in 0..len_min {
            match Self::try_calc_single(lhs, rhs[i]) {
                Some(v) => res[i] = MaybeUninit::new(v),
                None => return i,
            }
        }
        len_min
    }
    fn calc_until_overflow_rhs_immediate_baseline(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        let len_min = lhs.len().min(res.len());
        for i in 0..len_min {
            match Self::try_calc_single(lhs[i], rhs) {
                Some(v) => res[i] = MaybeUninit::new(v),
                None => return i,
            }
        }
        len_min
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_avx2(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize;

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize;
    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize;

    fn calc_until_overflow(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        #[cfg(target_feature = "avx2")]
        if rhs.len() >= AVX2_I64_ELEM_COUNT {
            return Self::calc_until_overflow_avx2(lhs, rhs, res);
        }
        Self::calc_until_overflow_baseline(lhs, rhs, res)
    }
    fn calc_until_overflow_rhs_immediate(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        #[cfg(target_feature = "avx2")]
        if lhs.len() >= AVX2_I64_ELEM_COUNT {
            return Self::calc_until_overflow_rhs_immediate_avx2(
                lhs, rhs, res,
            );
        }
        Self::calc_until_overflow_rhs_immediate_baseline(lhs, rhs, res)
    }
    fn calc_until_overflow_lhs_immediate(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        #[cfg(target_feature = "avx2")]
        if rhs.len() >= AVX2_I64_ELEM_COUNT {
            return Self::calc_until_overflow_lhs_immediate_avx2(
                lhs, rhs, res,
            );
        }
        Self::calc_until_overflow_lhs_immediate_baseline(lhs, rhs, res)
    }
}

pub struct BinOpAdd;
impl OverflowingBinOp for BinOpAdd {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_add(rhs)
    }

    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt {
        let mut res = BigInt::from_i64(lhs).unwrap();
        res += rhs;
        res
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_avx2(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        integer_add_stop_on_overflow_avx2(lhs, rhs, res)
    }
    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        integer_add_immediate_stop_on_overflow_avx2(lhs, rhs, res)
    }
    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // add is commutative
        integer_add_immediate_stop_on_overflow_avx2(rhs, lhs, res)
    }
}

pub struct BinOpSub;
impl OverflowingBinOp for BinOpSub {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_sub(rhs)
    }

    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt {
        let mut res = BigInt::from_i64(lhs).unwrap();
        res -= rhs;
        res
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_avx2(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        integer_sub_stop_on_overflow_avx2(lhs, rhs, res)
    }
    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        integer_sub_immediate_stop_on_overflow_avx2(lhs, rhs, res)
    }
    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        integer_sub_from_immediate_stop_on_overflow_avx2(lhs, rhs, res)
    }
}
