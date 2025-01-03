use std::mem::MaybeUninit;

use num::{BigInt, FromPrimitive};

use crate::operators::{
    errors::OperatorApplicationError, operator::OperatorId,
};

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

pub trait BinOp {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64>;

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

pub trait BigIntCapableBinOp: BinOp {
    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt;
}

pub trait ErroringBinOp: BinOp {
    fn get_error(op_id: OperatorId) -> OperatorApplicationError;
}

pub struct BinOpAdd;
impl BinOp for BinOpAdd {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_add(rhs)
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

impl BigIntCapableBinOp for BinOpAdd {
    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt {
        let mut res = BigInt::from_i64(lhs).unwrap();
        res += rhs;
        res
    }
}

pub struct BinOpSub;
impl BinOp for BinOpSub {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_sub(rhs)
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

impl BigIntCapableBinOp for BinOpSub {
    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt {
        let mut res = BigInt::from_i64(lhs).unwrap();
        res -= rhs;
        res
    }
}

pub struct BinOpDiv;
impl BinOp for BinOpDiv {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_div(rhs)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_avx2(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_rhs_immediate_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_lhs_immediate_baseline(lhs, rhs, res)
    }
}
impl ErroringBinOp for BinOpDiv {
    fn get_error(op_id: OperatorId) -> OperatorApplicationError {
        OperatorApplicationError::Borrowed {
            op_id,
            message: "Division by Zero",
        }
    }
}

pub struct BinOpMul;
impl BinOp for BinOpMul {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_mul(rhs)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_avx2(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_rhs_immediate_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_lhs_immediate_baseline(lhs, rhs, res)
    }
}

impl BigIntCapableBinOp for BinOpMul {
    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt {
        let mut bi = BigInt::from_i64(lhs).unwrap();
        bi *= rhs;
        bi
    }
}

pub struct BinOpPowerOf;
impl BinOp for BinOpPowerOf {
    fn try_calc_single(lhs: i64, rhs: i64) -> Option<i64> {
        lhs.checked_pow(rhs.try_into().ok()?)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_avx2(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_rhs_immediate_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_until_overflow_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<i64>],
    ) -> usize {
        // TODO: implement properly
        Self::calc_until_overflow_lhs_immediate_baseline(lhs, rhs, res)
    }
}

impl BigIntCapableBinOp for BinOpPowerOf {
    fn calc_into_bigint(lhs: i64, rhs: i64) -> BigInt {
        let bi = BigInt::from_i64(lhs).unwrap();
        // TODO: implement properly
        bi.pow(rhs.try_into().unwrap())
    }
}
