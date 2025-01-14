use std::mem::MaybeUninit;

#[cfg(target_feature = "avx2")]
pub const AVX2_I64_ELEM_COUNT: usize = 4;

pub trait CmpOp {
    fn calc_single(lhs: i64, rhs: i64) -> bool;

    fn calc_baseline(lhs: &[i64], rhs: &[i64], res: &mut [MaybeUninit<bool>]) {
        let len_min = lhs.len().min(rhs.len()).min(res.len());
        for i in 0..len_min {
            res[i] = MaybeUninit::new(Self::calc_single(lhs[i], rhs[i]));
        }
    }
    fn calc_lhs_immediate_baseline(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<bool>],
    ) {
        let len_min = rhs.len().min(res.len());
        for i in 0..len_min {
            res[i] = MaybeUninit::new(Self::calc_single(lhs, rhs[i]));
        }
    }
    fn calc_rhs_immediate_baseline(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<bool>],
    ) {
        let len_min = lhs.len().min(res.len());
        for i in 0..len_min {
            res[i] = MaybeUninit::new(Self::calc_single(lhs[i], rhs));
        }
    }

    #[cfg(target_feature = "avx2")]
    fn calc_avx2(lhs: &[i64], rhs: &[i64], res: &mut [MaybeUninit<bool>]) {
        Self::calc_baseline(lhs, rhs, res)
    }

    #[cfg(target_feature = "avx2")]
    fn calc_rhs_immediate_avx2(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<bool>],
    ) {
        Self::calc_rhs_immediate_baseline(lhs, rhs, res)
    }
    #[cfg(target_feature = "avx2")]
    fn calc_lhs_immediate_avx2(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<bool>],
    ) {
        Self::calc_lhs_immediate_baseline(lhs, rhs, res)
    }

    fn calc_until_overflow(
        lhs: &[i64],
        rhs: &[i64],
        res: &mut [MaybeUninit<bool>],
    ) {
        #[cfg(target_feature = "avx2")]
        if rhs.len() >= AVX2_I64_ELEM_COUNT {
            return Self::calc_avx2(lhs, rhs, res);
        }
        Self::calc_baseline(lhs, rhs, res)
    }
    fn calc_rhs_immediate(
        lhs: &[i64],
        rhs: i64,
        res: &mut [MaybeUninit<bool>],
    ) {
        #[cfg(target_feature = "avx2")]
        if lhs.len() >= AVX2_I64_ELEM_COUNT {
            return Self::calc_rhs_immediate_avx2(lhs, rhs, res);
        }
        Self::calc_rhs_immediate_baseline(lhs, rhs, res)
    }
    fn calc_lhs_immediate(
        lhs: i64,
        rhs: &[i64],
        res: &mut [MaybeUninit<bool>],
    ) {
        #[cfg(target_feature = "avx2")]
        if rhs.len() >= AVX2_I64_ELEM_COUNT {
            return Self::calc_lhs_immediate_avx2(lhs, rhs, res);
        }
        Self::calc_lhs_immediate_baseline(lhs, rhs, res)
    }
}

pub struct CmpOpEq;
impl CmpOp for CmpOpEq {
    fn calc_single(lhs: i64, rhs: i64) -> bool {
        lhs == rhs
    }
}
