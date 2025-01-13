use std::{mem::MaybeUninit, ops::Add};

use crate::operators::{
    errors::OperatorApplicationError, operator::OperatorId,
};

use super::binary_ops_int::ErroringBinOp;

pub trait BinOpIntFloat {
    fn try_calc_single(lhs: i64, rhs: f64) -> Option<f64>;

    fn calc_until_overflow_baseline(
        lhs: &[i64],
        rhs: &[f64],
        res: &mut [MaybeUninit<f64>],
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
        rhs: &[f64],
        res: &mut [MaybeUninit<f64>],
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
        rhs: f64,
        res: &mut [MaybeUninit<f64>],
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

    fn calc_until_overflow(
        lhs: &[i64],
        rhs: &[f64],
        res: &mut [MaybeUninit<f64>],
    ) -> usize {
        Self::calc_until_overflow_baseline(lhs, rhs, res)
    }
    fn calc_until_overflow_rhs_immediate(
        lhs: &[i64],
        rhs: f64,
        res: &mut [MaybeUninit<f64>],
    ) -> usize {
        Self::calc_until_overflow_rhs_immediate_baseline(lhs, rhs, res)
    }
    fn calc_until_overflow_lhs_immediate(
        lhs: i64,
        rhs: &[f64],
        res: &mut [MaybeUninit<f64>],
    ) -> usize {
        Self::calc_until_overflow_lhs_immediate_baseline(lhs, rhs, res)
    }
}

pub struct BinOpIntFloatAdd;
impl BinOpIntFloat for BinOpIntFloatAdd {
    fn try_calc_single(lhs: i64, rhs: f64) -> Option<f64> {
        // HACK
        #[allow(clippy::cast_precision_loss)]
        Some((lhs as f64).add(rhs))
    }
}

pub struct BinOpIntFloatSub;
impl BinOpIntFloat for BinOpIntFloatSub {
    fn try_calc_single(lhs: i64, rhs: f64) -> Option<f64> {
        // HACK
        #[allow(clippy::cast_precision_loss)]
        Some((lhs as f64) - rhs)
    }
}

pub struct BinOpIntFloatDiv;
impl BinOpIntFloat for BinOpIntFloatDiv {
    fn try_calc_single(lhs: i64, rhs: f64) -> Option<f64> {
        // HACK
        #[allow(clippy::cast_precision_loss)]
        Some((lhs as f64) / rhs)
    }
}
impl ErroringBinOp for BinOpIntFloatDiv {
    fn get_error(op_id: OperatorId) -> OperatorApplicationError {
        OperatorApplicationError::Borrowed {
            op_id,
            message: "Division by Zero",
        }
    }
}

pub struct BinOpIntFloatMul;
impl BinOpIntFloat for BinOpIntFloatMul {
    fn try_calc_single(lhs: i64, rhs: f64) -> Option<f64> {
        // HACK
        #[allow(clippy::cast_precision_loss)]
        Some((lhs as f64) * rhs)
    }
}

pub struct BinOpIntFloatPowerOf;
impl BinOpIntFloat for BinOpIntFloatPowerOf {
    fn try_calc_single(lhs: i64, rhs: f64) -> Option<f64> {
        // HACK
        #[allow(clippy::cast_precision_loss)]
        Some((lhs as f64).powf(rhs))
    }
}
