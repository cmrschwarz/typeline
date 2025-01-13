use std::{mem::MaybeUninit, ops::Add};

use crate::operators::{
    errors::OperatorApplicationError, operator::OperatorId,
};

use super::binary_ops_int::ErroringBinOp;

pub trait BinOpFloat {
    fn try_calc_single(lhs: f64, rhs: f64) -> Option<f64>;

    fn calc_until_overflow_baseline(
        lhs: &[f64],
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
        lhs: f64,
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
        lhs: &[f64],
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
        lhs: &[f64],
        rhs: &[f64],
        res: &mut [MaybeUninit<f64>],
    ) -> usize {
        Self::calc_until_overflow_baseline(lhs, rhs, res)
    }
    fn calc_until_overflow_rhs_immediate(
        lhs: &[f64],
        rhs: f64,
        res: &mut [MaybeUninit<f64>],
    ) -> usize {
        Self::calc_until_overflow_rhs_immediate_baseline(lhs, rhs, res)
    }
    fn calc_until_overflow_lhs_immediate(
        lhs: f64,
        rhs: &[f64],
        res: &mut [MaybeUninit<f64>],
    ) -> usize {
        Self::calc_until_overflow_lhs_immediate_baseline(lhs, rhs, res)
    }
}

pub struct BinOpFloatAdd;
impl BinOpFloat for BinOpFloatAdd {
    fn try_calc_single(lhs: f64, rhs: f64) -> Option<f64> {
        Some(lhs.add(rhs))
    }
}

pub struct BinOpFloatSub;
impl BinOpFloat for BinOpFloatSub {
    fn try_calc_single(lhs: f64, rhs: f64) -> Option<f64> {
        Some(lhs - rhs)
    }
}

pub struct BinOpFloatDiv;
impl BinOpFloat for BinOpFloatDiv {
    fn try_calc_single(lhs: f64, rhs: f64) -> Option<f64> {
        Some(lhs / rhs)
    }
}
impl ErroringBinOp for BinOpFloatDiv {
    fn get_error(op_id: OperatorId) -> OperatorApplicationError {
        OperatorApplicationError::Borrowed {
            op_id,
            message: "Division by Zero",
        }
    }
}

pub struct BinOpFloatMul;
impl BinOpFloat for BinOpFloatMul {
    fn try_calc_single(lhs: f64, rhs: f64) -> Option<f64> {
        Some(lhs * rhs)
    }
}

pub struct BinOpFloatPowerOf;
impl BinOpFloat for BinOpFloatPowerOf {
    fn try_calc_single(lhs: f64, rhs: f64) -> Option<f64> {
        Some(lhs.powf(rhs))
    }
}
