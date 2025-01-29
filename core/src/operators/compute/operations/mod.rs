pub mod add;
pub mod avx2;
pub mod bitwise_not;
pub mod div;
pub mod eq;
pub mod ge;
pub mod gt;
pub mod le;
pub mod logical_not;
pub mod lt;
pub mod mul;
pub mod ne;
pub mod negate;
pub mod pow;
pub mod sub;

use std::fmt::Debug;

use std::{self, convert::Infallible, mem::MaybeUninit};

use crate::{
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::field_data::FixedSizeFieldValueType,
};

pub trait ErrorToOperatorApplicationError {
    fn to_operator_application_error(
        self,
        op_id: OperatorId,
    ) -> OperatorApplicationError;
}

impl ErrorToOperatorApplicationError for Infallible {
    fn to_operator_application_error(
        self,
        _op_id: OperatorId,
    ) -> OperatorApplicationError {
        unreachable!()
    }
}

pub(super) fn calc_until_error_baseline<'a, Lhs, Rhs, Output, Error>(
    lhs: &[Lhs],
    rhs: &[Rhs],
    res: &'a mut [MaybeUninit<Output>],
    op_baseline: impl Fn(&Lhs, &Rhs) -> Result<Output, Error>,
) -> (usize, Option<Error>) {
    let len_min = lhs.len().min(rhs.len()).min(res.len());
    for i in 0..len_min {
        match op_baseline(&lhs[i], &rhs[i]) {
            Ok(v) => res[i] = MaybeUninit::new(v),
            Err(e) => {
                return (i, Some(e));
            }
        }
    }
    (len_min, None)
}

fn calc_until_error_lhs_immediate_baseline<'a, Lhs, Rhs, Output, Error>(
    lhs: &Lhs,
    rhs: &[Rhs],
    res: &'a mut [MaybeUninit<Output>],
    op_baseline: impl Fn(&Lhs, &Rhs) -> Result<Output, Error>,
) -> (usize, Option<Error>) {
    let len_min = rhs.len().min(res.len());
    for i in 0..len_min {
        match op_baseline(lhs, &rhs[i]) {
            Ok(v) => res[i] = MaybeUninit::new(v),
            Err(e) => {
                return (i, Some(e));
            }
        }
    }
    (len_min, None)
}

fn calc_until_error_rhs_immediate_baseline<'a, Lhs, Rhs, Output, Error>(
    lhs: &[Lhs],
    rhs: &Rhs,
    res: &'a mut [MaybeUninit<Output>],
    op_baseline: impl Fn(&Lhs, &Rhs) -> Result<Output, Error>,
) -> (usize, Option<Error>) {
    let len_min = lhs.len().min(res.len());
    for i in 0..len_min {
        match op_baseline(&lhs[i], rhs) {
            Ok(v) => res[i] = MaybeUninit::new(v),
            Err(e) => {
                return (i, Some(e));
            }
        }
    }
    (len_min, None)
}

// SAFETY: The `calc_until_error**` family of functions **must** initialize
// the first `n` elements of `res` where `n` is the first return value of
// the function. This trait is unsafe because this is not enforced
// by the compiler.
pub unsafe trait BinaryOp {
    type Lhs: FixedSizeFieldValueType;
    type Rhs: FixedSizeFieldValueType;
    type Output: FixedSizeFieldValueType;
    type Error: Debug + ErrorToOperatorApplicationError;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error>;

    fn calc_until_error<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_baseline(lhs, rhs, res, Self::try_calc_single)
    }

    fn calc_until_error_lhs_immediate<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_lhs_immediate_baseline(
            lhs,
            rhs,
            res,
            Self::try_calc_single,
        )
    }

    fn calc_until_error_rhs_immediate<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_rhs_immediate_baseline(
            lhs,
            rhs,
            res,
            Self::try_calc_single,
        )
    }
}

pub struct BinaryOpCommutationWrapper<Op: BinaryOp> {
    _op: std::marker::PhantomData<Op>,
}

unsafe impl<Op: BinaryOp> BinaryOp for BinaryOpCommutationWrapper<Op> {
    type Lhs = Op::Rhs;
    type Rhs = Op::Lhs;
    type Output = Op::Output;
    type Error = Op::Error;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Op::try_calc_single(rhs, lhs)
    }

    fn calc_until_error<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        Op::calc_until_error(rhs, lhs, res)
    }

    fn calc_until_error_lhs_immediate<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        Op::calc_until_error_rhs_immediate(rhs, lhs, res)
    }

    fn calc_until_error_rhs_immediate<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        Op::calc_until_error_lhs_immediate(rhs, lhs, res)
    }
}

// SAFETY: `calc_until_error` must initialize
// the first `n` elements of `res` where `n` is the first return value
// of the function
pub unsafe trait UnaryOp {
    type Value: FixedSizeFieldValueType;
    type Output: FixedSizeFieldValueType;
    type Error: Debug + ErrorToOperatorApplicationError;

    fn try_calc_single(lhs: &Self::Value)
        -> Result<Self::Output, Self::Error>;

    fn calc_until_error<'a>(
        values: &[Self::Value],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len_min = values.len().min(res.len());
        for i in 0..len_min {
            match Self::try_calc_single(&values[i]) {
                Ok(v) => res[i] = MaybeUninit::new(v),
                Err(e) => {
                    return (i, Some(e));
                }
            }
        }
        (len_min, None)
    }
}
