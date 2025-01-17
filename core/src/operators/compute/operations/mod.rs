pub mod add;
pub mod avx2;
pub mod div;
pub mod eq;
pub mod ge;
pub mod gt;
pub mod le;
pub mod lt;
pub mod mul;
pub mod ne;
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

fn calc_until_error<'a, Lhs, Rhs, Output, Error>(
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

fn calc_until_error_lhs_immediate<'a, Lhs, Rhs, Output, Error>(
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

fn calc_until_error_rhs_immediate<'a, Lhs, Rhs, Output, Error>(
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
        calc_until_error(lhs, rhs, res, Self::try_calc_single)
    }

    fn calc_until_error_lhs_immediate<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_lhs_immediate(lhs, rhs, res, Self::try_calc_single)
    }

    fn calc_until_error_rhs_immediate<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        calc_until_error_rhs_immediate(lhs, rhs, res, Self::try_calc_single)
    }
}
