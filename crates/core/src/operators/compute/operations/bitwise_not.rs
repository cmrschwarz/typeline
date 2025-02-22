use std::convert::Infallible;

use super::UnaryOp;

pub struct UnaryOpBitwiseNotI64;

unsafe impl UnaryOp for UnaryOpBitwiseNotI64 {
    type Value = i64;
    type Output = i64;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Value,
    ) -> Result<Self::Output, Self::Error> {
        Ok(!lhs)
    }
}
