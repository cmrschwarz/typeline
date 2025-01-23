use std::convert::Infallible;

use super::UnaryOp;

pub struct UnaryOpLogicalNotI64;
unsafe impl UnaryOp for UnaryOpLogicalNotI64 {
    type Value = i64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Value,
    ) -> Result<Self::Output, Self::Error> {
        Ok(*lhs != 0)
    }
}
