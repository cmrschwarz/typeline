use std::{fmt::Write, num::NonZeroUsize};

use arrayvec::ArrayString;

pub mod plattform;
pub mod string_store;
pub mod universe;

pub const LOG_2_OF_TEN: f64 = 3.321928094887362; //  sadly, `10.log2()` is not const evaluable yet
pub const USIZE_MAX_DECIMAL_DIGITS: usize =
    ((std::mem::size_of::<usize>() * 8) as f64 / LOG_2_OF_TEN + (1f64 - f64::EPSILON)) as usize;

pub fn non_zero_usize_to_str(val: NonZeroUsize) -> ArrayString<USIZE_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    res.write_fmt(format_args!("{val}")).unwrap();
    res
}
