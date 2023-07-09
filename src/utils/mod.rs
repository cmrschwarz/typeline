use std::fmt::Write;

use arrayvec::ArrayString;

pub mod plattform;
pub mod string_store;
pub mod universe;

pub const LOG_2_OF_TEN: f64 = 3.321928094887362; //  sadly, `10.log2()` is not const evaluable yet
pub const USIZE_MAX_DECIMAL_DIGITS: usize =
    ((std::mem::size_of::<usize>() * 8) as f64 / LOG_2_OF_TEN + (1f64 - f64::EPSILON)) as usize;
pub const I64_MAX_DECIMAL_DIGITS: usize =
    1 + (63 as f64 / LOG_2_OF_TEN + (1f64 - f64::EPSILON)) as usize;
pub const U64_MAX_DECIMAL_DIGITS: usize =
    (64 as f64 / LOG_2_OF_TEN + (1f64 - f64::EPSILON)) as usize;

pub fn usize_to_str(val: usize) -> ArrayString<USIZE_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    res.write_fmt(format_args!("{val}")).unwrap();
    res
}

pub fn u64_to_str(display_plus: bool, val: u64) -> ArrayString<U64_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    if display_plus {
        res.write_fmt(format_args!("{val:+}")).unwrap();
    } else {
        res.write_fmt(format_args!("{val}")).unwrap();
    }
    res
}
pub fn i64_to_str(display_plus: bool, val: i64) -> ArrayString<I64_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    if display_plus {
        res.write_fmt(format_args!("{val:+}")).unwrap();
    } else {
        res.write_fmt(format_args!("{val}")).unwrap();
    }
    res
}

pub const fn ilog2_usize(v: usize) -> usize {
    (std::mem::size_of::<usize>() * 8) - v.leading_zeros() as usize
}

pub fn i64_digits(display_plus_sign: bool, mut v: i64) -> usize {
    let sign_len = if v < 0 {
        v = -v;
        1
    } else {
        display_plus_sign as usize
    };
    let mut max = 10;
    for i in 0..I64_MAX_DECIMAL_DIGITS {
        if v < max {
            return i + 1 + sign_len;
        }
        max *= 10;
    }
    unreachable!();
}

pub const MAX_UTF8_CHAR_LEN: usize = 4;

// unnecessary overengineering to reduce sadness induced by having to look
// at idiv
#[inline(always)]
pub fn divide_by_char_len(len: usize, char_len: usize) -> usize {
    match char_len {
        1 => len / 1,
        2 => len / 2,
        3 => len / 3,
        4 => len / 4,
        _ => unreachable!(),
    }
}
