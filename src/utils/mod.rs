use std::fmt::Write;

use arrayvec::ArrayString;

pub mod identity_hasher;
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

pub enum CachingCallable<T, CTOR: FnOnce() -> T> {
    Unevaluated(CTOR),
    Cached(T),
    Dummy,
}

pub trait ValueProducingCallable<T> {
    fn call(&mut self) -> T;
}

impl<T: Clone, CTOR: FnOnce() -> T> ValueProducingCallable<T> for CachingCallable<T, CTOR> {
    fn call(&mut self) -> T {
        if let CachingCallable::Cached(v) = &self {
            return v.clone();
        }
        let v = std::mem::replace(self, CachingCallable::Dummy);
        let res = match v {
            CachingCallable::Unevaluated(func) => func(),
            _ => unreachable!(),
        };
        *self = CachingCallable::Cached(res.clone());
        res
    }
}
impl<T, F: FnMut() -> T> ValueProducingCallable<T> for F {
    fn call(&mut self) -> T {
        self()
    }
}

macro_rules! cached {
    ($b: block) => {{
        use crate::utils::CachingCallable;
        CachingCallable::Unevaluated(|| $b)
    }};
    ($x: expr) => {
        cached!({ $x })
    };
}

pub fn get_two_distinct_mut<'a, T>(
    slice: &'a mut [T],
    idx1: usize,
    idx2: usize,
) -> (&'a mut T, &'a mut T) {
    assert!(idx1 != idx2 && idx1 < slice.len() && idx2 < slice.len());
    unsafe {
        let ptr = slice.as_mut_ptr();
        (&mut *ptr.add(idx1), &mut *ptr.add(idx2))
    }
}
