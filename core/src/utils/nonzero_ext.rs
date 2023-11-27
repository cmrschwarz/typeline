use nonmax::{NonMaxU16, NonMaxU32, NonMaxUsize};
use std::num::{NonZeroU32, NonZeroUsize};

pub trait NonMaxU16Ext {
    const ZERO: NonMaxU16 = unsafe { NonMaxU16::new_unchecked(0) };
    const ONE: NonMaxU16 = unsafe { NonMaxU16::new_unchecked(1) };
    const MIN: NonMaxU16 = Self::ZERO;
    const MAX: NonMaxU16 = unsafe { NonMaxU16::new_unchecked(u16::MAX - 1) };
}

pub trait NonMaxU32Ext {
    const ZERO: NonMaxU32 = unsafe { NonMaxU32::new_unchecked(0) };
    const ONE: NonMaxU32 = unsafe { NonMaxU32::new_unchecked(1) };
    const MIN: NonMaxU32 = Self::ZERO;
    const MAX: NonMaxU32 = unsafe { NonMaxU32::new_unchecked(u32::MAX - 1) };
}

pub trait NonMaxUsizeExt {
    const ZERO: NonMaxUsize = unsafe { NonMaxUsize::new_unchecked(0) };
    const ONE: NonMaxUsize = unsafe { NonMaxUsize::new_unchecked(1) };
    const MIN: NonMaxUsize = Self::ZERO;
    const MAX: NonMaxUsize =
        unsafe { NonMaxUsize::new_unchecked(usize::MAX - 1) };
}

pub trait NonZeroU32Ext {
    const ONE: NonZeroU32 = NonZeroU32::MIN;
}

pub trait NonZeroUsizeExt {
    const ONE: NonZeroUsize = NonZeroUsize::MIN;
}

impl NonMaxU16Ext for NonMaxU16 {}
impl NonMaxU32Ext for NonMaxU32 {}
impl NonMaxUsizeExt for NonMaxUsize {}

impl NonZeroU32Ext for NonZeroU32 {}
impl NonZeroUsizeExt for NonZeroUsize {}

pub const fn nonmax_u16_wrapping_add(
    lhs: NonMaxU16,
    rhs: NonMaxU16,
) -> NonMaxU16 {
    let mut res = lhs.get().wrapping_add(rhs.get());
    if res == 0 {
        res = u16::MAX - 1;
    }
    unsafe { NonMaxU16::new_unchecked(res) }
}

pub const fn nonmax_u32_wrapping_sub(
    lhs: NonMaxU32,
    rhs: NonMaxU32,
) -> NonMaxU32 {
    let mut res = lhs.get().wrapping_sub(rhs.get());
    if res == u32::MAX {
        res = u32::MAX - 1;
    }
    unsafe { NonMaxU32::new_unchecked(res) }
}

pub const fn nonmax_usize_wrapping_sub(
    lhs: NonMaxUsize,
    rhs: NonMaxUsize,
) -> NonMaxUsize {
    let mut res = lhs.get().wrapping_sub(rhs.get());
    if res == usize::MAX {
        res = usize::MAX - 1;
    }
    unsafe { NonMaxUsize::new_unchecked(res) }
}
pub const fn nonzero_u32_wrapping_sub(
    lhs: NonZeroU32,
    rhs: NonZeroU32,
) -> NonZeroU32 {
    let mut res = lhs.get().wrapping_sub(rhs.get());
    if res == 0 {
        res = u32::MAX;
    }
    unsafe { NonZeroU32::new_unchecked(res) }
}

pub const fn nonzero_usize_wrapping_sub(
    lhs: NonZeroUsize,
    rhs: NonZeroUsize,
) -> NonZeroUsize {
    let mut res = lhs.get().wrapping_sub(rhs.get());
    if res == 0 {
        res = usize::MAX;
    }
    unsafe { NonZeroUsize::new_unchecked(res) }
}
