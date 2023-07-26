use std::fmt::Write;

pub mod encoding;
pub mod identity_hasher;
pub mod int_string_conversions;
pub mod plattform;
pub mod string_store;
pub mod temp_vec;
pub mod universe;

pub const fn ilog2_usize(v: usize) -> usize {
    (std::mem::size_of::<usize>() * 8) - v.leading_zeros() as usize
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

impl<T: Clone, CTOR: FnOnce() -> T> ValueProducingCallable<T>
    for CachingCallable<T, CTOR>
{
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
#[derive(Clone, Copy, Default)]
pub struct LengthCountingWriter {
    pub len: usize,
}
impl Write for LengthCountingWriter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.len += s.len();
        Ok(())
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.len += c.len_utf8();
        Ok(())
    }

    fn write_fmt(
        mut self: &mut Self,
        args: std::fmt::Arguments<'_>,
    ) -> std::fmt::Result {
        std::fmt::write(&mut self, args)
    }
}
#[derive(Clone, Copy, Default)]
pub struct LengthAndCharsCountingWriter {
    pub len: usize,
    pub char_count: usize,
}
impl Write for LengthAndCharsCountingWriter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.len += s.len();
        self.char_count += s.chars().count();
        Ok(())
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.len += c.len_utf8();
        self.char_count += 1;
        Ok(())
    }

    fn write_fmt(
        mut self: &mut Self,
        args: std::fmt::Arguments<'_>,
    ) -> std::fmt::Result {
        std::fmt::write(&mut self, args)
    }
}
