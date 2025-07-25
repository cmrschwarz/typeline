use std::{
    borrow::Cow,
    ops::{Range, RangeBounds},
};

use bstr::{ByteSlice, ByteVec, Utf8Error};
use indexland::range_bounds_to_range_usize;
use smallstr::SmallString;

pub mod aligned_buf;
pub mod bit_vec_deque;
pub mod compare_i64_bigint;
pub mod counting_writer;
pub mod dynamic_freelist;
pub mod encoding;
pub mod escaped_writer;
pub mod identity_hasher;
pub mod int_string_conversions;
pub mod integer_sum;
pub mod io;
pub mod lazy_lock_guard;
pub mod max_index;
pub mod maybe_boxed;
pub mod maybe_text;
pub mod paths_store;
pub mod plattform;
pub mod printable_unicode;
pub mod ringbuf;
pub mod size_classed_vec_deque;
pub mod small_box;
pub mod string_store;
pub mod test_utils;
pub mod text_write;
pub mod type_list;

pub const fn ilog2_usize(v: usize) -> usize {
    (std::mem::size_of::<usize>() * 8) - v.leading_zeros() as usize
}

pub unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            std::ptr::from_ref(p).cast(),
            std::mem::size_of::<T>(),
        )
    }
}

pub const MAX_UTF8_CHAR_LEN: usize = 4;

// unnecessary overengineering to reduce sadness induced by having to look
// at idiv
pub fn divide_by_char_len(len: usize, char_len: usize) -> usize {
    match char_len {
        1 => len,
        2 => len / 2,
        3 => len / 3,
        4 => len / 4,
        _ => unreachable!(),
    }
}

pub unsafe fn launder_slice<T>(slice: &[T]) -> &'static [T] {
    unsafe { std::mem::transmute(slice) }
}

pub fn utf8_codepoint_len_from_first_byte(first_byte: u8) -> Option<u8> {
    if first_byte >> 7 == 0 {
        return Some(1);
    }
    if first_byte >> 5 == 0b110 {
        return Some(2);
    }
    if first_byte >> 4 == 0b1110 {
        return Some(3);
    }
    if first_byte >> 3 == 0b11110 {
        return Some(4);
    }
    None
}

pub fn is_utf8_continuation_byte(b: u8) -> bool {
    b >> 6 == 0b11
}

pub fn valid_utf8_codepoint_begins(buf: &[u8]) -> usize {
    buf.char_indices()
        .map(|(start, _end, c)| {
            usize::from(
                c != char::REPLACEMENT_CHARACTER
                    || !is_utf8_continuation_byte(buf[start]),
            )
        })
        .sum()
}

pub fn pointer_range_len<T>(range: &Range<*const T>) -> usize {
    unsafe { range.end.offset_from(range.start) as usize }
}

pub fn retain_vec_range<T>(v: &mut Vec<T>, range: Range<usize>) {
    if range == (0..v.len()) {
        return;
    }
    v.truncate(range.end);
    v.drain(0..range.start);
}

pub fn retain_string_range(v: &mut String, range: Range<usize>) {
    if range == (0..v.len()) {
        return;
    }
    v.truncate(range.end);
    v.drain(0..range.start);
}

pub fn subrange(
    range: &Range<usize>,
    subrange: &Range<usize>,
) -> Range<usize> {
    let start = range.start + subrange.start;
    let end = start + subrange.len();
    debug_assert!(end <= range.end);
    start..end
}

pub fn sub_saturated(v: &mut usize, sub: usize) {
    *v = (*v).saturating_sub(sub);
}

pub fn insert_str_cow<'a>(
    cow: &'a mut Cow<str>,
    index: usize,
    string: &str,
) -> &'a mut String {
    match cow {
        Cow::Borrowed(b) => {
            let mut res = String::with_capacity(b.len() + string.len());
            res.push_str(&b[0..index]);
            res.push_str(string);
            res.push_str(&b[index..]);
            *cow = Cow::Owned(res);
            let Cow::Owned(res) = cow else { unreachable!() };
            res
        }
        Cow::Owned(o) => {
            o.insert_str(index, string);
            o
        }
    }
}

pub fn slice_cow<'a>(
    cow: &Cow<'a, [u8]>,
    range: impl RangeBounds<usize>,
) -> Cow<'a, [u8]> {
    let range = range_bounds_to_range_usize(range, cow.len());
    match cow {
        Cow::Borrowed(v) => Cow::Borrowed(&v[range]),
        Cow::Owned(v) => Cow::Owned(v[range].to_vec()),
    }
}

pub fn cow_to_str(cow: Cow<[u8]>) -> Result<Cow<str>, Utf8Error> {
    match cow {
        Cow::Borrowed(v) => Ok(Cow::Borrowed(v.to_str()?)),
        Cow::Owned(v) => Ok(Cow::Owned(
            v.into_string().map_err(|e| e.utf8_error().clone())?,
        )),
    }
}

pub fn cow_to_small_str<A: smallvec::Array<Item = u8>>(
    cow: Cow<str>,
) -> SmallString<A> {
    match cow {
        Cow::Borrowed(s) => SmallString::from_str(s),
        Cow::Owned(s) => SmallString::from_string(s),
    }
}

// SAFETY: this is the almighty 'cast anything into anything' function.
// Unlike `std::mem::transmute`, this allows T and Q to have theoretically
// different sizes from the perspective of the type checker. This is generally
// only useful if T and Q are known to be the exact same type at runtime, but
// this cannot be proven to the typechecker / might not hold in a dynamically
// unreachable branch. Use with extreme caution!
#[allow(clippy::needless_pass_by_value)]
pub unsafe fn force_cast<T, Q>(v: T) -> Q {
    unsafe { std::ptr::read(std::ptr::addr_of!(v).cast::<Q>()) }
}

#[macro_export]
macro_rules! debugbreak {
    () => {
        unsafe {
            #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
            std::arch::asm!("int3");

            #[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
            std::arch::asm!(".inst 0xd4200000");

            #[cfg(not(any(
                target_arch = "x86_64",
                target_arch = "x86",
                target_arch = "aarch64",
                target_arch = "arm"
            )))]
            unimplemented!(
                "debug breaks are not implemented for this architecture"
            );
        }
        // better debugger ergonomics
        let _ = 0;
    };
}
