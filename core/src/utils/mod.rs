use std::{
    borrow::Cow,
    ops::{Range, RangeBounds},
};

use bstr::{ByteSlice, ByteVec, Utf8Error};
use indexing_type::IndexingType;

#[macro_use]
pub mod index_vec;

pub mod aligned_buf;
pub mod bit_vec_deque;
pub mod counting_writer;
pub mod debuggable_nonmax;
pub mod dynamic_freelist;
pub mod encoding;
pub mod escaped_writer;
pub mod identity_hasher;
pub mod index_slice;
pub mod indexing_type;
pub mod int_string_conversions;
pub mod integer_sum;
pub mod io;
pub mod lazy_lock_guard;
pub mod maybe_boxed;
pub mod maybe_text;
pub mod offset_vec_deque;
pub mod paths_store;
pub mod phantom_slot;
pub mod plattform;
pub mod printable_unicode;
pub mod ringbuf;
pub mod size_classed_vec_deque;
pub mod small_box;
pub mod stable_vec;
pub mod string_store;
pub mod temp_vec;
pub mod test_utils;
pub mod text_write;
pub mod type_list;
pub mod universe;
pub mod multi_ref_mut_handout;
pub mod random_access_container;

pub const fn ilog2_usize(v: usize) -> usize {
    (std::mem::size_of::<usize>() * 8) - v.leading_zeros() as usize
}

pub unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            (p as *const T).cast(),
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

pub fn get_two_distinct_mut<T>(
    slice: &mut [T],
    idx1: usize,
    idx2: usize,
) -> (&mut T, &mut T) {
    assert!(idx1 != idx2, "indices must be unique");
    assert!(
        idx1 < slice.len() && idx2 < slice.len(),
        "indices must be in bounds"
    );
    unsafe {
        let ptr = slice.as_mut_ptr();
        (&mut *ptr.add(idx1), &mut *ptr.add(idx2))
    }
}

pub fn get_three_distinct_mut<T>(
    slice: &mut [T],
    idx1: usize,
    idx2: usize,
    idx3: usize,
) -> (&mut T, &mut T, &mut T) {
    assert!(idx1 != idx2 && idx2 != idx3, "indices must be unique");
    assert!(
        idx1 < slice.len() && idx2 < slice.len() && idx3 < slice.len(),
        "indices must be in bounds"
    );
    unsafe {
        let ptr = slice.as_mut_ptr();
        (
            &mut *ptr.add(idx1),
            &mut *ptr.add(idx2),
            &mut *ptr.add(idx3),
        )
    }
}

pub fn subslice_slice_pair<'a, T>(
    slices: (&'a [T], &'a [T]),
    range: Range<usize>,
) -> (&'a [T], &'a [T]) {
    let (s1, s2) = slices;
    let s1_len = s1.len();
    if range.start > s1_len {
        (&[], &s2[range.start - s1_len..range.end - s1_len])
    } else if range.end <= s1_len {
        (&s1[range.start..range.end], &[])
    } else {
        (
            &s1[range.start..],
            &s2[..range.len() - (s1_len - range.start)],
        )
    }
}

pub fn subslice_slice_pair_mut<'a, T>(
    slices: (&'a mut [T], &'a mut [T]),
    range: Range<usize>,
) -> (&'a mut [T], &'a mut [T]) {
    let (s1, s2) = slices;
    let s1_len = s1.len();
    if range.start > s1_len {
        (&mut [], &mut s2[range.start - s1_len..range.end - s1_len])
    } else {
        (
            &mut s1[range.start..],
            &mut s2[..range.len() - (s1_len - range.start)],
        )
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

pub fn range_bounds_to_range_wrapping<I: IndexingType>(
    rb: impl RangeBounds<I>,
    len: I,
) -> Range<I> {
    let start = match rb.start_bound() {
        std::ops::Bound::Included(i) => *i,
        std::ops::Bound::Excluded(i) => i.wrapping_add(I::one()),
        std::ops::Bound::Unbounded => I::ZERO,
    };
    let end = match rb.end_bound() {
        std::ops::Bound::Included(i) => i.wrapping_add(I::one()),
        std::ops::Bound::Excluded(i) => *i,
        std::ops::Bound::Unbounded => len,
    };
    start..end
}

pub fn range_bounds_to_range_usize<I: IndexingType>(
    rb: impl RangeBounds<I>,
    len: usize,
) -> Range<usize> {
    let start = match rb.start_bound() {
        std::ops::Bound::Included(i) => i.into_usize(),
        std::ops::Bound::Excluded(i) => i.into_usize() + 1,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match rb.end_bound() {
        std::ops::Bound::Included(i) => i.into_usize() + 1,
        std::ops::Bound::Excluded(i) => i.into_usize(),
        std::ops::Bound::Unbounded => len,
    };
    start..end
}

pub fn range_contains<I: PartialOrd>(
    range: Range<I>,
    subrange: Range<I>,
) -> bool {
    range.start <= subrange.start && range.end >= subrange.end
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

// SAFETY: this is the almighty 'cast anything into anything' function.
// This is generally only useful if T and Q are known to be the same
// at runtime, but this cannot be proven to the typechecker / might not hold in
// a dynamically unreachable branch. Use with extreme caution!
#[allow(clippy::needless_pass_by_value)]
pub unsafe fn force_cast<T, Q>(v: T) -> Q {
    unsafe { std::ptr::read(std::ptr::addr_of!(v).cast::<Q>()) }
}

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
