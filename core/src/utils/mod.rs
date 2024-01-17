use std::ops::Range;

pub mod aligned_buf;
pub mod counting_writer;
pub mod dynamic_freelist;
pub mod encoding;
pub mod escaped_writer;
pub mod identity_hasher;
pub mod indexing_type;
pub mod int_string_conversions;
pub mod io;
pub mod nonzero_ext;
pub mod offset_vec_deque;
pub mod paths_store;
pub mod plattform;
pub mod printable_unicode;
pub mod small_box;
pub mod stable_vec;
pub mod string_store;
pub mod temp_vec;
pub mod test_utils;
pub mod universe;

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

pub trait ValueProducingCallable<T> {
    fn call(&mut self) -> T;
}

impl<T, F: FnMut() -> T> ValueProducingCallable<T> for F {
    fn call(&mut self) -> T {
        self()
    }
}

pub fn get_two_distinct_mut<T>(
    slice: &mut [T],
    idx1: usize,
    idx2: usize,
) -> (&mut T, &mut T) {
    assert!(idx1 != idx2 && idx1 < slice.len() && idx2 < slice.len());
    unsafe {
        let ptr = slice.as_mut_ptr();
        (&mut *ptr.add(idx1), &mut *ptr.add(idx2))
    }
}

pub fn subslice_slice_pair<'a, T>(
    s1: &'a [T],
    s2: &'a [T],
    range: Range<usize>,
) -> (&'a [T], &'a [T]) {
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
    s1: &'a mut [T],
    s2: &'a mut [T],
    range: Range<usize>,
) -> (&'a mut [T], &'a mut [T]) {
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
