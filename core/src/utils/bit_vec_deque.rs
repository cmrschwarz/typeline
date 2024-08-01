use std::{
    alloc::Layout,
    fmt::{Debug, Write},
    ops::Index,
    ptr::NonNull,
};

use bitvec::{
    order::{BitOrder, LocalBits},
    slice::BitSlice,
    vec::BitVec,
};

pub struct BitVecDeque {
    data: NonNull<usize>,
    // all of these are in bits
    head: usize,
    len: usize,
    cap: usize,
}

unsafe impl Send for BitVecDeque {}

impl Default for BitVecDeque {
    fn default() -> Self {
        Self::new()
    }
}

const BITS_PER_WORD: usize = usize::BITS as usize;

impl BitVecDeque {
    pub const fn new() -> Self {
        Self {
            data: NonNull::dangling(),
            head: 0,
            len: 0,
            cap: 0,
        }
    }
    pub fn index_to_phys(&self, idx: usize) -> usize {
        let head_space = self.cap - self.head;
        if idx > head_space {
            idx - head_space
        } else {
            self.head + idx
        }
    }
    pub unsafe fn get_phys_word_ref_unchecked(
        &self,
        idx_phys: usize,
    ) -> &usize {
        unsafe { &*self.data.as_ptr().add(idx_phys / BITS_PER_WORD) }
    }
    pub unsafe fn get_phys_word_ref_unchecked_mut(
        &mut self,
        idx_phys: usize,
    ) -> &mut usize {
        unsafe { &mut *self.data.as_ptr().add(idx_phys / BITS_PER_WORD) }
    }
    unsafe fn get_unchecked_idx_phys(&self, idx_phys: usize) -> bool {
        let word = *unsafe { self.get_phys_word_ref_unchecked(idx_phys) };
        let offset = idx_phys % BITS_PER_WORD;
        (word >> (offset)) & 1 == 1
    }
    pub unsafe fn set_unchecked_idx_phys(
        &mut self,
        idx_phys: usize,
        value: bool,
    ) {
        let offset = idx_phys % BITS_PER_WORD;
        let mask = usize::MAX ^ (1 << offset);
        let word_ref =
            unsafe { self.get_phys_word_ref_unchecked_mut(idx_phys) };
        *word_ref = (*word_ref & mask) | (usize::from(value) << offset);
    }
    pub unsafe fn get_unchecked(&self, idx: usize) -> bool {
        let idx_phys = self.index_to_phys(idx);
        unsafe { self.get_unchecked_idx_phys(idx_phys) }
    }
    pub unsafe fn set_unchecked(&mut self, idx: usize, value: bool) {
        let idx_phys = self.index_to_phys(idx);
        unsafe { self.set_unchecked_idx_phys(idx_phys, value) }
    }
    pub unsafe fn set1_unchecked(&mut self, idx: usize) {
        let idx_phys = self.index_to_phys(idx);
        let offset = idx_phys % BITS_PER_WORD;
        let word_ref =
            unsafe { self.get_phys_word_ref_unchecked_mut(idx_phys) };
        *word_ref |= 1 << offset;
    }
    pub unsafe fn set0_unchecked(&mut self, idx: usize) {
        let idx_phys = self.index_to_phys(idx);
        let offset = idx_phys % BITS_PER_WORD;
        let mask = usize::MAX ^ (1 << offset);
        let word_ref =
            unsafe { self.get_phys_word_ref_unchecked_mut(idx_phys) };
        *word_ref &= mask;
    }
    pub unsafe fn toggle_unchecked(&mut self, idx: usize) {
        let idx_phys = self.index_to_phys(idx);
        let offset = idx_phys % BITS_PER_WORD;
        let word_ref =
            unsafe { self.get_phys_word_ref_unchecked_mut(idx_phys) };
        *word_ref ^= 1 << offset;
    }
    fn get_slices_len_bits(&self) -> (usize, usize) {
        let l1 = (self.cap - self.head).min(self.len);
        let l2 = self.len - l1;
        (l1, l2)
    }
    fn get_slices_len_words(&self) -> (usize, usize) {
        let (l1, l2) = self.get_slices_len_bits();
        (l1.div_ceil(BITS_PER_WORD), l2.div_ceil(BITS_PER_WORD))
    }
    pub unsafe fn realloc(&mut self, cap_new: usize) {
        debug_assert!(cap_new % BITS_PER_WORD == 0);
        debug_assert!(cap_new >= self.len + BITS_PER_WORD);
        let word_cap_old = self.cap / BITS_PER_WORD;
        let word_cap_new = cap_new / BITS_PER_WORD;
        let layout_old =
            unsafe { Layout::array::<usize>(word_cap_old).unwrap_unchecked() };
        let cap_old = self.cap;
        self.cap = cap_new;
        let head_old = self.head;
        self.head %= BITS_PER_WORD;
        if cap_new == 0 {
            if cap_old != 0 {
                unsafe {
                    std::alloc::dealloc(self.data.as_ptr().cast(), layout_old)
                };
            }
            self.data = NonNull::dangling();
            return;
        }

        let data_old = self.data.as_ptr();

        // this could potentially overflow on 32 bit, so we keep the checked
        // unwrap
        let layout_new = Layout::array::<usize>(word_cap_new).unwrap();

        #[allow(clippy::cast_ptr_alignment)]
        let data_new =
            unsafe { std::alloc::alloc(layout_new) }.cast::<usize>();
        self.data = unsafe { NonNull::new_unchecked(data_new) };

        if cap_old == 0 {
            return;
        }

        let (l1, l2) = self.get_slices_len_words();
        unsafe {
            let start_old = data_old.add(head_old);
            std::ptr::copy_nonoverlapping(start_old, data_new, l1);
            std::ptr::copy_nonoverlapping(data_old, data_new.add(l1), l2);
            std::alloc::dealloc(data_old.cast(), layout_old);
        }
    }
    pub fn grow(&mut self, target_cap: usize) {
        assert!(target_cap > self.cap);
        let mut cap_new = (self.cap * 2).max(1);
        if cap_new < target_cap {
            cap_new <<= target_cap.leading_zeros() - cap_new.leading_zeros();
        }
        unsafe { self.realloc(cap_new.next_multiple_of(BITS_PER_WORD)) };
    }
    pub fn reserve(&mut self, additional_cap: usize) {
        let target_cap = self.len + additional_cap;
        if self.cap >= target_cap {
            return;
        }
        self.grow(target_cap)
    }
    pub fn push_front(&mut self, value: bool) {
        self.reserve(1);
        if self.head > 0 {
            self.head -= 1;
        } else {
            self.head = self.cap - 1;
        }
        self.len += 1;
        unsafe { self.set_unchecked_idx_phys(self.head, value) };
    }
    pub fn pop_front(&mut self) -> bool {
        assert!(!self.is_empty());
        let res = unsafe { self.get_unchecked_idx_phys(self.head) };
        self.head += 1;
        if self.head == self.cap {
            self.head = 0;
        }
        self.len -= 1;
        res
    }
    pub fn push_back(&mut self, value: bool) {
        self.reserve(1);
        unsafe { self.set_unchecked_idx_phys(self.len, value) };
        self.len += 1;
    }
    pub fn pop_back(&mut self) -> bool {
        assert!(self.len > 0);
        self.len -= 1;
        unsafe { self.get_unchecked(self.len) }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn capacity(&self) -> usize {
        self.cap
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn get(&self, idx: usize) -> Option<bool> {
        if idx < self.len {
            Some(unsafe { self.get_unchecked(idx) })
        } else {
            None
        }
    }
    pub fn set(&mut self, idx: usize, value: bool) {
        assert!(idx < self.len);
        unsafe { self.set_unchecked(idx, value) }
    }
    pub fn slices(
        &self,
    ) -> (&BitSlice<usize, LocalBits>, &BitSlice<usize, LocalBits>) {
        let (l1, l2) = self.get_slices_len_bits();
        unsafe {
            let s1 = std::slice::from_raw_parts(
                self.data.as_ptr().add(self.head / BITS_PER_WORD),
                l1.div_ceil(BITS_PER_WORD),
            );
            let s2 = std::slice::from_raw_parts(
                self.data.as_ptr(),
                l2.div_ceil(BITS_PER_WORD),
            );
            let bs1 = &BitSlice::from_slice(s1)[self.head % BITS_PER_WORD..l1];
            let bs2 = &BitSlice::from_slice(s2)[..l2];
            (bs1, bs2)
        }
    }
    pub fn slices_mut(
        &mut self,
    ) -> (
        &mut BitSlice<usize, LocalBits>,
        &mut BitSlice<usize, LocalBits>,
    ) {
        let (l1, l2) = self.get_slices_len_bits();
        unsafe {
            let s1 = std::slice::from_raw_parts_mut(
                self.data.as_ptr().add(self.head / BITS_PER_WORD),
                l1.div_ceil(BITS_PER_WORD),
            );
            let s2 = std::slice::from_raw_parts_mut(
                self.data.as_ptr(),
                l2.div_ceil(BITS_PER_WORD),
            );
            let bs1 = &mut BitSlice::from_slice_mut(s1)
                [self.head % BITS_PER_WORD..l1];
            let bs2 = &mut BitSlice::from_slice_mut(s2)[..l2];
            (bs1, bs2)
        }
    }

    pub fn from_bitvec_with_endianness<O: BitOrder>(
        v: BitVec<usize, O>,
        swap_endianness: bool,
    ) -> Self {
        let len = v.len();
        let mut vec = v.into_vec();

        if swap_endianness {
            for word in &mut vec {
                *word = word.reverse_bits();
            }
        }
        let cap = vec.capacity() * BITS_PER_WORD;
        let ptr = vec.as_mut_ptr();
        std::mem::forget(vec);

        Self {
            data: unsafe { NonNull::new_unchecked(ptr) },
            head: 0,
            len,
            cap,
        }
    }

    pub fn into_bitvec_with_endianness<O: BitOrder>(
        v: BitVec<usize, O>,
        swap_endianness: bool,
    ) -> Self {
        let len = v.len();
        let mut vec = v.into_vec();

        if swap_endianness {
            for word in &mut vec {
                *word = word.reverse_bits();
            }
        }
        let cap = vec.capacity() * BITS_PER_WORD;
        let ptr = vec.as_mut_ptr();
        std::mem::forget(vec);

        Self {
            data: unsafe { NonNull::new_unchecked(ptr) },
            head: 0,
            len,
            cap,
        }
    }
    pub fn iter(&self) -> Iter {
        let (s1, s2) = self.slices();
        Iter {
            i1: s1.into_iter(),
            i2: s2.into_iter(),
        }
    }

    pub fn drop_front(&mut self, count: usize) {
        assert!(self.len >= count);
        let headspace = self.cap - self.head;
        if headspace > count {
            self.head += count;
        } else {
            self.head = count - headspace;
        }
    }
    pub fn drop_back(&mut self, count: usize) {
        assert!(self.len >= count);
        self.len -= count;
    }

    pub fn extend(&mut self, it: impl Iterator<Item = bool>) {
        // TODO: implement this efficiently
        for v in it {
            self.push_back(v);
        }
    }
}

impl<O: BitOrder> From<BitVec<usize, O>> for BitVecDeque {
    fn from(value: BitVec<usize, O>) -> Self {
        #[cfg(not(any(target_endian = "big", target_endian = "little")))]
        compile_error!("unsupported arch");

        Self::from_bitvec_with_endianness(value, cfg!(target_endian = "big"))
    }
}

#[derive(Clone)]
pub struct Iter<'a> {
    i1: bitvec::slice::Iter<'a, usize, LocalBits>,
    i2: bitvec::slice::Iter<'a, usize, LocalBits>,
}

impl<'a> ExactSizeIterator for Iter<'a> {
    fn len(&self) -> usize {
        self.i1.len() + self.i2.len()
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.i2.next_back() {
            return Some(*v);
        }
        self.i1.next_back().map(|v| *v)
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.i1.next() {
            return Some(*v);
        }
        std::mem::swap(&mut self.i1, &mut self.i2);
        self.i1.next().map(|v| *v)
    }

    fn nth(&mut self, count: usize) -> Option<Self::Item> {
        let i1_len = self.i1.len();
        if i1_len > count {
            return self.i1.nth(count).map(|v| *v);
        }
        std::mem::swap(&mut self.i1, &mut self.i2);
        self.i1.nth(count - i1_len).map(|v| *v)
    }
}

impl<'a> IntoIterator for &'a BitVecDeque {
    type Item = bool;

    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Index<usize> for BitVecDeque {
    type Output = bool;

    fn index(&self, index: usize) -> &Self::Output {
        if self
            .get(index)
            .expect("bitvecdeque index must be in bounds")
        {
            &true
        } else {
            &false
        }
    }
}

impl Drop for BitVecDeque {
    fn drop(&mut self) {
        if self.cap == 0 {
            return;
        }
        unsafe {
            std::alloc::dealloc(
                self.data.as_ptr().cast(),
                Layout::array::<usize>(self.cap / BITS_PER_WORD)
                    .unwrap_unchecked(),
            );
        }
    }
}

impl PartialEq for BitVecDeque {
    fn eq(&self, other: &Self) -> bool {
        self.slices() == other.slices()
    }
}

impl Debug for BitVecDeque {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        for (i, b) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_char(if b { '1' } else { '0' })?;
        }
        f.write_char(']')?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    // the bitvec macro is unfortunately non hygenic...
    #[allow(unused)]
    use bitvec::order::{Lsb0, Msb0};

    use bitvec::bitvec;

    use super::BitVecDeque;

    #[test]
    fn push_pop_back() {
        let mut foo = BitVecDeque::new();
        const COUNT: usize = 100;
        for i in 0..COUNT {
            foo.push_back(i % 2 == 0);
        }
        for i in (0..COUNT).rev() {
            assert_eq!(foo.pop_back(), i % 2 == 0);
        }
        assert!(foo.is_empty());
    }

    #[test]
    fn push_pop_front() {
        let mut foo = BitVecDeque::new();
        const COUNT: usize = 100;
        for i in 0..COUNT {
            foo.push_front(i % 2 == 0);
        }
        for i in (0..COUNT).rev() {
            assert_eq!(foo.pop_front(), i % 2 == 0);
        }
        assert!(foo.is_empty());
    }

    #[test]
    fn get() {
        let foo = BitVecDeque::from(bitvec![0, 0, 0, 1, 1, 0]);
        assert_eq!(
            &(0..6).map(|i| usize::from(foo[i])).collect::<Vec<_>>(),
            &[0, 0, 0, 1, 1, 0]
        );
    }

    #[test]
    fn get_with_shift() {
        let mut foo = BitVecDeque::from(bitvec![0, 0, 0, 1, 1, 0]);
        foo.pop_front();
        assert_eq!(
            &(0..5).map(|i| usize::from(foo[i])).collect::<Vec<_>>(),
            &[0, 0, 1, 1, 0]
        );
    }
}
