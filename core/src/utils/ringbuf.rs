use std::{alloc::Layout, io::Write, ptr::NonNull};

use std::ops::Range;
// this is conceptually just a Vec<u8>, but
// - the data buffer is at least `ALIGN` bytes aligned
// - it has less strict ownership guarantees (the data pointer is not `Unique`)
// - no unneccessarily strict rules about `set_len` that make it technically
//   illegal to call it before initializing the data
pub struct RingBuf<const ALIGN: usize> {
    data: NonNull<u8>,
    head: usize,
    len: usize,
    cap: usize,
}

unsafe impl<const ALIGN: usize> Send for RingBuf<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for RingBuf<ALIGN> {}

impl<const ALIGN: usize> Default for RingBuf<ALIGN> {
    fn default() -> Self {
        // make sure align is a sane value
        Layout::from_size_align(1, ALIGN).unwrap();
        Self {
            data: NonNull::dangling(),
            head: 0,
            len: 0,
            cap: 0,
        }
    }
}

impl<const ALIGN: usize> Drop for RingBuf<ALIGN> {
    fn drop(&mut self) {
        if self.cap != 0 {
            unsafe {
                std::alloc::dealloc(
                    self.data.as_ptr(),
                    Layout::from_size_align_unchecked(self.cap, ALIGN),
                );
            }
        }
    }
}

impl<const ALIGN: usize> Clone for RingBuf<ALIGN> {
    fn clone(&self) -> Self {
        let mut res = Self::with_capacity(self.cap);
        let (s1, s2) = self.as_slices();
        res.extend_from_slice(s1);
        res.extend_from_slice(s2);
        res
    }
}

impl<const ALIGN: usize> RingBuf<ALIGN> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(cap: usize) -> Self {
        let mut res = Self::default();
        unsafe { res.realloc(cap) };
        res
    }
    pub unsafe fn set_len(&mut self, len: usize) {
        self.len = len;
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn capacity(&self) -> usize {
        self.cap
    }
    pub fn to_physical_idx(&self, logical_idx: usize) -> usize {
        let space_bottom = self.cap - self.head;
        if logical_idx > space_bottom {
            return logical_idx - space_bottom;
        }
        self.head + logical_idx
    }
    pub unsafe fn realloc(&mut self, cap_new: usize) {
        let cap_old = self.cap;
        self.cap = cap_new;
        let head_old = self.head;
        self.head = 0;
        if cap_new == 0 {
            if cap_old != 0 {
                unsafe {
                    std::alloc::dealloc(
                        self.data.as_ptr(),
                        Layout::from_size_align_unchecked(cap_old, ALIGN),
                    )
                };
            }
            self.data = NonNull::dangling();
            return;
        }
        if cap_old == 0 {
            self.data = unsafe {
                NonNull::new_unchecked(std::alloc::alloc(
                    Layout::from_size_align_unchecked(cap_new, ALIGN),
                ))
            };
            return;
        }
        let data_old = self.data.as_ptr();
        let (l1, l2) = self.slice_lengths();
        unsafe {
            let data_new = std::alloc::alloc(
                Layout::from_size_align_unchecked(cap_new, ALIGN),
            );
            self.data = NonNull::new_unchecked(data_new);
            std::ptr::copy_nonoverlapping(
                data_old.add(head_old),
                data_new,
                l1,
            );
            std::ptr::copy_nonoverlapping(data_old, data_new.add(l1), l2);
            std::alloc::dealloc(
                data_old,
                Layout::from_size_align_unchecked(cap_old, ALIGN),
            );
        }
    }
    fn calculate_new_cap(&self, cap_needed: usize) -> usize {
        let mut cap_new = self.cap.max(ALIGN).max(16);
        while cap_new < cap_needed {
            cap_new <<= 1;
        }
        cap_new
    }
    pub fn reserve(&mut self, additional_cap: usize) {
        let cap_needed = self.len + additional_cap;
        if self.cap >= cap_needed {
            return;
        }
        let cap_new = self.calculate_new_cap(cap_needed);
        unsafe { self.realloc(cap_new) };
    }
    pub fn reserve_contiguous(&mut self, additional_cap: usize) {
        let space_bottom = (self.cap - self.head).saturating_sub(self.len);
        if space_bottom > additional_cap {
            return;
        }
        let cap_needed = self.cap + additional_cap;
        if cap_needed > self.cap {
            let cap_new = self.calculate_new_cap(self.cap + additional_cap);
            unsafe { self.realloc(cap_new) };
            return;
        }
        if space_bottom == 0 {
            return;
        }
        let ptr = self.data.as_ptr();
        unsafe { std::ptr::copy(ptr.add(self.head), ptr, self.len) };
        self.head = 0;
    }
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.reserve_contiguous(slice.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.tail_ptr_mut(),
                slice.len(),
            )
        };
        self.len += slice.len();
    }
    pub fn push(&mut self, v: u8) {
        self.reserve(1);
        unsafe {
            *self.tail_ptr_mut() = v;
        }
        self.len += 1;
    }
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }
    pub fn resize(&mut self, new_len: usize, value: u8) {
        if new_len > self.len {
            self.reserve(new_len - self.len);
            unsafe {
                std::ptr::write_bytes(
                    self.tail_ptr_mut(),
                    value,
                    new_len - self.len,
                )
            }
        }
        self.len = new_len;
    }
    pub fn clear(&mut self) {
        self.len = 0;
        self.head = 0;
    }
    pub fn slice_lengths(&self) -> (usize, usize) {
        let len_s1 = (self.cap - self.head).min(self.len);
        let len_s2 = self.len - len_s1;
        (len_s1, len_s2)
    }
    pub fn slice_ranges(&self) -> (Range<usize>, Range<usize>) {
        let (l1, l2) = self.slice_lengths();
        (self.head..self.head + l1, 0..l2)
    }
    pub fn as_slices(&self) -> (&[u8], &[u8]) {
        let ptr = self.data.as_ptr();
        let (l1, l2) = self.slice_lengths();
        unsafe {
            (
                std::slice::from_raw_parts(ptr.add(self.head), l1),
                std::slice::from_raw_parts(ptr, l2),
            )
        }
    }
    pub fn as_slices_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        let ptr = self.data.as_ptr();
        let (l1, l2) = self.slice_lengths();
        unsafe {
            (
                std::slice::from_raw_parts_mut(ptr.add(self.head), l1),
                std::slice::from_raw_parts_mut(ptr, l2),
            )
        }
    }
    pub fn head_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.head) }
    }
    pub fn head_ptr_mut(&mut self) -> *mut u8 {
        unsafe { self.data.as_ptr().add(self.head) }
    }
    pub fn tail_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.to_physical_idx(self.len)) }
    }
    pub fn tail_ptr_mut(&mut self) -> *mut u8 {
        unsafe { self.data.as_ptr().add(self.to_physical_idx(self.len)) }
    }
}

impl<const ALIGN: usize> Write for RingBuf<ALIGN> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
}
