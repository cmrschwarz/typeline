use std::{
    alloc::Layout,
    io::Write,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

pub struct AlignedBuf<const ALIGN: usize> {
    data: NonNull<u8>,
    len: usize,
    cap: usize,
}

impl<const ALIGN: usize> Default for AlignedBuf<ALIGN> {
    fn default() -> Self {
        // make sure align is a sane value
        Layout::from_size_align(1, ALIGN).unwrap();
        Self {
            data: NonNull::dangling(),
            len: Default::default(),
            cap: Default::default(),
        }
    }
}

impl<const ALIGN: usize> Drop for AlignedBuf<ALIGN> {
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

impl<const ALIGN: usize> AlignedBuf<ALIGN> {
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
    pub unsafe fn realloc(&mut self, cap_new: usize) {
        self.data = unsafe {
            if cap_new == 0 {
                if self.cap != 0 {
                    std::alloc::dealloc(
                        self.data.as_ptr(),
                        Layout::from_size_align_unchecked(self.cap, ALIGN),
                    );
                }
                NonNull::dangling()
            } else if self.cap == 0 {
                NonNull::new_unchecked(std::alloc::alloc(
                    Layout::from_size_align_unchecked(cap_new, ALIGN),
                ))
            } else {
                NonNull::new_unchecked(std::alloc::realloc(
                    self.data.as_ptr(),
                    Layout::from_size_align_unchecked(self.cap, ALIGN),
                    cap_new,
                ))
            }
        };
        self.cap = cap_new;
    }

    pub fn reserve(&mut self, additional_cap: usize) {
        let space_needed = self.len + additional_cap;
        if self.cap >= space_needed {
            return;
        }
        let mut cap_new = self.cap.max(1);
        while cap_new < space_needed {
            cap_new <<= 1;
        }
        unsafe { self.realloc(cap_new) }
    }
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.reserve(slice.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.data.as_ptr().add(self.len),
                slice.len(),
            )
        };
        self.len += slice.len();
    }
    pub fn push(&mut self, v: u8) {
        self.reserve(1);
        unsafe {
            *self.data.as_ptr().add(self.len) = v;
        }
        self.len += 1;
    }
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len)
    }
    pub fn resize(&mut self, new_len: usize, value: u8) {
        if new_len > self.len {
            self.reserve(new_len - self.len);
            unsafe {
                std::ptr::write_bytes(
                    self.data.as_ptr().add(self.len),
                    value,
                    new_len - self.len,
                )
            }
        }
        self.len = new_len
    }
    pub fn clear(&mut self) {
        self.len = 0
    }
}

impl<const ALIGN: usize> Deref for AlignedBuf<ALIGN> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

impl<const ALIGN: usize> DerefMut for AlignedBuf<ALIGN> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
    }
}

impl<const ALIGN: usize> Write for AlignedBuf<ALIGN> {
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

unsafe impl<const ALIGN: usize> Send for AlignedBuf<ALIGN> {}
