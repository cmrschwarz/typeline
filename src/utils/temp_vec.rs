use std::{alloc::Layout, ptr::NonNull};

pub struct TempVec {
    data: NonNull<u8>,
    size: u32,
    align: u8,
    capacity: usize,
}
//SAFETY: we have a unique pointer to our data (but ptr::Unique isn't stable yet...)
unsafe impl Send for TempVec {}

impl Default for TempVec {
    fn default() -> Self {
        Self {
            data: NonNull::dangling(),
            size: 0,
            align: 0,
            capacity: 0,
        }
    }
}

impl TempVec {
    fn dealloc_data(&mut self) {
        if self.capacity != 0 {
            unsafe {
                std::alloc::dealloc(
                    self.data.as_ptr(),
                    Layout::from_size_align(self.size as usize, self.align as usize).unwrap(),
                )
            };
            self.capacity = 0;
        }
    }
    pub fn get<T>(&mut self) -> Vec<T> {
        if self.capacity == 0 {
            return Vec::new();
        }
        let align = std::mem::align_of::<T>();
        let size = std::mem::size_of::<T>();
        assert!(size < u32::MAX as usize);
        let size = size as u32;
        if align != self.align as usize || size < self.size || size % self.size != 0 {
            self.dealloc_data();
            return Vec::new();
        }
        // SAFETY: the above if statements makes sure that the invariants of
        // this  are upheld
        let res = unsafe { Vec::from_raw_parts(self.data.as_ptr() as *mut T, 0, self.capacity) };
        self.capacity = 0;
        res
    }

    pub fn store<T>(&mut self, mut v: Vec<T>) {
        self.dealloc_data();
        self.capacity = v.capacity();
        self.size = std::mem::size_of::<T>().try_into().unwrap();
        self.align = std::mem::align_of::<T>().try_into().unwrap();
        self.data = unsafe { NonNull::new_unchecked(v.as_ptr() as *mut u8) };
        v.clear();
        std::mem::forget(v);
    }

    pub fn with<T, R>(&mut self, func: impl FnOnce(&mut Vec<T>) -> R) -> R {
        let mut v = self.get();
        let res = func(&mut v);
        self.store(v);
        res
    }
}
