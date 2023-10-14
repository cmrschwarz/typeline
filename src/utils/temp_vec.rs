use std::{
    alloc::Layout,
    mem::{align_of, size_of, ManuallyDrop},
    ptr::NonNull,
};

pub struct TempVec {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl<T> From<Vec<T>> for TempVec {
    fn from(v: Vec<T>) -> Self {
        let mut v = ManuallyDrop::new(v);
        v.clear();
        unsafe {
            TempVec {
                ptr: NonNull::new_unchecked(v.as_mut_ptr() as *mut u8),
                layout: Layout::array::<T>(v.capacity()).unwrap_unchecked(),
            }
        }
    }
}

impl<T> From<TempVec> for Vec<T> {
    fn from(v: TempVec) -> Self {
        let size = size_of::<T>();
        let capacity = if size == 0 { 0 } else { v.layout.size() / size };
        if Layout::array::<T>(capacity) != Ok(v.layout) {
            return Vec::default();
        }
        let v = ManuallyDrop::new(v);
        unsafe { Vec::from_raw_parts(v.ptr.as_ptr() as *mut T, 0, capacity) }
    }
}

impl Default for TempVec {
    fn default() -> Self {
        Self {
            ptr: NonNull::dangling(),
            layout: Layout::array::<()>(0).unwrap(),
        }
    }
}

impl Drop for TempVec {
    fn drop(&mut self) {
        if self.layout.size() == 0 {
            // we don't want to deallocate ZSTs or NonNull::dangling()
            return;
        }
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

impl TempVec {
    // convenience function for using TempVec as a Vec<T>
    pub fn with<T, R>(&mut self, op: impl FnOnce(&mut Vec<T>) -> R) -> R {
        let mut v = Vec::from(std::mem::take(self));
        let res = op(&mut v);
        *self = TempVec::from(v);
        res
    }
}

struct LayoutCompatible<T, U>(std::marker::PhantomData<(T, U)>);

impl<T, U> LayoutCompatible<T, U> {
    const SIZE_EQ: bool = size_of::<T>() == size_of::<U>();
    const ALIGN_EQ: bool = align_of::<T>() == align_of::<U>();
    const COMPATIBLE: bool = Self::SIZE_EQ && Self::ALIGN_EQ;
    const ASSERT_COMPATIBLE: () = assert!(Self::COMPATIBLE);
}

pub fn transmute_vec<T, U>(mut v: Vec<T>) -> Vec<U> {
    let _ = LayoutCompatible::<T, U>::ASSERT_COMPATIBLE;

    v.clear();

    let mut v = std::mem::ManuallyDrop::new(v);

    let (ptr, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    unsafe { Vec::from_raw_parts(std::mem::transmute(ptr), len, cap) }
}

pub fn temp_vec<T, U, R>(
    v: &mut Vec<T>,
    f: impl FnOnce(&mut Vec<U>) -> R,
) -> R {
    let mut tv = transmute_vec(std::mem::take(v));
    let res = f(&mut tv);
    *v = transmute_vec(tv);
    res
}
