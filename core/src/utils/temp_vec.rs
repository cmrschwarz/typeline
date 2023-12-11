use std::{
    alloc::Layout,
    mem::{align_of, size_of, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

pub struct TempVec {
    ptr: NonNull<u8>,
    layout: Layout,
}
unsafe impl Send for TempVec {}
unsafe impl Sync for TempVec {}

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

// convenience wrappers
impl TempVec {
    pub fn with<T, R>(&mut self, op: impl FnOnce(&mut Vec<T>) -> R) -> R {
        let mut v = Vec::from(std::mem::take(self));
        let res = op(&mut v);
        *self = TempVec::from(v);
        res
    }
    pub fn take<T>(&mut self) -> Vec<T> {
        Vec::from(std::mem::take(self))
    }
    pub fn give<T>(&mut self, v: Vec<T>) {
        *self = TempVec::from(v);
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
    #[allow(clippy::let_unit_value)]
    let _ = LayoutCompatible::<T, U>::ASSERT_COMPATIBLE;

    v.clear();

    let mut v = std::mem::ManuallyDrop::new(v);

    let (ptr, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    unsafe { Vec::from_raw_parts(ptr as *mut U, len, cap) }
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

pub struct BorrowedVec<'a, S, T> {
    source: &'a mut Vec<S>,
    vec: Vec<T>,
}

impl<'a, S, T> BorrowedVec<'a, S, T> {
    pub fn new(origin: &'a mut Vec<S>) -> Self {
        Self {
            vec: transmute_vec(std::mem::take(origin)),
            source: origin,
        }
    }
}

impl<'a, S, T> Drop for BorrowedVec<'a, S, T> {
    fn drop(&mut self) {
        let _ = std::mem::replace(
            self.source,
            transmute_vec::<T, S>(std::mem::take(&mut self.vec)),
        );
    }
}

impl<'a, S, T> Deref for BorrowedVec<'a, S, T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.vec
    }
}

impl<'a, S, T> DerefMut for BorrowedVec<'a, S, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.vec
    }
}
