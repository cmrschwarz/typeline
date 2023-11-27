use core::{
    cmp::Ordering, fmt, hash::Hash, marker::PhantomData, mem::MaybeUninit,
    ops, ptr,
};
#[cfg(feature = "unstable")]
use core::{marker::Unsize, ops::CoerceUnsized};
use std::{
    alloc::Layout,
    fmt::{Debug, Display, Formatter},
    hash::Hasher,
    mem::{align_of, align_of_val, size_of, size_of_val, ManuallyDrop},
};

#[cfg(feature = "unstable")]
impl<T: ?Sized + Unsize<U>, U: ?Sized, const SPACE: usize>
    CoerceUnsized<SmallBox<U, SPACE>> for SmallBox<T, SPACE>
{
}

#[macro_export]
macro_rules! smallbox {
    ( $e: expr ) => {{
        let value = $e;
        let target_type_ptr = &value as *const _;
        #[allow(unsafe_code)]
        unsafe {
            let res = $crate::utils::small_box::SmallBox::new_unchecked(
                &value,
                target_type_ptr,
            );
            let _ = std::mem::ManuallyDrop::new(value);
            res
        }
    }};
}

/// A small size optimized box

#[cfg(feature = "generic_const_exprs")]
type SufficientSmallBox<T: ?Sized, U> = SmallBox<T, { size_of::<U>() }>;

#[repr(C)]
pub struct SmallBox<T: ?Sized, const SPACE: usize> {
    ptr: *mut T,
    data: [MaybeUninit<u8>; SPACE],
    _phantom: PhantomData<T>,
}

impl<T: ?Sized, const SPACE: usize> SmallBox<T, SPACE> {
    pub fn new(val: T) -> Self
    where
        T: Sized,
    {
        smallbox!(val)
    }

    pub fn resize<const NEW_SPACE: usize>(self) -> SmallBox<T, NEW_SPACE> {
        let this = ManuallyDrop::new(self);

        let val_ref = &*this;
        let size = size_of_val(val_ref);
        let align = align_of_val(val_ref);

        if this.is_heap_allocated()
            && (size > NEW_SPACE
                || align > SmallBox::<T, NEW_SPACE>::data_align())
        {
            return SmallBox {
                ptr: this.ptr,
                data: [MaybeUninit::uninit(); NEW_SPACE],
                _phantom: PhantomData,
            };
        }

        unsafe {
            let res = SmallBox::<T, NEW_SPACE>::new_unchecked(
                this.as_ptr(),
                this.ptr,
            );
            if this.is_heap_allocated() {
                std::alloc::dealloc(
                    this.ptr as *mut u8,
                    Layout::for_value(val_ref),
                );
            }
            res
        }
    }

    pub fn is_stack_allocated(&self) -> bool {
        self.ptr.is_null()
    }
    pub fn is_heap_allocated(&self) -> bool {
        !self.is_stack_allocated()
    }

    pub fn data_align() -> usize {
        (1 << size_of::<*mut T>().trailing_zeros()).min(align_of::<Self>())
    }

    pub unsafe fn new_unchecked<U>(
        val: *const U,
        // only needed for the metadata of the fat pointer of the target type
        target_type_ptr: *const T,
    ) -> Self
    where
        U: ?Sized,
    {
        let val_ref = unsafe { &*val };
        let size = size_of_val(val_ref);
        let align = align_of_val(val_ref);

        let mut res = SmallBox {
            ptr: target_type_ptr as *mut T,
            data: [MaybeUninit::uninit(); SPACE],
            _phantom: PhantomData,
        };

        let (ptr_address_value, data_ptr) =
            if size <= SPACE && align <= Self::data_align() {
                // this covers ZSTs aswell
                (ptr::null_mut(), res.data.as_ptr() as *mut u8)
            } else {
                let layout = Layout::for_value::<U>(unsafe { &*val });
                let heap_ptr = unsafe { std::alloc::alloc(layout) };
                (heap_ptr, heap_ptr)
            };

        unsafe {
            *(&mut res.ptr as *mut *mut T as *mut *mut u8) = ptr_address_value;
            ptr::copy_nonoverlapping(val as *const u8, data_ptr, size);
        }
        res
    }

    fn as_ptr(&self) -> *const T {
        if self.is_heap_allocated() {
            return self.ptr;
        }
        // Overwrite the pointer but retain metadata in case of a fat pointer.
        let mut ptr = self.ptr;
        unsafe {
            *(&mut ptr as *mut *mut T as *mut *mut u8) =
                self.data.as_ptr() as *mut u8;
        }
        ptr
    }

    fn as_mut_ptr(&mut self) -> *mut T {
        self.as_ptr() as *mut T
    }

    pub fn into_inner(self) -> T
    where
        T: Sized,
    {
        let inner: T = unsafe { self.as_ptr().read() };

        // deallocates, but doesn't drop the boxed value
        if self.is_heap_allocated() {
            let layout = Layout::new::<T>();
            unsafe {
                std::alloc::dealloc(self.ptr as *mut u8, layout);
            }
        }
        std::mem::forget(self);

        inner
    }
}

impl<T: ?Sized, const SPACE: usize> ops::Deref for SmallBox<T, SPACE> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.as_ptr() }
    }
}

impl<T: ?Sized, const SPACE: usize> ops::DerefMut for SmallBox<T, SPACE> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.as_mut_ptr() }
    }
}

impl<T: ?Sized, const SPACE: usize> ops::Drop for SmallBox<T, SPACE> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.as_mut_ptr());
            if self.is_heap_allocated() {
                std::alloc::dealloc(
                    self.ptr as *mut u8,
                    Layout::for_value::<T>(&*self),
                );
            }
        }
    }
}

impl<T: Clone, const SPACE: usize> Clone for SmallBox<T, SPACE>
where
    T: Sized,
{
    fn clone(&self) -> Self {
        SmallBox::new((**self).clone())
    }
}

impl<T: ?Sized + Display, const SPACE: usize> Display for SmallBox<T, SPACE> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + Debug, const SPACE: usize> Debug for SmallBox<T, SPACE> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized, const SPACE: usize> fmt::Pointer for SmallBox<T, SPACE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.as_ptr(), f)
    }
}

impl<T: ?Sized + PartialEq, const SPACE: usize> PartialEq
    for SmallBox<T, SPACE>
{
    fn eq(&self, other: &SmallBox<T, SPACE>) -> bool {
        PartialEq::eq(&**self, &**other)
    }
}

impl<T: ?Sized + PartialOrd, const SPACE: usize> PartialOrd
    for SmallBox<T, SPACE>
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PartialOrd::partial_cmp(&**self, &**other)
    }
    fn lt(&self, other: &Self) -> bool {
        PartialOrd::lt(&**self, &**other)
    }
    fn le(&self, other: &Self) -> bool {
        PartialOrd::le(&**self, &**other)
    }
    fn ge(&self, other: &Self) -> bool {
        PartialOrd::ge(&**self, &**other)
    }
    fn gt(&self, other: &Self) -> bool {
        PartialOrd::gt(&**self, &**other)
    }
}

impl<T: ?Sized + Ord, const SPACE: usize> Ord for SmallBox<T, SPACE> {
    fn cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(&**self, &**other)
    }
}

impl<T: ?Sized + Eq, const SPACE: usize> Eq for SmallBox<T, SPACE> {}

impl<T: ?Sized + Hash, const SPACE: usize> Hash for SmallBox<T, SPACE> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

unsafe impl<T: ?Sized + Send, const SPACE: usize> Send for SmallBox<T, SPACE> {}
unsafe impl<T: ?Sized + Sync, const SPACE: usize> Sync for SmallBox<T, SPACE> {}

#[cfg(test)]
mod tests {
    use super::SmallBox;
    use core::cell::Cell;
    use std::mem::size_of;

    #[test]
    fn test_basic() {
        let sb1: SmallBox<usize, { size_of::<usize>() }> = SmallBox::new(42);
        assert_eq!(*sb1, 42);
        assert!(sb1.is_stack_allocated());

        let sb2: SmallBox<(usize, usize), 3> = SmallBox::new((1, 2));
        assert_eq!(*sb2, (1, 2));
        assert!(sb2.is_heap_allocated());
    }

    #[test]
    fn test_new_unchecked() {
        let val: [u64; 2] = [1, 2];

        unsafe {
            let val_ptr = &val as *const _;
            let sb: SmallBox<[u64], 16> =
                SmallBox::new_unchecked(val_ptr, val_ptr);
            assert_eq!(*sb, [1, 2]);
            assert!(sb.is_stack_allocated());
        }
    }

    #[test]
    #[deny(unsafe_code)]
    fn test_macro() {
        let sb1: SmallBox<u32, 8> = smallbox!(1234u32);
        assert_eq!(*sb1, 1234);
        assert!(sb1.is_stack_allocated());

        let sb2: SmallBox<u64, 7> = smallbox!(1234u64);
        assert_eq!(*sb2, 1234);
        assert!(sb2.is_heap_allocated());

        let is_thirteen: SmallBox<dyn Fn(u8) -> bool, 64> =
            smallbox!(|num: u8| num == 13);
        assert!(is_thirteen.is_stack_allocated());
        assert!(is_thirteen(13));
        assert!(!is_thirteen(12));

        let capture = [1; 42];
        let summize: SmallBox<dyn Fn(usize) -> usize, 0> =
            smallbox!(|v: usize| capture.iter().sum::<usize>() + v);
        // probably heap allocated, but we don't want to depend on the optimizer
        assert_eq!(summize(7), 49);
    }

    #[test]
    #[cfg(feature = "unstable")]
    fn test_coerce() {
        let is_thirteen: SmallBox<dyn Fn(u8) -> bool, 16> =
            SmallBox::new(|num: u8| num == 13);
        assert!(is_thirteen(13));
        assert!(!is_thirteen(12));

        // currently crashes the rust compiler
        //  let capture = [1; 42];
        //  let summize: SmallBox<dyn Fn(u32) -> u32, 0> =
        //      SmallBox::new(|v: u32| capture.iter().sum::<u32>() + v);
        //  assert_eq!(summize(7), 49);
    }

    #[test]
    fn test_drop() {
        struct DropTest<'a>(&'a Cell<bool>, u8);
        impl<'a> Drop for DropTest<'a> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let flag = Cell::new(false);
        let sb1: SmallBox<_, { size_of::<DropTest>() }> =
            SmallBox::new(DropTest(&flag, 0));
        assert!(sb1.is_stack_allocated());
        assert!(!flag.get());
        drop(sb1);
        assert!(flag.get());

        let flag = Cell::new(false);
        let sb2: SmallBox<_, 0> = SmallBox::new(DropTest(&flag, 0));
        assert!(sb2.is_heap_allocated());
        assert!(!flag.get());
        drop(sb2);
        assert!(flag.get());
    }

    #[test]
    fn test_resize() {
        let sb1 = SmallBox::<[u64; 2], 16>::new([1, 2]);
        assert!(sb1.is_stack_allocated());
        let sb2 = sb1.resize::<8>();
        assert!(sb2.is_heap_allocated());
        let sb3 = sb2.resize::<32>();
        assert!(sb3.is_stack_allocated());
    }

    #[test]
    fn test_clone() {
        let sb1: SmallBox<[u64; 2], 16> = smallbox!([1u64, 2]);
        assert!(sb1.is_stack_allocated());
        assert_eq!(sb1, sb1.clone());

        let sb2: SmallBox<[u64; 3], 5> = smallbox!([1u64, 2, 3]);
        assert!(sb2.is_heap_allocated());
        assert_eq!(sb2, sb2.clone());
    }

    #[test]
    fn test_dst() {
        let dst1: SmallBox<[i32], 8> = smallbox!([1, 2]);
        assert_eq!(*dst1, [1, 2]);
        assert!(dst1.is_stack_allocated());

        let dst2: SmallBox<[i32], 3> = smallbox!([1, 2]);
        assert_eq!(*dst2, [1, 2]);
        assert!(dst2.is_heap_allocated());

        let dst_size_0: SmallBox<[u8], 0> = smallbox!([42u8; 0]);
        assert_eq!(*dst_size_0, []);
        assert!(dst_size_0.is_stack_allocated());

        let zst: SmallBox<(), 0> = smallbox!(());
        assert!(zst.is_stack_allocated());
    }

    #[test]
    fn test_into_inner() {
        let sb1: SmallBox<_, 8> = SmallBox::new(21u8);
        assert!(sb1.is_stack_allocated());
        assert_eq!(sb1.into_inner(), 21);

        let sb2: SmallBox<_, 0> = SmallBox::new(vec![1, 2, 3]);
        assert!(sb2.is_heap_allocated());
        assert_eq!(sb2.into_inner(), vec![1, 2, 3]);
    }
}
