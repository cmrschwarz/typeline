use std::{
    mem::MaybeUninit,
    panic::{RefUnwindSafe, UnwindSafe},
};

/// An alias type for `T` that
/// - has the same size and align as T
/// - has no Drop impl
/// - implementis `Send`, `Sync`, `Unpin`, 'Clone', `UnwindSafe` and
///   `RefUnwindSafe` regardless of `T`
/// - preserves the Copy semantics of `T`. (`PhantomSlot<T>` is `Copy` iff `T`
///   is `Copy`)
/// This type is mainly intended for usage with `transmute_vec`
#[repr(C)]
pub struct PhantomSlot<T> {
    // field with the size of `T`
    _size: MaybeUninit<T>,

    // zero-sized field with the alignment of `T`
    _alignment: [T; 0],
}

unsafe impl<T> Send for PhantomSlot<T> {}
unsafe impl<T> Sync for PhantomSlot<T> {}
impl<T> Unpin for PhantomSlot<T> {}
impl<T> RefUnwindSafe for PhantomSlot<T> {}
impl<T> UnwindSafe for PhantomSlot<T> {}

impl<T> Clone for PhantomSlot<T> {
    fn clone(&self) -> Self {
        Self {
            _size: MaybeUninit::uninit(),
            _alignment: [],
        }
    }
}
impl<T: Copy> Copy for PhantomSlot<T> {}
