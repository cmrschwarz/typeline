use std::mem::{align_of, size_of, ManuallyDrop};

use super::index_vec::IndexVec;

pub struct LayoutCompatible<T, U>(std::marker::PhantomData<(T, U)>);
impl<T, U> LayoutCompatible<T, U> {
    pub const SIZE_EQ: bool = size_of::<T>() == size_of::<U>();
    pub const ALIGN_EQ: bool = align_of::<T>() == align_of::<U>();
    pub const COMPATIBLE: bool = Self::SIZE_EQ && Self::ALIGN_EQ;
    pub const ASSERT_COMPATIBLE: () = assert!(Self::COMPATIBLE);
}

pub trait TransmutableContainer: Default {
    type ElementType;
    type ContainerType<Q>: TransmutableContainer<
        ElementType = Q,
        ContainerType<Q> = <Self as TransmutableContainer>::ContainerType<Q>,
    >;

    fn transmute<Q>(self)
        -> <Self as TransmutableContainer>::ContainerType<Q>;
    fn transmute_from<Q>(
        src: <Self as TransmutableContainer>::ContainerType<Q>,
    ) -> Self;

    fn reclaim_temp<Q>(
        &mut self,
        temp: <Self as TransmutableContainer>::ContainerType<Q>,
    ) {
        *self = Self::transmute_from(temp);
    }
    fn reclaim_temp_take<Q>(
        &mut self,
        temp: &mut <Self as TransmutableContainer>::ContainerType<Q>,
    ) {
        *self = Self::transmute_from(std::mem::take(temp));
    }

    fn take_transmute<Q>(
        &mut self,
    ) -> <Self as TransmutableContainer>::ContainerType<Q> {
        std::mem::take(self).transmute()
    }

    fn borrow_container<Q>(&mut self) -> BorrowedContainer<Q, Self> {
        BorrowedContainer::new(self)
    }
}

#[inline]
pub fn convert_vec_cleared<T, U>(mut v: Vec<T>) -> Vec<U> {
    let same_align = align_of::<T>() == align_of::<U>();
    let space = v.capacity() * size_of::<T>();
    let capacity_new = space / size_of::<U>();
    let capacity_compatible = capacity_new * size_of::<U>() == space;

    if !(same_align && capacity_compatible) {
        return Vec::new();
    }
    // SAFETY: This clear is the reason why this function is sound.
    // That way there's never any transmutation of the values of the vec.
    v.clear();
    let mut v = ManuallyDrop::new(v);
    unsafe { Vec::from_raw_parts(v.as_mut_ptr().cast(), 0, capacity_new) }
}

#[inline]
pub fn transmute_vec<T, U>(mut v: Vec<T>) -> Vec<U> {
    #[allow(clippy::let_unit_value)]
    let () = LayoutCompatible::<T, U>::ASSERT_COMPATIBLE;

    v.clear();

    let mut v = std::mem::ManuallyDrop::new(v);

    let (ptr, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    unsafe { Vec::from_raw_parts(ptr.cast(), len, cap) }
}

#[derive(derive_more::Deref, derive_more::DerefMut)]
pub struct BorrowedContainer<'a, T, C: TransmutableContainer> {
    source: &'a mut C,
    #[deref]
    #[deref_mut]
    vec: <C as TransmutableContainer>::ContainerType<T>,
}

impl<'a, T, C: TransmutableContainer> BorrowedContainer<'a, T, C> {
    #[inline]
    pub fn new(origin: &'a mut C) -> Self {
        Self {
            vec: origin.take_transmute(),
            source: origin,
        }
    }
}

impl<'a, T, C: TransmutableContainer> Drop for BorrowedContainer<'a, T, C> {
    #[inline]
    fn drop(&mut self) {
        self.source.reclaim_temp::<T>(std::mem::take(&mut self.vec));
    }
}

impl<T> TransmutableContainer for Vec<T> {
    type ElementType = T;

    type ContainerType<Q> = Vec<Q>;

    fn transmute<Q>(
        self,
    ) -> <Self as TransmutableContainer>::ContainerType<Q> {
        transmute_vec(self)
    }

    fn transmute_from<Q>(
        src: <Self as TransmutableContainer>::ContainerType<Q>,
    ) -> Self {
        transmute_vec(src)
    }
}

impl<I, T> TransmutableContainer for IndexVec<I, T> {
    type ElementType = T;

    type ContainerType<Q> = IndexVec<I, Q>;

    fn transmute<Q>(
        self,
    ) -> <Self as TransmutableContainer>::ContainerType<Q> {
        IndexVec::from(transmute_vec(Vec::from(self)))
    }

    fn transmute_from<Q>(
        src: <Self as TransmutableContainer>::ContainerType<Q>,
    ) -> Self {
        IndexVec::from(transmute_vec(Vec::from(src)))
    }
}
