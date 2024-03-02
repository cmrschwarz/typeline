#![allow(clippy::inline_always)]

use super::debuggable_nonmax::{DebuggableNonMaxU32, DebuggableNonMaxUsize};

pub trait IndexingType:
    Clone
    + Copy
    + Default
    + IndexingTypeFromUsize
    + IndexingTypeIntoUsize
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
{
}
impl<I> IndexingType for I where
    I: Clone
        + Copy
        + Default
        + IndexingTypeFromUsize
        + IndexingTypeIntoUsize
        + Ord
{
}

// We can't use From/Into because this crate
// neither created the trait (e.g. From) nor the type (e.g. u32).
// We also can't provide a blanket impl for types implementing From<usize>,
// because when adding a manual implementation, we would get an error
// "upstream crate might implement From in a future version".
// Therefore we just manually implement this for all indexing types we
// want to use.
pub trait IndexingTypeIntoUsize {
    fn into_usize(self) -> usize;
}
pub trait IndexingTypeFromUsize: Sized {
    fn from_usize(v: usize) -> Self;
    fn from_isize(v: isize) -> Self {
        Self::from_usize(usize::try_from(v).unwrap())
    }
}

// usize
impl IndexingTypeIntoUsize for usize {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self
    }
}
impl IndexingTypeFromUsize for usize {
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v
    }
}

// u32
impl IndexingTypeIntoUsize for u32 {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self as usize
    }
}
impl IndexingTypeFromUsize for u32 {
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v as Self
    }
}

macro_rules! indexing_type_for_nonmax {
    ($nonmax: ident, $primitive: ident) => {
        impl IndexingTypeIntoUsize for $nonmax {
            #[inline(always)]
            fn into_usize(self) -> usize {
                self.get() as usize
            }
        }
        impl IndexingTypeFromUsize for $nonmax {
            #[inline(always)]
            fn from_usize(v: usize) -> Self {
                $nonmax::new(v as $primitive).unwrap()
            }
        }
    };
}

indexing_type_for_nonmax!(DebuggableNonMaxUsize, usize);
indexing_type_for_nonmax!(DebuggableNonMaxU32, u32);
