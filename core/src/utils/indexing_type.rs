#![allow(clippy::inline_always)]

use super::debuggable_nonmax::{
    DebuggableNonMaxU32, DebuggableNonMaxU64, DebuggableNonMaxUsize,
};

pub trait IndexingType:
    Clone + Copy + Default + PartialEq + Eq + PartialOrd + Ord
{
    // We can't use `From<usize>` because e.g. u32 does not implement
    // that, and we can't implement it for it (orphan rule).
    // We also can't have a blanket impl of this trait for types that implement
    // `From<usize>` because then we can't add any manual ones
    // (might conflict in future if `From` is added ...).
    fn from_usize(v: usize) -> Self;
    fn into_usize(self) -> usize;
}

impl IndexingType for usize {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self
    }
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v
    }
}

impl IndexingType for u32 {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self as usize
    }
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v as Self
    }
}

impl IndexingType for u64 {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self as usize
    }
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v as Self
    }
}

macro_rules! indexing_type_for_nonmax {
    ($nonmax: ident, $primitive: ident) => {
        impl IndexingType for $nonmax {
            #[inline(always)]
            fn into_usize(self) -> usize {
                self.get() as usize
            }
            #[inline(always)]
            fn from_usize(v: usize) -> Self {
                $nonmax::new(v as $primitive).unwrap()
            }
        }
    };
}

indexing_type_for_nonmax!(DebuggableNonMaxUsize, usize);
indexing_type_for_nonmax!(DebuggableNonMaxU32, u32);
indexing_type_for_nonmax!(DebuggableNonMaxU64, u64);

#[macro_export]
macro_rules! index_newtype {
    ( $visibility: vis $name: ident, $src_type: path) => {
        #[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $visibility struct $name ($src_type);

        impl From<usize> for $name {
            fn from(v: usize) -> $name {
                $name(<$src_type as $crate::utils::indexing_type::IndexingType>::from_usize(v))
            }
        }
        impl From<$name> for usize {
            fn from(v: $name) -> usize {
                <$src_type as $crate::utils::indexing_type::IndexingType>::into_usize(v.0)
            }
        }

        impl $crate::utils::indexing_type::IndexingType for $name {
            #[inline(always)]
            fn into_usize(self) -> usize {
                <$src_type as $crate::utils::indexing_type::IndexingType>::into_usize(self.0)
            }
            #[inline(always)]
            fn from_usize(v: usize) -> Self {
                $name(<$src_type as $crate::utils::indexing_type::IndexingType>::from_usize(v))
            }
        }
    };
}
