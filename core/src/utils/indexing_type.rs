#![allow(clippy::inline_always)]

use std::{hash::Hash, ops::Range};

use super::debuggable_nonmax::{
    DebuggableNonMaxU32, DebuggableNonMaxU64, DebuggableNonMaxUsize,
};

pub trait IndexingType:
    Default + Clone + Copy + PartialEq + Eq + PartialOrd + Ord + Hash
{
    type IndexBaseType;
    // We can't use `From<usize>` because e.g. u32 does not implement
    // that, and we can't implement it for it (orphan rule).
    // We also can't have a blanket impl of this trait for types that implement
    // `From<usize>` because then we can't add any manual ones
    // (might conflict in future if `From` is added ...).
    fn from_usize(v: usize) -> Self;
    fn into_usize(self) -> usize;
}

pub struct IndexingTypeRange<I> {
    pub start: I,
    pub end: I,
}

impl<I> IndexingTypeRange<I> {
    pub fn new(r: Range<I>) -> Self {
        Self {
            start: r.start,
            end: r.end,
        }
    }
}

impl<I: IndexingType> Iterator for IndexingTypeRange<I> {
    type Item = I;

    fn next(&mut self) -> Option<I> {
        if self.start == self.end {
            return None;
        }
        let curr = self.start;
        self.start = I::from_usize(curr.into_usize() + 1);
        Some(curr)
    }
}

impl IndexingType for usize {
    type IndexBaseType = Self;
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
    type IndexBaseType = Self;
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
    type IndexBaseType = Self;
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
            type IndexBaseType = Self;
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
    { $( $(#[$attrs: meta])* $type_vis: vis struct $name: ident ($base_vis: vis $base_type: path); )* } => {$(
        $(#[$attrs])*
        #[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        $type_vis struct $name ($base_vis $base_type);

        impl $crate::utils::indexing_type::IndexingType for $name {
            type IndexBaseType = $base_type;
            #[inline(always)]
            fn into_usize(self) -> usize {
                <$base_type as $crate::utils::indexing_type::IndexingType>::into_usize(self.0)
            }
            #[inline(always)]
            fn from_usize(v: usize) -> Self {
                $name(<$base_type as $crate::utils::indexing_type::IndexingType>::from_usize(v))
            }
        }

        impl $name {
            pub const fn new(v: $base_type) -> Self {
                $name(v)
            }
            pub fn zero() -> Self {
                <$name as $crate::utils::indexing_type::IndexingType>::from_usize(0)
            }
        }

        impl From<usize> for $name {
            #[inline(always)]
            fn from(v: usize) -> $name {
                $name(<$base_type as $crate::utils::indexing_type::IndexingType>::from_usize(v))
            }
        }
        impl From<$name> for usize {
            #[inline(always)]
            fn from(v: $name) -> usize {
                <$base_type as $crate::utils::indexing_type::IndexingType>::into_usize(v.0)
            }
        }

        impl std::fmt::Debug for $name {
            #[inline]
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.0, f)
            }
        }

        impl std::fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

    )*};
}
