use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{
        Index, IndexMut, Range, RangeFrom, RangeInclusive, RangeTo,
        RangeToInclusive,
    },
};

use derive_more::{Deref, DerefMut};

use super::{indexing_type::IndexingType, range_bounds_to_range};

#[derive(Clone, PartialEq, Eq, Hash, Deref, DerefMut)]
pub struct IndexVec<I, T> {
    #[deref]
    #[deref_mut]
    data: Vec<T>,
    _phantom: PhantomData<fn(I) -> T>,
}

impl<I, T> From<Vec<T>> for IndexVec<I, T> {
    fn from(value: Vec<T>) -> Self {
        IndexVec {
            data: value,
            _phantom: PhantomData,
        }
    }
}

impl<I, T> Default for IndexVec<I, T> {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

pub struct IndexVecIterEnumerated<'a, I, T> {
    base_iter: std::slice::Iter<'a, T>,
    pos: I,
}

impl<'a, I: IndexingType, T> Iterator for IndexVecIterEnumerated<'a, I, T> {
    type Item = (I, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.base_iter.next()?;
        let idx = self.pos;
        self.pos = I::from_usize(self.pos.into_usize() + 1);
        Some((idx, value))
    }
}

impl<I: IndexingType, T> IndexVec<I, T> {
    pub const fn new() -> Self {
        Self {
            data: Vec::new(),
            _phantom: PhantomData,
        }
    }
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            data: Vec::with_capacity(cap),
            _phantom: PhantomData,
        }
    }
    pub fn next_free_idx(&self) -> I {
        I::from_usize(self.data.len())
    }
    pub fn push_get_id(&mut self, v: T) -> I {
        let id = self.next_free_idx();
        self.data.push(v);
        id
    }
    pub fn iter_enumerated(&self) -> IndexVecIterEnumerated<I, T> {
        IndexVecIterEnumerated {
            base_iter: self.data.iter(),
            pos: I::default(),
        }
    }
}

impl<I: IndexingType, T: Debug> Debug for IndexVec<I, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.data, f)
    }
}

impl<I: IndexingType, T> Index<I> for IndexVec<I, T> {
    type Output = T;
    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.data[index.into_usize()]
    }
}

impl<I: IndexingType, T> IndexMut<I> for IndexVec<I, T> {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.data[index.into_usize()]
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a IndexVec<I, T> {
    type Item = &'a T;

    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a mut IndexVec<I, T> {
    type Item = &'a mut T;

    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter_mut()
    }
}

impl<I, T> FromIterator<T> for IndexVec<I, T> {
    fn from_iter<ITER: IntoIterator<Item = T>>(iter: ITER) -> Self {
        Self::from(Vec::from_iter(iter))
    }
}

impl<I: IndexingType, T> Index<Range<I>> for IndexVec<I, T> {
    type Output = [T];

    fn index(&self, index: Range<I>) -> &Self::Output {
        &self.data[index.start.into_usize()..index.end.into_usize()]
    }
}

impl<I: IndexingType, T> IndexMut<Range<I>> for IndexVec<I, T> {
    fn index_mut(&mut self, index: Range<I>) -> &mut Self::Output {
        &mut self.data[index.start.into_usize()..index.end.into_usize()]
    }
}

macro_rules! slice_index_impl {
    ($range_type: ident) => {
        impl<I: IndexingType, T> Index<$range_type<I>> for IndexVec<I, T> {
            type Output = [T];
            #[inline]
            fn index(&self, rb: $range_type<I>) -> &Self::Output {
                &self.data[range_bounds_to_range(rb, self.len())]
            }
        }

        impl<I: IndexingType, T> IndexMut<$range_type<I>> for IndexVec<I, T> {
            #[inline]
            fn index_mut(&mut self, rb: $range_type<I>) -> &mut Self::Output {
                let range = range_bounds_to_range(rb, self.len());
                &mut self.data[range]
            }
        }
    };
    ($($range_types: ident),+) => {
        $( slice_index_impl!($range_types); ) *
    };
}

slice_index_impl!(RangeInclusive, RangeFrom, RangeTo, RangeToInclusive);
