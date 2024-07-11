use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{
        Deref, DerefMut, Index, IndexMut, Range, RangeFrom, RangeInclusive,
        RangeTo, RangeToInclusive,
    },
};

use ref_cast::RefCast;

use super::{
    get_two_distinct_mut,
    indexing_type::{IndexingType, IndexingTypeRange},
    range_bounds_to_range,
};

#[macro_export]
macro_rules! index_vec {
    ($($anything: tt)+) => {
        IndexVec::from(vec![$($anything)+])
    };
}

#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct IndexSlice<I, T> {
    _phantom: PhantomData<fn(I) -> T>,
    data: [T],
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IndexVec<I, T> {
    data: Vec<T>,
    _phantom: PhantomData<fn(I) -> T>,
}

impl<I, T> Deref for IndexVec<I, T> {
    type Target = IndexSlice<I, T>;

    fn deref(&self) -> &Self::Target {
        IndexSlice::ref_cast(&*self.data)
    }
}
impl<I, T> DerefMut for IndexVec<I, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        IndexSlice::ref_cast_mut(&mut *self.data)
    }
}

impl<I, T> From<Vec<T>> for IndexVec<I, T> {
    fn from(value: Vec<T>) -> Self {
        IndexVec {
            data: value,
            _phantom: PhantomData,
        }
    }
}

impl<I, T> From<IndexVec<I, T>> for Vec<T> {
    fn from(value: IndexVec<I, T>) -> Self {
        value.data
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

pub struct IndexSliceIterEnumerated<'a, I, T> {
    base_iter: std::slice::Iter<'a, T>,
    pos: I,
}

impl<'a, I: IndexingType, T> Iterator for IndexSliceIterEnumerated<'a, I, T> {
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
    pub fn extend(&mut self, iter: impl IntoIterator<Item = T>) {
        self.data.extend(iter)
    }
    pub fn push(&mut self, v: T) {
        self.data.push(v)
    }
    pub fn pop(&mut self) -> Option<T> {
        self.data.pop()
    }
    pub fn resize_with(&mut self, new_len: usize, f: impl FnMut() -> T) {
        self.data.resize_with(new_len, f)
    }

    pub fn truncate_len(&mut self, len: usize) {
        self.data.truncate(len);
    }

    pub fn as_vec(&self) -> &Vec<T> {
        &self.data
    }
    pub fn as_vec_mut(&mut self) -> &mut Vec<T> {
        &mut self.data
    }
    pub fn into_boxed_slice(self) -> Box<IndexSlice<I, T>> {
        IndexSlice::ref_cast_box(self.data.into_boxed_slice())
    }
    pub fn push_get_id(&mut self, v: T) -> I {
        let id = self.next_idx();
        self.data.push(v);
        id
    }
    pub fn truncate(&mut self, new_end_index: I) {
        self.data.truncate(new_end_index.into_usize());
    }
    pub fn iter_enumerated(&self) -> IndexSliceIterEnumerated<I, T> {
        IndexSliceIterEnumerated {
            base_iter: self.data.iter(),
            pos: I::default(),
        }
    }
    pub fn indices(&self) -> IndexingTypeRange<I> {
        IndexingTypeRange::new(I::zero()..self.next_idx())
    }
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

impl<I: IndexingType, T> IndexSlice<I, T> {
    pub fn iter_enumerated(
        &self,
        initial_offset: I,
    ) -> IndexSliceIterEnumerated<I, T> {
        IndexSliceIterEnumerated {
            base_iter: self.data.iter(),
            pos: initial_offset,
        }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn first(&self) -> Option<&T> {
        self.data.first()
    }
    pub fn last(&self) -> Option<&T> {
        self.data.last()
    }
    pub fn slice(&self) -> &[T] {
        &self.data
    }
    pub fn slice_mut(&mut self) -> &mut [T] {
        &mut self.data
    }
    pub fn ref_cast_box(slice_box: Box<[T]>) -> Box<Self> {
        unsafe {
            let base_box_raw = Box::into_raw(slice_box);
            let index_box_raw = IndexSlice::ref_cast_mut(&mut *base_box_raw)
                as *mut IndexSlice<I, T>;
            Box::from_raw(index_box_raw)
        }
    }
    pub fn next_idx(&self) -> I {
        I::from_usize(self.data.len())
    }
    pub fn last_idx(&self) -> Option<I> {
        self.len().checked_sub(1).map(I::from_usize)
    }

    pub fn get(&self, idx: I) -> Option<&T> {
        self.data.get(idx.into_usize())
    }
    pub fn get_mut(&mut self, idx: I) -> Option<&mut T> {
        self.data.get_mut(idx.into_usize())
    }
    pub fn iter(&self) -> std::slice::Iter<T> {
        self.data.iter()
    }
    pub fn iter_mut(&mut self) -> std::slice::IterMut<T> {
        self.data.iter_mut()
    }
    pub fn two_distinct_mut(&mut self, a: I, b: I) -> (&mut T, &mut T) {
        get_two_distinct_mut(&mut self.data, a.into_usize(), b.into_usize())
    }
}

impl<I: IndexingType, T: Debug> Debug for IndexSlice<I, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.data, f)
    }
}

impl<I: IndexingType, T> Index<I> for IndexSlice<I, T> {
    type Output = T;
    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.data[index.into_usize()]
    }
}

impl<I: IndexingType, T> IndexMut<I> for IndexSlice<I, T> {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.data[index.into_usize()]
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a IndexVec<I, T> {
    type Item = &'a T;

    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a mut IndexVec<I, T> {
    type Item = &'a mut T;

    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a IndexSlice<I, T> {
    type Item = &'a T;

    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a mut IndexSlice<I, T> {
    type Item = &'a mut T;

    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<I, T> FromIterator<T> for IndexVec<I, T> {
    fn from_iter<ITER: IntoIterator<Item = T>>(iter: ITER) -> Self {
        Self::from(Vec::from_iter(iter))
    }
}

impl<I: IndexingType, T> Index<Range<I>> for IndexSlice<I, T> {
    type Output = IndexSlice<I, T>;

    fn index(&self, index: Range<I>) -> &Self::Output {
        IndexSlice::ref_cast(
            &self.data[index.start.into_usize()..index.end.into_usize()],
        )
    }
}

impl<I: IndexingType, T> IndexMut<Range<I>> for IndexSlice<I, T> {
    fn index_mut(&mut self, index: Range<I>) -> &mut Self::Output {
        IndexSlice::ref_cast_mut(
            &mut self.data[index.start.into_usize()..index.end.into_usize()],
        )
    }
}

macro_rules! slice_index_impl {
    ($range_type: ident) => {
        impl<I: IndexingType, T> Index<$range_type<I>> for IndexSlice<I, T> {
            type Output = IndexSlice<I, T>;
            #[inline]
            fn index(&self, rb: $range_type<I>) -> &Self::Output {
                IndexSlice::ref_cast(&self.data[range_bounds_to_range(rb, self.len())])
            }
        }

        impl<I: IndexingType, T> IndexMut<$range_type<I>> for IndexSlice<I, T> {
            #[inline]
            fn index_mut(&mut self, rb: $range_type<I>) -> &mut Self::Output {
                let range = range_bounds_to_range(rb, self.len());
                IndexSlice::ref_cast_mut(&mut self.data[range])
            }
        }
    };
    ($($range_types: ident),+) => {
        $( slice_index_impl!($range_types); ) *
    };
}

slice_index_impl!(RangeInclusive, RangeFrom, RangeTo, RangeToInclusive);
