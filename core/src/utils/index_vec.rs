use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use ref_cast::RefCast;

use super::{
    index_slice::{IndexSlice, IndexSliceIterEnumerated},
    indexing_type::{IndexingType, IndexingTypeRange},
};

#[macro_export]
macro_rules! index_vec {
    ($($anything: tt)+) => {
        IndexVec::from(vec![$($anything)+])
    };
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl<I: IndexingType, T: Debug> Debug for IndexVec<I, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.data, f)
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
        IndexSlice::from_boxed_slice(self.data.into_boxed_slice())
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
        IndexSliceIterEnumerated::new(self.data.iter(), I::default())
    }
    pub fn indices(&self) -> IndexingTypeRange<I> {
        IndexingTypeRange::new(I::zero()..self.next_idx())
    }
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

impl<I: IndexingType, T> IntoIterator for IndexVec<I, T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
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

impl<I, T> FromIterator<T> for IndexVec<I, T> {
    fn from_iter<ITER: IntoIterator<Item = T>>(iter: ITER) -> Self {
        Self::from(Vec::from_iter(iter))
    }
}

impl<I: IndexingType, T: PartialEq, const N: usize> PartialEq<IndexVec<I, T>>
    for [T; N]
{
    fn eq(&self, other: &IndexVec<I, T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<I: IndexingType, T: PartialEq, const N: usize> PartialEq<[T; N]>
    for IndexVec<I, T>
{
    fn eq(&self, other: &[T; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<I: IndexingType, T: PartialEq> PartialEq<IndexSlice<I, T>>
    for IndexVec<I, T>
{
    fn eq(&self, other: &IndexSlice<I, T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<I: IndexingType, T: PartialEq> PartialEq<IndexVec<I, T>>
    for IndexSlice<I, T>
{
    fn eq(&self, other: &IndexVec<I, T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<I: IndexingType, T: PartialEq> PartialEq<IndexVec<I, T>> for [T] {
    fn eq(&self, other: &IndexVec<I, T>) -> bool {
        self == other.as_slice()
    }
}

impl<I: IndexingType, T: PartialEq> PartialEq<[T]> for IndexVec<I, T> {
    fn eq(&self, other: &[T]) -> bool {
        self.as_slice() == other
    }
}
