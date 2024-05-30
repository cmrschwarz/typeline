use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Index, IndexMut},
};

use derive_more::{Deref, DerefMut};

use super::indexing_type::IndexingType;

#[derive(Clone, PartialEq, Eq, Hash, Deref, DerefMut)]
pub struct IndexVec<I, T> {
    #[deref]
    #[deref_mut]
    data: Vec<T>,
    _phantom: PhantomData<fn(I) -> T>,
}

impl<I: IndexingType, T: Debug> Debug for IndexVec<I, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.data, f)
    }
}

impl<I: IndexingType, T> Index<I> for IndexVec<I, T> {
    type Output = T;

    fn index(&self, index: I) -> &Self::Output {
        &self.data[index.into_usize()]
    }
}

impl<I: IndexingType, T> IndexMut<I> for IndexVec<I, T> {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.data[index.into_usize()]
    }
}
