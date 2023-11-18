use std::{
    collections::VecDeque,
    marker::PhantomData,
    ops::{Index, IndexMut},
};

use super::indexing_type::IndexingType;

pub struct OffsetVecDeque<I, T> {
    pub data: VecDeque<T>,
    pub offset: I,
}

impl<I: IndexingType, T> Index<I> for OffsetVecDeque<I, T> {
    type Output = T;

    fn index(&self, index: I) -> &Self::Output {
        &self.data[index.into_usize().wrapping_sub(self.offset.into_usize())]
    }
}
impl<I: IndexingType, T> IndexMut<I> for OffsetVecDeque<I, T> {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.data
            [index.into_usize().wrapping_sub(self.offset.into_usize())]
    }
}
impl<I: IndexingType, T> Default for OffsetVecDeque<I, T> {
    fn default() -> Self {
        Self {
            data: VecDeque::default(),
            offset: I::from_usize(0),
        }
    }
}
impl<I: IndexingType, T: Clone> Clone for OffsetVecDeque<I, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            offset: self.offset.clone(),
        }
    }
}

impl<I: IndexingType, T> OffsetVecDeque<I, T> {
    pub fn max_index(&self) -> I {
        I::from_usize(
            self.data
                .len()
                .wrapping_add(self.offset.into_usize())
                .wrapping_sub(1),
        )
    }
    pub fn next_free_index(&self) -> I {
        I::from_usize(self.data.len().wrapping_add(self.offset.into_usize()))
    }
}
