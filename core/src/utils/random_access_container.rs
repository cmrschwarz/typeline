use std::{
    collections::VecDeque,
    ops::{Index, IndexMut},
};

/// Very crude generalization over Vec<T> and VecDeque<T>,
/// used for cases where algorithms need to work on both, like
/// `merge_action_lists`
pub trait RandomAccessContainer<T>:
    Index<usize, Output = T> + IndexMut<usize, Output = T>
{
    fn get(&self, index: usize) -> Option<&T>;
    fn last_mut(&mut self) -> Option<&mut T>;
    fn push(&mut self, v: T);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> RandomAccessContainer<T> for Vec<T> {
    fn get(&self, index: usize) -> Option<&T> {
        <[T]>::get(self, index)
    }
    fn last_mut(&mut self) -> Option<&mut T> {
        <[T]>::last_mut(self)
    }
    fn push(&mut self, v: T) {
        <Vec<_>>::push(self, v)
    }
    fn len(&self) -> usize {
        <[T]>::len(self)
    }
}
impl<T> RandomAccessContainer<T> for VecDeque<T> {
    fn get(&self, index: usize) -> Option<&T> {
        <VecDeque<_>>::get(self, index)
    }
    fn last_mut(&mut self) -> Option<&mut T> {
        self.back_mut()
    }
    fn push(&mut self, v: T) {
        <VecDeque<_>>::push_back(self, v)
    }
    fn len(&self) -> usize {
        <VecDeque<_>>::len(self)
    }
}
