use std::ops::{Index, IndexMut};

use indexland::Idx;

#[derive(Clone, Default)]
pub struct DynamicArrayFreelist<I, T> {
    data: Vec<T>,
    array_length: usize,
    free_slots: Vec<I>,
}

impl<I: Idx, T: Default + Clone> DynamicArrayFreelist<I, T> {
    pub fn new(array_length: usize) -> Self {
        Self {
            data: Vec::with_capacity(array_length * 4),
            array_length,
            free_slots: Vec::with_capacity(4),
        }
    }
    pub fn claim(&mut self) -> (I, &mut [T]) {
        if let Some(idx) = self.free_slots.pop() {
            let i = idx.into_usize();
            return (idx, &mut self.data[i..i + self.array_length]);
        }
        let idx = self.data.len();
        self.data.extend(
            std::iter::repeat(Default::default()).take(self.array_length),
        );
        (
            I::from_usize(idx),
            &mut self.data[idx..idx + self.array_length],
        )
    }
    pub fn release(&mut self, idx: I) {
        self.free_slots.push(idx);
    }
    pub fn get(&self, idx: I) -> &[T] {
        let i = idx.into_usize();
        &self.data[i..i + self.array_length]
    }
    pub fn get_mut(&mut self, idx: I) -> &mut [T] {
        let i = idx.into_usize();
        &mut self.data[i..i + self.array_length]
    }
}

impl<I: Idx, T: Default + Clone> Index<I>
    for DynamicArrayFreelist<I, T>
{
    type Output = [T];

    fn index(&self, index: I) -> &Self::Output {
        self.get(index)
    }
}

impl<I: Idx, T: Default + Clone> IndexMut<I>
    for DynamicArrayFreelist<I, T>
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.get_mut(index)
    }
}
