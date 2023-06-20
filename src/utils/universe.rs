use std::{
    fmt::Debug,
    ops::{Deref, Index, IndexMut},
    slice::IterMut,
};

pub trait UniverseIndex:
    Clone
    + Copy
    + Default
    + TryFrom<usize, Error = Self::TryFromErrorType>
    + Into<usize>
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
{
    type TryFromErrorType: Debug;
}
impl<I, ET: Debug> UniverseIndex for I
where
    I: Clone + Copy + Default + TryFrom<usize, Error = ET> + Into<usize> + Ord,
{
    type TryFromErrorType = ET;
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
struct UniverseIdx<I: UniverseIndex>(I);

impl<I: UniverseIndex> UniverseIdx<I> {
    fn from_usize(val: usize) -> Self {
        UniverseIdx(<I as TryFrom<usize>>::try_from(val).unwrap())
    }
    fn to_usize(self) -> usize {
        self.0.into()
    }
}

impl<I: UniverseIndex> Deref for UniverseIdx<I> {
    type Target = I;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct Universe<I: UniverseIndex, T> {
    data: Vec<T>,
    unused_ids: Vec<UniverseIdx<I>>,
}

impl<I: UniverseIndex, T: Default> Deref for Universe<I, T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<I: UniverseIndex, T> Default for Universe<I, T> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            unused_ids: Default::default(),
        }
    }
}

impl<I: UniverseIndex, T> Universe<I, T> {
    pub fn push(&mut self, val: T) -> I {
        if let Some(id) = self.unused_ids.pop() {
            self.data[id.to_usize()] = val;
            *id
        } else {
            let id = UniverseIdx::from_usize(self.data.len());
            self.data.push(val);
            *id
        }
    }
    pub fn release(&mut self, id: I) {
        let index = UniverseIdx(id).to_usize();
        if self.data.len() == index + 1 {
            self.data.pop();
            return;
        }
        self.unused_ids.push(UniverseIdx(id));
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn clear(&mut self) {
        self.unused_ids.clear();
        self.data.clear();
    }
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.data.as_mut_slice()
    }
    // makes sure that the next n `claim`s that are not interrupted
    // by `release`s or `clear`s will get ascending ids
    pub fn reserve_ordered(&mut self, n: usize) {
        let len = self.unused_ids.len().min(n);
        self.unused_ids[0..len].sort();
    }

    pub fn iter_mut(&mut self) -> IterMut<T> {
        self.data.iter_mut()
    }
}

// separate impl since only available if T: Default
impl<I: UniverseIndex, T: Default> Universe<I, T> {
    pub fn claim(&mut self) -> I {
        if let Some(id) = self.unused_ids.pop() {
            *id
        } else {
            let id = self.data.len();
            self.data.push(Default::default());
            *UniverseIdx::from_usize(id)
        }
    }
}

impl<I: UniverseIndex, T> Index<I> for Universe<I, T> {
    type Output = T;

    fn index(&self, index: I) -> &Self::Output {
        &self.data[index.into()]
    }
}

impl<I: UniverseIndex, T> IndexMut<I> for Universe<I, T> {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.data[index.into()]
    }
}
