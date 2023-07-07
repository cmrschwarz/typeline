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
        self.unused_ids[0..len].sort_unstable();
    }

    pub fn iter_mut(&mut self) -> IterMut<T> {
        self.data.iter_mut()
    }

    pub fn any_used(&mut self) -> Option<&mut T> {
        self.unused_ids.sort_unstable();
        let mut i = 0;
        let mut unused_iter = self.unused_ids.iter().peekable();
        loop {
            if i == self.data.len() {
                return None;
            }
            if Some(i) == unused_iter.peek().map(|id| id.to_usize()) {
                i += 1;
                unused_iter.next();
                continue;
            }
            return Some(&mut self.data[i]);
        }
    }

    pub fn reserve_id_with(
        &mut self,
        id: I,
        defaults_func: impl FnMut() -> T,
        func: impl FnOnce() -> T,
    ) {
        let index = UniverseIdx(id).to_usize();
        let prev_len = self.data.len();
        if prev_len <= index {
            self.data.resize_with(index, defaults_func);
            self.data.push(func());
            self.unused_ids.extend(
                (prev_len..index)
                    .into_iter()
                    .map(|idx| UniverseIdx::from_usize(idx)),
            );
        }
    }
    // returns the id that will be used by the next claim
    // useful for cases where claim_with needs to know the id beforehand
    pub fn peek_claim_id(&self) -> I {
        if let Some(id) = self.unused_ids.last() {
            **id
        } else {
            *UniverseIdx::from_usize(self.data.len())
        }
    }
    pub fn has_unclaimed_entries(&self) -> bool {
        !self.unused_ids.is_empty()
    }
    pub fn claimed_entry_count(&self) -> usize {
        self.data.len() - self.unused_ids.len()
    }
    pub fn claim_with(&mut self, f: impl FnOnce() -> T) -> I {
        if let Some(id) = self.unused_ids.pop() {
            *id
        } else {
            let id = self.data.len();
            self.data.push(f());
            *UniverseIdx::from_usize(id)
        }
    }
    pub fn claim_with_value(&mut self, value: T) -> I {
        if let Some(id) = self.unused_ids.pop() {
            self[*id] = value;
            *id
        } else {
            let id = self.data.len();
            self.data.push(value);
            *UniverseIdx::from_usize(id)
        }
    }
    pub fn calc_id(&self, entry: &T) -> I {
        let range = self.data.as_ptr_range();
        let ptr = entry as *const T;
        assert!(range.contains(&ptr));
        *UniverseIdx::from_usize(unsafe { ptr.offset_from(range.start) } as usize)
    }
}

// separate impl since only available if T: Default
impl<I: UniverseIndex, T: Default> Universe<I, T> {
    pub fn claim(&mut self) -> I {
        self.claim_with(Default::default)
    }
    pub fn reserve_id(&mut self, id: I) {
        self.reserve_id_with(id, Default::default, Default::default);
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
