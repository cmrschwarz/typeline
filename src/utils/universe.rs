use std::{
    marker::PhantomData,
    ops::{Deref, Index, IndexMut},
};

use nonmax::{NonMaxU32, NonMaxUsize};

use super::get_two_distinct_mut;
//TODO: create a Vec using this Index type but without the whole reclaiming mechainic

pub trait UniverseIndex:
    Clone
    + Copy
    + Default
    + UniverseIndexFromUsize
    + UniverseIndexIntoUsize
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
{
}

impl<I> UniverseIndex for I where
    I: Clone + Copy + Default + UniverseIndexFromUsize + UniverseIndexIntoUsize + Ord
{
}

pub trait UniverseIndexIntoUsize {
    fn into_usize(self) -> usize;
}
pub trait UniverseIndexFromUsize: Sized {
    fn from_usize(v: usize) -> Self;
}

impl UniverseIndexIntoUsize for NonMaxU32 {
    fn into_usize(self) -> usize {
        self.get() as usize
    }
}
impl UniverseIndexFromUsize for NonMaxU32 {
    fn from_usize(v: usize) -> Self {
        NonMaxU32::new(v as u32).unwrap()
    }
}
impl UniverseIndexIntoUsize for NonMaxUsize {
    fn into_usize(self) -> usize {
        self.get() as usize
    }
}
impl UniverseIndexFromUsize for NonMaxUsize {
    fn from_usize(v: usize) -> Self {
        NonMaxUsize::new(v).unwrap()
    }
}
impl UniverseIndexIntoUsize for usize {
    fn into_usize(self) -> usize {
        self
    }
}
impl UniverseIndexFromUsize for usize {
    fn from_usize(v: usize) -> Self {
        v
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
struct UniverseIdx<I: UniverseIndex>(I);
impl<I: UniverseIndex> UniverseIdx<I> {
    #[inline]
    fn from_usize(v: usize) -> Self {
        UniverseIdx(I::from_usize(v))
    }
    #[inline]
    fn to_usize(self) -> usize {
        self.0.into_usize()
    }
}

impl<I: UniverseIndex> Deref for UniverseIdx<I> {
    type Target = I;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
enum UniverseEntry<T> {
    Occupied(T),
    Vacant(Option<NonMaxUsize>),
}

impl<T> UniverseEntry<T> {
    pub fn as_option(&self) -> Option<&T> {
        match self {
            UniverseEntry::Occupied(v) => Some(v),
            UniverseEntry::Vacant(_) => None,
        }
    }
    pub fn as_option_mut(&mut self) -> Option<&mut T> {
        match self {
            UniverseEntry::Occupied(v) => Some(v),
            UniverseEntry::Vacant(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct Universe<I, T> {
    data: Vec<UniverseEntry<T>>,
    first_vacant_entry: Option<NonMaxUsize>,
    occupied_entry_count: usize,
    _phantom_data: PhantomData<I>,
}

impl<I, T> Default for Universe<I, T> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            first_vacant_entry: Default::default(),
            occupied_entry_count: Default::default(),
            _phantom_data: Default::default(),
        }
    }
}

impl<I: UniverseIndex, T> Universe<I, T> {
    fn build_vacant_entry(&mut self, index: usize) -> UniverseEntry<T> {
        // SAFETY: we can never have usize::MAX entries before running out of memory
        self.first_vacant_entry = Some(unsafe { NonMaxUsize::new_unchecked(index) });
        UniverseEntry::Vacant(self.first_vacant_entry)
    }
    pub fn release(&mut self, id: I) {
        self.occupied_entry_count -= 1;
        let index = UniverseIdx(id).to_usize();
        if self.data.len() == index + 1 {
            self.data.pop();
            return;
        }
        self.data[index] = self.build_vacant_entry(index);
    }
    pub fn used_capacity(&mut self) -> usize {
        self.data.len()
    }
    pub fn occupied_entries(&mut self) -> usize {
        self.occupied_entry_count
    }
    pub fn clear(&mut self) {
        self.data.clear();
        self.first_vacant_entry = None;
        self.occupied_entry_count = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter().filter_map(|e| {
            if let UniverseEntry::Occupied(v) = e {
                Some(v)
            } else {
                None
            }
        })
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.data.iter_mut().filter_map(|e| {
            if let UniverseEntry::Occupied(v) = e {
                Some(v)
            } else {
                None
            }
        })
    }
    pub fn iter_options(&self) -> impl Iterator<Item = Option<&T>> {
        self.data.iter().map(|e| {
            if let UniverseEntry::Occupied(v) = e {
                Some(v)
            } else {
                None
            }
        })
    }
    pub fn iter_options_mut(&mut self) -> impl Iterator<Item = Option<&mut T>> {
        self.data.iter_mut().map(|e| {
            if let UniverseEntry::Occupied(v) = e {
                Some(v)
            } else {
                None
            }
        })
    }
    pub fn iter_enumerated(&mut self) -> impl Iterator<Item = (I, &T)> {
        self.data.iter().enumerate().filter_map(|(i, e)| {
            if let UniverseEntry::Occupied(v) = e {
                Some((UniverseIdx::from_usize(i).0, v))
            } else {
                None
            }
        })
    }
    pub fn any_used(&mut self) -> Option<&mut T> {
        self.iter_mut().next()
    }
    pub fn reserve_id_with(&mut self, id: I, func: impl FnOnce() -> T) {
        self.occupied_entry_count += 1;
        let index = UniverseIdx(id).to_usize();
        let mut len = self.data.len();
        while len < index {
            let ve = self.build_vacant_entry(len);
            self.data.push(ve);
            len += 1;
        }
        self.data.push(UniverseEntry::Occupied(func()))
    }
    // returns the id that will be used by the next claim
    // useful for cases where claim_with needs to know the id beforehand
    pub fn peek_claim_id(&self) -> I {
        I::from_usize(if let Some(id) = self.first_vacant_entry {
            id.get()
        } else {
            self.data.len()
        })
    }
    pub fn is_empty(&self) -> bool {
        self.occupied_entry_count == 0
    }
    pub fn claim_with(&mut self, f: impl FnOnce() -> T) -> I {
        self.occupied_entry_count += 1;
        if let Some(id) = self.first_vacant_entry {
            let index = id.get();
            match self.data[index] {
                UniverseEntry::Vacant(next) => self.first_vacant_entry = next,
                UniverseEntry::Occupied(_) => unreachable!(),
            }
            self.data[index] = UniverseEntry::Occupied(f());
            I::from_usize(index)
        } else {
            let id = self.data.len();
            self.data.push(UniverseEntry::Occupied(f()));
            I::from_usize(id)
        }
    }
    pub fn claim_with_value(&mut self, value: T) -> I {
        self.claim_with(|| value)
    }
    pub fn calc_id(&self, entry: &T) -> I {
        let offset_in_entry = if let UniverseEntry::Occupied(v) = &self.data[0] {
            unsafe {
                (v as *const T as *const u8).offset_from(self.data.as_ptr() as *const u8) as usize
            }
        } else {
            panic!("element not in Universe")
        };
        let ptr = unsafe {
            (entry as *const T as *const u8).sub(offset_in_entry) as *const UniverseEntry<T>
        };
        let range = self.data.as_ptr_range();
        assert!(range.contains(&ptr));
        *UniverseIdx::from_usize(unsafe { ptr.offset_from(range.start) } as usize)
    }
    pub fn get(&self, id: I) -> Option<&T> {
        match self.data.get(id.into_usize()) {
            Some(UniverseEntry::Occupied(v)) => Some(v),
            _ => None,
        }
    }
    pub fn get_mut(&mut self, id: I) -> Option<&mut T> {
        match self.data.get_mut(id.into_usize()) {
            Some(UniverseEntry::Occupied(v)) => Some(v),
            _ => None,
        }
    }
    pub fn get_two_distinct_mut(&mut self, id1: I, id2: I) -> (Option<&mut T>, Option<&mut T>) {
        let idx1 = id1.into_usize();
        let idx2 = id2.into_usize();

        let (a, b) = get_two_distinct_mut(&mut self.data, idx1, idx2);
        (a.as_option_mut(), b.as_option_mut())
    }
    pub fn two_distinct_mut(&mut self, id1: I, id2: I) -> (&mut T, &mut T) {
        let (a, b) = self.get_two_distinct_mut(id1, id2);
        (a.unwrap(), b.unwrap())
    }
}

// separate impl since only available if T: Default
impl<I: UniverseIndex, T: Default> Universe<I, T> {
    pub fn claim(&mut self) -> I {
        self.claim_with(Default::default)
    }
    pub fn reserve_id(&mut self, id: I) {
        self.reserve_id_with(id, Default::default);
    }
}

impl<I: UniverseIndex, T> Index<I> for Universe<I, T> {
    type Output = T;
    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        match &self.data[index.into_usize()] {
            UniverseEntry::Occupied(v) => v,
            UniverseEntry::Vacant(_) => panic!("index out of bounds"),
        }
    }
}

impl<I: UniverseIndex, T> IndexMut<I> for Universe<I, T> {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        match &mut self.data[index.into_usize()] {
            UniverseEntry::Occupied(v) => v,
            UniverseEntry::Vacant(_) => panic!("index out of bounds"),
        }
    }
}
