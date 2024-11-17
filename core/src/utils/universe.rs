use std::{
    marker::PhantomData,
    ops::{Index, IndexMut},
};

use arrayvec::ArrayVec;

use super::{
    debuggable_nonmax::DebuggableNonMaxUsize, get_three_distinct_mut,
    indexing_type::IndexingType, temp_vec::TransmutableContainer,
};

use super::get_two_distinct_mut;
// TODO: create a Vec using this Index type but without the whole reclaiming
// mechainic

#[derive(Clone)]
pub enum UniverseEntry<T> {
    Occupied(T),
    // PERF: maybe use the indexing type / a derived non max type
    // here instead to save memory for u32s, e.g. in `GroupList`
    Vacant(Option<DebuggableNonMaxUsize>),
}

#[derive(Clone)]
pub struct Universe<I, T> {
    data: Vec<UniverseEntry<T>>,
    first_vacant_entry: Option<DebuggableNonMaxUsize>,
    _phantom_data: PhantomData<I>,
}

pub struct UniverseMultiRefMutHandout<'a, I, T, const CAP: usize> {
    universe: &'a mut Universe<I, T>,
    handed_out: ArrayVec<I, CAP>,
}

pub unsafe trait RefHandoutStack<I, T> {
    type Child<'b>: RefHandoutStack<I, T>
    where
        Self: 'b;
    fn claim(&mut self, idx: I) -> (Self::Child<'_>, &mut T);
    fn assert_unused(&mut self, idx: I) -> &mut Universe<I, T>;
}

pub struct RefHandoutStackNode<'a, I, T, P> {
    base: &'a mut P,
    idx: I,
    _phantom: PhantomData<fn() -> T>,
}

pub struct RefHandoutStackBase<'a, I, T> {
    universe: &'a mut Universe<I, T>,
}

unsafe impl<'a, I: IndexingType, T> RefHandoutStack<I, T>
    for RefHandoutStackBase<'a, I, T>
{
    type Child<'b>
        = RefHandoutStackNode<'b, I, T, Self>
    where
        Self: 'b;

    fn claim<'b>(&'b mut self, idx: I) -> (Self::Child<'b>, &'b mut T) {
        let elem = unsafe {
            std::mem::transmute::<&'_ mut T, &'b mut T>(
                &mut self.universe[idx],
            )
        };
        let child = RefHandoutStackNode {
            base: self,
            idx,
            _phantom: PhantomData,
        };
        (child, elem)
    }

    fn assert_unused(&mut self, _idx: I) -> &mut Universe<I, T> {
        self.universe
    }
}

unsafe impl<'a, I: IndexingType, T, P: RefHandoutStack<I, T>>
    RefHandoutStack<I, T> for RefHandoutStackNode<'a, I, T, P>
{
    type Child<'b>
        = RefHandoutStackNode<'b, I, T, Self>
    where
        Self: 'b;

    fn claim<'b>(&'b mut self, idx: I) -> (Self::Child<'b>, &'b mut T) {
        let universe = self.assert_unused(idx);
        let elem = unsafe {
            std::mem::transmute::<&'_ mut T, &'b mut T>(&mut universe[idx])
        };

        let child = RefHandoutStackNode {
            base: self,
            idx,
            _phantom: PhantomData,
        };
        (child, elem)
    }

    fn assert_unused(&mut self, idx: I) -> &mut Universe<I, T> {
        assert!(idx != self.idx);
        self.base.assert_unused(idx)
    }
}

impl<T> UniverseEntry<T> {
    pub fn as_option_mut(&mut self) -> Option<&mut T> {
        match self {
            UniverseEntry::Occupied(v) => Some(v),
            UniverseEntry::Vacant(_) => None,
        }
    }
}

// if we autoderive this, I would have to implement Default
impl<I: IndexingType, T> Default for Universe<I, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: IndexingType, T> Universe<I, T> {
    pub const fn new() -> Self {
        Self {
            data: Vec::new(),
            first_vacant_entry: None,
            _phantom_data: PhantomData,
        }
    }
    pub fn multi_ref_mut_handout<const CAP: usize>(
        &mut self,
    ) -> UniverseMultiRefMutHandout<I, T, CAP> {
        UniverseMultiRefMutHandout {
            universe: self,
            handed_out: ArrayVec::new(),
        }
    }
    pub fn ref_mut_handout_stack(&mut self) -> RefHandoutStackBase<I, T> {
        RefHandoutStackBase { universe: self }
    }
    fn build_vacant_entry(&mut self, index: usize) -> UniverseEntry<T> {
        // SAFETY: we can never have usize::MAX entries before running out of
        // memory
        let res = UniverseEntry::Vacant(self.first_vacant_entry);
        self.first_vacant_entry =
            Some(unsafe { DebuggableNonMaxUsize::new_unchecked(index) });
        res
    }
    pub fn release(&mut self, id: I) {
        let index = id.into_usize();
        if self.data.len() == index + 1 {
            self.data.pop();
            return;
        }
        self.data[index] = self.build_vacant_entry(index);
    }
    pub fn used_capacity(&mut self) -> usize {
        self.data.len()
    }
    pub fn clear(&mut self) {
        self.data.clear();
        self.first_vacant_entry = None;
    }
    pub fn indices(&self) -> UniverseIndexIter<I, T> {
        UniverseIndexIter {
            index: I::zero(),
            base: self.data.iter(),
        }
    }
    pub fn iter(&self) -> UniverseIter<T> {
        UniverseIter {
            base: self.data.iter(),
        }
    }
    pub fn iter_mut(&mut self) -> UniverseIterMut<T> {
        UniverseIterMut {
            base: self.data.iter_mut(),
        }
    }
    pub fn iter_enumerated(&self) -> UniverseEnumeratedIter<I, T> {
        UniverseEnumeratedIter {
            base: &self.data,
            idx: I::from_usize(0),
        }
    }
    pub fn iter_enumerated_mut(&mut self) -> UniverseEnumeratedIterMut<I, T> {
        UniverseEnumeratedIterMut {
            base: &mut self.data,
            idx: I::from_usize(0),
        }
    }
    pub fn any_used(&mut self) -> Option<&mut T> {
        self.iter_mut().next()
    }
    pub fn reserve_id_with(&mut self, id: I, func: impl FnOnce() -> T) {
        let index = id.into_usize();
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

    pub fn claim_with(&mut self, f: impl FnOnce() -> T) -> I {
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
        let offset_in_entry = if let UniverseEntry::Occupied(v) = &self.data[0]
        {
            unsafe {
                (v as *const T)
                    .cast::<u8>()
                    .offset_from(self.data.as_ptr().cast())
            }
        } else {
            panic!("element not in Universe")
        };
        let ptr = unsafe {
            (entry as *const T)
                .cast::<u8>()
                .sub(usize::try_from(offset_in_entry).unwrap_unchecked())
                .cast()
        };
        let range = self.data.as_ptr_range();
        assert!(range.contains(&ptr));
        I::from_usize(unsafe { ptr.offset_from(range.start) } as usize)
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
    pub fn get_two_distinct_mut(
        &mut self,
        id1: I,
        id2: I,
    ) -> (Option<&mut T>, Option<&mut T>) {
        let idx1 = id1.into_usize();
        let idx2 = id2.into_usize();

        let (a, b) = get_two_distinct_mut(&mut self.data, idx1, idx2);
        (a.as_option_mut(), b.as_option_mut())
    }
    pub fn get_three_distinct_mut(
        &mut self,
        id1: I,
        id2: I,
        id3: I,
    ) -> (Option<&mut T>, Option<&mut T>, Option<&mut T>) {
        let idx1 = id1.into_usize();
        let idx2 = id2.into_usize();
        let idx3 = id3.into_usize();

        let (a, b, c) =
            get_three_distinct_mut(&mut self.data, idx1, idx2, idx3);
        (a.as_option_mut(), b.as_option_mut(), c.as_option_mut())
    }
    pub fn two_distinct_mut(&mut self, id1: I, id2: I) -> (&mut T, &mut T) {
        let (a, b) = self.get_two_distinct_mut(id1, id2);
        (a.unwrap(), b.unwrap())
    }
    pub fn three_distinct_mut(
        &mut self,
        id1: I,
        id2: I,
        id3: I,
    ) -> (&mut T, &mut T, &mut T) {
        let (a, b, c) = self.get_three_distinct_mut(id1, id2, id3);
        (a.unwrap(), b.unwrap(), c.unwrap())
    }
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
    pub fn next_index_phys(&self) -> I {
        I::from_usize(self.data.len())
    }
}

impl<'a, I: IndexingType, T, const CAP: usize>
    UniverseMultiRefMutHandout<'a, I, T, CAP>
{
    pub fn claim(&mut self, i: I) -> &'a mut T {
        assert!(!self.handed_out.contains(&i));
        self.handed_out.push(i);
        // SAFETY: this type dynamically ensures that each index is handed out
        // exactly once through the assert above
        unsafe {
            std::mem::transmute::<&'_ mut T, &'a mut T>(&mut self.universe[i])
        }
    }

    pub fn claim_new(&mut self, value: T) -> (I, &'a mut T) {
        let id = self.universe.claim_with_value(value);
        (id, self.claim(id))
    }
}

// separate impl since only available if T: Default
impl<I: IndexingType, T: Default> Universe<I, T> {
    pub fn claim(&mut self) -> I {
        self.claim_with(Default::default)
    }
    pub fn reserve_id(&mut self, id: I) {
        self.reserve_id_with(id, Default::default);
    }
}

impl<I: IndexingType, T> Index<I> for Universe<I, T> {
    type Output = T;
    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        match &self.data[index.into_usize()] {
            UniverseEntry::Occupied(v) => v,
            UniverseEntry::Vacant(_) => panic!("index out of bounds"),
        }
    }
}

impl<I: IndexingType, T> IndexMut<I> for Universe<I, T> {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        match &mut self.data[index.into_usize()] {
            UniverseEntry::Occupied(v) => v,
            UniverseEntry::Vacant(_) => panic!("index out of bounds"),
        }
    }
}

#[derive(Clone)]
pub struct UniverseIter<'a, T> {
    base: std::slice::Iter<'a, UniverseEntry<T>>,
}

#[derive(Clone)]
pub struct UniverseIndexIter<'a, I, T> {
    index: I,
    base: std::slice::Iter<'a, UniverseEntry<T>>,
}

impl<'a, T> Iterator for UniverseIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.base.next() {
                Some(UniverseEntry::Occupied(v)) => return Some(v),
                Some(UniverseEntry::Vacant(_)) => continue,
                None => return None,
            }
        }
    }
}

impl<'a, I: IndexingType, T> Iterator for UniverseIndexIter<'a, I, T> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.base.next()?;
            let res = self.index;
            self.index = I::from_usize(res.into_usize() + 1);
            if matches!(next, UniverseEntry::Vacant(_)) {
                continue;
            }
            return Some(res);
        }
    }
}

pub struct UniverseIterMut<'a, T> {
    base: std::slice::IterMut<'a, UniverseEntry<T>>,
}

impl<'a, T> Iterator for UniverseIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.base.next() {
                Some(UniverseEntry::Occupied(v)) => return Some(v),
                Some(UniverseEntry::Vacant(_)) => continue,
                None => return None,
            }
        }
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a Universe<I, T> {
    type Item = &'a T;
    type IntoIter = UniverseIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a mut Universe<I, T> {
    type Item = &'a mut T;
    type IntoIter = UniverseIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

#[derive(Clone)]
pub struct UniverseEnumeratedIter<'a, I, T> {
    base: &'a [UniverseEntry<T>],
    idx: I,
}

impl<'a, I: IndexingType, T> Iterator for UniverseEnumeratedIter<'a, I, T> {
    type Item = (I, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.idx.into_usize()..self.base.len() {
            let idx = self.idx;
            self.idx = I::from_usize(i + 1);
            match &self.base[i] {
                UniverseEntry::Occupied(v) => return Some((idx, v)),
                UniverseEntry::Vacant(_) => continue,
            }
        }
        None
    }
}

pub struct UniverseEnumeratedIterMut<'a, I, T> {
    base: &'a mut [UniverseEntry<T>],
    idx: I,
}

impl<'a, I: IndexingType, T> Iterator for UniverseEnumeratedIterMut<'a, I, T> {
    type Item = (I, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.idx.into_usize()..self.base.len() {
            let idx = self.idx;
            self.idx = I::from_usize(i + 1);
            match &self.base[i] {
                UniverseEntry::Occupied(_) => {
                    // SAFETY: the iterator makes sure that each element
                    // is only handed out once
                    let v = unsafe { &mut *self.base.as_mut_ptr().add(i) };
                    let UniverseEntry::Occupied(v) = v else {
                        unreachable!()
                    };
                    return Some((idx, v));
                }
                UniverseEntry::Vacant(_) => continue,
            }
        }
        None
    }
}

#[derive(Clone)]
pub struct CountedUniverse<I, T> {
    universe: Universe<I, T>,
    occupied_entries: usize,
}

impl<I: IndexingType, T> CountedUniverse<I, T> {
    pub const fn new() -> Self {
        Self {
            universe: Universe::new(),
            occupied_entries: 0,
        }
    }
    pub fn release(&mut self, id: I) {
        self.occupied_entries -= 1;
        self.universe.release(id)
    }
    pub fn used_capacity(&mut self) -> usize {
        self.universe.used_capacity()
    }
    pub fn clear(&mut self) {
        self.universe.clear();
        self.occupied_entries = 0;
    }
    pub fn indices(&self) -> UniverseIndexIter<I, T> {
        self.universe.indices()
    }
    pub fn iter(&self) -> UniverseIter<T> {
        self.universe.iter()
    }
    pub fn iter_mut(&mut self) -> UniverseIterMut<T> {
        self.universe.iter_mut()
    }
    pub fn iter_enumerated(&self) -> UniverseEnumeratedIter<I, T> {
        self.universe.iter_enumerated()
    }
    pub fn iter_enumerated_mut(&mut self) -> UniverseEnumeratedIterMut<I, T> {
        self.universe.iter_enumerated_mut()
    }
    pub fn any_used(&mut self) -> Option<&mut T> {
        self.universe.any_used()
    }
    pub fn reserve_id_with(&mut self, id: I, func: impl FnOnce() -> T) {
        self.universe.reserve_id_with(id, func)
    }
    pub fn peek_claim_id(&self) -> I {
        self.universe.peek_claim_id()
    }
    pub fn claim_with(&mut self, f: impl FnOnce() -> T) -> I {
        self.occupied_entries += 1;
        self.universe.claim_with(f)
    }
    pub fn claim_with_value(&mut self, value: T) -> I {
        self.claim_with(|| value)
    }
    pub fn calc_id(&self, entry: &T) -> I {
        self.universe.calc_id(entry)
    }
    pub fn get(&self, id: I) -> Option<&T> {
        self.universe.get(id)
    }
    pub fn get_mut(&mut self, id: I) -> Option<&mut T> {
        self.universe.get_mut(id)
    }
    pub fn get_two_distinct_mut(
        &mut self,
        id1: I,
        id2: I,
    ) -> (Option<&mut T>, Option<&mut T>) {
        self.universe.get_two_distinct_mut(id1, id2)
    }
    pub fn two_distinct_mut(&mut self, id1: I, id2: I) -> (&mut T, &mut T) {
        self.universe.two_distinct_mut(id1, id2)
    }
    pub fn is_empty(&self) -> bool {
        self.occupied_entries == 0
    }
    pub fn occupied_entry_count(&self) -> usize {
        self.occupied_entries
    }
    pub fn next_index_phys(&self) -> I {
        self.universe.next_index_phys()
    }
}

// autoderiving this currently fails on stable
impl<I: IndexingType, T> Default for CountedUniverse<I, T> {
    fn default() -> Self {
        Self::new()
    }
}

// separate impl since only available if T: Default
impl<I: IndexingType, T: Default> CountedUniverse<I, T> {
    pub fn claim(&mut self) -> I {
        self.claim_with(Default::default)
    }
    pub fn reserve_id(&mut self, id: I) {
        self.reserve_id_with(id, Default::default);
    }
}

impl<I: IndexingType, T> Index<I> for CountedUniverse<I, T> {
    type Output = T;
    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        self.universe.index(index)
    }
}

impl<I: IndexingType, T> IndexMut<I> for CountedUniverse<I, T> {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.universe.index_mut(index)
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a CountedUniverse<I, T> {
    type Item = &'a T;
    type IntoIter = UniverseIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, I: IndexingType, T> IntoIterator for &'a mut CountedUniverse<I, T> {
    type Item = &'a mut T;
    type IntoIter = UniverseIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<I: IndexingType, T, II: IntoIterator<Item = T>> From<II>
    for Universe<I, T>
{
    fn from(ii: II) -> Self {
        let mut u = Universe::default();
        for i in ii {
            u.claim_with_value(i);
        }
        u
    }
}

impl<I: IndexingType, T> TransmutableContainer for Universe<I, T> {
    type ElementType = T;

    type ContainerType<Q> = Universe<I, Q>;

    fn transmute<Q>(
        self,
    ) -> <Self as TransmutableContainer>::ContainerType<Q> {
        Universe {
            data: self.data.transmute(),
            first_vacant_entry: None,
            _phantom_data: PhantomData,
        }
    }

    fn transmute_from<Q>(
        src: <Self as TransmutableContainer>::ContainerType<Q>,
    ) -> Self {
        Self {
            data: src.data.transmute(),
            first_vacant_entry: None,
            _phantom_data: PhantomData,
        }
    }
}
