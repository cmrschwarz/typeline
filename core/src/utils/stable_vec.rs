use std::{
    alloc::Layout,
    cell::Cell,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Index, IndexMut},
    ptr::NonNull,
};

pub const CHUNK_SIZE: usize = 32;

type Chunk<T> = [MaybeUninit<T>; CHUNK_SIZE];

// !Sync (enforced by NonNull), because we have interior mutabiltiy
pub struct StableVec<T> {
    data: Cell<NonNull<*mut Chunk<T>>>,
    chunk_capacity: Cell<usize>,
    len: Cell<usize>,
}

unsafe impl<T: Send> Send for StableVec<T> {}

impl<T> Default for StableVec<T> {
    fn default() -> Self {
        Self {
            data: Cell::new(NonNull::dangling()),
            chunk_capacity: Cell::new(if Self::holds_zsts() {
                usize::MAX / CHUNK_SIZE
            } else {
                0
            }),
            len: Cell::new(0),
        }
    }
}

impl<T> StableVec<T> {
    fn holds_zsts() -> bool {
        std::mem::size_of::<T>() == 0
    }
    fn chunk_layout() -> Layout {
        Layout::new::<Chunk<T>>()
    }
    fn layout(chunk_capacity: usize) -> Layout {
        Layout::array::<Chunk<T>>(chunk_capacity).unwrap()
    }
    pub fn with_capacity(cap: usize) -> Self {
        let v = Self::default();
        v.reserve(cap);
        v
    }
    pub fn capacity(&self) -> usize {
        self.chunk_capacity.get() * CHUNK_SIZE
    }
    pub fn len(&self) -> usize {
        self.len.get()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn reserve(&self, cap: usize) {
        let total_cap = cap + self.len.get();
        if total_cap <= self.capacity() {
            return;
        }
        let chunk_count_new = if total_cap == self.capacity() {
            self.chunk_capacity.get() * 2
        } else {
            total_cap.next_power_of_two().div_ceil(CHUNK_SIZE)
        };
        if Self::holds_zsts() {
            self.chunk_capacity.set(chunk_count_new);
            return;
        }
        unsafe {
            let data = if self.chunk_capacity.get() == 0 {
                std::alloc::alloc(Self::layout(self.chunk_capacity.get()))
            } else {
                std::alloc::realloc(
                    self.data.get().as_ptr() as *mut u8,
                    Self::layout(self.chunk_capacity.get()),
                    chunk_count_new,
                )
            } as *mut *mut Chunk<T>;

            for i in self.chunk_capacity.get()..chunk_count_new {
                data.add(i)
                    .write(std::alloc::alloc(Self::chunk_layout())
                        as *mut Chunk<T>);
            }
            self.data.set(NonNull::new_unchecked(data));
        }
        self.chunk_capacity.set(chunk_count_new);
    }
    pub fn push(&self, v: T) {
        let chunk_index = self.len.get() / CHUNK_SIZE;
        let offset = self.len.get() % CHUNK_SIZE;
        if chunk_index == self.chunk_capacity.get() {
            self.reserve(1)
        }
        unsafe {
            self.get_chunk_ptr_unchecked(chunk_index)
                .add(offset)
                .write(v);
        }
        self.len.set(self.len.get() + 1);
    }
    pub unsafe fn get_chunk_ptr_unchecked(&self, chunk_idx: usize) -> *mut T {
        unsafe { (*self.data.get().as_ptr().add(chunk_idx)) as *mut T }
    }
    pub unsafe fn get_element_pointer_unchecked(
        &self,
        index: usize,
    ) -> *mut T {
        unsafe {
            self.get_chunk_ptr_unchecked(index / CHUNK_SIZE)
                .add(index % CHUNK_SIZE)
        }
    }
    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        unsafe { &*self.get_element_pointer_unchecked(index) }
    }
    pub unsafe fn get_unchecked_mut(&mut self, index: usize) -> &mut T {
        unsafe { &mut *self.get_element_pointer_unchecked(index) }
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        if self.len() <= index {
            return None;
        }
        unsafe { Some(self.get_unchecked(index)) }
    }
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if self.len() <= index {
            return None;
        }
        unsafe { Some(self.get_unchecked_mut(index)) }
    }
    pub fn last(&self) -> Option<&T> {
        self.get(self.len().saturating_sub(1))
    }
    pub fn last_mut(&mut self) -> Option<&mut T> {
        self.get_mut(self.len().saturating_sub(1))
    }
    pub fn first(&self) -> Option<&T> {
        self.get(0)
    }
    pub fn first_mut(&mut self) -> Option<&mut T> {
        self.get_mut(0)
    }
}

impl<T> Drop for StableVec<T> {
    fn drop(&mut self) {
        if Self::holds_zsts() || self.chunk_capacity.get() == 0 {
            return;
        }
        if std::mem::needs_drop::<T>() {
            let full_chunks = self.len.get() / CHUNK_SIZE;
            let remainder = self.len.get() % CHUNK_SIZE;
            unsafe {
                for i in 0..full_chunks {
                    std::ptr::drop_in_place(self.get_chunk_ptr_unchecked(i)
                        as *mut [T; CHUNK_SIZE]);
                }
                if remainder > 0 {
                    std::ptr::drop_in_place(
                        std::ptr::slice_from_raw_parts_mut(
                            self.get_chunk_ptr_unchecked(full_chunks),
                            remainder,
                        ),
                    );
                }
            }
        }
        unsafe {
            std::alloc::dealloc(
                self.data.get().as_ptr() as *mut _,
                Self::layout(self.chunk_capacity.get()),
            );
        }
    }
}

impl<T> Index<usize> for StableVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}
impl<T> IndexMut<usize> for StableVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

pub struct StableVecIter<'a, T> {
    vec: &'a StableVec<T>,
    pos: usize,
}

impl<'a, T> StableVecIter<'a, T> {
    pub fn new(vec: &'a StableVec<T>) -> Self {
        Self { vec, pos: 0 }
    }
}

impl<'a, T> Iterator for StableVecIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.pos;
        if idx == self.vec.len() {
            return None;
        }
        self.pos += 1;
        unsafe { Some(self.vec.get_unchecked(idx)) }
    }
}

impl<'a, T> IntoIterator for &'a StableVec<T> {
    type Item = &'a T;
    type IntoIter = StableVecIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        StableVecIter::new(self)
    }
}

pub struct StableVecIterMut<'a, T> {
    vec: &'a mut StableVec<T>,
    pos: usize,
}

impl<'a, T> StableVecIterMut<'a, T> {
    pub fn new(vec: &'a mut StableVec<T>) -> Self {
        Self { vec, pos: 0 }
    }
}

impl<'a, T> Iterator for StableVecIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.pos;
        if idx == self.vec.len() {
            return None;
        }
        self.pos += 1;
        unsafe { Some(&mut *self.vec.get_element_pointer_unchecked(idx)) }
    }
}

impl<'a, T> IntoIterator for &'a mut StableVec<T> {
    type Item = &'a mut T;
    type IntoIter = StableVecIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        StableVecIterMut::new(self)
    }
}

pub struct StableVecIntoIter<T> {
    vec: StableVec<T>,
    pos: usize,
}

impl<T> StableVecIntoIter<T> {
    pub fn new(vec: StableVec<T>) -> Self {
        Self { vec, pos: 0 }
    }
    unsafe fn drop_remaining_elements(&self) {
        if !std::mem::needs_drop::<T>() {
            return;
        }
        let pos = self.pos;
        let len = self.vec.len();
        if pos == len {
            return;
        }
        let curr_chunk_rem = CHUNK_SIZE.min(len - pos) - pos % CHUNK_SIZE;
        let curr_chunk_end = pos + curr_chunk_rem;
        let first_full_chunk = curr_chunk_end / CHUNK_SIZE;
        let full_chunks = (len - curr_chunk_end) / CHUNK_SIZE;
        let full_chunks_end = curr_chunk_end + full_chunks * CHUNK_SIZE;
        let last_chunk_rem = len - full_chunks_end;
        unsafe {
            std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                self.vec.get_element_pointer_unchecked(self.pos),
                curr_chunk_rem,
            ));
            for i in first_full_chunk..first_full_chunk + full_chunks {
                std::ptr::drop_in_place(self.vec.get_chunk_ptr_unchecked(i)
                    as *mut [T; CHUNK_SIZE]);
            }
            if last_chunk_rem > 0 {
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                    self.vec.get_element_pointer_unchecked(full_chunks_end),
                    last_chunk_rem,
                ));
            }
        }
    }
    pub fn into_empty_vec(self) -> StableVec<T> {
        unsafe { self.drop_remaining_elements() };
        let vec = unsafe { std::ptr::read(&self.vec as *const StableVec<T>) };
        vec.len.set(0);
        let _ = ManuallyDrop::new(self);
        vec
    }
}

impl<T> IntoIterator for StableVec<T> {
    type Item = T;
    type IntoIter = StableVecIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        StableVecIntoIter::new(self)
    }
}

impl<T> Iterator for StableVecIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.pos;
        if idx == self.vec.len() {
            return None;
        }
        self.pos += 1;
        unsafe {
            Some(std::ptr::read(self.vec.get_element_pointer_unchecked(idx)))
        }
    }
}

impl<T> Drop for StableVecIntoIter<T> {
    fn drop(&mut self) {
        if StableVec::<T>::holds_zsts() {
            return;
        }

        unsafe {
            std::alloc::dealloc(
                self.vec.data.get().as_ptr() as *mut _,
                StableVec::<T>::layout(self.vec.chunk_capacity.get()),
            );
        }
    }
}
