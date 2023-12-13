use std::{alloc::Layout, cell::Cell, mem::MaybeUninit, ptr::NonNull};

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
        if total_cap < self.capacity() {
            return;
        }
        let chunk_count_new = if total_cap == self.capacity() {
            self.chunk_capacity.get() * 2
        } else {
            total_cap.next_power_of_two() / CHUNK_SIZE
        };
        unsafe {
            let data = std::alloc::realloc(
                self.data.get().as_ptr() as *mut u8,
                Self::layout(self.chunk_capacity.get()),
                chunk_count_new,
            ) as *mut *mut Chunk<T>;
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
        let index = self.len.get() / CHUNK_SIZE;
        let offset = self.len.get() % CHUNK_SIZE;
        if index == self.chunk_capacity.get() {
            self.reserve(1)
        }

        unsafe {
            (self.data.get().as_ptr().add(index).read() as *mut T)
                .add(offset)
                .write(v);
        }
        self.len.set(self.len.get() + 1);
    }
}

impl<T> Drop for StableVec<T> {
    fn drop(&mut self) {
        if Self::holds_zsts() {
            return;
        }
        let full_chunks = self.len.get() / CHUNK_SIZE;
        let remainder = self.len.get() % CHUNK_SIZE;
        unsafe {
            for i in 0..full_chunks {
                std::ptr::drop_in_place(
                    *self.data.get().as_ptr().add(i) as *mut [T; CHUNK_SIZE]
                );
            }
            if remainder > 0 {
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                    *self.data.get().as_ptr().add(full_chunks) as *mut T,
                    remainder,
                ));
            }
            std::alloc::dealloc(
                self.data.get().as_ptr() as *mut _,
                Self::layout(self.chunk_capacity.get()),
            );
        }
    }
}
