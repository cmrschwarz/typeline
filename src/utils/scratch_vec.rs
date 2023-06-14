use num::Integer;
use std::mem::{align_of, size_of, ManuallyDrop};
use std::ops::{Deref, DerefMut};

pub struct ScratchVec<'a, M, T> {
    memory_source: &'a mut Vec<M>,
    data: ManuallyDrop<Vec<T>>,
}

impl<M, T> Deref for ScratchVec<'_, M, T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<M, T> DerefMut for ScratchVec<'_, M, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<M, T> Drop for ScratchVec<'_, M, T> {
    fn drop(&mut self) {
        self.data.clear();
        *self.memory_source = unsafe {
            Vec::from_raw_parts(
                self.data.as_mut_ptr() as *mut M,
                0,
                self.data.capacity() * size_of::<T>() / size_of::<M>(),
            )
        };
    }
}

impl<'a, M, T> ScratchVec<'a, M, T> {
    pub fn new(memory_source: &'a mut Vec<M>) -> ScratchVec<'a, M, T> {
        assert!(
            align_of::<M>() == align_of::<T>(),
            "incorrect memory alignment for ScratchVec"
        );
        let source_buf_size = memory_source.capacity() * size_of::<M>();
        assert!(
            source_buf_size.is_multiple_of(&size_of::<T>()),
            "incompatible type sizes for ScratchVec"
        );
        memory_source.clear();
        let mut source_temp = ManuallyDrop::new(std::mem::replace(&mut *memory_source, Vec::new()));
        let data = unsafe {
            ManuallyDrop::new(Vec::from_raw_parts(
                source_temp.as_mut_ptr() as *mut T,
                0,
                source_buf_size / size_of::<T>(),
            ))
        };
        ScratchVec {
            memory_source,
            data,
        }
    }
}
