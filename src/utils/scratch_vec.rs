use num::Integer;
use std::mem::{align_of, size_of, ManuallyDrop};
use std::ops::{Deref, DerefMut};

pub struct ScratchVec<'a, M, T> {
    memory_source: &'a mut Vec<M>,
    data: Vec<T>,
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
        // we give back the vector we have stolen
        // (unless we were into'd, then we just give back a zero capacity vector)
        *self.memory_source = repurpose_vec(&mut self.data);
    }
}

impl<'a, M, T> Into<Vec<T>> for ScratchVec<'a, M, T> {
    fn into(mut self) -> Vec<T> {
        let vec = std::mem::replace::<Vec<T>>(self.data.as_mut(), Default::default());
        vec
    }
}

impl<'a, M, T> ScratchVec<'a, M, T> {
    pub fn new(memory_source: &'a mut Vec<M>) -> ScratchVec<'a, M, T> {
        let data = repurpose_vec(memory_source);
        ScratchVec {
            memory_source,
            data,
        }
    }
}

pub fn repurpose_vec<M, T>(source: &mut Vec<M>) -> Vec<T> {
    assert!(
        align_of::<M>() == align_of::<T>(),
        "incorrect memory alignment for repurposed vector"
    );
    let source_buf_size = source.capacity() * size_of::<M>();
    assert!(
        source_buf_size.is_multiple_of(&size_of::<T>()),
        "incompatible type sizes for repurposed vector"
    );
    source.clear();
    let mut source_temp = ManuallyDrop::new(std::mem::replace(&mut *source, Vec::new()));
    // SAFETY: we're basically converting source_temp from Vec<M> to Vec<T>.
    // this is valid because we've asserted the invariants of Vec::from_raw_parts right before this call
    unsafe {
        Vec::from_raw_parts(
            source_temp.as_mut_ptr() as *mut T,
            0,
            source_buf_size / size_of::<T>(),
        )
    }
}
