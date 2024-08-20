use arrayvec::ArrayVec;

use super::{index_slice::IndexSlice, indexing_type::IndexingType};

pub struct MultiRefMutHandout<'a, I, T, const CAP: usize = 2> {
    data: &'a mut IndexSlice<I, T>,
    handed_out: ArrayVec<I, CAP>,
}

impl<'a, I: IndexingType, T, const CAP: usize>
    MultiRefMutHandout<'a, I, T, CAP>
{
    pub fn new(data: &'a mut IndexSlice<I, T>) -> Self {
        Self {
            data,
            handed_out: ArrayVec::new(),
        }
    }

    pub fn claim(&mut self, i: I) -> &'a mut T {
        assert!(!self.handed_out.contains(&i));
        self.handed_out.push(i);
        // SAFETY: this type dynamically ensures that each index is handed out
        // exactly once through the assert above
        unsafe {
            std::mem::transmute::<&'_ mut T, &'a mut T>(&mut self.data[i])
        }
    }
}
