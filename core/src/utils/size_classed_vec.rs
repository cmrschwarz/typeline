use std::{marker::PhantomData, ops::Range};

pub enum SizeClassedVec {
    Sc8(Vec<u8>),
    Sc16(Vec<u16>),
    Sc32(Vec<u32>),
    Sc64(Vec<u64>),
}

impl Default for SizeClassedVec {
    fn default() -> Self {
        SizeClassedVec::Sc8(Vec::new())
    }
}

impl SizeClassedVec {
    fn required_size_class_for_value(value: usize) -> u32 {
        (usize::BITS - value.leading_zeros() + 7) / 8 * 8
    }
    pub fn promote_to_size_class_of_value(&mut self, value: usize) {
        self.promote_to_size_class(Self::required_size_class_for_value(value));
    }
    pub fn get(&self, index: usize) -> usize {
        match self {
            SizeClassedVec::Sc8(v) => v[index] as usize,
            SizeClassedVec::Sc16(v) => v[index] as usize,
            SizeClassedVec::Sc32(v) => v[index] as usize,
            SizeClassedVec::Sc64(v) => v[index] as usize,
        }
    }
    pub fn try_get(&self, index: usize) -> Option<usize> {
        match self {
            SizeClassedVec::Sc8(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVec::Sc16(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVec::Sc32(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVec::Sc64(v) => v.get(index).map(|v| *v as usize),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            SizeClassedVec::Sc8(v) => v.len(),
            SizeClassedVec::Sc16(v) => v.len(),
            SizeClassedVec::Sc32(v) => v.len(),
            SizeClassedVec::Sc64(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn size_class(&self) -> usize {
        match self {
            SizeClassedVec::Sc8(_) => 8,
            SizeClassedVec::Sc16(_) => 16,
            SizeClassedVec::Sc32(_) => 32,
            SizeClassedVec::Sc64(_) => 64,
        }
    }
    pub fn size_class_max(&self) -> usize {
        match self {
            SizeClassedVec::Sc8(_) => u8::MAX as usize,
            SizeClassedVec::Sc16(_) => u16::MAX as usize,
            SizeClassedVec::Sc32(_) => u32::MAX as usize,
            SizeClassedVec::Sc64(_) => u64::MAX as usize,
        }
    }
    pub fn set_truncated(&mut self, index: usize, value: usize) {
        match self {
            SizeClassedVec::Sc8(v) => v[index] = value as u8,
            SizeClassedVec::Sc16(v) => v[index] = value as u16,
            SizeClassedVec::Sc32(v) => v[index] = value as u32,
            SizeClassedVec::Sc64(v) => v[index] = value as u64,
        }
    }
    pub fn set(&mut self, index: usize, value: usize) {
        if value > self.size_class_max() {
            self.promote_to_size_class_of_value(value);
        }
        self.set_truncated(index, value);
    }
    pub fn map_value(
        &mut self,
        index: usize,
        f: impl Fn(usize) -> usize,
    ) -> usize {
        let res = f(self.get(index));
        self.set(index, res);
        res
    }
    pub fn add_value(&mut self, index: usize, value: usize) -> usize {
        self.map_value(index, |v| v + value)
    }
    pub fn sub_value(&mut self, index: usize, value: usize) -> usize {
        self.map_value(index, |v| v - value)
    }
    pub fn promote_to_size_class(&mut self, sc: u32) {
        match sc {
            8 => (),
            16 => match self {
                SizeClassedVec::Sc8(v) => {
                    *self = SizeClassedVec::Sc16(
                        v.iter().copied().map(u16::from).collect(),
                    );
                }
                SizeClassedVec::Sc16(_)
                | SizeClassedVec::Sc32(_)
                | SizeClassedVec::Sc64(_) => (),
            },
            32 => match self {
                SizeClassedVec::Sc8(v) => {
                    *self = SizeClassedVec::Sc32(
                        v.iter().copied().map(u32::from).collect(),
                    );
                }
                SizeClassedVec::Sc16(v) => {
                    *self = SizeClassedVec::Sc32(
                        v.iter().copied().map(u32::from).collect(),
                    );
                }
                SizeClassedVec::Sc32(_) | SizeClassedVec::Sc64(_) => (),
            },
            64 => match self {
                SizeClassedVec::Sc8(v) => {
                    *self = SizeClassedVec::Sc64(
                        v.iter().copied().map(u64::from).collect(),
                    );
                }
                SizeClassedVec::Sc16(v) => {
                    *self = SizeClassedVec::Sc64(
                        v.iter().copied().map(u64::from).collect(),
                    );
                }
                SizeClassedVec::Sc32(v) => {
                    *self = SizeClassedVec::Sc64(
                        v.iter().copied().map(u64::from).collect(),
                    );
                }
                SizeClassedVec::Sc64(_) => (),
            },
            _ => panic!("invalid size class: {sc}"),
        }
    }
    pub fn as_ptr_range(&self) -> Range<*const u8> {
        fn range2u8<T>(r: Range<*const T>) -> Range<*const u8> {
            Range {
                start: r.start as *const u8,
                end: r.end as *const u8,
            }
        }
        match self {
            SizeClassedVec::Sc8(v) => v.as_ptr_range(),
            SizeClassedVec::Sc16(v) => range2u8(v.as_ptr_range()),
            SizeClassedVec::Sc32(v) => range2u8(v.as_ptr_range()),
            SizeClassedVec::Sc64(v) => range2u8(v.as_ptr_range()),
        }
    }
    pub fn as_mut_ptr_range(&mut self) -> Range<*mut u8> {
        fn range2u8mut<T>(r: Range<*mut T>) -> Range<*mut u8> {
            Range {
                start: r.start as *mut u8,
                end: r.end as *mut u8,
            }
        }
        match self {
            SizeClassedVec::Sc8(v) => v.as_mut_ptr_range(),
            SizeClassedVec::Sc16(v) => range2u8mut(v.as_mut_ptr_range()),
            SizeClassedVec::Sc32(v) => range2u8mut(v.as_mut_ptr_range()),
            SizeClassedVec::Sc64(v) => range2u8mut(v.as_mut_ptr_range()),
        }
    }
    pub fn iter(&self) -> Iter {
        let range = self.as_ptr_range();
        let stride = (self.size_class() / 8) as u8;
        Iter {
            ptr: range.start,
            end: range.end,
            stride,
            _phantom_data: PhantomData,
        }
    }
}

impl<'a> IntoIterator for &'a SizeClassedVec {
    type Item = usize;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct Iter<'a> {
    ptr: *const u8,
    end: *const u8,
    stride: u8,
    _phantom_data: PhantomData<&'a usize>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr == self.end {
            return None;
        }
        let res = unsafe {
            match self.stride {
                1 => *(self.ptr as *const u8) as usize,
                2 => *(self.ptr as *const u16) as usize,
                4 => *(self.ptr as *const u32) as usize,
                8 => *(self.ptr as *const u64) as usize,
                _ => std::hint::unreachable_unchecked(),
            }
        };
        unsafe {
            self.ptr = self.ptr.add(self.stride as usize);
        }
        Some(res)
    }
}
