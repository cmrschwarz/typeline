use std::{
    collections::VecDeque,
    fmt::Debug,
    marker::PhantomData,
    ops::{Range, RangeBounds},
};

use super::pointer_range_len;

pub enum SizeClassedVecDeque {
    Sc8(VecDeque<u8>),
    Sc16(VecDeque<u16>),
    Sc32(VecDeque<u32>),
    Sc64(VecDeque<u64>),
}

impl Default for SizeClassedVecDeque {
    fn default() -> Self {
        SizeClassedVecDeque::Sc8(VecDeque::new())
    }
}

impl SizeClassedVecDeque {
    fn required_size_class_for_value(value: usize) -> u32 {
        let bytes = (usize::BITS - value.max(1).leading_zeros() + 7) / 8;
        match bytes {
            1 => 8,
            2 => 16,
            3 | 4 => 32,
            #[allow(clippy::manual_range_patterns)]
            // manual range leads to better performance (optimized as lookup
            // table) see https://godbolt.org/z/88K5M9hjo
            5 | 6 | 7 | 8 => 64,
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }
    pub fn promote_to_size_class_of_value(&mut self, value: usize) {
        self.promote_to_size_class(Self::required_size_class_for_value(value));
    }
    pub unsafe fn get_unchecked(&self, index: usize) -> usize {
        unsafe {
            match self {
                SizeClassedVecDeque::Sc8(v) => {
                    *v.get(index).unwrap_unchecked() as usize
                }
                SizeClassedVecDeque::Sc16(v) => {
                    *v.get(index).unwrap_unchecked() as usize
                }
                SizeClassedVecDeque::Sc32(v) => {
                    *v.get(index).unwrap_unchecked() as usize
                }
                SizeClassedVecDeque::Sc64(v) => {
                    *v.get(index).unwrap_unchecked() as usize
                }
            }
        }
    }
    pub fn try_get(&self, index: usize) -> Option<usize> {
        match self {
            SizeClassedVecDeque::Sc8(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVecDeque::Sc16(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVecDeque::Sc32(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVecDeque::Sc64(v) => v.get(index).map(|v| *v as usize),
        }
    }
    pub fn get(&self, index: usize) -> usize {
        match self {
            SizeClassedVecDeque::Sc8(v) => v[index] as usize,
            SizeClassedVecDeque::Sc16(v) => v[index] as usize,
            SizeClassedVecDeque::Sc32(v) => v[index] as usize,
            SizeClassedVecDeque::Sc64(v) => v[index] as usize,
        }
    }
    pub fn first(&self) -> Option<usize> {
        self.try_get(0)
    }
    pub fn last(&self) -> Option<usize> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        Some(unsafe { self.get_unchecked(len - 1) })
    }
    pub fn len(&self) -> usize {
        match self {
            SizeClassedVecDeque::Sc8(v) => v.len(),
            SizeClassedVecDeque::Sc16(v) => v.len(),
            SizeClassedVecDeque::Sc32(v) => v.len(),
            SizeClassedVecDeque::Sc64(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn size_class(&self) -> u32 {
        match self {
            SizeClassedVecDeque::Sc8(_) => 8,
            SizeClassedVecDeque::Sc16(_) => 16,
            SizeClassedVecDeque::Sc32(_) => 32,
            SizeClassedVecDeque::Sc64(_) => 64,
        }
    }
    pub fn size_class_max(&self) -> usize {
        match self {
            SizeClassedVecDeque::Sc8(_) => u8::MAX as usize,
            SizeClassedVecDeque::Sc16(_) => u16::MAX as usize,
            SizeClassedVecDeque::Sc32(_) => u32::MAX as usize,
            SizeClassedVecDeque::Sc64(_) => u64::MAX as usize,
        }
    }
    pub fn set_truncated(&mut self, index: usize, value: usize) {
        match self {
            SizeClassedVecDeque::Sc8(v) => v[index] = value as u8,
            SizeClassedVecDeque::Sc16(v) => v[index] = value as u16,
            SizeClassedVecDeque::Sc32(v) => v[index] = value as u32,
            SizeClassedVecDeque::Sc64(v) => v[index] = value as u64,
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
                SizeClassedVecDeque::Sc8(v) => {
                    *self = SizeClassedVecDeque::Sc16(
                        v.iter().copied().map(u16::from).collect(),
                    );
                }
                SizeClassedVecDeque::Sc16(_)
                | SizeClassedVecDeque::Sc32(_)
                | SizeClassedVecDeque::Sc64(_) => (),
            },
            32 => match self {
                SizeClassedVecDeque::Sc8(v) => {
                    *self = SizeClassedVecDeque::Sc32(
                        v.iter().copied().map(u32::from).collect(),
                    );
                }
                SizeClassedVecDeque::Sc16(v) => {
                    *self = SizeClassedVecDeque::Sc32(
                        v.iter().copied().map(u32::from).collect(),
                    );
                }
                SizeClassedVecDeque::Sc32(_)
                | SizeClassedVecDeque::Sc64(_) => (),
            },
            64 => match self {
                SizeClassedVecDeque::Sc8(v) => {
                    *self = SizeClassedVecDeque::Sc64(
                        v.iter().copied().map(u64::from).collect(),
                    );
                }
                SizeClassedVecDeque::Sc16(v) => {
                    *self = SizeClassedVecDeque::Sc64(
                        v.iter().copied().map(u64::from).collect(),
                    );
                }
                SizeClassedVecDeque::Sc32(v) => {
                    *self = SizeClassedVecDeque::Sc64(
                        v.iter().copied().map(u64::from).collect(),
                    );
                }
                SizeClassedVecDeque::Sc64(_) => (),
            },
            _ => panic!("invalid size class: {sc}"),
        }
    }
    pub fn as_ptr_ranges(&self) -> (Range<*const u8>, Range<*const u8>) {
        fn slices2ranges<T>(
            slices: (&[T], &[T]),
        ) -> (Range<*const u8>, Range<*const u8>) {
            (
                Range {
                    start: slices.0.as_ptr().cast(),
                    end: slices.0.as_ptr_range().end.cast(),
                },
                Range {
                    start: slices.1.as_ptr().cast(),
                    end: slices.1.as_ptr_range().end.cast(),
                },
            )
        }
        match self {
            SizeClassedVecDeque::Sc8(v) => slices2ranges(v.as_slices()),
            SizeClassedVecDeque::Sc16(v) => slices2ranges(v.as_slices()),
            SizeClassedVecDeque::Sc32(v) => slices2ranges(v.as_slices()),
            SizeClassedVecDeque::Sc64(v) => slices2ranges(v.as_slices()),
        }
    }
    pub fn as_mut_ptr_ranges(&mut self) -> (Range<*mut u8>, Range<*mut u8>) {
        #[allow(clippy::needless_pass_by_value)]
        fn slices2ranges<T>(
            slices: (&mut [T], &mut [T]),
        ) -> (Range<*mut u8>, Range<*mut u8>) {
            (
                Range {
                    start: slices.0.as_mut_ptr().cast(),
                    end: slices.0.as_mut_ptr_range().end.cast(),
                },
                Range {
                    start: slices.1.as_mut_ptr().cast(),
                    end: slices.1.as_mut_ptr_range().end.cast(),
                },
            )
        }
        match self {
            SizeClassedVecDeque::Sc8(v) => slices2ranges(v.as_mut_slices()),
            SizeClassedVecDeque::Sc16(v) => slices2ranges(v.as_mut_slices()),
            SizeClassedVecDeque::Sc32(v) => slices2ranges(v.as_mut_slices()),
            SizeClassedVecDeque::Sc64(v) => slices2ranges(v.as_mut_slices()),
        }
    }
    pub fn iter(&self) -> Iter {
        let (range_1, range_2) = self.as_ptr_ranges();
        let stride = (self.size_class() / 8) as u8;
        Iter {
            range_1,
            range_2,
            stride,
            _phantom_data: PhantomData,
        }
    }
    pub fn push_back_truncated(&mut self, value: usize) {
        match self {
            SizeClassedVecDeque::Sc8(v) => v.push_back(value as u8),
            SizeClassedVecDeque::Sc16(v) => v.push_back(value as u16),
            SizeClassedVecDeque::Sc32(v) => v.push_back(value as u32),
            SizeClassedVecDeque::Sc64(v) => v.push_back(value as u64),
        }
    }
    pub fn push_back(&mut self, value: usize) {
        if value > self.size_class_max() {
            self.promote_to_size_class_of_value(value);
        }
        self.push_back_truncated(value);
    }
    pub fn push_front_truncated(&mut self, value: usize) {
        match self {
            SizeClassedVecDeque::Sc8(v) => v.push_front(value as u8),
            SizeClassedVecDeque::Sc16(v) => v.push_front(value as u16),
            SizeClassedVecDeque::Sc32(v) => v.push_front(value as u32),
            SizeClassedVecDeque::Sc64(v) => v.push_front(value as u64),
        }
    }
    pub fn push_front(&mut self, value: usize) {
        if value > self.size_class_max() {
            self.promote_to_size_class_of_value(value);
        }
        self.push_front_truncated(value);
    }
    pub fn drain(&mut self, range: impl RangeBounds<usize>) {
        match self {
            SizeClassedVecDeque::Sc8(v) => {
                v.drain(range);
            }
            SizeClassedVecDeque::Sc16(v) => {
                v.drain(range);
            }
            SizeClassedVecDeque::Sc32(v) => {
                v.drain(range);
            }
            SizeClassedVecDeque::Sc64(v) => {
                v.drain(range);
            }
        }
    }
    pub fn extend(&mut self, values: impl Iterator<Item = usize> + Clone) {
        self.promote_to_size_class_of_value(values.clone().max().unwrap_or(0));
        self.extend_truncated(values);
    }
    pub fn extend_truncated(&mut self, values: impl Iterator<Item = usize>) {
        match self {
            SizeClassedVecDeque::Sc8(v) => v.extend(values.map(|v| v as u8)),
            SizeClassedVecDeque::Sc16(v) => v.extend(values.map(|v| v as u16)),
            SizeClassedVecDeque::Sc32(v) => v.extend(values.map(|v| v as u32)),
            SizeClassedVecDeque::Sc64(v) => v.extend(values.map(|v| v as u64)),
        }
    }
}

impl<'a> IntoIterator for &'a SizeClassedVecDeque {
    type Item = usize;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct Iter<'a> {
    range_1: Range<*const u8>,
    range_2: Range<*const u8>,
    stride: u8,
    _phantom_data: PhantomData<&'a usize>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range_1.start == self.range_1.end {
            if self.range_2.is_empty() {
                return None;
            }
            std::mem::swap(&mut self.range_1, &mut self.range_2);
        }
        let res = unsafe {
            #[allow(clippy::cast_ptr_alignment)]
            match self.stride {
                1 => *(self.range_1.start.cast::<u8>()) as usize,
                2 => *(self.range_1.start.cast::<u16>()) as usize,
                4 => *(self.range_1.start.cast::<u32>()) as usize,
                8 => *(self.range_1.start.cast::<u64>()) as usize,
                _ => std::hint::unreachable_unchecked(),
            }
        };
        unsafe {
            self.range_1.start = self.range_1.start.add(self.stride as usize);
        }
        Some(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {
    fn len(&self) -> usize {
        let size = pointer_range_len(&self.range_1)
            + pointer_range_len(&self.range_2);
        let shift = self.stride.ilog2() as usize;
        size >> shift
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.range_2.end == self.range_2.start {
            if self.range_1.is_empty() {
                return None;
            }
            std::mem::swap(&mut self.range_1, &mut self.range_2);
        }
        unsafe {
            self.range_2.end = self.range_2.end.sub(self.stride as usize);
        }
        let res = unsafe {
            #[allow(clippy::cast_ptr_alignment)]
            match self.stride {
                1 => *(self.range_2.end.cast::<u8>()) as usize,
                2 => *(self.range_2.end.cast::<u16>()) as usize,
                4 => *(self.range_2.end.cast::<u32>()) as usize,
                8 => *(self.range_2.end.cast::<u64>()) as usize,
                _ => std::hint::unreachable_unchecked(),
            }
        };
        Some(res)
    }
}

impl Debug for SizeClassedVecDeque {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sc8(v) => Debug::fmt(v, f),
            Self::Sc16(v) => Debug::fmt(v, f),
            Self::Sc32(v) => Debug::fmt(v, f),
            Self::Sc64(v) => Debug::fmt(v, f),
        }
    }
}
