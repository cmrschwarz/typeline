use std::{
    collections::{vec_deque, VecDeque},
    fmt::Debug,
};

use metamatch::metamatch;

use super::bit_vec_deque::{self, BitVecDeque};

pub enum SizeClassedVecDeque {
    Sc1(BitVecDeque),
    Sc8(VecDeque<u8>),
    Sc16(VecDeque<u16>),
    Sc32(VecDeque<u32>),
    Sc64(VecDeque<u64>),
}

impl Default for SizeClassedVecDeque {
    fn default() -> Self {
        SizeClassedVecDeque::new()
    }
}

impl SizeClassedVecDeque {
    pub const fn new() -> Self {
        SizeClassedVecDeque::Sc1(BitVecDeque::new())
    }
    fn required_size_class_for_value(value: usize) -> u32 {
        if value < 2 {
            return 1;
        }
        let bytes = (usize::BITS - value.leading_zeros()).div_ceil(8);
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
        metamatch!(match self {
            #[expand((SC, T) in [
                (Sc8, u8), (Sc16, u16), (Sc32, u32), (Sc64, u64)
            ])]
            SizeClassedVecDeque::SC(v) => {
                let v = v.get(index);
                *unsafe { v.unwrap_unchecked() } as usize
            }
            SizeClassedVecDeque::Sc1(v) =>
                unsafe { v.get_unchecked(index) }.into(),
        })
    }
    pub fn try_get(&self, index: usize) -> Option<usize> {
        metamatch!(match self {
            #[expand(SC in [Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDeque::SC(v) => v.get(index).map(|v| *v as usize),
            SizeClassedVecDeque::Sc1(v) => v.get(index).map(usize::from),
        })
    }
    pub fn get(&self, index: usize) -> usize {
        metamatch!(match self {
            #[expand(SC in [ Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDeque::SC(v) => v[index] as usize,
            SizeClassedVecDeque::Sc1(v) => usize::from(v[index]),
        })
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
        metamatch!(match self {
            #[expand(SC in [Sc1, Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDeque::SC(v) => v.len(),
        })
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn size_class(&self) -> u32 {
        match self {
            SizeClassedVecDeque::Sc1(_) => 1,
            SizeClassedVecDeque::Sc8(_) => 8,
            SizeClassedVecDeque::Sc16(_) => 16,
            SizeClassedVecDeque::Sc32(_) => 32,
            SizeClassedVecDeque::Sc64(_) => 64,
        }
    }
    pub fn size_class_max(&self) -> usize {
        metamatch!(match self {
            #[expand((SC, T) in [
                (Sc8, u8), (Sc16, u16), (Sc32, u32), (Sc64, u64)
            ])]
            SizeClassedVecDeque::SC(_) => T::MAX as usize,
            SizeClassedVecDeque::Sc1(_) => 1,
        })
    }
    pub fn set_truncated(&mut self, index: usize, value: usize) {
        metamatch!(match self {
            #[expand((SC, T) in [
                (Sc8, u8), (Sc16, u16), (Sc32, u32), (Sc64, u64)
            ])]
            SizeClassedVecDeque::SC(v) => v[index] = value as T,
            SizeClassedVecDeque::Sc1(v) => v.set(index, value & 1 == 1),
        })
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
        metamatch!(match sc {
            1 => (),
            #[expand( (SIZE, T_TGT, SC_TGT, PROMOTE, KEEP) in [
                (8,  u8,  Sc8,  [],                [Sc8, Sc16, Sc32, Sc64]),
                (16, u16, Sc16, [Sc8],             [Sc16, Sc32, Sc64]),
                (32, u32, Sc32, [Sc8, Sc16],       [Sc32, Sc64]),
                (64, u64, Sc64, [Sc8, Sc16, Sc32], [Sc64]),
            ])]
            SIZE => metamatch!(match self {
                SizeClassedVecDeque::Sc1(v) => {
                    *self = SizeClassedVecDeque::SC_TGT(
                        v.iter().map(T_TGT::from).collect(),
                    )
                }

                #[expand(SC in PROMOTE)]
                SizeClassedVecDeque::SC(v) => {
                    *self = SizeClassedVecDeque::SC_TGT(
                        v.iter().copied().map(T_TGT::from).collect(),
                    );
                }

                #[expand_pattern(SC in KEEP)]
                SizeClassedVecDeque::SC(_) => (),
            }),
            _ => panic!("invalid size class: {sc}"),
        })
    }
    pub fn iter(&self) -> SizeClassedVecDequeIter {
        metamatch!(match self {
            #[expand(SC in [Sc1, Sc8, Sc16, Sc32, Sc64])]
            Self::SC(v) => SizeClassedVecDequeIter::SC(v.iter()),
        })
    }
    pub fn push_back_truncated(&mut self, value: usize) {
        metamatch!(match self {
            #[expand((SC, T) in [
                (Sc8, u8), (Sc16, u16), (Sc32, u32), (Sc64, u64)
            ])]
            SizeClassedVecDeque::SC(v) => v.push_back(value as T),
            SizeClassedVecDeque::Sc1(v) => v.push_back(value & 1 == 1),
        })
    }
    pub fn push_back(&mut self, value: usize) {
        if value > self.size_class_max() {
            self.promote_to_size_class_of_value(value);
        }
        self.push_back_truncated(value);
    }
    pub fn push_front_truncated(&mut self, value: usize) {
        metamatch!(match self {
            #[expand((SC, T) in [
                (Sc8, u8), (Sc16, u16), (Sc32, u32), (Sc64, u64)
            ])]
            SizeClassedVecDeque::SC(v) => v.push_front(value as T),
            SizeClassedVecDeque::Sc1(v) => v.push_front(value & 1 == 1),
        })
    }
    pub fn push_front(&mut self, value: usize) {
        if value > self.size_class_max() {
            self.promote_to_size_class_of_value(value);
        }
        self.push_front_truncated(value);
    }
    pub fn drop_front(&mut self, count: usize) {
        metamatch!(match self {
            #[expand(SC in [Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDeque::SC(v) => {
                v.drain(0..count);
            }
            SizeClassedVecDeque::Sc1(v) => v.drop_front(count),
        })
    }
    pub fn extend(
        &mut self,
        values: impl IntoIterator<IntoIter = impl Iterator<Item = usize> + Clone>,
    ) {
        let iter = values.into_iter();
        self.promote_to_size_class_of_value(iter.clone().max().unwrap_or(0));
        self.extend_truncated(iter);
    }
    pub fn extend_truncated(
        &mut self,
        values: impl IntoIterator<Item = usize>,
    ) {
        metamatch!(match self {
            #[expand((SC, T) in [
                (Sc8, u8), (Sc16, u16), (Sc32, u32), (Sc64, u64)
            ])]
            SizeClassedVecDeque::SC(v) =>
                v.extend(values.into_iter().map(|v| v as T)),

            SizeClassedVecDeque::Sc1(v) =>
                v.extend(values.into_iter().map(|v| v & 1 == 1)),
        })
    }

    // can't implement trait because we need the iterator to be cloneable
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter(
        prev_parent_groups: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
    ) -> SizeClassedVecDeque {
        let mut res = SizeClassedVecDeque::new();
        res.extend(prev_parent_groups);
        res
    }
}

impl<'a> IntoIterator for &'a SizeClassedVecDeque {
    type Item = usize;
    type IntoIter = SizeClassedVecDequeIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Clone)]
pub enum SizeClassedVecDequeIter<'a> {
    Sc1(bit_vec_deque::Iter<'a>),
    Sc8(vec_deque::Iter<'a, u8>),
    Sc16(vec_deque::Iter<'a, u16>),
    Sc32(vec_deque::Iter<'a, u32>),
    Sc64(vec_deque::Iter<'a, u64>),
}

impl<'a> Iterator for SizeClassedVecDequeIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        metamatch!(match self {
            #[expand(SC in [Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDequeIter::SC(it) => it.next().map(|v| *v as usize),
            SizeClassedVecDequeIter::Sc1(it) => it.next().map(usize::from),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        metamatch!(match self {
            #[expand(SC in [Sc1, Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDequeIter::SC(it) => it.size_hint(),
        })
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        metamatch!(match self {
            #[expand(SC in [Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDequeIter::SC(it) => it.nth(n).map(|v| *v as usize),
            SizeClassedVecDequeIter::Sc1(it) => it.nth(n).map(usize::from),
        })
    }
}

impl<'a> ExactSizeIterator for SizeClassedVecDequeIter<'a> {
    fn len(&self) -> usize {
        metamatch!(match self {
            #[expand(SC in [Sc8, Sc16, Sc32, Sc64])]
            SizeClassedVecDequeIter::SC(it) => it.len(),
            SizeClassedVecDequeIter::Sc1(it) => it.len(),
        })
    }
}

impl<'a> DoubleEndedIterator for SizeClassedVecDequeIter<'a> {
    fn next_back(&mut self) -> Option<usize> {
        metamatch!(match self {
            #[expand(SC in [Sc8, Sc16, Sc32, Sc64])]
            Self::SC(it) => it.next_back().map(|v| *v as usize),
            Self::Sc1(it) => it.next_back().map(usize::from),
        })
    }
}

impl Debug for SizeClassedVecDeque {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        metamatch!(match self {
            #[expand(SC in [Sc1, Sc8, Sc16, Sc32, Sc64])]
            Self::SC(v) => Debug::fmt(v, f),
        })
    }
}

impl PartialEq for SizeClassedVecDeque {
    fn eq(&self, other: &Self) -> bool {
        metamatch!(match (self, other) {
            #[expand(SC in [Sc1, Sc8, Sc16, Sc32, Sc64])]
            (Self::SC(lhs), Self::SC(rhs)) => lhs == rhs,
            _ => false,
        })
    }
}
impl Eq for SizeClassedVecDeque {}
