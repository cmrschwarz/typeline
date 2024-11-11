use std::marker::PhantomData;

use crate::record_data::{
    field_data::{
        FieldData, FieldValueFormat, FieldValueHeader, FieldValueRepr,
        RunLength,
    },
    field_value_ref::{TypedField, ValidTypedRange},
};

use super::{
    super::field_data_ref::FieldDataRef,
    field_iter::FieldIter,
    field_iterator::{FieldIterOpts, FieldIterator},
};

#[derive(Clone)]
pub struct BoundedIter<I>
where
    I: FieldIterator,
{
    pub(super) iter: I,
    pub(super) min: usize,
    pub(super) max: usize,
    _phantom_data: PhantomData<&'static FieldData>,
}
impl<'a, I> BoundedIter<I>
where
    I: FieldIterator,
{
    pub fn new(
        iter: I,
        min: usize, // inclusive
        max: usize, // exclusive
    ) -> Self {
        let pos = iter.get_next_field_pos();
        assert!(pos >= min && pos < max);
        Self {
            iter,
            min,
            max,
            _phantom_data: PhantomData,
        }
    }
    pub fn new_relative(
        iter: I,
        backwards: usize, // inclusive
        forward: usize,   // inclusive
    ) -> Self {
        let pos = iter.get_next_field_pos();
        Self {
            iter,
            min: pos.saturating_sub(backwards),
            max: pos.saturating_add(forward).saturating_add(1),
            _phantom_data: PhantomData,
        }
    }
    pub fn range_fwd(&self) -> usize {
        self.max - self.get_next_field_pos() - 1
    }
    pub fn range_bwd(&self) -> usize {
        self.get_next_field_pos() - self.min
    }
}
impl<'a, I> FieldIterator for BoundedIter<I>
where
    I: FieldIterator,
{
    type FieldDataRefType = I::FieldDataRefType;
    fn field_data_ref(&self) -> &Self::FieldDataRefType {
        self.iter.field_data_ref()
    }
    fn get_next_field_pos(&self) -> usize {
        self.iter.get_next_field_pos()
    }
    fn is_next_valid(&self) -> bool {
        if self.get_next_field_pos() == self.max {
            return false;
        }
        self.iter.is_next_valid()
    }
    fn is_prev_valid(&self) -> bool {
        if self.get_next_field_pos() == self.min {
            return false;
        }
        self.iter.is_prev_valid()
    }
    fn get_next_field_format(&self) -> FieldValueFormat {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_format()
    }
    fn get_next_field_data(&self) -> usize {
        self.iter.get_next_field_data()
    }
    fn get_prev_field_data_end(&self) -> usize {
        self.iter.get_prev_field_data_end()
    }
    fn get_next_field_header(&self) -> FieldValueHeader {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_header()
    }
    fn get_next_field_header_data_start(&self) -> usize {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_field_header_data_start()
    }
    fn get_next_header_index(&self) -> usize {
        self.iter.get_next_header_index()
    }
    fn get_prev_header_index(&self) -> usize {
        debug_assert!(self.is_prev_valid());
        self.iter.get_prev_header_index()
    }
    fn get_next_typed_field(&mut self) -> TypedField {
        debug_assert!(self.is_next_valid());
        self.iter.get_next_typed_field()
    }
    fn field_run_length_fwd(&self) -> RunLength {
        self.range_fwd()
            .min(self.iter.field_run_length_fwd() as usize)
            as RunLength
    }
    fn field_run_length_bwd(&self) -> RunLength {
        self.range_bwd()
            .min(self.iter.field_run_length_bwd() as usize)
            as RunLength
    }
    fn next_header(&mut self) -> RunLength {
        let range = self.range_fwd();
        let rl_rem = self.iter.field_run_length_fwd() as usize;
        if range < rl_rem {
            self.iter.next_n_fields(range, true) as RunLength
        } else {
            self.iter.next_header()
        }
    }
    fn prev_header(&mut self) -> RunLength {
        let range = self.range_fwd();
        let rl_rem = self.iter.field_run_length_bwd() as usize;
        if range < rl_rem {
            self.iter.prev_n_fields(range, true) as RunLength
        } else {
            self.iter.prev_header()
        }
    }
    fn next_field(&mut self) -> RunLength {
        if self.get_next_field_pos() == self.max {
            0
        } else {
            self.iter.next_field()
        }
    }
    fn prev_field(&mut self) -> RunLength {
        if self.get_next_field_pos() == self.min {
            0
        } else {
            self.iter.prev_field()
        }
    }
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueRepr; N],
        opts: FieldIterOpts,
    ) -> usize {
        let n = n.min(self.range_fwd());
        self.iter.next_n_fields_with_fmt(n, kinds, opts)
    }
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueRepr; N],
        opts: FieldIterOpts,
    ) -> usize {
        let n = n.min(self.range_bwd());
        self.iter.prev_n_fields_with_fmt(n, kinds, opts)
    }
    fn typed_field_fwd(&mut self, limit: usize) -> Option<TypedField> {
        self.iter.typed_field_fwd(limit.min(self.range_fwd()))
    }
    fn typed_field_bwd(&mut self, limit: usize) -> Option<TypedField> {
        self.iter.typed_field_bwd(limit.min(self.range_bwd()))
    }
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<ValidTypedRange> {
        self.iter.typed_range_fwd(limit.min(self.range_fwd()), opts)
    }
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<ValidTypedRange> {
        self.iter.typed_range_bwd(limit.min(self.range_bwd()), opts)
    }

    fn into_base_iter(self) -> FieldIter<Self::FieldDataRefType> {
        self.iter.into_base_iter()
    }
}
impl<'a, R: FieldDataRef, I: FieldIterator<FieldDataRefType = R>>
    From<BoundedIter<I>> for FieldIter<R>
{
    fn from(value: BoundedIter<I>) -> Self {
        value.into_base_iter()
    }
}

pub struct UnfoldRunLength<I, T> {
    iter: I,
    last: Option<T>,
    remaining_run_len: RunLength,
}

impl<I: Iterator<Item = (T, RunLength)>, T: Clone> UnfoldRunLength<I, T> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            last: None,
            remaining_run_len: 0,
        }
    }
}

pub trait UnfoldIterRunLength<T>: Sized {
    fn unfold_rl(self) -> UnfoldRunLength<Self, T>;
}

impl<T: Clone, I: Iterator<Item = (T, RunLength)>> UnfoldIterRunLength<T>
    for I
{
    fn unfold_rl(self) -> UnfoldRunLength<Self, T> {
        UnfoldRunLength::new(self)
    }
}

impl<I: Iterator<Item = (T, RunLength)>, T: Clone> Iterator
    for UnfoldRunLength<I, T>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_run_len > 0 {
            self.remaining_run_len -= 1;
            return self.last.clone();
        } else if let Some((v, rl)) = self.iter.next() {
            self.remaining_run_len = rl - 1;
            self.last = Some(v);
        } else {
            self.last = None;
        }
        self.last.clone()
    }
}
