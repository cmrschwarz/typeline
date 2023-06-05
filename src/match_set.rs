use std::{
    collections::{HashMap, VecDeque},
    mem::ManuallyDrop,
    ops::Range,
    sync::Arc,
};

use crate::{
    match_value::{
        self, MatchValue, MatchValueFormat, MatchValueKind, MatchValueRepresentation,
        MatchValueShared, MatchValueType,
    },
    string_store::StringStoreEntry,
};

type MatchValueIndex = usize;

//PERF: Arc :(
pub enum MatchDataShape {
    Value(MatchValueIndex, MatchValueFormat),
    Array(MatchValueIndex, Arc<[MatchDataShape]>),
    TypedArray(MatchValueIndex, MatchValueFormat),
    Dict(
        MatchValueIndex,
        Arc<HashMap<StringStoreEntry, MatchDataShape>>,
    ),
}

pub struct MatchSetShared {
    values: VecDeque<MatchValueShared>,
    shape: MatchDataShape,
    value_formats: Box<[MatchValueFormat]>,
}

impl From<MatchSetShared> for MatchSet {
    fn from(mss: MatchSetShared) -> Self {
        MatchSet {
            values: unsafe { std::mem::transmute(mss.values) },
            shape: mss.shape,
            value_formats: mss.value_formats,
        }
    }
}

pub struct MatchSet {
    values: VecDeque<MatchValue>,
    shape: MatchDataShape,
    value_formats: Box<[MatchValueFormat]>,
}

impl MatchSet {
    #[inline(always)]
    pub fn value_formats(&self) -> &[MatchValueFormat] {
        &self.value_formats
    }

    #[inline(always)]
    pub fn shape(&self) -> &MatchDataShape {
        &self.shape
    }

    #[inline(always)]
    pub fn values_per_match(&self) -> usize {
        self.value_formats.len()
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.values.len() / self.value_formats.len()
    }

    #[inline(always)]
    unsafe fn visit_raw<F: Fn(&MatchValue)>(
        &self,
        range: Range<usize>,
        index: MatchValueIndex,
        func: F,
    ) {
        let stride = self.values_per_match();
        let end = self.values.len().min(index + range.end * stride);
        let mut i = range.start * stride + index;
        //PERF: we could use values.as_slices to speed this up
        while i < end {
            func(&self.values[i]);
            i += stride;
        }
    }
    #[inline(always)]
    unsafe fn visit_raw_mut<F: Fn(&mut MatchValue)>(
        &mut self,
        range: Range<usize>,
        index: MatchValueIndex,
        func: F,
    ) {
        let stride = self.values_per_match();
        let end = self.values.len().min(index + range.end * stride);
        let mut i = range.start * stride + index;
        //PERF: we could use values.as_slices to speed this up
        while i < end {
            func(&mut self.values[i]);
            i += stride;
        }
    }
    #[inline(always)]
    unsafe fn visit_value<T: MatchValueType>(
        &self,
        range: Range<usize>,
        index: MatchValueIndex,
        func: impl Fn(&T::ValueType),
    ) {
        match self.value_formats[index as usize].repr {
            MatchValueRepresentation::Local => {
                self.visit_raw(range, index, |v| func(T::value_local(v)))
            }
            MatchValueRepresentation::Shared => {
                self.visit_raw(range, index, |v| func(T::value_shared(v)))
            }
        }
    }
    #[inline(always)]
    unsafe fn drop_typed_range<T: MatchValueType>(
        &mut self,
        range: Range<usize>,
        index: MatchValueIndex,
    ) {
        match self.value_formats[index as usize].repr {
            MatchValueRepresentation::Local => self.visit_raw_mut(range, index, |v| {
                ManuallyDrop::drop(T::storage_local_mut(v))
            }),
            MatchValueRepresentation::Shared => self.visit_raw_mut(range, index, |v| {
                ManuallyDrop::drop(T::storage_shared_mut(v))
            }),
        }
    }
    #[inline(always)]
    pub unsafe fn drop_value_range(&mut self, range: Range<usize>) {
        let len = self.value_formats.len();
        for i in 0..len {
            let range = range.clone();
            let fmt = self.value_formats[i];
            debug_assert!(fmt.is_valid());
            let idx = i as MatchValueIndex;
            unsafe {
                use MatchValueKind::*;
                match fmt.kind {
                    TypedArray => self.drop_typed_range::<match_value::TypedArray>(range, idx),
                    Array => self.drop_typed_range::<match_value::Array>(range, idx),
                    Object => self.drop_typed_range::<match_value::Object>(range, idx),
                    Text => self.drop_typed_range::<match_value::Text>(range, idx),
                    Bytes => self.drop_typed_range::<match_value::Bytes>(range, idx),
                    Error => self.drop_typed_range::<match_value::Error>(range, idx),
                    Html => self.drop_typed_range::<match_value::Html>(range, idx),
                    Integer | Null => (),
                }
            }
        }
    }
}

impl Drop for MatchSet {
    fn drop(&mut self) {
        unsafe { self.drop_value_range(0..usize::MAX) }
    }
}
