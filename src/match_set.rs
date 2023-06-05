use std::{collections::VecDeque, mem::ManuallyDrop};

use indexmap::IndexMap;

use crate::{
    match_value::{MatchValue, MatchValueFormat, MatchValueShared},
    match_value_into_iter::MatchValueIntoIter,
    string_store::StringStoreEntry,
};

#[repr(C)]
pub struct MatchSet {
    values: Vec<ManuallyDrop<VecDeque<MatchValue>>>,
    format: ManuallyDrop<IndexMap<StringStoreEntry, MatchValueFormat>>,
}

#[repr(C)]
pub struct MatchSetShared {
    values: Vec<VecDeque<MatchValueShared>>,
    format: IndexMap<StringStoreEntry, MatchValueFormat>,
}

impl From<MatchSetShared> for MatchSet {
    fn from(mss: MatchSetShared) -> Self {
        MatchSet {
            values: unsafe { std::mem::transmute(mss.values) },
            format: ManuallyDrop::new(mss.format),
        }
    }
}

impl MatchSet {
    #[inline(always)]
    pub fn format(&self) -> &IndexMap<StringStoreEntry, MatchValueFormat> {
        &self.format
    }

    #[inline(always)]
    pub fn values_per_match(&self) -> usize {
        self.format.len()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.values.len() / self.values_per_match()
    }
}

impl Drop for MatchSet {
    fn drop(&mut self) {
        for (i, format) in unsafe { ManuallyDrop::take(&mut self.format) }
            .into_values()
            .enumerate()
        {
            let _drop_me = MatchValueIntoIter::new(
                &format,
                unsafe { ManuallyDrop::take(&mut self.values[i]) }.into_iter(),
            );
        }
    }
}
