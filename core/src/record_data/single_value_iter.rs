use std::{borrow::Borrow, ops::Deref, sync::RwLockReadGuard};

use super::{
    field_data::{
        field_value_flags, FieldValueFormat, FieldValueHeader, RunLength,
    },
    field_value::FieldValue,
    field_value_ref::{TypedRange, ValidTypedRange},
    ref_iter::RefAwareTypedRange,
    scope_manager::Atom,
};

pub struct SingleValueIter<V: Deref<Target = FieldValue>> {
    value_ref: V,
    dummy_header: FieldValueHeader,
    run_len_rem: usize,
    run_len_total: usize,
}

pub type AtomIter<'a> = SingleValueIter<RwLockReadGuard<'a, FieldValue>>;
pub type FieldValueIter<'a> = SingleValueIter<&'a FieldValue>;

impl<V: Deref<Target = FieldValue>> SingleValueIter<V> {
    pub fn new(value: V, run_length: usize) -> Self {
        let value_deref = &*value;
        Self {
            dummy_header: FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: value_deref.repr(),
                    flags: field_value_flags::SHARED_VALUE,
                    size: value_deref.repr().size() as u16,
                },
                run_length: RunLength::MAX,
            },
            value_ref: value,
            run_len_rem: run_length,
            run_len_total: run_length,
        }
    }
    pub fn typed_range_fwd(
        &mut self,
        limit: usize,
    ) -> Option<RefAwareTypedRange> {
        if self.run_len_rem == 0 {
            return None;
        }

        let field_count =
            limit.min(RunLength::MAX as usize).min(self.run_len_rem);

        self.run_len_rem -= field_count;

        let range = TypedRange {
            headers: std::slice::from_ref(&self.dummy_header),
            data: self.value_ref.borrow().as_slice(),
            field_count,
            first_header_run_length_oversize: RunLength::MAX
                - field_count as RunLength,
            last_header_run_length_oversize: 0,
        };
        Some(RefAwareTypedRange {
            base: unsafe { ValidTypedRange::new_unchecked(range) },
            refs: None,
            field_ref_offset: None,
        })
    }
    pub fn fields_consumed(&self) -> usize {
        self.run_len_total - self.run_len_rem
    }
}

impl<'a> AtomIter<'a> {
    pub fn from_atom(atom: &'a Atom, run_length: usize) -> Self {
        Self::new(atom.value.read().unwrap(), run_length)
    }
}
