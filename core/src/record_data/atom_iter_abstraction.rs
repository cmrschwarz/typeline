use std::sync::RwLockReadGuard;

use super::{
    field_data::{
        field_value_flags, FieldValueFormat, FieldValueHeader, RunLength,
    },
    field_value::FieldValue,
    field_value_ref::{TypedRange, ValidTypedRange},
    iters::{FieldIterOpts, FieldIterator},
    match_set::MatchSetManager,
    ref_iter::{AutoDerefIter, RefAwareTypedRange},
    scope_manager::Atom,
};

pub struct AtomIter<'a> {
    value_ref: RwLockReadGuard<'a, FieldValue>,
    dummy_header: FieldValueHeader,
    run_len_rem: usize,
    run_len_total: usize,
}

pub enum AutoDerefIterOrAtom<'a, I> {
    Iter(AutoDerefIter<'a, I>),
    Atom(AtomIter<'a>),
}

impl<'a, I: FieldIterator<'a>> AutoDerefIterOrAtom<'a, I> {
    pub fn typed_range_fwd(
        &mut self,
        msm: &MatchSetManager,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<RefAwareTypedRange> {
        match self {
            AutoDerefIterOrAtom::Iter(iter) => {
                iter.typed_range_fwd(msm, limit, opts)
            }
            AutoDerefIterOrAtom::Atom(iter) => iter.typed_range_fwd(limit),
        }
    }
}

impl<'a> AtomIter<'a> {
    pub fn new(atom: &'a Atom, run_length: usize) -> Self {
        let value = atom.value.read().unwrap();
        Self {
            dummy_header: FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: value.repr(),
                    flags: field_value_flags::SHARED_VALUE,
                    size: value.repr().size() as u16,
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
            data: self.value_ref.as_slice(),
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
