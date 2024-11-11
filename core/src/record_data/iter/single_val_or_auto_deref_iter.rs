use crate::record_data::match_set::MatchSetManager;

use super::{
    field_iterator::{FieldIterOpts, FieldIterator},
    ref_iter::{AutoDerefIter, RefAwareTypedRange},
    single_value_iter::{AtomIter, FieldValueIter},
};

#[allow(clippy::large_enum_variant)]
pub enum SingleValOrAutoDerefIter<'a, I> {
    Iter(AutoDerefIter<'a, I>),
    Atom(AtomIter<'a>),
    FieldValue(FieldValueIter<'a>),
}

impl<'a, I: FieldIterator> SingleValOrAutoDerefIter<'a, I> {
    pub fn typed_range_fwd(
        &mut self,
        msm: &MatchSetManager,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<RefAwareTypedRange> {
        match self {
            SingleValOrAutoDerefIter::Iter(iter) => {
                iter.typed_range_fwd(msm, limit, opts)
            }
            SingleValOrAutoDerefIter::Atom(iter) => {
                iter.typed_range_fwd(limit)
            }
            SingleValOrAutoDerefIter::FieldValue(iter) => {
                iter.typed_range_fwd(limit)
            }
        }
    }
}
