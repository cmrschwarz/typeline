use crate::record_data::{
    field_data::field_value_flags,
    iters::FieldIterator,
    match_set::MatchSetManager,
    ref_iter::{AutoDerefIter, RefAwareStreamValueIter},
    stream_value::StreamValueManager,
    typed::TypedSlice,
};

pub fn buffer_remaining_stream_values_in_sv_iter(
    sv_mgr: &mut StreamValueManager,
    iter: RefAwareStreamValueIter,
) {
    for (sv_id, _range, _rl) in iter {
        sv_mgr.stream_values[sv_id].promote_to_buffer();
    }
}

pub fn buffer_remaining_stream_values_in_auto_deref_iter<
    'a,
    I: FieldIterator<'a>,
>(
    match_set_mgr: &mut MatchSetManager,
    sv_mgr: &mut StreamValueManager,
    mut iter: AutoDerefIter<'a, I>,
    limit: usize,
) {
    while let Some(range) =
        iter.typed_range_fwd(match_set_mgr, limit, field_value_flags::DEFAULT)
    {
        if let TypedSlice::StreamValueId(svs) = range.base.data {
            buffer_remaining_stream_values_in_sv_iter(
                sv_mgr,
                RefAwareStreamValueIter::from_range(&range, svs),
            );
        }
    }
}
