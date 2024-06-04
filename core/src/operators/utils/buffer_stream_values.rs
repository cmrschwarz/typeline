use crate::record_data::{
    field_data::field_value_flags,
    field_value_ref::FieldValueSlice,
    field_value_slice_iter::FieldValueRangeIter,
    iters::FieldIterator,
    match_set::MatchSetManager,
    ref_iter::AutoDerefIter,
    stream_value::{StreamValueId, StreamValueManager},
};

pub fn buffer_remaining_stream_values_in_sv_iter(
    sv_mgr: &mut StreamValueManager,
    iter: FieldValueRangeIter<StreamValueId>,
    contiguous: bool,
) -> usize {
    let mut lines = 0;
    for (sv_id, rl) in iter {
        let sv = &mut sv_mgr.stream_values[*sv_id];
        if contiguous {
            sv.make_contiguous()
        } else {
            sv.make_buffered();
        }
        sv.ref_count += rl as usize;
        lines += rl as usize;
    }
    lines
}

pub fn buffer_remaining_stream_values_in_auto_deref_iter<
    'a,
    I: FieldIterator<'a>,
>(
    match_set_mgr: &MatchSetManager,
    sv_mgr: &mut StreamValueManager,
    mut iter: AutoDerefIter<'a, I>,
    limit: usize,
    contiguous: bool,
) -> usize {
    let mut lines = 0;
    while let Some(range) =
        iter.typed_range_fwd(match_set_mgr, limit, field_value_flags::DEFAULT)
    {
        if let FieldValueSlice::StreamValueId(svs) = range.base.data {
            lines += buffer_remaining_stream_values_in_sv_iter(
                sv_mgr,
                FieldValueRangeIter::from_range(&range, svs),
                contiguous,
            );
        }
    }
    lines
}
