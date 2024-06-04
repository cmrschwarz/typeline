use std::sync::{Arc, Mutex, MutexGuard};

use smallstr::SmallString;

use crate::{
    job::JobData,
    record_data::{
        field::Field,
        field_data::field_value_flags,
        field_value::FieldValue,
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueRangeIter,
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
        },
        stream_value::StreamValueId,
    },
    utils::universe::CountedUniverse,
};

use super::{
    errors::OperatorApplicationError,
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

pub type FieldValueSink = Vec<FieldValue>;

#[derive(Default, Clone)]
pub struct FieldValueSinkHandle {
    data: Arc<Mutex<FieldValueSink>>,
}

impl FieldValueSinkHandle {
    pub fn get(&self) -> MutexGuard<FieldValueSink> {
        self.data.lock().unwrap()
    }
}

#[derive(Clone)]
pub struct OpFieldValueSink {
    pub handle: FieldValueSinkHandle,
}

impl OpFieldValueSink {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        SmallString::from("<Field Value Sink>")
    }
}

pub fn create_op_field_value_sink(
    handle: &'_ FieldValueSinkHandle,
) -> OperatorData {
    OperatorData::FieldValueSink(OpFieldValueSink {
        handle: handle.clone(),
    })
}

struct StreamValueHandle {
    start_idx: usize,
    run_len: usize,
}

pub struct TfFieldValueSink<'a> {
    handle: &'a Mutex<FieldValueSink>,
    batch_iter: IterId,
    stream_value_handles: CountedUniverse<usize, StreamValueHandle>,
}

pub fn build_tf_field_value_sink<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    ss: &'a OpFieldValueSink,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::FieldValueSink(TfFieldValueSink {
        handle: &ss.handle.data,
        batch_iter: jd.add_iter_for_tf_state(tf_state),
        stream_value_handles: CountedUniverse::default(),
    })
}

fn push_field_values(fvs: &mut FieldValueSink, v: FieldValue, run_len: usize) {
    fvs.extend(std::iter::repeat_with(|| v.clone()).take(run_len - 1));
    fvs.push(v);
}

pub fn push_errors(
    err: OperatorApplicationError,
    run_length: usize,
    mut field_pos: usize,
    last_interruption_end: &mut usize,
    output_field: &mut Field,
) {
    field_pos += run_length;
    output_field
        .iter_hall
        .push_null(field_pos - *last_interruption_end, true);
    output_field
        .iter_hall
        .push_error(err, run_length, false, false);

    *last_interruption_end = field_pos;
}

pub fn handle_tf_field_value_sink(
    jd: &mut JobData,
    tf_id: TransformId,
    ss: &mut TfFieldValueSink<'_>,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);
    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();
    let base_iter = jd
        .field_mgr
        .lookup_iter(tf.input_field, &input_field, ss.batch_iter)
        .bounded(0, batch_size);
    let starting_pos = base_iter.get_next_field_pos();
    let mut iter =
        AutoDerefIter::new(&jd.field_mgr, tf.input_field, base_iter);
    let mut fvs = ss.handle.lock().unwrap();
    // interruptions are either errors or field separators
    // they interrupt a run of nulls that we output foi successes
    let mut last_interruption_end = 0;
    let mut field_pos = fvs.len();
    while let Some(range) = iter.typed_range_fwd(
        &jd.match_set_mgr,
        usize::MAX,
        field_value_flags::DEFAULT,
    ) {
        match range.base.data {
            FieldValueSlice::TextInline(text) => {
                for (v, rl, _offs) in
                    RefAwareInlineTextIter::from_range(&range, text)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Text(v.to_string()),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Bytes(v.to_vec()),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::TextBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareTextBufferIter::from_range(&range, bytes)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Text(v.to_string()),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Bytes(v.to_vec()),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::Int(ints) => {
                for (v, rl) in FieldValueRangeIter::from_range(&range, ints) {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Int(*v),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::Float(vals) => {
                for (v, rl) in FieldValueRangeIter::from_range(&range, vals) {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Float(*v),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::BigInt(vals) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, vals)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::BigInt(Box::new(v.clone())),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::Rational(vals) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, vals)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Rational(Box::new(v.clone())),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::Custom(custom_types) => {
                for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                    &range,
                    custom_types,
                ) {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Custom(v.clone()),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            FieldValueSlice::Null(_) => {
                push_field_values(
                    &mut fvs,
                    FieldValue::Null,
                    range.base.field_count,
                );
            }
            FieldValueSlice::Error(errs) => {
                let mut pos = field_pos;
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, errs)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Error(v.clone()),
                        rl as usize,
                    );
                    push_errors(
                        v.clone(),
                        rl as usize,
                        pos,
                        &mut last_interruption_end,
                        &mut output_field,
                    );
                    pos += rl as usize;
                }
            }
            FieldValueSlice::Undefined(_) => {
                push_field_values(
                    &mut fvs,
                    FieldValue::Undefined,
                    range.base.field_count,
                );
            }
            FieldValueSlice::StreamValueId(sv_ids) => {
                let mut pos = field_pos;
                for (svid, rl) in
                    FieldValueRangeIter::from_range(&range, sv_ids)
                {
                    let start_idx = pos;
                    let run_len = rl as usize;
                    pos += run_len;
                    let sv = &mut jd.sv_mgr.stream_values[*svid];
                    if !sv.done {
                        let handle_id =
                            ss.stream_value_handles.claim_with_value(
                                StreamValueHandle { start_idx, run_len },
                            );
                        sv.subscribe(*svid, tf_id, handle_id, true, true);
                        continue;
                    }
                    push_field_values(&mut fvs, sv.to_field_value(), run_len);
                }
            }
            FieldValueSlice::Array(arrays) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, arrays)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Array(v.clone()),
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::Object(object) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, object)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Object(v.clone()),
                        rl as usize,
                    );
                }
            }
        }
        field_pos += range.base.field_count;
    }
    let base_iter = iter.into_base_iter();
    let consumed_fields = base_iter.get_next_field_pos() - starting_pos;
    // TODO: get rid of this once we removed the short fields mechanism
    // from sequence
    if consumed_fields < batch_size {
        push_field_values(
            &mut fvs,
            FieldValue::Undefined,
            batch_size - consumed_fields,
        );
    }
    jd.field_mgr
        .store_iter(input_field_id, ss.batch_iter, base_iter);
    let last_success_run_length = field_pos - last_interruption_end;
    if last_success_run_length > 0 {
        output_field
            .iter_hall
            .push_null(last_success_run_length, true);
    }
    drop(input_field);
    drop(output_field);
    let streams_done = ss.stream_value_handles.is_empty();
    if streams_done && ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}

pub fn handle_tf_field_value_sink_stream_value_update(
    jd: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfFieldValueSink<'_>,
    sv_id: StreamValueId,
    custom: usize,
) {
    let mut fvs = tf.handle.lock().unwrap();
    let svh = &mut tf.stream_value_handles[custom];
    let sv = &mut jd.sv_mgr.stream_values[sv_id];
    debug_assert!(sv.done);
    for fv in &mut fvs[svh.start_idx..svh.start_idx + svh.run_len] {
        *fv = sv.to_field_value();
    }
    jd.sv_mgr.drop_field_value_subscription(sv_id, None);
    tf.stream_value_handles.release(custom);
    if tf.stream_value_handles.is_empty() {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
}
