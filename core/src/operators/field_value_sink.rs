use std::sync::{Arc, Mutex, MutexGuard};

use smallstr::SmallString;

use crate::{
    job_session::JobData,
    record_data::{
        field::Field,
        field_data::{field_value_flags, FieldValueRepr},
        field_value::FieldValue,
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
        },
        stream_value::{StreamValueData, StreamValueId},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
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
    sess: &mut JobData,
    _op_base: &OperatorBase,
    ss: &'a OpFieldValueSink,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(FieldValueRepr::BytesInline);
    TransformData::FieldValueSink(TfFieldValueSink {
        handle: &ss.handle.data,
        batch_iter: sess.field_mgr.claim_iter(tf_state.input_field),
        stream_value_handles: Default::default(),
    })
}

fn append_to_index(
    fvs: &mut FieldValueSink,
    start_idx: usize,
    run_len: usize,
    data: &[u8],
    utf8: bool,
) {
    for fv in &mut fvs[start_idx..start_idx + run_len] {
        match fv {
            FieldValue::Text(t) => {
                if utf8 {
                    t.push_str(unsafe { std::str::from_utf8_unchecked(data) });
                } else {
                    let mut b = std::mem::take(t).into_bytes();
                    b.extend_from_slice(data);
                    *fv = FieldValue::Bytes(b);
                }
            }
            FieldValue::Bytes(b) => b.extend_from_slice(data),
            _ => unreachable!(),
        }
    }
}

fn push_field_values(fvs: &mut FieldValueSink, v: FieldValue, run_len: usize) {
    fvs.extend(std::iter::repeat_with(|| v.clone()).take(run_len - 1));
    fvs.push(v);
}

pub fn push_errors(
    err: OperatorApplicationError,
    run_length: usize,
    mut field_pos: usize,
    last_error_end: &mut usize,
    output_field: &mut Field,
) {
    field_pos += run_length;
    let successes_so_far = field_pos - *last_error_end;
    if successes_so_far > 0 {
        output_field
            .iter_hall
            .push_null(field_pos - *last_error_end, true);
        output_field
            .iter_hall
            .push_error(err, run_length, false, false);
    } else {
        output_field
            .iter_hall
            .push_error(err, run_length, true, true);
    }
    *last_error_end = field_pos;
}

pub fn handle_tf_field_value_sink(
    sess: &mut JobData,
    tf_id: TransformId,
    ss: &mut TfFieldValueSink<'_>,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &mut sess.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let input_field = sess
        .field_mgr
        .get_cow_field_ref(&mut sess.match_set_mgr, tf.input_field);
    let mut output_field = sess.field_mgr.fields[tf.output_field].borrow_mut();
    let base_iter = sess
        .field_mgr
        .lookup_iter(tf.input_field, &input_field, ss.batch_iter)
        .bounded(0, batch_size);
    let starting_pos = base_iter.get_next_field_pos();
    let mut iter =
        AutoDerefIter::new(&sess.field_mgr, tf.input_field, base_iter);
    let mut fvs = ss.handle.lock().unwrap();
    let mut last_error_end = 0;
    let mut field_pos = fvs.len();
    while let Some(range) = iter.typed_range_fwd(
        &mut sess.match_set_mgr,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::TextInline(text) => {
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
            TypedSlice::BytesInline(bytes) => {
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
            TypedSlice::BytesBuffer(bytes) => {
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
            TypedSlice::Int(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Int(*v),
                        rl as usize,
                    );
                }
            }
            TypedSlice::Custom(custom_types) => {
                for (v, rl) in
                    TypedSliceIter::from_range(&range.base, custom_types)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Custom(v.clone()),
                        rl as usize,
                    );
                }
            }
            TypedSlice::FieldReference(_) => unreachable!(),
            TypedSlice::SlicedFieldReference(_) => unreachable!(),
            TypedSlice::Null(_) => {
                push_field_values(
                    &mut fvs,
                    FieldValue::Null,
                    range.base.field_count,
                );
            }
            TypedSlice::Error(errs) => {
                let mut pos = field_pos;
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Error(v.clone()),
                        rl as usize,
                    );
                    push_errors(
                        v.clone(),
                        rl as usize,
                        pos,
                        &mut last_error_end,
                        &mut output_field,
                    );
                    pos += rl as usize;
                }
            }
            TypedSlice::Undefined(_) => {
                push_field_values(
                    &mut fvs,
                    FieldValue::Undefined,
                    range.base.field_count,
                );
            }
            TypedSlice::StreamValueId(svs) => {
                let mut pos = field_pos;
                for (svid, range, rl) in
                    RefAwareStreamValueIter::from_range(&range, svs)
                {
                    let sv = &mut sess.sv_mgr.stream_values[svid];

                    match &sv.data {
                        StreamValueData::Bytes(bytes) => {
                            let data =
                                range.map(|r| &bytes[r]).unwrap_or(bytes);
                            let fv = if sv.done || sv.bytes_are_chunk {
                                if sv.bytes_are_utf8 {
                                    FieldValue::Bytes(data.to_vec())
                                } else {
                                    FieldValue::Text(
                                        unsafe {
                                            std::str::from_utf8_unchecked(data)
                                        }
                                        .to_string(),
                                    )
                                }
                            } else {
                                // just to initialize the slots
                                FieldValue::Bytes(Vec::new())
                            };
                            push_field_values(&mut fvs, fv, rl as usize);
                            if !sv.done {
                                sv.subscribe(
                                    tf_id,
                                    ss.stream_value_handles.peek_claim_id(),
                                    sv.is_buffered(),
                                );
                                ss.stream_value_handles.claim_with_value(
                                    StreamValueHandle {
                                        start_idx: pos,
                                        run_len: rl as usize,
                                    },
                                );
                            }
                        }
                        StreamValueData::Error(e) => {
                            push_field_values(
                                &mut fvs,
                                FieldValue::Error(e.clone()),
                                rl as usize,
                            );
                            push_errors(
                                e.clone(),
                                rl as usize,
                                pos,
                                &mut last_error_end,
                                &mut output_field,
                            );
                        }
                        StreamValueData::Dropped => unreachable!(),
                    }
                    pos += rl as usize;
                }
            }
            TypedSlice::BigInt(_)
            | TypedSlice::Float(_)
            | TypedSlice::Rational(_) => {
                todo!();
            }
            TypedSlice::Array(arrays) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, arrays)
                {
                    push_field_values(
                        &mut fvs,
                        FieldValue::Array(v.clone()),
                        rl as usize,
                    );
                }
            }
            TypedSlice::Object(object) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, object)
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
    if consumed_fields < batch_size {
        push_field_values(
            &mut fvs,
            FieldValue::Undefined,
            batch_size - consumed_fields,
        );
    }
    sess.field_mgr
        .store_iter(input_field_id, ss.batch_iter, base_iter);
    let success_count = field_pos - last_error_end;
    if success_count > 0 {
        output_field.iter_hall.push_null(success_count, true);
    }
    drop(input_field);
    drop(output_field);
    let streams_done = ss.stream_value_handles.is_empty();
    if input_done && streams_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}

pub fn handle_tf_field_value_sink_stream_value_update(
    sess: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfFieldValueSink<'_>,
    sv_id: StreamValueId,
    custom: usize,
) {
    let mut out = tf.handle.lock().unwrap();
    let svh = &mut tf.stream_value_handles[custom];
    let sv = &mut sess.sv_mgr.stream_values[sv_id];

    match &sv.data {
        StreamValueData::Bytes(b) => {
            if sv.bytes_are_chunk || sv.done {
                let out_bytes = match &mut out[svh.start_idx] {
                    FieldValue::Text(t) => t.as_bytes(),
                    FieldValue::Bytes(b) => b,
                    _ => unreachable!(),
                };
                let buf = if sv.bytes_are_chunk {
                    b.as_slice()
                } else {
                    // this could have been promoted to buffer after
                    // we started treating it as a chunk, so skip any
                    // data we already have
                    &b[out_bytes.len()..]
                };
                append_to_index(
                    &mut out,
                    svh.start_idx,
                    svh.run_len,
                    buf,
                    sv.bytes_are_utf8,
                );
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            for i in svh.start_idx..svh.start_idx + svh.run_len {
                out[i] = FieldValue::Error(e.clone());
            }
        }
        StreamValueData::Dropped => panic!("dropped stream value observed"),
    }

    if sv.done || matches!(sv.data, StreamValueData::Error(_)) {
        sess.sv_mgr.drop_field_value_subscription(sv_id, None);
        tf.stream_value_handles.release(custom);
        if tf.stream_value_handles.is_empty() {
            sess.tf_mgr.update_ready_state(tf_id);
        }
    }
}
