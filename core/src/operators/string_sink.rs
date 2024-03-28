use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

use bstr::ByteSlice;
use smallstr::SmallString;

use crate::{
    job::JobData,
    operators::print::error_to_string,
    record_data::{
        field::Field,
        field_data::field_value_flags,
        field_value::FormattingContext,
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueSliceIter,
        formattable::RealizedFormatKey,
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueSliceIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
            RefAwareTextBufferIter,
        },
        stream_value::{StreamValue, StreamValueData, StreamValueId},
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        int_string_conversions::i64_to_str,
        text_write::{MaybeTextWriteFlaggedAdapter, TextWriteIoAdapter},
        universe::CountedUniverse,
    },
    NULL_STR, UNDEFINED_STR,
};

use super::{
    errors::OperatorApplicationError,
    operator::{DefaultOperatorName, OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Default)]
pub struct StringSink {
    pub data: Vec<String>,
    pub errors: Vec<(usize, Arc<OperatorApplicationError>)>,
    pub error_indices: HashMap<usize, usize, BuildIdentityHasher>,
}

impl StringSink {
    pub fn append_error(
        &mut self,
        index: usize,
        err: Arc<OperatorApplicationError>,
    ) {
        self.error_indices.insert(index, self.errors.len());
        self.errors.push((index, err))
    }
    pub fn get_first_error(&self) -> Option<Arc<OperatorApplicationError>> {
        self.errors.first().map(|(_i, e)| e.clone())
    }
    pub fn get_first_error_message(&self) -> Option<&str> {
        self.errors.first().map(|(_i, e)| e.message())
    }
}

#[derive(Default, Clone)]
pub struct StringSinkHandle {
    data: Arc<Mutex<StringSink>>,
}

pub struct StringSinkDataGuard<'a> {
    data_guard: MutexGuard<'a, StringSink>,
}
impl<'a> Deref for StringSinkDataGuard<'a> {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.data_guard.data
    }
}
impl<'a> DerefMut for StringSinkDataGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data_guard.data
    }
}

impl StringSinkHandle {
    pub fn get(&self) -> MutexGuard<StringSink> {
        self.data.lock().unwrap()
    }
    pub fn get_data(
        &self,
    ) -> Result<StringSinkDataGuard, Arc<OperatorApplicationError>> {
        let guard = self.data.lock().unwrap();
        if let Some((_, err)) = guard.errors.first() {
            return Err(err.clone());
        }
        Ok(StringSinkDataGuard { data_guard: guard })
    }
    pub fn clear(&self) {
        let mut guard = self.get();
        guard.data.clear();
        guard.errors.clear();
    }
}

#[derive(Clone)]
pub struct OpStringSink {
    pub handle: StringSinkHandle,
}

impl OpStringSink {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        SmallString::from("<String Sink>")
    }
}

pub fn create_op_string_sink(handle: &'_ StringSinkHandle) -> OperatorData {
    OperatorData::StringSink(OpStringSink {
        handle: handle.clone(),
    })
}

struct StreamValueHandle {
    start_idx: usize,
    run_len: usize,
    contains_error: bool,
}

pub struct TfStringSink<'a> {
    handle: &'a Mutex<StringSink>,
    iter_id: IterId,
    stream_value_handles: CountedUniverse<usize, StreamValueHandle>,
}

pub fn build_tf_string_sink<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    ss: &'a OpStringSink,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::StringSink(TfStringSink {
        handle: &ss.handle.data,
        iter_id: jd.add_iter_for_tf_state(tf_state),
        stream_value_handles: CountedUniverse::default(),
    })
}
fn push_string(out: &mut StringSink, string: String, run_len: usize) {
    out.data
        .extend(std::iter::repeat_with(|| string.clone()).take(run_len - 1));
    out.data.push(string);
}
fn push_str(out: &mut StringSink, str: &str, run_len: usize) {
    push_string(out, str.to_owned(), run_len);
}
fn push_invalid_utf8(
    op_id: OperatorId,
    field_pos: usize,
    out: &mut StringSink,
    bytes: &[u8],
    run_len: usize,
) {
    let err = Arc::new(OperatorApplicationError::new("invalid utf-8", op_id));
    for i in field_pos..field_pos + run_len {
        out.append_error(i, err.clone());
    }
    push_string(out, String::from_utf8_lossy(bytes).to_string(), run_len);
}

fn push_bytes(
    op_id: OperatorId,
    field_pos: usize,
    out: &mut StringSink,
    bytes: &[u8],
    run_len: usize,
) {
    match bytes.to_str() {
        Ok(s) => push_str(out, s, run_len),
        Err(_) => push_invalid_utf8(op_id, field_pos, out, bytes, run_len),
    }
}

fn push_bytes_buffer(
    op_id: OperatorId,
    field_pos: usize,
    out: &mut StringSink,
    bytes: Vec<u8>,
    run_len: usize,
) {
    match String::from_utf8(bytes) {
        Ok(s) => push_string(out, s, run_len),
        Err(e) => {
            push_invalid_utf8(op_id, field_pos, out, e.as_bytes(), run_len)
        }
    }
}

fn append_stream_val(
    op_id: OperatorId,
    sv: &StreamValue,
    sv_handle: &mut StreamValueHandle,
    out: &mut StringSink,
    start_idx: usize,
    run_len: usize,
) {
    debug_assert!(run_len > 0);
    let end_idx = start_idx + run_len;
    match &sv.data {
        StreamValueData::Bytes(b) => {
            if sv.is_buffered && !sv.done {
                return;
            }
            let buf = if sv.is_buffered {
                // this could have been promoted to buffer after
                // we started treating it as a chunk, so skip any
                // data we already have
                &b[out.data[start_idx].len()..]
            } else {
                b.as_slice()
            };
            let Ok(text) = std::str::from_utf8(buf) else {
                if !sv_handle.contains_error {
                    sv_handle.contains_error = true;
                    let err = Arc::new(OperatorApplicationError::new(
                        "invalid utf-8",
                        op_id,
                    ));
                    for i in start_idx..end_idx {
                        out.append_error(i, err.clone());
                    }
                }
                let lossy = String::from_utf8_lossy(buf);
                for i in start_idx..end_idx {
                    out.data[i].push_str(&lossy);
                }
                return;
            };
            for i in start_idx..end_idx {
                out.data[i].push_str(text);
            }
        }
        StreamValueData::Text(t) => {
            if sv.is_buffered && !sv.done {
                return;
            }
            let text = if sv.is_buffered {
                // same as above for Bytes
                &t[out.data[start_idx].len()..]
            } else {
                t.as_str()
            };
            for i in start_idx..end_idx {
                out.data[i].push_str(text);
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            let err = Arc::new(e.clone());
            for i in start_idx..end_idx {
                out.data[i] = error_to_string(e);
                out.append_error(i, err.clone());
            }
        }
    }
}
pub fn push_errors(
    out: &mut StringSink,
    err: OperatorApplicationError,
    run_length: usize,
    mut field_pos: usize,
    last_interruption_end: &mut usize,
    output_field: &mut Field,
) {
    push_string(out, error_to_string(&err), run_length);
    let e = Arc::new(err.clone());
    for i in 0..run_length {
        out.append_error(field_pos + i, e.clone());
    }
    field_pos += run_length;
    let successes_so_far = field_pos - *last_interruption_end;
    if successes_so_far > 0 {
        output_field
            .iter_hall
            .push_null(field_pos - *last_interruption_end, true);
        output_field
            .iter_hall
            .push_error(err, run_length, false, false);
    } else {
        output_field
            .iter_hall
            .push_error(err, run_length, true, true);
    }
    *last_interruption_end = field_pos;
}
pub fn handle_tf_string_sink(
    jd: &mut JobData,
    tf_id: TransformId,
    ss: &mut TfStringSink<'_>,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
    let input_field_id = tf.input_field;
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);
    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();
    let base_iter = jd
        .field_mgr
        .lookup_iter(tf.input_field, &input_field, ss.iter_id)
        .bounded(0, batch_size);
    let starting_pos = base_iter.get_next_field_pos();
    let mut iter =
        AutoDerefIter::new(&jd.field_mgr, tf.input_field, base_iter);
    let mut out = ss.handle.lock().unwrap();
    let mut field_pos = out.data.len();
    let mut string_store = None;
    // interruption meaning error or group separator
    let mut last_interruption_end = 0;
    let print_rationals_raw =
        jd.get_transform_chain(tf_id).settings.print_rationals_raw;
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
                    push_str(&mut out, v, rl as usize);
                }
            }
            FieldValueSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    push_bytes(op_id, field_pos, &mut out, v, rl as usize);
                }
            }
            FieldValueSlice::TextBuffer(text) => {
                for (v, rl, _offs) in
                    RefAwareTextBufferIter::from_range(&range, text)
                {
                    push_str(&mut out, v, rl as usize);
                }
            }
            FieldValueSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    push_bytes(op_id, field_pos, &mut out, v, rl as usize);
                }
            }
            FieldValueSlice::Int(ints) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, ints) {
                    let v = i64_to_str(false, *v);
                    push_str(&mut out, v.as_str(), rl as usize);
                }
            }
            FieldValueSlice::Custom(custom_types) => {
                for (v, rl) in RefAwareFieldValueSliceIter::from_range(
                    &range,
                    custom_types,
                ) {
                    let mut data = Vec::new();
                    let mut w = MaybeTextWriteFlaggedAdapter::new(
                        TextWriteIoAdapter(&mut data),
                    );
                    match v.format_raw(&mut w, &RealizedFormatKey::default()) {
                        Err(e) => push_errors(
                            &mut out,
                            OperatorApplicationError::new_s(
                                format!(
                                    "failed to stringify custom type '{}': {e}",
                                    v.type_name()
                                ),
                                op_id,
                            ),
                            range.base.field_count,
                            field_pos,
                            &mut last_interruption_end,
                            &mut output_field,
                        ),

                        Ok(()) if w.is_utf8() => {
                            push_string(
                                &mut out,
                                unsafe { String::from_utf8_unchecked(data) },
                                rl as usize,
                            );
                        }
                        Ok(()) => {
                            push_bytes(
                                op_id,
                                field_pos,
                                &mut out,
                                &data,
                                rl as usize,
                            );
                        }
                    }
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            FieldValueSlice::Null(_) => {
                push_str(&mut out, NULL_STR, range.base.field_count);
            }
            FieldValueSlice::Error(errs) => {
                let mut pos = field_pos;
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, errs)
                {
                    push_errors(
                        &mut out,
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
                push_errors(
                    &mut out,
                    OperatorApplicationError::new("value is undefined", op_id),
                    range.base.field_count,
                    field_pos,
                    &mut last_interruption_end,
                    &mut output_field,
                );
            }
            FieldValueSlice::StreamValueId(svs) => {
                let mut pos = field_pos;
                for (sv_id, range, rl) in
                    RefAwareStreamValueIter::from_range(&range, svs)
                {
                    let rl = rl as usize;
                    let sv = &mut jd.sv_mgr.stream_values[sv_id];
                    match &sv.data {
                        StreamValueData::Bytes(bytes) => {
                            let bytes =
                                range.map(|r| &bytes[r]).unwrap_or(bytes);
                            if sv.done || !sv.is_buffered {
                                push_bytes(op_id, pos, &mut out, bytes, rl);
                            } else {
                                push_string(&mut out, String::new(), rl);
                            }
                        }
                        StreamValueData::Text(text) => {
                            let text = range.map(|r| &text[r]).unwrap_or(text);
                            if sv.done || !sv.is_buffered {
                                push_str(&mut out, text, rl);
                            } else {
                                push_string(&mut out, String::new(), rl);
                            }
                        }
                        StreamValueData::Error(e) => push_errors(
                            &mut out,
                            e.clone(),
                            rl,
                            pos,
                            &mut last_interruption_end,
                            &mut output_field,
                        ),
                    }
                    if !sv.done {
                        let handle_id = ss
                            .stream_value_handles
                            .claim_with_value(StreamValueHandle {
                                start_idx: pos,
                                run_len: rl,
                                contains_error: false,
                            });
                        sv.subscribe(sv_id, tf_id, handle_id, sv.is_buffered);
                    }
                    pos += rl;
                }
            }
            FieldValueSlice::BigInt(_)
            | FieldValueSlice::Float(_)
            | FieldValueSlice::Rational(_) => {
                todo!();
            }
            FieldValueSlice::Array(arrays) => {
                let ss = string_store.get_or_insert_with(|| {
                    jd.session_data.string_store.read().unwrap()
                });
                let fc = FormattingContext {
                    ss,
                    fm: &jd.field_mgr,
                    msm: &jd.match_set_mgr,
                    print_rationals_raw,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::default(),
                };
                for (a, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, arrays)
                {
                    let mut data = Vec::new();
                    a.format(&mut TextWriteIoAdapter(&mut data), &fc).unwrap();
                    push_bytes_buffer(
                        op_id,
                        field_pos,
                        &mut out,
                        data,
                        rl as usize,
                    );
                }
            }
            FieldValueSlice::Object(object) => {
                let ss = string_store.get_or_insert_with(|| {
                    jd.session_data.string_store.read().unwrap()
                });
                let fc = FormattingContext {
                    ss,
                    fm: &jd.field_mgr,
                    msm: &jd.match_set_mgr,
                    print_rationals_raw,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::default(),
                };
                for (a, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, object)
                {
                    let mut data = Vec::new();
                    a.format(&mut TextWriteIoAdapter(&mut data), &fc).unwrap();
                    push_bytes_buffer(
                        op_id,
                        field_pos,
                        &mut out,
                        data,
                        rl as usize,
                    );
                }
            }
        }
        field_pos += range.base.field_count;
    }
    let base_iter = iter.into_base_iter();
    let consumed_fields = base_iter.get_next_field_pos() - starting_pos;
    // TODO: remove once sequence is sane
    if consumed_fields < batch_size {
        push_str(&mut out, UNDEFINED_STR, batch_size - consumed_fields);
    }
    jd.field_mgr
        .store_iter(input_field_id, ss.iter_id, base_iter);
    let final_success_run_length = field_pos - last_interruption_end;
    if final_success_run_length > 0 {
        output_field
            .iter_hall
            .push_null(final_success_run_length, true);
    }
    drop(input_field);
    drop(output_field);
    let streams_done = ss.stream_value_handles.is_empty();

    if !streams_done && ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr
        .submit_batch(tf_id, batch_size, ps.input_done && streams_done);
}

pub fn handle_tf_string_sink_stream_value_update(
    jd: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfStringSink<'_>,
    sv_id: StreamValueId,
    custom: usize,
) {
    let mut out = tf.handle.lock().unwrap();
    let svh = &mut tf.stream_value_handles[custom];
    let sv = &mut jd.sv_mgr.stream_values[sv_id];
    append_stream_val(
        jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
        sv,
        svh,
        &mut out,
        svh.start_idx,
        svh.run_len,
    );
    if sv.done {
        jd.sv_mgr.drop_field_value_subscription(sv_id, Some(tf_id));
        tf.stream_value_handles.release(custom);
        if tf.stream_value_handles.is_empty() {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
    }
}
