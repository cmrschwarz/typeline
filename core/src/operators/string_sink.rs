use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

use bstr::ByteSlice;
use smallstr::SmallString;

use crate::{
    job_session::JobData,
    operators::print::error_to_string,
    record_data::{
        field::Field,
        field_data::{field_value_flags, FieldDataRepr},
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
        },
        stream_value::{StreamValue, StreamValueData, StreamValueId},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        int_string_conversions::i64_to_str, universe::CountedUniverse,
    },
};

use super::{
    errors::OperatorApplicationError,
    operator::{DefaultOperatorName, OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
    utils::NULL_STR,
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

pub struct StreamValueHandle {
    start_idx: usize,
    run_len: usize,
    contains_error: bool,
}

pub struct TfStringSink<'a> {
    handle: &'a Mutex<StringSink>,
    batch_iter: IterId,
    stream_value_handles: CountedUniverse<usize, StreamValueHandle>,
}

pub fn build_tf_string_sink<'a>(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    ss: &'a OpStringSink,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(FieldDataRepr::BytesInline);
    TransformData::StringSink(TfStringSink {
        handle: &ss.handle.data,
        batch_iter: sess.field_mgr.claim_iter(tf_state.input_field),
        stream_value_handles: Default::default(),
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
            if !sv.bytes_are_chunk && !sv.done {
                return;
            }
            let buf = if sv.bytes_are_chunk {
                b.as_slice()
            } else {
                // this could have been promoted to buffer after
                // we started treating it as a chunk, so skip any
                // data we already have
                &b[out.data[start_idx].len()..]
            };
            let text = if sv.bytes_are_utf8 {
                unsafe { std::str::from_utf8_unchecked(buf) }
            } else {
                match std::str::from_utf8(buf) {
                    Ok(text) => text,
                    Err(_) => {
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
                    }
                }
            };
            for i in start_idx..end_idx {
                out.data[i].push_str(text);
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            let err = Arc::new(e.clone());
            push_string(out, error_to_string(e), run_len);
            for i in start_idx..end_idx {
                out.append_error(i, err.clone());
            }
        }
        StreamValueData::Dropped => panic!("dropped stream value observed"),
    }
}
pub fn push_errors(
    out: &mut StringSink,
    err: OperatorApplicationError,
    run_length: usize,
    mut field_pos: usize,
    last_error_end: &mut usize,
    output_field: &mut Field,
) {
    push_string(out, error_to_string(&err), run_length);
    let e = Arc::new(err.clone());
    for i in 0..run_length {
        out.append_error(field_pos + i, e.clone());
    }
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
pub fn handle_tf_string_sink(
    sess: &mut JobData,
    tf_id: TransformId,
    ss: &mut TfStringSink<'_>,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
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
    let mut out = ss.handle.lock().unwrap();
    let mut field_pos = out.data.len();

    let mut last_error_end = 0;
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
                    push_str(&mut out, v, rl as usize);
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    push_bytes(op_id, field_pos, &mut out, v, rl as usize);
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    push_bytes(op_id, field_pos, &mut out, v, rl as usize);
                }
            }
            TypedSlice::Int(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let v = i64_to_str(false, *v);
                    push_str(&mut out, v.as_str(), rl as usize);
                }
            }
            TypedSlice::Custom(custom_types) => {
                for (v, rl) in
                    TypedSliceIter::from_range(&range.base, custom_types)
                {
                    if let Some(len) = v.stringified_len() {
                        let mut data = Vec::with_capacity(len);
                        v.stringify(&mut data)
                            .expect("custom stringify failed");
                        if v.stringifies_as_valid_utf8() {
                            push_string(
                                &mut out,
                                unsafe { String::from_utf8_unchecked(data) },
                                rl as usize,
                            );
                        } else {
                            push_bytes(
                                op_id,
                                field_pos,
                                &mut out,
                                &data,
                                rl as usize,
                            );
                        }
                    } else {
                        push_errors(
                            &mut out,
                            OperatorApplicationError::new_s(
                                format!(
                                    "cannot stringify custom type {}",
                                    v.type_name()
                                ),
                                op_id,
                            ),
                            range.base.field_count,
                            field_pos,
                            &mut last_error_end,
                            &mut output_field,
                        );
                    }
                }
            }
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::Null(_) => {
                push_str(&mut out, NULL_STR, range.base.field_count);
            }
            TypedSlice::Error(errs) => {
                let mut pos = field_pos;
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    push_errors(
                        &mut out,
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
                push_errors(
                    &mut out,
                    OperatorApplicationError::new("value is undefined", op_id),
                    range.base.field_count,
                    field_pos,
                    &mut last_error_end,
                    &mut output_field,
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
                            if sv.done || sv.bytes_are_chunk {
                                if sv.bytes_are_utf8 {
                                    push_bytes(
                                        op_id,
                                        pos,
                                        &mut out,
                                        data,
                                        rl as usize,
                                    );
                                } else {
                                    push_str(
                                        &mut out,
                                        unsafe {
                                            std::str::from_utf8_unchecked(data)
                                        },
                                        rl as usize,
                                    );
                                }
                            } else {
                                // to initialize the slots
                                push_string(
                                    &mut out,
                                    String::new(),
                                    rl as usize,
                                );
                            }
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
                                        contains_error: false,
                                    },
                                );
                            }
                        }
                        StreamValueData::Error(e) => push_errors(
                            &mut out,
                            e.clone(),
                            rl as usize,
                            pos,
                            &mut last_error_end,
                            &mut output_field,
                        ),
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
            TypedSlice::Array(_) => {
                todo!();
            }
            TypedSlice::Object(_) => {
                todo!();
            }
        }
        field_pos += range.base.field_count;
    }
    let base_iter = iter.into_base_iter();
    let consumed_fields = base_iter.get_next_field_pos() - starting_pos;
    if consumed_fields < batch_size {
        push_str(&mut out, NULL_STR, batch_size - consumed_fields);
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

pub fn handle_tf_string_sink_stream_value_update(
    sess: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfStringSink<'_>,
    sv_id: StreamValueId,
    custom: usize,
) {
    let mut out = tf.handle.lock().unwrap();
    let svh = &mut tf.stream_value_handles[custom];
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    append_stream_val(
        sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
        sv,
        svh,
        &mut out,
        svh.start_idx,
        svh.run_len,
    );
    if sv.done {
        sess.sv_mgr.drop_field_value_subscription(sv_id, None);
        tf.stream_value_handles.release(custom);
        if tf.stream_value_handles.is_empty() {
            sess.tf_mgr.update_ready_state(tf_id);
        }
    }
}
