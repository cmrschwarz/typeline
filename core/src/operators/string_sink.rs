use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

use bstr::ByteSlice;
use metamatch::metamatch;

use crate::{
    job::JobData,
    operators::print::error_to_string,
    record_data::{
        field::Field,
        field_data::field_value_flags,
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueRangeIter,
        formattable::{
            Formattable, FormattingContext, RealizedFormatKey, TypeReprFormat,
        },
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
        },
        stream_value::{
            StorageAgnosticStreamValueDataRef, StreamValue,
            StreamValueDataOffset, StreamValueUpdate,
        },
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        int_string_conversions::{f64_to_str, i64_to_str},
        lazy_lock_guard::LazyRwLockGuard,
        text_write::{
            MaybeTextWriteFlaggedAdapter, MaybeTextWritePanicAdapter,
            TextWriteIoAdapter,
        },
        universe::CountedUniverse,
    },
    NULL_STR, UNDEFINED_STR,
};

use super::{
    errors::OperatorApplicationError,
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Default)]
pub struct StringSink {
    pub data: Vec<String>,
    pub errors: Vec<(usize, Arc<OperatorApplicationError>)>,
    pub error_indices: HashMap<usize, usize, BuildIdentityHasher>,
}

impl StringSink {
    pub fn insert_error(
        &mut self,
        data_idx: usize,
        err: Arc<OperatorApplicationError>,
    ) {
        self.data[data_idx] = error_to_string(&err);
        self.error_indices.insert(data_idx, self.errors.len());
        self.errors.push((data_idx, err));
    }
    pub fn append_error(&mut self, err: Arc<OperatorApplicationError>) {
        let data_idx = self.data.len();
        let err_idx = self.errors.len();
        self.data.push(error_to_string(&err));
        self.errors.push((data_idx, err));
        self.error_indices.insert(data_idx, err_idx);
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

pub fn create_op_string_sink(handle: &'_ StringSinkHandle) -> OperatorData {
    OperatorData::StringSink(OpStringSink {
        handle: handle.clone(),
    })
}

struct StreamValueHandle {
    start_idx: usize,
    run_len: usize,
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
        out.insert_error(i, err.clone());
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
    sv: &mut StreamValue,
    out: &mut StringSink,
    start_idx: usize,
    run_len: usize,
    offset: StreamValueDataOffset,
) -> Result<(), Arc<OperatorApplicationError>> {
    debug_assert!(run_len > 0);
    let end_idx = start_idx + run_len;
    if let Some(e) = &sv.error {
        return Err(e.clone());
    }
    let mut iter = sv.data_cursor(offset, false);
    while let Some(data) = iter.next() {
        match data.storage_agnostic() {
            StorageAgnosticStreamValueDataRef::Bytes(b) => {
                let Ok(text) = std::str::from_utf8(b) else {
                    let lossy = String::from_utf8_lossy(b);
                    let e = Arc::new(OperatorApplicationError::new(
                        "invalid utf-8",
                        op_id,
                    ));
                    for i in start_idx..end_idx {
                        out.data[i].push_str(&lossy);
                        out.insert_error(i, e.clone());
                    }
                    return Err(e);
                };
                for i in start_idx..end_idx {
                    out.data[i].push_str(text);
                }
            }
            StorageAgnosticStreamValueDataRef::Text(t) => {
                for i in start_idx..end_idx {
                    out.data[i].push_str(t);
                }
            }
        }
    }
    Ok(())
}
pub fn push_errors(
    out: &mut StringSink,
    err: OperatorApplicationError,
    run_length: usize,
    mut field_pos: usize,
    last_interruption_end: &mut usize,
    output_field: &mut Field,
) {
    let e = Arc::new(err.clone());
    for _ in 0..run_length {
        out.append_error(e.clone());
    }
    let successes_so_far = field_pos - *last_interruption_end;
    field_pos += run_length;
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
    let mut string_store = LazyRwLockGuard::new(&jd.session_data.string_store);
    // interruption meaning error or group separator
    let mut last_interruption_end = field_pos;
    let print_rationals_raw =
        jd.get_transform_chain(tf_id).settings.print_rationals_raw;
    while let Some(range) = iter.typed_range_fwd(
        &jd.match_set_mgr,
        usize::MAX,
        field_value_flags::DEFAULT,
    ) {
        metamatch!(match range.base.data {
            FieldValueSlice::Null(_) => {
                push_str(&mut out, NULL_STR, range.base.field_count);
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

            #[expand((REP, ITER) in [
                (TextInline, RefAwareInlineTextIter),
                (TextBuffer, RefAwareTextBufferIter),
            ])]
            FieldValueSlice::REP(v) => {
                for (v, rl, _offs) in ITER::from_range(&range, v) {
                    push_str(&mut out, v, rl as usize);
                }
            }

            #[expand((REP, ITER) in [
                (BytesInline, RefAwareInlineBytesIter),
                (BytesBuffer, RefAwareBytesBufferIter),
            ])]
            FieldValueSlice::REP(v) => {
                for (v, rl, _offs) in ITER::from_range(&range, v) {
                    push_bytes(op_id, field_pos, &mut out, v, rl as usize);
                }
            }

            #[expand((REP, CONV_FN) in [
                (Int, i64_to_str(false, *v)),
                (Float, f64_to_str(*v))
            ])]
            FieldValueSlice::REP(ints) => {
                for (v, rl) in FieldValueRangeIter::from_range(&range, ints) {
                    let v = CONV_FN;
                    push_str(&mut out, v.as_str(), rl as usize);
                }
            }

            FieldValueSlice::BigInt(values) => {
                let mut rfk = RealizedFormatKey::default();
                for (a, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, values)
                {
                    let mut data = String::new();
                    a.format(
                        &mut rfk,
                        &mut MaybeTextWritePanicAdapter(&mut data),
                    )
                    .unwrap();
                    push_string(&mut out, data, rl as usize);
                }
            }

            #[expand(REP in [BigRational, Array, Object, Argument])]
            FieldValueSlice::REP(values) => {
                let mut fc = FormattingContext {
                    ss: &mut string_store,
                    fm: &jd.field_mgr,
                    msm: &jd.match_set_mgr,
                    print_rationals_raw,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::with_type_repr(
                        TypeReprFormat::Typed,
                    ),
                };
                for (a, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, values)
                {
                    let mut data = String::new();
                    a.format(
                        &mut fc,
                        &mut MaybeTextWritePanicAdapter(&mut data),
                    )
                    .unwrap();
                    push_string(&mut out, data, rl as usize);
                }
            }

            FieldValueSlice::Custom(custom_types) => {
                for (v, rl) in RefAwareFieldValueRangeIter::from_range(
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

            FieldValueSlice::Error(errs) => {
                let mut pos = field_pos;
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, errs)
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

            FieldValueSlice::StreamValueId(svs) => {
                let mut pos = field_pos;
                for (sv_id, rl) in FieldValueRangeIter::from_range(&range, svs)
                {
                    let rl = rl as usize;
                    let sv = &mut jd.sv_mgr.stream_values[*sv_id];
                    let start_idx = out.data.len();
                    out.data.push(String::new());
                    let res = append_stream_val(
                        op_id,
                        sv,
                        &mut out,
                        start_idx,
                        1,
                        StreamValueDataOffset::default(),
                    );
                    for i in 1..rl {
                        let s = out.data[start_idx].clone();
                        out.data.push(s);
                        if let Err(e) = &res {
                            out.insert_error(start_idx + i, e.clone());
                        }
                    }

                    if !sv.done {
                        let handle_id = ss
                            .stream_value_handles
                            .claim_with_value(StreamValueHandle {
                                start_idx: pos,
                                run_len: rl,
                            });
                        sv.subscribe(
                            *sv_id,
                            tf_id,
                            handle_id,
                            sv.is_buffered(),
                            true,
                        );
                    }
                    pos += rl;
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        });
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
    tf: &mut TfStringSink<'_>,
    update: StreamValueUpdate,
) {
    let mut out = tf.handle.lock().unwrap();
    let handle_id = update.custom;
    let sv_handle = &mut tf.stream_value_handles[handle_id];
    let sv_in = &mut jd.sv_mgr.stream_values[update.sv_id];
    if let Err(e) = &append_stream_val(
        jd.tf_mgr.transforms[update.tf_id].op_id.unwrap(),
        sv_in,
        &mut out,
        sv_handle.start_idx,
        sv_handle.run_len,
        update.data_offset,
    ) {
        for i in sv_handle.start_idx..sv_handle.start_idx + sv_handle.run_len {
            out.insert_error(i, e.clone());
        }
    }
    if sv_in.done {
        jd.sv_mgr
            .drop_field_value_subscription(update.sv_id, Some(update.tf_id));
        tf.stream_value_handles.release(handle_id);
        if tf.stream_value_handles.is_empty() {
            jd.tf_mgr.push_tf_in_ready_stack(update.tf_id);
        }
    }
}
