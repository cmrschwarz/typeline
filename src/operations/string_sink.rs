use std::{
    borrow::Cow,
    ops::Deref,
    ops::DerefMut,
    sync::{Arc, Mutex, MutexGuard},
};

use bstr::ByteSlice;
use indexmap::IndexMap;
use smallstr::SmallString;

use crate::{
    field_data::{field_value_flags, push_interface::PushInterface},
    operations::print::{write_error, write_integer, write_null, write_raw_bytes, write_unset},
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
    },
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::universe::Universe,
    worker_thread_session::{FieldId, JobData},
};
use crate::{
    field_data::{
        iter_hall::IterId, iters::FieldIterator, typed::TypedSlice, typed_iters::TypedSliceIter,
        FieldValueKind,
    },
    ref_iter::RefAwareStreamValueIter,
};

use super::{
    errors::{OperatorApplicationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId, DEFAULT_OP_NAME_SMALL_STR_LEN},
    print::{write_stream_val_check_done, write_success, write_text},
    transform::{TransformData, TransformId, TransformState},
};

pub struct StringSink {
    pub data: Vec<String>,
    pub errors: IndexMap<usize, Arc<OperatorApplicationError>>,
}

#[derive(Clone)]
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
    pub fn new() -> StringSinkHandle {
        StringSinkHandle {
            data: Arc::new(Mutex::new(StringSink {
                data: Default::default(),
                errors: Default::default(),
            })),
        }
    }
    pub fn get(&self) -> MutexGuard<StringSink> {
        self.data.lock().unwrap()
    }
    pub fn get_data(&self) -> Result<StringSinkDataGuard, Arc<OperatorApplicationError>> {
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
    handle: StringSinkHandle,
    transparent: bool,
}

impl OpStringSink {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        if self.transparent {
            SmallString::from("<String Sink (Transparent)>")
        } else {
            SmallString::from("<String Sink>")
        }
    }
}

pub fn create_op_string_sink(handle: &'_ StringSinkHandle) -> OperatorData {
    OperatorData::StringSink(OpStringSink {
        handle: handle.clone(),
        transparent: false,
    })
}

pub fn create_op_string_sink_transparent(handle: &'_ StringSinkHandle) -> OperatorData {
    OperatorData::StringSink(OpStringSink {
        handle: handle.clone(),
        transparent: true,
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
    stream_value_handles: Universe<usize, StreamValueHandle>,
    buf: Vec<u8>,
    output_field: Option<FieldId>,
}

pub fn setup_op_string_sink(
    op_id: OperatorId,
    op_base: &OperatorBase,
    op: &mut OpStringSink,
) -> Result<(), OperatorSetupError> {
    if op_base.append_mode && op.transparent {
        return Err(OperatorSetupError {
            op_id,
            message: "A transparent String Sink cannot be in append mode".into(),
        });
    }
    Ok(())
}

pub fn setup_tf_string_sink<'a>(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    ss: &'a OpStringSink,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
    let output_field = if ss.transparent {
        if !tf_state.is_appending {
            sess.record_mgr.remove_field(tf_state.output_field);
        }
        tf_state.output_field = tf_state.input_field;
        None
    } else {
        Some(tf_state.output_field)
    };
    TransformData::StringSink(TfStringSink {
        handle: &ss.handle.data,
        batch_iter: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
        stream_value_handles: Default::default(),
        buf: Default::default(),
        output_field,
    })
}

fn push_string(
    sess: &JobData<'_>,
    tf_id: TransformId,
    field_pos: usize,
    out: &mut StringSink,
    buf: Vec<u8>,
    run_len: usize,
) {
    let str = match String::from_utf8(buf) {
        Ok(s) => s,
        Err(e) => {
            let err = Arc::new(OperatorApplicationError {
                op_id: sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                message: Cow::Borrowed("invalid utf-8"),
            });
            for i in field_pos..field_pos + run_len {
                out.errors.insert(i, err.clone());
            }
            String::from_utf8_lossy(e.as_bytes()).to_string()
        }
    };
    out.data
        .extend(std::iter::repeat_with(|| str.clone()).take(run_len - 1));
    out.data.push(str);
}
fn push_string_clear_buf(
    sess: &JobData<'_>,
    tf_id: TransformId,
    field_pos: usize,
    out: &mut StringSink,
    buf: &mut Vec<u8>,
    run_len: usize,
) {
    let data = std::mem::replace::<Vec<u8>>(buf, Vec::new());
    push_string(sess, tf_id, field_pos, out, data, run_len);
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
                match buf.to_str() {
                    Ok(text) => text,
                    Err(_) => {
                        if !sv_handle.contains_error {
                            sv_handle.contains_error = true;
                            let err = Arc::new(OperatorApplicationError {
                                op_id: op_id,
                                message: Cow::Borrowed("invalid utf-8"),
                            });
                            for i in start_idx..end_idx {
                                out.errors.insert(i, err.clone());
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
            for i in start_idx..end_idx {
                write_error::<false>(unsafe { out.data[i].as_mut_vec() }, e, 1).unwrap();
                out.errors.insert(i, err.clone());
            }
        }
        StreamValueData::Dropped => panic!("dropped stream value observed"),
    }
}

pub fn handle_tf_string_sink(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    ss: &mut TfStringSink<'_>,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_field = sess.record_mgr.fields[tf.input_field].borrow();
    let mut output_field = ss
        .output_field
        .map(|id| sess.record_mgr.fields[id].borrow_mut());
    let base_iter = input_field
        .field_data
        .get_iter(ss.batch_iter)
        .bounded(0, batch_size);
    let starting_pos = base_iter.get_next_field_pos();
    let mut iter = AutoDerefIter::new(
        &sess.record_mgr.fields,
        &mut sess.record_mgr.match_sets,
        tf.input_field,
        base_iter,
        None,
    );
    let mut out = ss.handle.lock().unwrap();
    let buf = &mut ss.buf;
    let mut field_pos = out.data.len();

    let mut last_errror_end = 0;
    while let Some(range) = iter.typed_range_fwd(
        &mut sess.record_mgr.match_sets,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    write_text::<false>(buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    write_raw_bytes::<false>(buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    push_string(sess, tf_id, field_pos, &mut out, v.to_owned(), rl as usize);
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    write_integer::<false>(buf, *v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::Null(_) => {
                write_null::<false>(buf, range.base.field_count).unwrap();
                push_string_clear_buf(
                    sess,
                    tf_id,
                    field_pos,
                    &mut out,
                    buf,
                    range.base.field_count,
                );
            }
            TypedSlice::Error(errs) => {
                let mut pos = field_pos;
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    write_error::<false>(buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                    let e = Arc::new(v.clone());
                    for i in 0..rl as usize {
                        out.errors.insert(pos + i, e.clone());
                    }
                    pos += rl as usize;
                    let successes_so_far = pos - last_errror_end;
                    output_field.as_mut().map(|of| {
                        if successes_so_far > 0 {
                            of.field_data.push_success(pos - last_errror_end, true);
                            of.field_data
                                .push_error(v.clone(), rl as usize, false, false);
                        } else {
                            of.field_data.push_error(v.clone(), rl as usize, true, true);
                        }
                    });
                    last_errror_end = pos;
                }
            }
            TypedSlice::Unset(_) => {
                write_unset::<false>(buf, 1).unwrap();
                push_string_clear_buf(
                    sess,
                    tf_id,
                    field_pos,
                    &mut out,
                    buf,
                    range.base.field_count,
                );
            }
            TypedSlice::Success(_) => {
                write_success::<false>(buf, 1).unwrap();
                push_string_clear_buf(
                    sess,
                    tf_id,
                    field_pos,
                    &mut out,
                    buf,
                    range.base.field_count,
                );
            }
            TypedSlice::StreamValueId(svs) => {
                for (svid, offsets, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                    let sv = &mut sess.sv_mgr.stream_values[svid];
                    if !write_stream_val_check_done::<false>(buf, sv, offsets, 1).unwrap() {
                        sv.subscribe(tf_id, ss.stream_value_handles.len(), sv.is_buffered());
                        ss.stream_value_handles.claim_with_value(StreamValueHandle {
                            start_idx: out.data.len(),
                            run_len: rl as usize,
                            contains_error: false,
                        });
                    }
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                todo!();
            }
        }
        field_pos += range.base.field_count;
    }
    let consumed_fields = field_pos - starting_pos;
    input_field
        .field_data
        .store_iter(ss.batch_iter, iter.into_base_iter());
    output_field.as_mut().map(|of| {
        of.field_data
            .push_success(field_pos - last_errror_end, true);
    });
    drop(input_field);
    drop(output_field);
    let streams_done = ss.stream_value_handles.claimed_entry_count() == 0;
    if streams_done {
        sess.tf_mgr.update_ready_state(tf_id);
    }
    if input_done && streams_done {
        sess.unlink_transform(tf_id, consumed_fields);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, consumed_fields);
    }
}

pub fn handle_tf_string_sink_stream_value_update(
    sess: &mut JobData<'_>,
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
        if tf.stream_value_handles.claimed_entry_count() == 0 {
            sess.tf_mgr.update_ready_state(tf_id);
        }
    }
}
