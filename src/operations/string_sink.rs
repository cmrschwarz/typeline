use std::{
    borrow::Cow,
    sync::{Arc, Mutex, MutexGuard},
};

use bstr::ByteSlice;

use crate::{
    fd_ref_iter::FDRefIterLazy,
    field_data::{
        fd_iter::{
            FDIterator, FDTypedSlice, FDTypedValue, InlineBytesIter, InlineTextIter, TypedSliceIter,
        },
        fd_iter_hall::FDIterId,
        field_value_flags,
    },
    operations::print::{
        write_error, write_integer, write_null, write_raw_bytes, write_type_error, write_unset,
    },
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::universe::Universe,
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::OperatorApplicationError,
    operator::{OperatorData, OperatorId},
    print::{write_stream_val_check_done, write_text},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct StringSinkHandle {
    data: Arc<Mutex<Vec<String>>>,
}

impl StringSinkHandle {
    pub fn new() -> StringSinkHandle {
        StringSinkHandle {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn get(&self) -> MutexGuard<Vec<String>> {
        self.data.lock().unwrap()
    }
}

#[derive(Clone)]
pub struct OpStringSink {
    handle: StringSinkHandle,
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
    handle: &'a Mutex<Vec<String>>,
    batch_iter: FDIterId,
    stream_value_handles: Universe<usize, StreamValueHandle>,
    buf: Vec<u8>,
}

pub fn setup_tf_string_sink<'a>(
    sess: &mut JobData,
    ss: &'a OpStringSink,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    let tf = TfStringSink {
        handle: &ss.handle.data,
        batch_iter: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
        stream_value_handles: Default::default(),
        buf: Default::default(),
    };
    (TransformData::StringSink(tf), tf_state.input_field)
}

fn push_string(
    sess: &JobData<'_>,
    tf_id: TransformId,
    field_pos: usize,
    out: &mut Vec<String>,
    buf: Vec<u8>,
    run_len: usize,
) {
    let str = match String::from_utf8(buf) {
        Ok(s) => s,
        Err(e) => {
            sess.record_mgr.push_entry_error(
                sess.tf_mgr.transforms[tf_id].match_set_id,
                field_pos,
                OperatorApplicationError {
                    op_id: sess.tf_mgr.transforms[tf_id].op_id,
                    message: Cow::Borrowed("invalid utf-8"),
                },
                run_len,
            );
            String::from_utf8_lossy(e.as_bytes()).to_string()
        }
    };
    out.extend(std::iter::repeat_with(|| str.clone()).take(run_len - 1));
    out.push(str);
}
fn push_string_clear_buf(
    sess: &JobData<'_>,
    tf_id: TransformId,
    field_pos: usize,
    out: &mut Vec<String>,
    buf: &mut Vec<u8>,
    run_len: usize,
) {
    let data = std::mem::replace::<Vec<u8>>(buf, Vec::new());
    push_string(sess, tf_id, field_pos, out, data, run_len);
}

fn append_stream_val(
    op_id: OperatorId,
    sv: &StreamValue,
    out: &mut Vec<String>,
    start_idx: usize,
    run_len: usize,
) -> Result<(), OperatorApplicationError> {
    let end_idx = start_idx + run_len;
    match &sv.data {
        StreamValueData::BytesChunk(c) => match c.to_str() {
            Ok(text) => {
                for i in start_idx..end_idx {
                    out[i].push_str(text);
                }
            }
            Err(_) => {
                let lossy = String::from_utf8_lossy(c.as_bytes());
                for i in start_idx..end_idx {
                    out[i].push_str(&lossy);
                }
                return Err(OperatorApplicationError {
                    op_id: op_id,
                    message: Cow::Borrowed("invalid utf-8"),
                });
            }
        },
        StreamValueData::BytesBuffer(b) => {
            if sv.done {
                match b.to_str() {
                    Ok(s) => {
                        for i in start_idx..end_idx {
                            out[i].push_str(s);
                        }
                    }
                    Err(_) => {
                        let lossy = String::from_utf8_lossy(b);
                        for i in (start_idx..end_idx).skip(1) {
                            out[i].push_str(&lossy);
                        }
                        out[start_idx] = lossy.to_string();
                        return Err(OperatorApplicationError {
                            op_id: op_id,
                            message: Cow::Borrowed("invalid utf-8"),
                        });
                    }
                }
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            for i in start_idx..end_idx {
                write_error::<false>(unsafe { out[i].as_mut_vec() }, e, 1).unwrap();
            }
        }
        StreamValueData::Dropped => panic!("dropped stream value observed"),
    }
    Ok(())
}

pub fn handle_tf_string_sink(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfStringSink<'_>,
) {
    let (batch, input_field_id) = sess.claim_batch(tf_id);
    let input_field = sess.record_mgr.fields[input_field_id].borrow();
    let mut iter = input_field
        .field_data
        .get_iter(tf.batch_iter)
        .bounded(0, batch);
    let starting_pos = iter.get_next_field_pos();
    let mut field_pos = starting_pos;
    let mut out = tf.handle.lock().unwrap();
    let mut fd_ref_iter = FDRefIterLazy::default();

    let buf = &mut tf.buf;

    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                for (v, rl) in InlineTextIter::from_typed_range(&range, text) {
                    write_text::<false>(buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (v, rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    write_raw_bytes::<false>(buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            FDTypedSlice::BytesBuffer(bytes) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, bytes) {
                    push_string(sess, tf_id, field_pos, &mut out, v.clone(), rl as usize);
                }
            }
            FDTypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, ints) {
                    write_integer::<false>(buf, *v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            FDTypedSlice::Reference(refs) => {
                let mut iter = fd_ref_iter.setup_iter_from_typed_range(
                    &sess.record_mgr.fields,
                    &mut sess.record_mgr.match_sets,
                    field_pos,
                    &range,
                    refs,
                );
                while let Some(fr) =
                    iter.typed_range_fwd(&mut sess.record_mgr.match_sets, usize::MAX)
                {
                    match fr.data {
                        FDTypedValue::StreamValueId(_) => todo!(),
                        FDTypedValue::BytesInline(v) => {
                            write_raw_bytes::<false>(buf, &v[fr.begin..fr.end], 1).unwrap()
                        }
                        FDTypedValue::BytesBuffer(v) => {
                            write_raw_bytes::<false>(buf, &v[fr.begin..fr.end], 1).unwrap()
                        }
                        FDTypedValue::TextInline(v) => {
                            write_text::<false>(buf, &v[fr.begin..fr.end], 1).unwrap()
                        }
                        _ => panic!("invalid target type for FieldReference"),
                    }
                    push_string_clear_buf(
                        sess,
                        tf_id,
                        field_pos,
                        &mut out,
                        buf,
                        fr.header.run_length as usize,
                    );
                }
            }
            FDTypedSlice::Null(_) => {
                write_null::<false>(buf, range.field_count).unwrap();
            }
            FDTypedSlice::Error(errs) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, errs) {
                    write_error::<false>(buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            FDTypedSlice::Unset(_) => {
                write_unset::<false>(buf, 1).unwrap();
                push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, range.field_count);
            }
            FDTypedSlice::StreamValueId(svs) => {
                for (svid, rl) in TypedSliceIter::from_typed_range(&range, svs) {
                    let sv = &mut sess.sv_mgr.stream_values[*svid];
                    if !write_stream_val_check_done::<false>(buf, sv, 1).unwrap() {
                        sv.subscribe(tf_id, tf.stream_value_handles.len(), sv.is_buffered());
                        tf.stream_value_handles.claim_with_value(StreamValueHandle {
                            start_idx: out.len(),
                            run_len: rl as usize,
                            contains_error: false,
                        });
                    }
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, buf, rl as usize);
                }
            }
            FDTypedSlice::Html(_) | FDTypedSlice::Object(_) => {
                write_type_error::<false>(buf, range.field_count).unwrap();
            }
        }
        field_pos += range.field_count;
    }
    let consumed_fields = field_pos - starting_pos;
    input_field.field_data.store_iter(tf.batch_iter, iter);
    drop(input_field);
    if tf.stream_value_handles.claimed_entry_count() == 0 {
        sess.tf_mgr.update_ready_state(tf_id);
    }
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, consumed_fields);
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
    match append_stream_val(
        sess.tf_mgr.transforms[tf_id].op_id,
        sv,
        &mut out,
        svh.start_idx,
        svh.run_len,
    ) {
        Ok(_) => (),
        Err(oae) => {
            if !svh.contains_error {
                sess.record_mgr.push_entry_error(
                    sess.tf_mgr.transforms[tf_id].match_set_id,
                    svh.start_idx,
                    oae,
                    svh.run_len,
                );
                svh.contains_error = true;
            }
        }
    }
    if sv.done {
        sess.sv_mgr.drop_field_value_subscription(sv_id, None);
        tf.stream_value_handles.release(custom);
        if tf.stream_value_handles.claimed_entry_count() == 0 {
            sess.tf_mgr.update_ready_state(tf_id);
        }
    }
}
