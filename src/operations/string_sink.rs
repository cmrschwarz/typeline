use std::{
    borrow::Cow,
    sync::{Arc, Mutex, MutexGuard},
};

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
        write_error, write_inline_text, write_integer, write_null, write_raw_bytes,
        write_type_error, write_unset,
    },
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::OperatorApplicationError,
    operator::OperatorData,
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

pub struct TfStringSink<'a> {
    handle: &'a Mutex<Vec<String>>,
    batch_iter: FDIterId,
}

pub fn setup_tf_string_sink<'a>(
    sess: &mut JobData,
    ss: &'a OpStringSink,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    let tf = TfStringSink {
        handle: &ss.handle.data,
        batch_iter: sess.entry_data.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
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
            sess.entry_data.push_entry_error(
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

pub fn handle_tf_string_sink_batch_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfStringSink<'_>,
) {
    let (batch, input_field_id) = sess.claim_batch(tf_id, &[]);
    let input_field = sess.entry_data.fields[input_field_id].borrow();
    let mut iter = input_field
        .field_data
        .get_iter(tf.batch_iter)
        .bounded(0, batch);
    let starting_pos = iter.get_next_field_pos();
    let mut field_pos = starting_pos;
    let mut out = tf.handle.lock().unwrap();
    let mut fd_ref_iter = FDRefIterLazy::default();

    let mut buf = Vec::new();

    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                for (v, rl) in InlineTextIter::from_typed_range(&range, text) {
                    write_inline_text::<false>(&mut buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, &mut buf, rl as usize);
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (v, rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    write_raw_bytes::<false>(&mut buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, &mut buf, rl as usize);
                }
            }
            FDTypedSlice::BytesBuffer(bytes) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, bytes) {
                    push_string(sess, tf_id, field_pos, &mut out, v.clone(), rl as usize);
                }
            }
            FDTypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, ints) {
                    write_integer::<false>(&mut buf, *v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, &mut buf, rl as usize);
                }
            }
            FDTypedSlice::Reference(refs) => {
                let mut iter = fd_ref_iter.setup_iter_from_typed_range(
                    &sess.entry_data.fields,
                    &mut sess.entry_data.match_sets,
                    field_pos,
                    &range,
                    refs,
                );
                while let Some(fr) =
                    iter.typed_range_fwd(&mut sess.entry_data.match_sets, usize::MAX)
                {
                    match fr.data {
                        FDTypedValue::StreamValueId(_) => todo!(),
                        FDTypedValue::BytesInline(v) => {
                            write_raw_bytes::<false>(&mut buf, &v[fr.begin..fr.end], 1).unwrap()
                        }
                        FDTypedValue::BytesBuffer(v) => {
                            write_raw_bytes::<false>(&mut buf, &v[fr.begin..fr.end], 1).unwrap()
                        }
                        FDTypedValue::TextInline(v) => {
                            write_inline_text::<false>(&mut buf, &v[fr.begin..fr.end], 1).unwrap()
                        }
                        _ => panic!("invalid target type for FieldReference"),
                    }
                    push_string_clear_buf(
                        sess,
                        tf_id,
                        field_pos,
                        &mut out,
                        &mut buf,
                        fr.run_len as usize,
                    );
                }
            }
            FDTypedSlice::Null(_) => {
                write_null::<false>(&mut buf, range.field_count).unwrap();
            }
            FDTypedSlice::Error(errs) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, errs) {
                    write_error::<false>(&mut buf, v, 1).unwrap();
                    push_string_clear_buf(sess, tf_id, field_pos, &mut out, &mut buf, rl as usize);
                }
            }
            FDTypedSlice::Unset(_) => {
                write_unset::<false>(&mut buf, 1).unwrap();
                push_string_clear_buf(
                    sess,
                    tf_id,
                    field_pos,
                    &mut out,
                    &mut buf,
                    range.field_count,
                );
            }
            FDTypedSlice::StreamValueId(_) => {
                panic!("hit stream value in batch mode");
            }
            FDTypedSlice::Html(_) | FDTypedSlice::Object(_) => {
                write_type_error::<false>(&mut buf, range.field_count).unwrap();
            }
        }
        field_pos += range.field_count;
    }
    //test whether we got the batch that was advertised
    debug_assert!(iter.get_next_field_pos() - starting_pos == batch);
    input_field.field_data.store_iter(tf.batch_iter, iter);
    drop(input_field);
    sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
}

pub fn handle_tf_string_sink_stream_mode(
    _sess: &mut JobData<'_>,
    _tf_id: TransformId,
    _tf: &mut TfStringSink<'_>,
) {
    todo!()
}
