use std::{borrow::Cow, io::Write};

use bstr::{BStr, ByteSlice};
use is_terminal::IsTerminal;

use crate::{
    fd_ref_iter::FDRefIterLazy,
    field_data::{
        fd_iter::{
            FDIterator, FDTypedSlice, FDTypedValue, InlineBytesIter, InlineTextIter, TypedSliceIter,
        },
        fd_iter_hall::FDIterId,
        field_value_flags,
    },
    options::argument::CliArgIdx,
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::OperatorData,
    transform::{TransformData, TransformId, TransformState},
};

pub struct TfPrint {
    flush_on_every_print: bool,
    pending_batch_size: usize,
    current_stream_val: Option<StreamValueId>,
    iter_id: FDIterId,
}

pub fn parse_op_print(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "print takes no arguments (for now)",
            arg_idx,
        ));
    }
    Ok(OperatorData::Print)
}

pub fn setup_tf_print(
    sess: &mut JobData,
    tf_state: &mut TransformState,
) -> (TransformData<'static>, FieldId) {
    let tf = TfPrint {
        // TODO: should we make a config option for this?
        flush_on_every_print: std::io::stdout().is_terminal(),
        pending_batch_size: 0,
        current_stream_val: None,
        iter_id: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
    };
    (TransformData::Print(tf), tf_state.input_field)
}

pub fn create_op_print() -> OperatorData {
    OperatorData::Print
}

pub fn write_raw_bytes<const NEWLINE: bool>(
    stream: &mut impl Write,
    bytes: &[u8],
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    for i in 0..run_len {
        stream
            .write(bytes)
            .and_then(|_| if NEWLINE { stream.write(b"\n") } else { Ok(0) })
            .map_err(|e| (i, e))?;
    }
    Ok(())
}
pub fn write_text<const NEWLINE: bool>(
    stream: &mut impl Write,
    text: &str,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, text.as_bytes(), run_len)
}

pub fn write_bytes_utf8_lossy<const NEWLINE: bool>(
    stream: &mut impl Write,
    bytes: &[u8],
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, bytes.to_str_lossy().as_bytes(), run_len)
}

pub fn write_integer<const NEWLINE: bool>(
    stream: &mut impl Write,
    v: i64,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    for i in 0..run_len {
        stream
            .write_fmt(format_args!("{v}\n"))
            .map_err(|e| (i, e))?;
    }
    Ok(())
}
pub fn write_null<const NEWLINE: bool>(
    stream: &mut impl Write,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, b"null", run_len)
}
pub fn write_unset<const NEWLINE: bool>(
    stream: &mut impl Write,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, b"<Unset>", run_len)
}
pub fn write_type_error<const NEWLINE: bool>(
    stdout: &mut impl Write,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stdout, b"<Type Error>", run_len)
}

// SAFETY: guaranteed to write valid utf-8
pub fn write_error<const NEWLINE: bool>(
    stream: &mut impl Write,
    e: &OperatorApplicationError,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    for i in 0..run_len {
        stream
            .write_fmt(format_args!("{e}\n"))
            .and_then(|_| if NEWLINE { stream.write(b"\n") } else { Ok(0) })
            .map_err(|e| (i, e))?;
    }
    Ok(())
}

pub fn write_stream_val_check_done<const NEWLINE: bool>(
    stream: &mut impl Write,
    sv: &StreamValue,
    run_len: usize,
) -> Result<bool, (usize, std::io::Error)> {
    let rl_to_attempt = if sv.done || run_len == 0 {
        run_len as usize
    } else {
        1
    };
    match &sv.data {
        StreamValueData::BytesChunk(c) => {
            for i in 0..rl_to_attempt {
                stream
                    .write(c)
                    .and_then(|_| if NEWLINE { stream.write(b"\n") } else { Ok(0) })
                    .map_err(|e| (i, e))?;
            }
        }
        StreamValueData::BytesBuffer(b) => {
            if sv.done {
                for i in 0..rl_to_attempt {
                    stream
                        .write(b)
                        .and_then(|_| if NEWLINE { stream.write(b"\n") } else { Ok(0) })
                        .map_err(|e| (i, e))?;
                }
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            write_error::<NEWLINE>(stream, e, run_len)?;
        }
        StreamValueData::Dropped => panic!("dropped stream value observed"),
    }
    Ok(sv.done)
}

pub fn handle_tf_print_raw(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfPrint,
    field_pos: &mut usize,
    field_pos_batch_end: &mut usize,
) -> Result<(), (usize, std::io::Error)> {
    let mut stdout = std::io::stdout().lock();
    debug_assert!(!tf.current_stream_val.is_some());
    let (batch, input_field_id);
    if tf.pending_batch_size == 0 {
        (batch, input_field_id) = sess.claim_batch(tf_id, &[]);
        tf.pending_batch_size = batch;
    } else {
        input_field_id = sess.tf_mgr.transforms[tf_id].input_field;
        batch = tf.pending_batch_size;
    }
    let input_field = sess.record_mgr.fields[input_field_id].borrow();
    let mut iter = input_field
        .field_data
        .get_iter(tf.iter_id)
        .bounded(0, batch);
    let starting_pos = iter.get_next_field_pos();
    *field_pos = starting_pos;
    *field_pos_batch_end = starting_pos + batch;

    let mut fd_ref_iter = FDRefIterLazy::default();

    'iter: while let Some(range) =
        iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8)
    {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                for (v, rl) in InlineTextIter::from_typed_range(&range, text) {
                    write_text::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (v, rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    write_raw_bytes::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            FDTypedSlice::BytesBuffer(bytes) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, bytes) {
                    write_raw_bytes::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            FDTypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, ints) {
                    write_integer::<true>(&mut stdout, *v, rl as usize)?;
                }
            }
            FDTypedSlice::Reference(refs) => {
                let mut iter = fd_ref_iter.setup_iter_from_typed_range(
                    &sess.record_mgr.fields,
                    &mut sess.record_mgr.match_sets,
                    *field_pos,
                    &range,
                    refs,
                );
                while let Some(fr) =
                    iter.typed_range_fwd(&mut sess.record_mgr.match_sets, usize::MAX)
                {
                    match fr.data {
                        FDTypedValue::StreamValueId(_) => todo!(),
                        FDTypedValue::BytesInline(v) => write_raw_bytes::<true>(
                            &mut stdout,
                            &v[fr.begin..fr.end],
                            fr.run_len as usize,
                        )?,
                        FDTypedValue::TextInline(v) => write_text::<true>(
                            &mut stdout,
                            &v[fr.begin..fr.end],
                            fr.run_len as usize,
                        )?,
                        _ => panic!("invalid target type for FieldReference"),
                    }
                }
            }
            FDTypedSlice::Null(_) => {
                write_null::<true>(&mut stdout, range.field_count)?;
            }
            FDTypedSlice::Error(errs) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, errs) {
                    write_error::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            FDTypedSlice::Unset(_) => {
                write_unset::<true>(&mut stdout, range.field_count)?;
            }
            FDTypedSlice::StreamValueId(svs) => {
                for (sv_id, rl) in TypedSliceIter::from_typed_range(&range, svs) {
                    let sv = &mut sess.sv_mgr.stream_values[*sv_id];
                    if !write_stream_val_check_done::<true>(&mut stdout, sv, rl as usize)? {
                        tf.current_stream_val = Some(*sv_id);
                        iter.move_to_field_pos(*field_pos);
                        if rl > 1 {
                            sv.promote_to_buffer();
                        }
                        sv.subscribe(tf_id, rl as usize, false);
                        break 'iter;
                    }
                    *field_pos += rl as usize;
                }
                continue; // skip the field pos increase at the bottom of this loop because we already did that
            }
            FDTypedSlice::Html(_) | FDTypedSlice::Object(_) => {
                write_type_error::<true>(&mut stdout, range.field_count)?;
            }
        }
        *field_pos += range.field_count;
    }
    input_field.field_data.store_iter(tf.iter_id, iter);
    drop(input_field);
    if tf.flush_on_every_print {
        drop(stdout.flush());
    }
    let consumed_fields = *field_pos - starting_pos;
    sess.tf_mgr.transforms[tf_id].is_stream_subscriber = tf.current_stream_val.is_some();
    sess.tf_mgr.update_ready_state(tf_id);
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, consumed_fields);

    Ok(())
}

pub fn handle_tf_print(sess: &mut JobData<'_>, tf_id: TransformId, tf: &mut TfPrint) {
    let mut field_pos: usize = 0;
    let mut field_pos_batch_end: usize = 0;
    if let Err((i, err)) =
        handle_tf_print_raw(sess, tf_id, tf, &mut field_pos, &mut field_pos_batch_end)
    {
        let fp = field_pos + i;
        sess.record_mgr.push_entry_error(
            sess.tf_mgr.transforms[tf_id].match_set_id,
            fp,
            OperatorApplicationError {
                op_id: sess.tf_mgr.transforms[tf_id].op_id,
                message: Cow::Owned(err.to_string()),
            },
            field_pos_batch_end - fp,
        );
    }
}

pub fn handle_tf_print_stream_value_update(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfPrint,
    svid: StreamValueId,
    custom: usize,
) {
    let mut stdout = std::io::stdout().lock();
    let sv = &sess.sv_mgr.stream_values[svid];
    let run_len = custom;
    match write_stream_val_check_done::<true>(&mut stdout, sv, run_len) {
        Ok(true) => sess.tf_mgr.update_ready_state(tf_id),
        Ok(false) => (),
        Err((i, e)) => {
            sess.record_mgr.push_entry_error(
                sess.tf_mgr.transforms[tf_id].match_set_id,
                sess.record_mgr.fields[sess.tf_mgr.transforms[tf_id].input_field]
                    .borrow()
                    .field_data
                    .get_iter(tf.iter_id)
                    .get_next_field_pos()
                    - run_len
                    + i,
                OperatorApplicationError {
                    op_id: sess.tf_mgr.transforms[tf_id].op_id,
                    message: Cow::Owned(e.to_string()),
                },
                run_len - i,
            );
        }
    }
}
