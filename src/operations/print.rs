use std::{borrow::Cow, io::Write};

use bstr::{BStr, ByteSlice};
use is_terminal::IsTerminal;

use crate::{
    field_data::{
        fd_iter::{
            FDIterator, FDTypedSlice, FDTypedValue, InlineBytesIter, InlineTextIter, TypedSliceIter,
        },
        fd_iter_hall::FDIterId,
        field_value_flags, FieldReference, RunLength,
    },
    options::argument::CliArgIdx,
    stream_field_data::{StreamFieldValue, StreamFieldValueData, StreamValueId},
    worker_thread_session::{EntryData, FieldId, JobData, MatchSetId, FIELD_REF_LOOKUP_ITER_ID},
};

use super::{
    errors::{io_error_to_op_error, OperatorApplicationError, OperatorCreationError},
    operator::OperatorData,
    transform::{TransformData, TransformId},
};

pub struct TfPrint {
    flush_on_every_print: bool,
    consumed_entries: usize,
    dropped_entries: usize,
    current_stream_val: Option<StreamValueId>,
    batch_iter: FDIterId,
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
    _ms_id: MatchSetId,
    input_field: FieldId,
) -> (TransformData<'static>, FieldId) {
    let tf = TfPrint {
        // TODO: should we make a config option for this?
        flush_on_every_print: std::io::stdout().is_terminal(),
        consumed_entries: 0,
        dropped_entries: 0,
        current_stream_val: None,
        batch_iter: sess.entry_data.fields[input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
    };
    (TransformData::Print(tf), input_field)
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
pub fn write_inline_text<const NEWLINE: bool>(
    stream: &mut impl Write,
    text: &str,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, text.as_bytes(), run_len)
}

pub fn write_inline_bytes_utf8_lossy<const NEWLINE: bool>(
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

fn write_stream_val_check_done<const NEWLINE: bool>(
    stream: &mut impl Write,
    sv: &StreamFieldValue,
) -> Result<bool, std::io::Error> {
    match &sv.data {
        StreamFieldValueData::BytesChunk(c) => {
            stream.write(c)?;
            if sv.done && NEWLINE {
                stream.write(b"\n")?;
            }
        }
        StreamFieldValueData::BytesBuffer(b) => {
            if sv.done {
                stream.write(b)?;
                if NEWLINE {
                    stream.write(b"\n")?;
                }
            }
        }
        StreamFieldValueData::Error(err) => {
            debug_assert!(sv.done);
            println!("error: {err}");
        }
        StreamFieldValueData::Dropped => panic!("dropped stream value observed"),
    }
    Ok(sv.done)
}
pub fn write_values_behind_field_ref<'a, const NEWLINE: bool>(
    stream: &mut impl Write,
    r: &FieldReference,
    iter: &mut impl FDIterator<'a>,
    mut rl: RunLength,
) -> Result<(), (usize, std::io::Error)> {
    let mut handled_elements = 0;
    while rl > 0 {
        let tr = iter.typed_range_fwd(rl as usize, 0).unwrap();
        rl -= tr.field_count as RunLength;
        match tr.data {
            FDTypedSlice::StreamValueId(_) => panic!("hit stream value in batch mode"),
            FDTypedSlice::BytesInline(v) => {
                for (bytes, rl) in InlineBytesIter::from_typed_range(&tr, v) {
                    write_raw_bytes::<NEWLINE>(stream, &bytes[r.begin..r.end], rl as usize)
                        .map_err(|(i, e)| (i + handled_elements, e))?;
                }
            }
            FDTypedSlice::TextInline(v) => {
                for (text, rl) in InlineTextIter::from_typed_range(&tr, v) {
                    write_inline_text::<NEWLINE>(stream, &text[r.begin..r.end], rl as usize)
                        .map_err(|(i, e)| (i + handled_elements, e))?;
                }
            }
            _ => panic!("Field Reference must refer to byte stream"),
        }
        handled_elements += tr.field_count;
    }
    Ok(())
}

pub fn handle_tf_print_batch_mode_raw(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfPrint,
    field_pos: &mut usize,
    field_pos_batch_end: &mut usize,
) -> Result<(), (usize, std::io::Error)> {
    let (batch, input_field_id) = sess.claim_batch(tf_id, &[]);
    let input_field = sess.entry_data.fields[input_field_id].borrow();
    let mut iter = input_field
        .field_data
        .get_iter(tf.batch_iter)
        .bounded(0, batch);
    let starting_pos = iter.get_next_field_pos();
    *field_pos = starting_pos;
    *field_pos_batch_end = starting_pos + batch;
    let mut stdout = std::io::stdout().lock();

    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                for (v, rl) in InlineTextIter::from_typed_range(&range, text) {
                    write_inline_text::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (v, rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    write_raw_bytes::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            FDTypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, ints) {
                    write_integer::<true>(&mut stdout, *v, rl as usize)?;
                }
            }
            FDTypedSlice::Reference(refs) => {
                let mut field_pos = starting_pos;
                for (r, rl) in TypedSliceIter::from_typed_range(&range, refs) {
                    // PERF: this is terrible
                    EntryData::apply_field_commands(
                        &sess.entry_data.fields,
                        &mut sess.entry_data.match_sets,
                        r.field,
                    );
                    let field = sess.entry_data.fields[r.field].borrow();
                    let mut iter = field.field_data.get_iter(FIELD_REF_LOOKUP_ITER_ID);
                    iter.move_to_field_pos(field_pos);
                    field_pos += rl as usize;
                    write_values_behind_field_ref::<true>(&mut stdout, r, &mut iter, rl)?;
                    field.field_data.store_iter(FIELD_REF_LOOKUP_ITER_ID, iter);
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
            FDTypedSlice::StreamValueId(_) => {
                panic!("hit stream value in batch mode");
            }
            FDTypedSlice::Html(_) | FDTypedSlice::Object(_) => {
                write_type_error::<true>(&mut stdout, range.field_count)?;
            }
        }
        *field_pos += range.field_count;
    }
    //test whether we got the batch that was advertised
    debug_assert!(iter.get_next_field_pos() - starting_pos == batch);
    input_field.field_data.store_iter(tf.batch_iter, iter);
    drop(input_field);
    sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
    Ok(())
}

pub fn handle_tf_print_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, tf: &mut TfPrint) {
    let mut field_pos: usize = 0;
    let mut field_pos_batch_end: usize = 0;
    if let Err((i, err)) =
        handle_tf_print_batch_mode_raw(sess, tf_id, tf, &mut field_pos, &mut field_pos_batch_end)
    {
        let fp = field_pos + i;
        sess.entry_data.push_entry_error(
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

pub fn handle_tf_print_stream_mode_raw(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf_print: &mut TfPrint,
    field_pos: &mut usize,
) -> Result<(), (usize, std::io::Error)> {
    let tf = &mut sess.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let input_field = sess.entry_data.fields[input_field_id].borrow();
    let sfd = &input_field.stream_field_data;
    let mut stdout = std::io::stdout().lock();
    tf_print.dropped_entries += sfd.entries_dropped;

    if let Some(id) = tf_print.current_stream_val {
        let res = write_stream_val_check_done::<true>(&mut stdout, &sfd.get_value_mut(id));
        match res {
            Ok(false) => (),
            Ok(true) => {
                tf_print.consumed_entries += 1;
                tf_print.current_stream_val = None;
            }
            Err(err) => {
                let _err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id, err);
                //TODO: we need the field ref manager for this
                //sess.push_entry_error(sess.tf_mgr.transforms[tf_id].match_set_id, err);
                tf_print.current_stream_val = None;
            }
        }
    }
    if tf_print.current_stream_val.is_none() {
        let mut iter = input_field.field_data.iter();
        iter.next_n_fields(tf_print.consumed_entries - tf_print.dropped_entries);
        while iter.is_next_valid() {
            let field_val = iter.get_next_typed_field();
            *field_pos = iter.get_next_field_pos();
            match field_val.value {
                FDTypedValue::Unset(_) => write_unset::<true>(&mut stdout, 1)?,
                FDTypedValue::Null(_) => write_null::<true>(&mut stdout, 1)?,
                FDTypedValue::Integer(v) => write_integer::<true>(&mut stdout, v, 1)?,
                FDTypedValue::Reference(r) => {
                    let fd = &sess.entry_data.fields[r.field].borrow().field_data;
                    let mut iter = fd.get_iter(FIELD_REF_LOOKUP_ITER_ID);
                    iter.move_to_field_pos(iter.get_next_field_pos());
                    write_values_behind_field_ref::<true>(
                        &mut stdout,
                        r,
                        &mut iter,
                        field_val.header.run_length,
                    )?;
                    fd.store_iter(FIELD_REF_LOOKUP_ITER_ID, iter);
                }
                FDTypedValue::TextInline(text) => write_inline_text::<true>(&mut stdout, text, 1)?,
                FDTypedValue::BytesInline(bytes) => {
                    write_inline_bytes::<true>(&mut stdout, bytes, 1)?
                }
                FDTypedValue::Error(error) => write_error::<true>(&mut stdout, error, 1)?,
                FDTypedValue::Html(_) | FDTypedValue::Object(_) => {
                    write_type_error::<true>(&mut stdout, 1)?
                }
                FDTypedValue::StreamValueId(id) => {
                    let sfd = &input_field.stream_field_data;
                    let sv = sfd.get_value(id);
                    write_stream_val_check_done::<true>(&mut stdout, &sv).map_err(|e| (0, e))?;
                }
            }
            tf_print.consumed_entries += 1;
        }
    }
    if tf_print.flush_on_every_print {
        let _ = std::io::stdout().flush();
    }
    sess.tf_mgr.push_successor_in_ready_queue(tf_id);
    Ok(())
}

pub fn handle_tf_print_stream_mode(sess: &mut JobData<'_>, tf_id: TransformId, tf: &mut TfPrint) {
    let mut field_pos: usize = 0;
    if let Err((i, err)) = handle_tf_print_stream_mode_raw(sess, tf_id, tf, &mut field_pos) {
        sess.entry_data.push_entry_error(
            sess.tf_mgr.transforms[tf_id].match_set_id,
            field_pos + i,
            OperatorApplicationError {
                op_id: sess.tf_mgr.transforms[tf_id].op_id,
                message: Cow::Owned(err.to_string()),
            },
            1,
        );
    };
}
