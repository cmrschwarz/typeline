use std::io::Write;

use bstr::BStr;
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

pub fn parse_print_op(
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

pub fn print_inline_text(text: &str, run_len: usize) {
    for _ in 0..run_len {
        println!("{text}");
    }
}
pub fn print_inline_bytes(text: &[u8], run_len: usize) {
    let mut stdout = std::io::stdout().lock();
    for _ in 0..run_len {
        // TODO: handle errors here
        // HACK
        stdout
            .write(text)
            .and_then(|_| stdout.write(&['\n' as u8]))
            .unwrap();
    }
}

pub fn print_integer(v: i64, run_len: usize) {
    for _ in 0..run_len {
        println!("{v}");
    }
}

pub fn print_null(run_len: usize) {
    for _ in 0..run_len {
        println!("null");
    }
}
pub fn print_unset(run_len: usize) {
    for _ in 0..run_len {
        println!("<unset>");
    }
}
pub fn print_error(_sess: &JobData, e: &OperatorApplicationError, run_len: usize) {
    for _ in 0..run_len {
        println!("{}", e); //TODO: improve this
    }
}
pub fn print_type_error(run_len: usize) {
    for _ in 0..run_len {
        println!("<Type Error>");
    }
}
fn print_stream_val_check_done(sv: &StreamFieldValue) -> Result<bool, std::io::Error> {
    match &sv.data {
        StreamFieldValueData::BytesChunk(c) => {
            let mut stdout = std::io::stdout().lock();
            stdout.write(c)?;
            if sv.done {
                stdout.write(&['\n' as u8])?;
            }
        }
        StreamFieldValueData::BytesBuffer(b) => {
            if sv.done {
                std::io::stdout().lock().write(b)?;
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
fn print_values_behind_field_ref<'a>(
    r: &FieldReference,
    iter: &mut impl FDIterator<'a>,
    mut rl: RunLength,
) {
    while rl > 0 {
        let tr = iter.typed_range_fwd(rl as usize, 0).unwrap();
        rl -= tr.field_count as RunLength;
        match tr.data {
            FDTypedSlice::StreamValueId(_) => panic!("hit stream value in batch mode"),
            FDTypedSlice::BytesInline(v) => {
                for (bytes, rl) in InlineBytesIter::from_typed_range(&tr, v) {
                    print_inline_bytes(&bytes[r.begin..r.end], rl as usize)
                }
            }
            FDTypedSlice::TextInline(v) => {
                for (text, rl) in InlineTextIter::from_typed_range(&tr, v) {
                    print_inline_text(&text[r.begin..r.end], rl as usize)
                }
            }
            _ => panic!("Field Reference must refer to byte stream"),
        }
    }
}

pub fn handle_tf_print_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, tf: &mut TfPrint) {
    let (batch, input_field_id) = sess.claim_batch(tf_id, &[]);
    let input_field = sess.entry_data.fields[input_field_id].borrow();
    let mut iter = input_field
        .field_data
        .get_iter(tf.batch_iter)
        .bounded(0, batch);
    let starting_pos = iter.get_next_field_pos();
    let mut field_pos_before_batch = starting_pos;

    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                for (v, rl) in InlineTextIter::from_typed_range(&range, text) {
                    print_inline_text(v, rl as usize);
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (v, rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    print_inline_bytes(v, rl as usize);
                }
            }
            FDTypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, ints) {
                    print_integer(*v, rl as usize);
                }
            }
            FDTypedSlice::Reference(refs) => {
                let mut field_pos = field_pos_before_batch;
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
                    print_values_behind_field_ref(r, &mut iter, rl);
                    field.field_data.store_iter(FIELD_REF_LOOKUP_ITER_ID, iter);
                }
            }
            FDTypedSlice::Null(_) => {
                print_null(range.field_count);
            }
            FDTypedSlice::Error(errs) => {
                for (v, rl) in TypedSliceIter::from_typed_range(&range, errs) {
                    print_error(sess, v, rl as usize)
                }
            }
            FDTypedSlice::Unset(_) => {
                print_unset(range.field_count);
            }
            FDTypedSlice::StreamValueId(_) => {
                panic!("hit stream value in batch mode");
            }
            FDTypedSlice::Html(_) | FDTypedSlice::Object(_) => {
                print_type_error(range.field_count);
            }
        }
        field_pos_before_batch += range.field_count;
    }
    //test whether we got the batch that was advertised
    debug_assert!(iter.get_next_field_pos() - starting_pos == batch);
    input_field.field_data.store_iter(tf.batch_iter, iter);
    drop(input_field);
    sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
}

pub fn handle_tf_print_stream_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf_print: &mut TfPrint,
) {
    let tf = &mut sess.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let input_field = sess.entry_data.fields[input_field_id].borrow();
    let sfd = &input_field.stream_field_data;
    tf_print.dropped_entries += sfd.entries_dropped;

    if let Some(id) = tf_print.current_stream_val {
        let res = print_stream_val_check_done(&sfd.get_value_mut(id));
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
            match field_val.value {
                FDTypedValue::Unset(_) => print_unset(1),
                FDTypedValue::Null(_) => print_null(1),
                FDTypedValue::Integer(v) => print_integer(v, 1),
                FDTypedValue::Reference(r) => {
                    let fd = &sess.entry_data.fields[r.field].borrow().field_data;
                    let mut iter = fd.get_iter(FIELD_REF_LOOKUP_ITER_ID);
                    iter.move_to_field_pos(iter.get_next_field_pos());
                    print_values_behind_field_ref(r, &mut iter, field_val.header.run_length);
                    fd.store_iter(FIELD_REF_LOOKUP_ITER_ID, iter);
                }
                FDTypedValue::TextInline(text) => print_inline_text(text, 1),
                FDTypedValue::BytesInline(bytes) => print_inline_bytes(bytes, 1),
                FDTypedValue::Error(error) => print_error(sess, error, 1),
                FDTypedValue::Html(_) | FDTypedValue::Object(_) => print_type_error(1),
                FDTypedValue::StreamValueId(id) => {
                    let sfd = &input_field.stream_field_data;
                    let sv = sfd.get_value(id);
                    match print_stream_val_check_done(&sv) {
                        Ok(false) => {
                            tf_print.current_stream_val = Some(id);
                            break;
                        }
                        Ok(true) => (),
                        Err(err) => {
                            let _err =
                                io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id, err);
                            let _ms_id = sess.tf_mgr.transforms[tf_id].match_set_id;
                            tf_print.current_stream_val = None;
                            //TODO: we need the field ref manager for this
                            //sess.push_entry_error(ms_id, err);
                        }
                    }
                }
            }
            tf_print.consumed_entries += 1;
        }
    }
    if tf_print.flush_on_every_print {
        let _ = std::io::stdout().flush();
    }
    sess.tf_mgr.push_successor_in_ready_queue(tf_id);
}
