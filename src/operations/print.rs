use std::io::Write;

use bstring::bstr;

use crate::{
    field_data::{field_value_flags, FieldReference},
    field_data_iterator::{FDIterator, FDTypedSlice, FDTypedValue},
    options::argument::CliArgIdx,
    stream_field_data::{StreamFieldValue, StreamFieldValueData, StreamValueId},
    worker_thread_session::{FieldId, JobData, MatchSetId, WorkerThreadSession},
};

use super::{
    errors::{io_error_to_op_error, OperatorApplicationError, OperatorCreationError},
    operator::OperatorData,
    transform::{TransformData, TransformId},
};

pub struct TfPrint {
    consumed_entries: usize,
    dropped_entries: usize,
    current_stream_val: Option<StreamValueId>,
}

pub fn parse_print_op(
    value: Option<&bstr>,
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
    _sess: &mut WorkerThreadSession,
    _ms_id: MatchSetId,
    input_field: FieldId,
) -> (TransformData<'static>, FieldId) {
    let tf = TfPrint {
        consumed_entries: 0,
        dropped_entries: 0,
        current_stream_val: None,
    };
    (TransformData::Print(tf), input_field)
}

pub fn print_inline_text(text: &str, run_len: usize) {
    for _ in 0..run_len {
        println!("{text}");
    }
}
pub fn print_integer(v: i64, run_len: usize) {
    for _ in 0..run_len {
        println!("{v}");
    }
}
pub fn print_ref(_sess: &JobData, r: &FieldReference, run_len: usize) {
    for _ in 0..run_len {
        //TODO
        println!("reference: -> {} [{}, {})", r.field, r.begin, r.end);
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
    }
    Ok(sv.done)
}

pub fn handle_tf_print_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, _tf: &mut TfPrint) {
    let (batch, input_field) = sess.claim_batch(tf_id);
    let mut iter = sess.fields[input_field].field_data.iter().bounded(batch, 0);
    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                let mut data_end = text.len();
                for i in 0..range.field_count {
                    let data_start = data_end - range.headers[i].size as usize;
                    print_inline_text(
                        &text[data_start..data_end],
                        range.headers[i].run_length as usize,
                    );
                    data_end = data_start;
                }
            }
            FDTypedSlice::Integer(ints) => {
                for i in 0..range.field_count {
                    print_integer(ints[i], range.headers[i].run_length as usize);
                }
            }
            FDTypedSlice::Reference(refs) => {
                for i in 0..range.field_count {
                    print_ref(sess, &refs[i], range.headers[i].run_length as usize);
                }
            }
            FDTypedSlice::Null(_) => {
                print_null(range.field_count);
            }
            FDTypedSlice::Error(errs) => {
                for (i, e) in errs.iter().enumerate() {
                    print_error(sess, e, range.headers[i].run_length as usize)
                }
            }
            FDTypedSlice::Unset(_) => {
                print_unset(range.field_count);
            }
            FDTypedSlice::StreamValueId(_) => {
                panic!("hit stream value in batch mode");
            }
            FDTypedSlice::Html(_) | FDTypedSlice::BytesInline(_) | FDTypedSlice::Object(_) => {
                print_type_error(range.field_count);
            }
        }
    }
    sess.inform_successor_batch_available(tf_id, batch);
}

pub fn handle_tf_print_stream_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf_print: &mut TfPrint,
) {
    let tf = &mut sess.transforms[tf_id];
    let input_field_id = tf.input_field;
    let input_field = &mut sess.fields[input_field_id];
    let sfd = &mut input_field.stream_field_data;
    tf_print.dropped_entries += sfd.entries_dropped;

    if let Some(id) = tf_print.current_stream_val {
        let res = print_stream_val_check_done(&sfd.get_value_mut(&sfd.values, id));
        match res {
            Ok(false) => (),
            Ok(true) => tf_print.current_stream_val = None,
            Err(err) => {
                let _err = io_error_to_op_error(&sess.transforms, tf_id, err);
                //TODO: we need the field ref manager for this
                //sess.push_entry_error(sess.transforms[tf_id].match_set_id, err);
                tf_print.current_stream_val = None;
            }
        }
    }
    let input_field = &sess.fields[input_field_id];
    if tf_print.current_stream_val.is_none() {
        let mut iter = input_field.field_data.iter();
        iter.next_n_fields(tf_print.consumed_entries - tf_print.dropped_entries);
        while iter.is_next_valid() {
            let field_val = iter.get_next_typed_field();
            match field_val.value {
                FDTypedValue::Unset(_) => {
                    print_unset(1);
                }
                FDTypedValue::Null(_) => {
                    print_null(1);
                }
                FDTypedValue::Integer(v) => {
                    print_integer(v, 1);
                }
                FDTypedValue::Reference(r) => {
                    print_ref(sess, r, 1);
                }
                FDTypedValue::Error(_)
                | FDTypedValue::Html(_)
                | FDTypedValue::BytesInline(_)
                | FDTypedValue::TextInline(_)
                | FDTypedValue::Object(_) => {
                    print_type_error(1);
                }
                FDTypedValue::StreamValueId(id) => {
                    let sfd = &input_field.stream_field_data;
                    let sv = sfd.get_value(&sfd.values, id);
                    match print_stream_val_check_done(&sv) {
                        Ok(false) => {
                            tf_print.current_stream_val = Some(id);
                            break;
                        }
                        Ok(true) => (),
                        Err(err) => {
                            let _err = io_error_to_op_error(&sess.transforms, tf_id, err);
                            let _ms_id = sess.transforms[tf_id].match_set_id;
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
}
