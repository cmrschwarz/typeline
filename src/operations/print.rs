use std::{
    borrow::Cow,
    io::{BufWriter, Write},
};

use bstr::{BStr, ByteSlice};
use is_terminal::IsTerminal;

use crate::{
    field_data::{
        field_value_flags, iter_hall::IterId, iters::FieldIterator, push_interface::PushInterface,
        typed::TypedSlice, typed_iters::TypedSliceIter, FieldValueKind,
    },
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
        RefAwareStreamValueIter,
    },
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::OperatorData,
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpPrint {}
pub struct TfPrint {
    flush_on_every_print: bool,
    current_stream_val: Option<StreamValueId>,
    iter_id: IterId,
    output_field: FieldId,
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
    Ok(OperatorData::Print(OpPrint {}))
}

pub fn setup_tf_print(
    sess: &mut JobData,
    _op: &OpPrint,
    tf_state: &mut TransformState,
) -> (TransformData<'static>, FieldId) {
    let output_field = sess.record_mgr.add_field(
        tf_state.match_set_id,
        sess.record_mgr.get_min_apf_idx(tf_state.input_field),
        None,
    );
    let tf = TfPrint {
        // TODO: should we make a config option for this?
        flush_on_every_print: std::io::stdout().is_terminal(),
        current_stream_val: None,
        iter_id: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
        output_field,
    };

    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
    (TransformData::Print(tf), output_field)
}

pub fn create_op_print() -> OperatorData {
    OperatorData::Print(OpPrint {})
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
    let nl = if NEWLINE { "\n" } else { "" };
    for i in 0..run_len {
        stream
            .write_fmt(format_args!("{v}{nl}"))
            .map_err(|e| (i, e))?;
    }
    Ok(())
}
pub const NULL_STR: &'static str = "null";
pub const SUCCESS_STR: &'static str = "<Success>";
pub const UNSET_STR: &'static str = "<Unset>";
pub const ERROR_PREFIX_STR: &'static str = "Error: ";

pub fn write_null<const NEWLINE: bool>(
    stream: &mut impl Write,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, NULL_STR.as_bytes(), run_len)
}
pub fn write_unset<const NEWLINE: bool>(
    stream: &mut impl Write,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, UNSET_STR.as_bytes(), run_len)
}
pub fn write_success<const NEWLINE: bool>(
    stream: &mut impl Write,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    write_raw_bytes::<NEWLINE>(stream, SUCCESS_STR.as_bytes(), run_len)
}

// SAFETY: guaranteed to write valid utf-8
pub fn write_error<const NEWLINE: bool>(
    stream: &mut impl Write,
    e: &OperatorApplicationError,
    run_len: usize,
) -> Result<(), (usize, std::io::Error)> {
    for i in 0..run_len {
        stream
            .write_fmt(format_args!("Error: {e}"))
            .and_then(|_| if NEWLINE { stream.write(b"\n") } else { Ok(0) })
            .map_err(|e| (i, e))?;
    }
    Ok(())
}

pub fn error_to_string(e: &OperatorApplicationError) -> String {
    format_args!("Error: {e}").to_string()
}

pub fn write_stream_val_check_done<const NEWLINE: bool>(
    stream: &mut impl Write,
    sv: &StreamValue,
    offsets: Option<core::ops::Range<usize>>,
    run_len: usize,
) -> Result<bool, (usize, std::io::Error)> {
    let rl_to_attempt = if sv.done || run_len == 0 {
        run_len as usize
    } else {
        1
    };
    debug_assert!(sv.done || offsets.is_none());
    match &sv.data {
        StreamValueData::Bytes(c) => {
            if !sv.bytes_are_chunk && !sv.done {
                return Ok(false);
            }
            let mut data = c.as_slice();
            if let Some(offsets) = offsets {
                debug_assert!(sv.is_buffered());
                data = &data[offsets];
            }
            for i in 0..rl_to_attempt {
                stream
                    .write(data)
                    .and_then(|_| {
                        if NEWLINE && sv.done {
                            stream.write(b"\n")
                        } else {
                            Ok(0)
                        }
                    })
                    .map_err(|e| (i, e))?;
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            debug_assert!(offsets.is_none());
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
    batch: usize,
    handled_field_count: &mut usize,
) -> Result<(), (usize, std::io::Error)> {
    let mut stdout = BufWriter::new(std::io::stdout().lock());
    debug_assert!(!tf.current_stream_val.is_some());
    let input_field_id = sess.tf_mgr.transforms[tf_id].input_field;
    let input_field = sess.record_mgr.fields[input_field_id].borrow();
    let base_iter = input_field
        .field_data
        .get_iter(tf.iter_id)
        .bounded(0, batch);
    let starting_pos = base_iter.get_next_field_pos();
    let mut field_pos = starting_pos;
    let mut iter = AutoDerefIter::new(
        &sess.record_mgr.fields,
        &mut sess.record_mgr.match_sets,
        input_field_id,
        base_iter,
        None,
    );

    'iter: while let Some(range) = iter.typed_range_fwd(
        &mut sess.record_mgr.match_sets,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    write_text::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    write_raw_bytes::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    write_raw_bytes::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    write_integer::<true>(&mut stdout, *v, rl as usize)?;
                }
            }
            TypedSlice::Null(_) => {
                write_null::<true>(&mut stdout, range.base.field_count)?;
            }
            TypedSlice::Error(errs) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    write_error::<true>(&mut stdout, v, rl as usize)?;
                }
            }
            TypedSlice::Unset(_) => {
                write_unset::<true>(&mut stdout, range.base.field_count)?;
            }
            TypedSlice::Success(_) => {
                write_success::<true>(&mut stdout, range.base.field_count)?;
            }
            TypedSlice::StreamValueId(svs) => {
                for (sv_id, offsets, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                    let sv = &mut sess.sv_mgr.stream_values[sv_id];
                    if !write_stream_val_check_done::<true>(&mut stdout, sv, offsets, rl as usize)?
                    {
                        tf.current_stream_val = Some(sv_id);
                        iter.move_to_field_pos(field_pos);
                        if rl > 1 {
                            sv.promote_to_buffer();
                        }
                        sv.subscribe(tf_id, rl as usize, false);
                        break 'iter;
                    }
                    field_pos += rl as usize;
                }
                continue; // skip the field pos increase at the bottom of this loop because we already did that
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                todo!();
            }
            TypedSlice::Reference(_) => unreachable!(),
        }
        field_pos += range.base.field_count;
        *handled_field_count += range.base.field_count;
    }
    input_field
        .field_data
        .store_iter(tf.iter_id, iter.into_base_iter());
    drop(input_field);
    if tf.flush_on_every_print {
        stdout.flush().ok();
    }
    let consumed_fields = field_pos - starting_pos;
    sess.tf_mgr.transforms[tf_id].is_stream_subscriber = tf.current_stream_val.is_some();
    sess.tf_mgr.update_ready_state(tf_id);
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, consumed_fields);

    Ok(())
}

pub fn handle_tf_print(sess: &mut JobData<'_>, tf_id: TransformId, tf: &mut TfPrint) {
    let batch = sess.claim_batch(tf_id);
    let mut handled_field_count = 0;
    let res = handle_tf_print_raw(sess, tf_id, tf, batch, &mut handled_field_count);
    let mut output_field = sess.record_mgr.fields[tf.output_field].borrow_mut();
    match res {
        Ok(()) => output_field
            .field_data
            .push_success(handled_field_count, true),
        Err((err_idx, err)) => {
            let nsucc = handled_field_count + err_idx;
            let nfail = batch - nsucc;
            if nsucc > 0 {
                output_field.field_data.push_success(nsucc, true);
            }
            let e = OperatorApplicationError {
                op_id: sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                message: Cow::Owned(err.to_string()),
            };
            output_field.field_data.push_error(e, nfail, false, true);
        }
    }
}

pub fn handle_tf_print_stream_value_update(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfPrint,
    sv_id: StreamValueId,
    custom: usize,
) {
    let mut stdout = std::io::stdout().lock();
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    let run_len = custom;
    match write_stream_val_check_done::<true>(&mut stdout, sv, None, run_len) {
        Ok(true) => sess.tf_mgr.update_ready_state(tf_id),
        Ok(false) => (),
        Err((idx, e)) => {
            debug_assert!(idx == 0);
            sess.record_mgr.fields[tf.output_field]
                .borrow_mut()
                .field_data
                .push_error(
                    OperatorApplicationError {
                        op_id: sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                        message: Cow::Owned(e.to_string()),
                    },
                    run_len,
                    true,
                    false,
                );
            sess.sv_mgr
                .drop_field_value_subscription(sv_id, Some(tf_id));
        }
    }
}
