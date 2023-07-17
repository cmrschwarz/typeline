use std::{
    borrow::Cow,
    io::{BufWriter, Write},
};

use crate::{
    field_data::{
        field_value_flags,
        iter_hall::IterId,
        iters::{FieldIterator, UnfoldIterRunLength},
        push_interface::PushInterface,
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
        FieldValueKind,
    },
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
        RefAwareStreamValueIter, RefAwareUnfoldIterRunLength,
    },
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::i64_to_str,
    worker_thread_session::{JobSession, RecordManager},
};
use bstr::BStr;
use is_terminal::IsTerminal;

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpPrint {}
pub struct TfPrint {
    flush_on_every_print: bool,
    current_stream_val: Option<StreamValueId>,
    iter_id: IterId,
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
    sess: &mut JobSession,
    _op_base: &OperatorBase,
    _op: &OpPrint,
    tf_state: &mut TransformState,
) -> TransformData<'static> {
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
    TransformData::Print(TfPrint {
        // TODO: should we make a config option for this?
        flush_on_every_print: std::io::stdout().is_terminal(),
        current_stream_val: None,
        iter_id: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
    })
}

pub fn create_op_print() -> OperatorData {
    OperatorData::Print(OpPrint {})
}

pub const NULL_STR: &'static str = "null";
pub const SUCCESS_STR: &'static str = "<Success>";
pub const UNSET_STR: &'static str = "<Unset>";
pub const ERROR_PREFIX_STR: &'static str = "Error: ";

// SAFETY: guaranteed to write valid utf-8
pub fn write_error(
    stream: &mut impl Write,
    e: &OperatorApplicationError,
) -> Result<(), std::io::Error> {
    stream.write_fmt(format_args!("{ERROR_PREFIX_STR}{e}"))
}

pub fn error_to_string(e: &OperatorApplicationError) -> String {
    format_args!("Error: {e}").to_string()
}

pub fn write_stream_val_check_done(
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
                    .and_then(|_| if sv.done { stream.write(b"\n") } else { Ok(0) })
                    .map_err(|e| (i, e))?;
            }
        }
        StreamValueData::Error(e) => {
            debug_assert!(sv.done);
            debug_assert!(offsets.is_none());
            for i in 0..rl_to_attempt {
                stream
                    .write_fmt(format_args!("{ERROR_PREFIX_STR}{e}\n"))
                    .map_err(|e| (i, e))?;
            }
        }
        StreamValueData::Dropped => panic!("dropped stream value observed"),
    }
    Ok(sv.done)
}

pub fn handle_tf_print_raw(
    sess: &mut JobSession<'_>,
    tf_id: TransformId,
    tf: &mut TfPrint,
    batch: usize,
    input_done: bool,
    handled_field_count: &mut usize,
) -> Result<(), std::io::Error> {
    let mut stdout = BufWriter::new(std::io::stdout().lock());
    debug_assert!(!tf.current_stream_val.is_some());
    let input_field_id = sess.tf_mgr.transforms[tf_id].input_field;

    let input_field = RecordManager::borrow_field_cow(&sess.record_mgr.fields, input_field_id);
    let base_iter = RecordManager::get_iter_cow_aware(
        &sess.record_mgr.fields,
        input_field_id,
        &input_field,
        tf.iter_id,
    )
    .bounded(0, batch);
    let starting_pos = base_iter.get_next_field_pos();
    let mut field_pos = starting_pos;
    let mut iter = AutoDerefIter::new(&sess.record_mgr.fields, input_field_id, base_iter);

    'iter: while let Some(range) = iter.typed_range_fwd(
        &mut sess.record_mgr.match_sets,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for v in RefAwareInlineTextIter::from_range(&range, text).unfold_rl() {
                    stdout.write(v.as_bytes())?;
                    stdout.write(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for v in RefAwareInlineBytesIter::from_range(&range, bytes).unfold_rl() {
                    stdout.write(v)?;
                    stdout.write(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for v in RefAwareBytesBufferIter::from_range(&range, bytes).unfold_rl() {
                    stdout.write(v)?;
                    stdout.write(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let v = i64_to_str(false, *v);
                    for _ in 0..rl {
                        stdout.write(v.as_bytes())?;
                        stdout.write(b"\n")?;
                        *handled_field_count += 1;
                    }
                }
            }
            TypedSlice::Null(_) => {
                for _ in 0..range.base.field_count {
                    stdout.write_fmt(format_args!("{NULL_STR}\n"))?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::Error(errs) => {
                for v in TypedSliceIter::from_range(&range.base, errs).unfold_rl() {
                    stdout.write_fmt(format_args!("{v}\n"))?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::Unset(_) => {
                for _ in 0..range.base.field_count {
                    stdout.write_fmt(format_args!("{UNSET_STR}\n"))?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::Success(_) => {
                for _ in 0..range.base.field_count {
                    stdout.write_fmt(format_args!("{SUCCESS_STR}\n"))?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::StreamValueId(svs) => {
                let mut pos = field_pos;
                for (sv_id, offsets, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                    pos += rl as usize;
                    let sv = &mut sess.sv_mgr.stream_values[sv_id];
                    if !write_stream_val_check_done(&mut stdout, sv, offsets, rl as usize).map_err(
                        |(i, e)| {
                            *handled_field_count += i;
                            e
                        },
                    )? {
                        tf.current_stream_val = Some(sv_id);
                        iter.move_to_field_pos(pos);
                        if rl > 1 {
                            sv.promote_to_buffer();
                        }
                        sv.subscribe(tf_id, rl as usize, false);
                        break 'iter;
                    }
                    *handled_field_count += rl as usize;
                }
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                todo!();
            }
            TypedSlice::Reference(_) => unreachable!(),
        }
        field_pos += range.base.field_count;
    }
    RecordManager::store_iter_cow_aware(
        &sess.record_mgr.fields,
        input_field_id,
        &input_field,
        tf.iter_id,
        iter.into_base_iter(),
    );
    drop(input_field);
    if tf.flush_on_every_print {
        stdout.flush().ok();
    }
    let consumed_fields = field_pos - starting_pos;
    sess.tf_mgr.transforms[tf_id].is_stream_subscriber = tf.current_stream_val.is_some();
    if input_done {
        sess.unlink_transform(tf_id, consumed_fields);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, consumed_fields);
    }

    Ok(())
}

pub fn handle_tf_print(sess: &mut JobSession<'_>, tf_id: TransformId, tf: &mut TfPrint) {
    let (batch, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let mut handled_field_count = 0;
    let res = handle_tf_print_raw(sess, tf_id, tf, batch, input_done, &mut handled_field_count);
    let op_id = sess.tf_mgr.transforms[tf_id].op_id.unwrap();
    let mut output_field = sess.prepare_output_field(tf_id);
    match res {
        Ok(()) => output_field
            .field_data
            .push_success(handled_field_count, true),
        Err(err) => {
            let nsucc = handled_field_count;
            let nfail = batch - nsucc;
            if nsucc > 0 {
                output_field.field_data.push_success(nsucc, true);
            }
            let e = OperatorApplicationError {
                op_id,
                message: Cow::Owned(err.to_string()),
            };
            output_field.field_data.push_error(e, nfail, false, true);
        }
    }
}

pub fn handle_tf_print_stream_value_update(
    sess: &mut JobSession<'_>,
    tf_id: TransformId,
    _print: &mut TfPrint,
    sv_id: StreamValueId,
    custom: usize,
) {
    let mut stdout = std::io::stdout().lock();
    let tf = &sess.tf_mgr.transforms[tf_id];
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    let run_len = custom;
    match write_stream_val_check_done(&mut stdout, sv, None, run_len) {
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
