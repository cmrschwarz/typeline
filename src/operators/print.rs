use std::{
    borrow::Cow,
    io::{BufWriter, IsTerminal, StdoutLock, Write},
};

use crate::{
    job_session::JobData,
    operators::utils::buffer_stream_values::{
        buffer_remaining_stream_values_in_auto_deref_iter,
        buffer_remaining_stream_values_in_sv_iter,
    },
    options::argument::CliArgIdx,
    record_data::{
        field_data::{field_value_flags, FieldValueKind},
        iter_hall::IterId,
        iters::{FieldIterator, UnfoldIterRunLength},
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
            RefAwareUnfoldIterRunLength,
        },
        stream_value::{StreamValue, StreamValueData, StreamValueId},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    utils::int_string_conversions::i64_to_str,
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
    utils::{ERROR_PREFIX_STR, NULL_STR, SUCCESS_STR},
};

#[derive(Clone)]
pub struct OpPrint {}
pub struct TfPrint {
    flush_on_every_print: bool,
    current_stream_val: Option<StreamValueId>,
    iter_id: IterId,
}

pub fn parse_op_print(
    value: Option<&[u8]>,
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
    _op_base: &OperatorBase,
    _op: &OpPrint,
    tf_state: &mut TransformState,
) -> TransformData<'static> {
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
    TransformData::Print(TfPrint {
        // TODO: should we make a config option for this?
        flush_on_every_print: std::io::stdout().is_terminal(),
        current_stream_val: None,
        iter_id: sess.field_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .iter_hall
            .claim_iter(),
    })
}

pub fn create_op_print() -> OperatorData {
    OperatorData::Print(OpPrint {})
}

pub fn typed_slice_zst_str(ts: &TypedSlice) -> &'static str {
    match ts {
        TypedSlice::Success(_) => SUCCESS_STR,
        TypedSlice::Null(_) => NULL_STR,
        _ => unreachable!(),
    }
}

// SAFETY: guaranteed to write valid utf-8
pub fn write_error(
    stream: &mut impl Write,
    e: &OperatorApplicationError,
) -> Result<(), std::io::Error> {
    stream.write_fmt(format_args!("{ERROR_PREFIX_STR}{e}"))
}

pub fn error_to_string(e: &OperatorApplicationError) -> String {
    format_args!("{ERROR_PREFIX_STR}{e}").to_string()
}

pub fn write_stream_val_check_done(
    stream: &mut impl Write,
    sv: &StreamValue,
    offsets: Option<core::ops::Range<usize>>,
    run_len: usize,
) -> Result<bool, (usize, std::io::Error)> {
    let rl_to_attempt = if sv.done || run_len == 0 { run_len } else { 1 };
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
                        if sv.done {
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
    sess: &mut JobData,
    tf_id: TransformId,
    print: &mut TfPrint,
    batch_size: usize,
    handled_field_count: &mut usize,
    stdout: &mut BufWriter<StdoutLock<'_>>,
) -> Result<(), std::io::Error> {
    debug_assert!(print.current_stream_val.is_none());
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;

    let input_field = sess
        .field_mgr
        .get_cow_field_ref(input_field_id, tf.has_unconsumed_input());
    let base_iter = sess
        .field_mgr
        .lookup_iter(input_field_id, &input_field, print.iter_id)
        .bounded(0, batch_size);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut field_pos = field_pos_start;
    let mut iter =
        AutoDerefIter::new(&sess.field_mgr, input_field_id, base_iter);

    'iter: while let Some(range) = iter.typed_range_fwd(
        &mut sess.match_set_mgr,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for v in RefAwareInlineTextIter::from_range(&range, text)
                    .unfold_rl()
                {
                    stdout.write_all(v.as_bytes())?;
                    stdout.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for v in RefAwareInlineBytesIter::from_range(&range, bytes)
                    .unfold_rl()
                {
                    stdout.write_all(v)?;
                    stdout.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for v in RefAwareBytesBufferIter::from_range(&range, bytes)
                    .unfold_rl()
                {
                    stdout.write_all(v)?;
                    stdout.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let v = i64_to_str(false, *v);
                    for _ in 0..rl {
                        stdout.write_all(v.as_bytes())?;
                        stdout.write_all(b"\n")?;
                        *handled_field_count += 1;
                    }
                }
            }
            TypedSlice::Error(errs) => {
                for v in
                    TypedSliceIter::from_range(&range.base, errs).unfold_rl()
                {
                    stdout
                        .write_fmt(format_args!("{ERROR_PREFIX_STR}{v}\n"))?;
                    *handled_field_count += 1;
                }
            }
            TypedSlice::Null(_) | TypedSlice::Success(_) => {
                let zst_str = typed_slice_zst_str(&range.base.data);
                for _ in 0..range.base.field_count {
                    stdout.write_fmt(format_args!("{zst_str}\n"))?;
                    *handled_field_count += 1;
                }
            }

            TypedSlice::StreamValueId(svs) => {
                let mut pos = field_pos;
                let mut sv_iter =
                    RefAwareStreamValueIter::from_range(&range, svs);
                while let Some((sv_id, offsets, rl)) = sv_iter.next() {
                    pos += rl as usize;
                    *handled_field_count += rl as usize;
                    let sv = &mut sess.sv_mgr.stream_values[sv_id];
                    let done = write_stream_val_check_done(
                        stdout,
                        sv,
                        offsets,
                        rl as usize,
                    )
                    .map_err(|(i, e)| {
                        *handled_field_count += i;
                        e
                    })?;
                    if !done {
                        print.current_stream_val = Some(sv_id);
                        iter.move_to_field_pos(pos);
                        if rl > 1 {
                            sv.promote_to_buffer();
                        }
                        sv.subscribe(tf_id, rl as usize, false);
                        sess.field_mgr.fields[input_field_id]
                            .borrow()
                            .request_clear_delay();
                        sess.tf_mgr.unclaim_batch_size(
                            tf_id,
                            batch_size - (pos - field_pos_start),
                        );
                        buffer_remaining_stream_values_in_sv_iter(
                            &mut sess.sv_mgr,
                            sv_iter,
                        );
                        buffer_remaining_stream_values_in_auto_deref_iter(
                            &mut sess.match_set_mgr,
                            &mut sess.sv_mgr,
                            iter.clone(),
                            usize::MAX,
                        );
                        break 'iter;
                    }
                }
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                todo!();
            }
            TypedSlice::Reference(_) => unreachable!(),
        }
        field_pos += range.base.field_count;
    }
    while *handled_field_count < batch_size {
        stdout.write_fmt(format_args!("{NULL_STR}\n"))?;
        *handled_field_count += 1;
    }
    sess.field_mgr
        .store_iter(input_field_id, print.iter_id, iter);
    Ok(())
}

pub fn handle_tf_print(
    sess: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfPrint,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let mut handled_field_count = 0;
    let mut stdout = BufWriter::new(std::io::stdout().lock());
    let res = handle_tf_print_raw(
        sess,
        tf_id,
        tf,
        batch_size,
        &mut handled_field_count,
        &mut stdout,
    );
    if tf.flush_on_every_print {
        stdout.flush().ok();
    }
    drop(stdout);
    let op_id = sess.tf_mgr.transforms[tf_id].op_id.unwrap();
    let of_id = sess.tf_mgr.prepare_output_field(
        &mut sess.field_mgr,
        &mut sess.match_set_mgr,
        tf_id,
    );
    let mut output_field = sess.field_mgr.fields[of_id].borrow_mut();
    let mut outputs_produced = handled_field_count;
    match res {
        Ok(()) => {
            if handled_field_count > 0 {
                output_field
                    .iter_hall
                    .push_success(handled_field_count, true);
            }
        }
        Err(err) => {
            let nsucc = handled_field_count;
            let nfail = batch_size - nsucc;
            if nsucc > 0 {
                output_field.iter_hall.push_success(nsucc, true);
            }
            let e = OperatorApplicationError {
                op_id,
                message: Cow::Owned(err.to_string()),
            };
            output_field.iter_hall.push_error(e, nfail, false, true);
            outputs_produced += nfail;
        }
    }
    drop(output_field);
    if input_done {
        sess.unlink_transform(tf_id, outputs_produced);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, outputs_produced);
    }
}

pub fn handle_tf_print_stream_value_update(
    sess: &mut JobData,
    tf_id: TransformId,
    print: &mut TfPrint,
    sv_id: StreamValueId,
    custom: usize,
) {
    // we don't use a buffered writer here because stream chunks
    // are large and we want to avoid the copy
    let mut stdout = std::io::stdout().lock();
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    let run_len = custom;
    let mut success_count = run_len;
    let mut error_count = 0;
    let mut err_message = Cow::Borrowed("");
    match write_stream_val_check_done(&mut stdout, sv, None, run_len) {
        Ok(false) => {
            return;
        }
        Ok(true) => (),
        Err((idx, e)) => {
            error_count = run_len - idx;
            success_count = run_len - error_count;
            err_message = Cow::Owned(e.to_string());
        }
    }
    let tf = &sess.tf_mgr.transforms[tf_id];
    let mut output_field = sess.field_mgr.fields[tf.output_field].borrow_mut();
    if success_count > 0 {
        output_field.iter_hall.push_success(success_count, true);
    }
    if error_count > 0 {
        output_field.iter_hall.push_error(
            OperatorApplicationError {
                op_id: sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                message: err_message,
            },
            run_len,
            true,
            false,
        );
        sess.sv_mgr
            .drop_field_value_subscription(sv_id, Some(tf_id));
    }
    if print.flush_on_every_print {
        stdout.flush().ok();
    }
    print.current_stream_val = None;
    let input_field = sess.field_mgr.fields[tf.input_field].borrow();
    input_field.drop_clear_delay_request();
    sess.tf_mgr.update_ready_state(tf_id);
}
