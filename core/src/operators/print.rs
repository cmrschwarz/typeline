use std::io::{IsTerminal, Write};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
    utils::writable::{AnyWriter, WritableTarget},
};
use crate::{
    job::JobData,
    operators::utils::buffer_stream_values::{
        buffer_remaining_stream_values_in_auto_deref_iter,
        buffer_remaining_stream_values_in_sv_iter,
    },
    options::argument::CliArgIdx,
    record_data::{
        field_data::field_value_flags,
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueSliceIter,
        formattable::{
            format_error_raw, format_rational_as_decimals_raw, Formattable,
            FormattingContext, RealizedFormatKey, RATIONAL_DIGITS,
        },
        iter_hall::IterId,
        iters::{FieldIterator, UnfoldIterRunLength},
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueSliceIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
            RefAwareUnfoldIterRunLength,
        },
        stream_value::{
            StreamValue, StreamValueDataOffset, StreamValueId,
            StreamValueUpdate,
        },
    },
    utils::{
        int_string_conversions::i64_to_str, lazy_lock_guard::LazyRwLockGuard,
        text_write::TextWriteIoAdapter,
    },
    NULL_STR, UNDEFINED_STR,
};

// ENHANCE: bikeshed a better format for this
const ERROR_PREFIX_STR: &str = "ERROR: ";

pub struct OpPrint {
    target: WritableTarget,
}

pub struct TfPrint {
    flush_on_every_print: bool,
    current_stream_val: Option<StreamValueId>,
    iter_id: IterId,
    streams_kept_alive: usize,
    target: AnyWriter,
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
    Ok(OperatorData::Print(OpPrint {
        target: WritableTarget::Stdout,
    }))
}

pub fn build_tf_print(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &OpPrint,
    tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Print(TfPrint {
        // ENHANCE: should we config options for this stuff?
        flush_on_every_print: matches!(op.target, WritableTarget::Stdout)
            && std::io::stdout().is_terminal(),
        current_stream_val: None,
        streams_kept_alive: 0,
        iter_id: jd.add_iter_for_tf_state(tf_state),
        target: op.target.take_writer(true),
    })
}

pub fn create_op_print() -> OperatorData {
    OperatorData::Print(OpPrint {
        target: WritableTarget::Stdout,
    })
}
pub fn create_op_print_with_target(target: WritableTarget) -> OperatorData {
    OperatorData::Print(OpPrint { target })
}

pub fn typed_slice_zst_str(ts: &FieldValueSlice) -> &'static str {
    match ts {
        FieldValueSlice::Undefined(_) => UNDEFINED_STR,
        FieldValueSlice::Null(_) => NULL_STR,
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
// in case of an error, returns the number of successfully completed
// elements in addition to the io error
pub fn write_stream_val_check_done(
    sv: &StreamValue,
    offset: StreamValueDataOffset,
    stream: &mut impl Write,
    run_len: usize,
) -> Result<bool, (usize, std::io::Error)> {
    let rl_to_attempt = if sv.done || run_len == 0 { run_len } else { 1 };
    if let Some(e) = &sv.error {
        debug_assert!(sv.done);
        let tw = &mut TextWriteIoAdapter(stream);
        for i in 0..rl_to_attempt {
            format_error_raw(tw, e).map_err(|e| (i, e))?;
        }
        return Ok(true);
    }
    for data in sv.data_iter(offset) {
        // TODO: handle other data types?
        stream.write(data.as_bytes()).map_err(|e| (0, e))?;
    }
    if sv.done {
        stream.write(b"\n").map_err(|e| (0, e))?;
    }
    for i in 1..rl_to_attempt {
        for data in sv.data_iter(offset) {
            stream.write(data.as_bytes()).map_err(|e| (i, e))?;
        }
    }
    Ok(sv.done)
}

pub fn handle_tf_print_raw(
    jd: &mut JobData,
    tf_id: TransformId,
    batch_size: usize,
    print: &mut TfPrint,
    // we need these even in case of an error, so it's an output parameter :/
    handled_field_count: &mut usize,
    last_group_separator_end: &mut usize,
) -> std::io::Result<()> {
    let mut stream = print.target.aquire(true);
    *handled_field_count = 0;
    *last_group_separator_end = 0;
    let tf = &jd.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;

    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
    let base_iter = jd
        .field_mgr
        .lookup_iter(input_field_id, &input_field, print.iter_id)
        .bounded(0, batch_size);
    let mut iter =
        AutoDerefIter::new(&jd.field_mgr, input_field_id, base_iter);
    let mut string_store = LazyRwLockGuard::new(&jd.session_data.string_store);
    let print_rationals_raw =
        jd.get_transform_chain(tf_id).settings.print_rationals_raw;
    'iter: while let Some(range) = iter.typed_range_fwd(
        &jd.match_set_mgr,
        usize::MAX,
        field_value_flags::DEFAULT,
    ) {
        match range.base.data {
            FieldValueSlice::TextInline(text) => {
                for v in RefAwareInlineTextIter::from_range(&range, text)
                    .unfold_rl()
                {
                    stream.write_all(v.as_bytes())?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::BytesInline(bytes) => {
                for v in RefAwareInlineBytesIter::from_range(&range, bytes)
                    .unfold_rl()
                {
                    stream.write_all(v)?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::TextBuffer(bytes) => {
                for v in RefAwareTextBufferIter::from_range(&range, bytes)
                    .unfold_rl()
                {
                    stream.write_all(v.as_bytes())?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::BytesBuffer(bytes) => {
                for v in RefAwareBytesBufferIter::from_range(&range, bytes)
                    .unfold_rl()
                {
                    stream.write_all(v)?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::Int(ints) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, ints) {
                    let v = i64_to_str(false, *v);
                    for _ in 0..rl {
                        stream.write_all(v.as_bytes())?;
                        stream.write_all(b"\n")?;
                        *handled_field_count += 1;
                    }
                }
            }
            FieldValueSlice::Error(errs) => {
                for v in RefAwareFieldValueSliceIter::from_range(&range, errs)
                    .unfold_rl()
                {
                    stream
                        .write_fmt(format_args!("{ERROR_PREFIX_STR}{v}\n"))?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::Custom(custom_types) => {
                for v in RefAwareFieldValueSliceIter::from_range(
                    &range,
                    custom_types,
                )
                .unfold_rl()
                {
                    v.format_raw(
                        &mut TextWriteIoAdapter(&mut stream),
                        &RealizedFormatKey::default(),
                    )?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::Null(_) | FieldValueSlice::Undefined(_) => {
                let zst_str = typed_slice_zst_str(&range.base.data);
                for _ in 0..range.base.field_count {
                    stream.write_fmt(format_args!("{zst_str}\n"))?;
                    *handled_field_count += 1;
                }
            }

            FieldValueSlice::StreamValueId(svs) => {
                let mut pos = *handled_field_count;
                let mut sv_iter = FieldValueSliceIter::from_range(&range, svs);
                while let Some((&sv_id, rl)) = sv_iter.next() {
                    pos += rl as usize;
                    *handled_field_count += rl as usize;
                    let sv = &mut jd.sv_mgr.stream_values[sv_id];
                    let done = write_stream_val_check_done(
                        sv,
                        StreamValueDataOffset::default(),
                        &mut stream,
                        rl as usize,
                    )
                    .map_err(|(i, e)| {
                        *handled_field_count += i;
                        e
                    })?;
                    if print.streams_kept_alive > 0 {
                        let rc_diff = (rl as usize)
                            .saturating_sub(print.streams_kept_alive);
                        sv.ref_count -= rc_diff;
                        print.streams_kept_alive -= rc_diff;
                    }
                    if done {
                        jd.sv_mgr.check_stream_value_ref_count(sv_id);
                    } else {
                        print.current_stream_val = Some(sv_id);
                        if rl > 1 {
                            sv.make_buffered();
                        }
                        sv.subscribe(sv_id, tf_id, rl as usize, false, true);
                        jd.tf_mgr.unclaim_batch_size(
                            tf_id,
                            batch_size - *handled_field_count,
                        );
                        print.streams_kept_alive +=
                            buffer_remaining_stream_values_in_sv_iter(
                                &mut jd.sv_mgr,
                                sv_iter,
                                false,
                            );
                        print.streams_kept_alive +=
                            buffer_remaining_stream_values_in_auto_deref_iter(
                                &jd.match_set_mgr,
                                &mut jd.sv_mgr,
                                iter.clone(),
                                usize::MAX,
                                false,
                            );
                        iter.move_to_field_pos(pos);
                        break 'iter;
                    }
                }
            }
            FieldValueSlice::BigInt(big_ints) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, big_ints)
                {
                    for _ in 0..rl {
                        stream.write_fmt(format_args!("{v}\n"))?;
                        *handled_field_count += 1;
                    }
                }
            }
            FieldValueSlice::Float(floats) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, floats)
                {
                    for _ in 0..rl {
                        stream.write_fmt(format_args!("{v}\n"))?;
                        *handled_field_count += 1;
                    }
                }
            }
            FieldValueSlice::Rational(rationals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, rationals)
                {
                    for _ in 0..rl {
                        if print_rationals_raw {
                            stream.write_fmt(format_args!("{v}\n"))?;
                        } else {
                            format_rational_as_decimals_raw(
                                &mut TextWriteIoAdapter(&mut stream),
                                v,
                                RATIONAL_DIGITS,
                            )?;
                            stream.write_all(b"\n")?;
                        }
                        *handled_field_count += 1;
                    }
                }
            }
            FieldValueSlice::Array(arrays) => {
                let mut fc = FormattingContext {
                    ss: &mut string_store,
                    fm: &jd.field_mgr,
                    msm: &jd.match_set_mgr,
                    print_rationals_raw,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::default(),
                };
                for a in
                    RefAwareFieldValueSliceIter::from_range(&range, arrays)
                        .unfold_rl()
                {
                    a.format(&mut fc, &mut TextWriteIoAdapter(&mut stream))?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::Object(objects) => {
                let mut fc = FormattingContext {
                    ss: &mut string_store,
                    fm: &jd.field_mgr,
                    msm: &jd.match_set_mgr,
                    print_rationals_raw,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::default(),
                };
                for o in
                    RefAwareFieldValueSliceIter::from_range(&range, objects)
                        .unfold_rl()
                {
                    o.format(&mut fc, &mut TextWriteIoAdapter(&mut stream))?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        }
    }
    // TODO: remove this once `sequence` became a reasonable member of society
    while *handled_field_count < batch_size
        && print.current_stream_val.is_none()
    {
        stream.write_fmt(format_args!("{UNDEFINED_STR}\n"))?;
        *handled_field_count += 1;
    }
    jd.field_mgr.store_iter(input_field_id, print.iter_id, iter);
    Ok(())
}

pub fn handle_tf_print(
    jd: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfPrint,
) {
    if tf.current_stream_val.is_some() {
        return;
    }
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);

    let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();
    let of_id = jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );

    let mut handled_field_count = 0;
    let mut last_group_separator = 0;
    let res = handle_tf_print_raw(
        jd,
        tf_id,
        batch_size,
        tf,
        &mut handled_field_count,
        &mut last_group_separator,
    );
    if tf.flush_on_every_print {
        tf.target.aquire(false).flush().ok();
    }
    let mut output_field = jd.field_mgr.fields[of_id].borrow_mut();
    let mut outputs_produced = handled_field_count;
    match res {
        Ok(()) => {
            if handled_field_count > 0 {
                output_field.iter_hall.push_null(handled_field_count, true);
            }
        }
        Err(err) => {
            let nsucc = handled_field_count;
            let nfail = batch_size - nsucc;
            if nsucc > 0 {
                output_field.iter_hall.push_null(nsucc, true);
            }
            let e = OperatorApplicationError::new_s(err.to_string(), op_id);
            output_field.iter_hall.push_error(e, nfail, false, true);
            outputs_produced += nfail;
        }
    }
    drop(output_field);

    let streams_done = tf.current_stream_val.is_none();

    if ps.next_batch_ready && streams_done {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr.submit_batch(
        tf_id,
        outputs_produced,
        ps.input_done && streams_done,
    );
}

pub fn handle_tf_print_stream_value_update(
    jd: &mut JobData,
    print: &mut TfPrint,
    update: StreamValueUpdate,
) {
    // we don't use a buffered writer here because stream chunks
    // are large and we want to avoid the copy
    let sv = &mut jd.sv_mgr.stream_values[update.sv_id];
    let run_len = update.custom;
    let mut success_count = run_len;
    let mut error_count = 0;
    let mut err_message = None;

    let mut stream = print.target.aquire(false);

    match write_stream_val_check_done(
        sv,
        update.data_offset,
        &mut stream,
        run_len,
    ) {
        Ok(false) => {
            return;
        }
        Ok(true) => (),
        Err((idx, e)) => {
            error_count = run_len - idx;
            success_count = run_len - error_count;
            err_message = Some(e.to_string());
        }
    }

    if print.flush_on_every_print {
        let _ = stream.flush();
    }
    let tf = &jd.tf_mgr.transforms[update.tf_id];
    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();
    if success_count > 0 {
        output_field.iter_hall.push_null(success_count, true);
    }
    if let Some(err_message) = err_message {
        output_field.iter_hall.push_error(
            OperatorApplicationError::new_s(
                err_message,
                jd.tf_mgr.transforms[update.tf_id].op_id.unwrap(),
            ),
            error_count,
            true,
            false,
        );
        jd.sv_mgr
            .drop_field_value_subscription(update.sv_id, Some(update.tf_id));
    }

    print.current_stream_val = None;
    jd.tf_mgr.push_tf_in_ready_stack(update.tf_id);
}
