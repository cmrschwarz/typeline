use std::io::{IsTerminal, Write};

use metamatch::metamatch;

use super::{
    errors::OperatorApplicationError,
    operator::{Operator, TransformInstatiation},
    transform::{Transform, TransformId, TransformState},
    utils::writable::{AnyWriter, WritableTarget},
};
use crate::{
    cli::{
        call_expr::{CallExpr, ParsedArgValue},
        CliArgumentError,
    },
    job::{JobData, PipelineState, TransformManager},
    operators::utils::buffer_stream_values::{
        buffer_remaining_stream_values_in_auto_deref_iter,
        buffer_remaining_stream_values_in_sv_iter,
    },
    options::chain_settings::{RationalsPrintMode, SettingRationalsPrintMode},
    record_data::{
        field::{Field, FieldManager},
        field_data::FieldValueType,
        field_value::{Null, Undefined},
        field_value_ref::FieldValueSlice,
        formattable::{
            format_error_raw, format_rational, Formattable, FormattingContext,
            RealizedFormatKey,
        },
        iter::{
            field_iterator::{FieldIterOpts, FieldIterator},
            field_value_slice_iter::FieldValueRangeIter,
            iter_adapters::UnfoldIterRunLength,
            ref_iter::{
                AutoDerefIter, RefAwareBytesBufferIter,
                RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
                RefAwareInlineTextIter, RefAwareTextBufferIter,
                RefAwareUnfoldIterRunLength,
            },
        },
        iter_hall::FieldIterId,
        match_set::MatchSetManager,
        push_interface::PushInterface,
        stream_value::{
            StreamValue, StreamValueDataOffset, StreamValueId,
            StreamValueManager, StreamValueUpdate,
        },
    },
    utils::{
        int_string_conversions::{bool_to_str, f64_to_str, i64_to_str},
        lazy_lock_guard::LazyRwLockGuard,
        string_store::StringStore,
        text_write::TextWriteIoAdapter,
    },
    NULL_STR, UNDEFINED_STR,
};

// ENHANCE: bikeshed a better format for this
const ERROR_PREFIX_STR: &str = "ERROR: ";

#[derive(Debug, Clone, Copy)]
pub struct PrintOptions {
    pub ignore_nulls: bool,
    pub propagate_errors: bool,
}

impl Default for PrintOptions {
    fn default() -> Self {
        Self {
            ignore_nulls: false,
            propagate_errors: true,
        }
    }
}

pub struct OpPrint {
    target: WritableTarget,
    opts: PrintOptions,
}

pub struct TfPrint<'a> {
    current_stream_val: Option<StreamValueId>,
    iter_id: FieldIterId,
    streams_kept_alive: usize,
    target: AnyWriter<'a>,
    flush_on_every_print: bool,
    rationals_print_mode: RationalsPrintMode,
    opts: PrintOptions,
}

pub fn parse_op_print(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, CliArgumentError> {
    let mut opts = PrintOptions::default();
    for arg in expr.parsed_args_iter() {
        match arg.value {
            ParsedArgValue::Flag(flag) => match flag {
                "n" => opts.ignore_nulls = true,
                "e" => opts.propagate_errors = true,
                _ => {
                    return Err(expr.error_flag_unsupported(flag, arg.span));
                }
            },
            ParsedArgValue::NamedArg { key, .. } => {
                return Err(expr.error_named_args_unsupported(key, arg.span));
            }
            ParsedArgValue::PositionalArg { .. } => {
                return Err(expr.error_positional_args_unsupported(arg.span));
            }
        }
    }
    Ok(Box::new(OpPrint {
        target: WritableTarget::Stdout,
        opts,
    }))
}

pub fn create_op_print() -> Box<dyn Operator> {
    Box::new(OpPrint {
        target: WritableTarget::Stdout,
        opts: PrintOptions {
            ignore_nulls: false,
            propagate_errors: true,
        },
    })
}
pub fn create_op_print_with_opts(
    target: WritableTarget,
    opts: PrintOptions,
) -> Box<dyn Operator> {
    Box::new(OpPrint { target, opts })
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
    tf_mgr: &mut TransformManager,
    fm: &FieldManager,
    msm: &MatchSetManager,
    sv_mgr: &mut StreamValueManager,
    mut string_store: LazyRwLockGuard<StringStore>,
    tf_id: TransformId,
    batch_size: usize,
    print: &mut TfPrint,
    // we need these even in case of an error, so it's an output parameter :/
    outputs_produced: &mut usize,
    handled_field_count: &mut usize,
    last_group_separator_end: &mut usize,
    output_field: &mut Field,
) -> std::io::Result<()> {
    let mut stream = print.target.aquire(true);
    *handled_field_count = 0;
    *last_group_separator_end = 0;
    let tf = &tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;

    let input_field = fm.get_cow_field_ref(msm, input_field_id);
    let base_iter = fm
        .lookup_iter(input_field_id, &input_field, print.iter_id)
        .bounded(0, batch_size);
    let mut iter = AutoDerefIter::new(fm, input_field_id, base_iter);
    'iter: while let Some(range) =
        iter.typed_range_fwd(msm, usize::MAX, FieldIterOpts::default())
    {
        metamatch!(match range.base.data {
            FieldValueSlice::Null(_) if print.opts.ignore_nulls => {
                *handled_field_count += range.base.field_count;
            }

            #[expand(REP in [Null, Undefined])]
            FieldValueSlice::REP(_) => {
                let zst_str = REP::REPR.to_str().as_bytes();
                for _ in 0..range.base.field_count {
                    stream.write_all(zst_str)?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }

            #[expand((REP, ITER, VAL) in [
                (TextInline, RefAwareInlineTextIter, v.as_bytes()),
                (BytesInline, RefAwareInlineBytesIter, v),
                (TextBuffer, RefAwareTextBufferIter, v.as_bytes()),
                (BytesBuffer, RefAwareBytesBufferIter, v),
            ])]
            FieldValueSlice::REP(text) => {
                for v in ITER::from_range(&range, text).unfold_rl() {
                    stream.write_all(VAL)?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }

            #[expand((REP, CONV_FN) in [
                (Bool, bool_to_str(*v)),
                (Int, i64_to_str(false, *v)),
                (Float, f64_to_str(*v))
            ])]
            FieldValueSlice::REP(ints) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, ints)
                {
                    let v = CONV_FN;
                    for _ in 0..rl {
                        stream.write_all(v.as_bytes())?;
                        stream.write_all(b"\n")?;
                        *handled_field_count += 1;
                    }
                }
            }

            FieldValueSlice::BigInt(big_ints) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, big_ints)
                {
                    for _ in 0..rl {
                        stream.write_fmt(format_args!("{v}\n"))?;
                        *handled_field_count += 1;
                    }
                }
            }

            FieldValueSlice::BigRational(rationals) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, rationals)
                {
                    for _ in 0..rl {
                        format_rational(
                            &mut TextWriteIoAdapter(&mut stream),
                            v,
                            print.rationals_print_mode,
                        )?;
                        stream.write_all(b"\n")?;
                        *handled_field_count += 1;
                    }
                }
            }

            #[expand(REP in [Array, Object, Argument, OpDecl])]
            FieldValueSlice::REP(arrays) => {
                let mut fc = FormattingContext {
                    ss: Some(&mut string_store),
                    fm: Some(fm),
                    msm: Some(msm),
                    rationals_print_mode: print.rationals_print_mode,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::default(),
                };
                for a in
                    RefAwareFieldValueRangeIter::from_range(&range, arrays)
                        .unfold_rl()
                {
                    a.format(&mut fc, &mut TextWriteIoAdapter(&mut stream))?;
                    stream.write_all(b"\n")?;
                    *handled_field_count += 1;
                }
            }

            FieldValueSlice::Error(errs) => {
                let field_count_before = *handled_field_count;
                for v in RefAwareFieldValueRangeIter::from_range(&range, errs)
                    .unfold_rl()
                {
                    stream
                        .write_fmt(format_args!("{ERROR_PREFIX_STR}{v}\n"))?;
                    *handled_field_count += 1;
                }
                if print.opts.propagate_errors {
                    output_field.iter_hall.push_null(
                        field_count_before - *outputs_produced,
                        true,
                    );
                    output_field
                        .iter_hall
                        .extend_from_ref_aware_range(range, true, false);
                    *outputs_produced = *handled_field_count;
                }
            }
            FieldValueSlice::Custom(custom_types) => {
                for v in RefAwareFieldValueRangeIter::from_range(
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
            FieldValueSlice::StreamValueId(svs) => {
                let mut pos = *handled_field_count;
                let mut sv_iter = FieldValueRangeIter::from_range(&range, svs);
                while let Some((&sv_id, rl)) = sv_iter.next() {
                    pos += rl as usize;
                    *handled_field_count += rl as usize;
                    let sv = &mut sv_mgr.stream_values[sv_id];
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
                        sv_mgr.check_stream_value_ref_count(sv_id);
                    } else {
                        print.current_stream_val = Some(sv_id);
                        if rl > 1 {
                            sv.make_buffered();
                        }
                        sv.subscribe(sv_id, tf_id, rl as usize, false, true);
                        tf_mgr.unclaim_batch_size(
                            tf_id,
                            batch_size - *handled_field_count,
                        );
                        print.streams_kept_alive +=
                            buffer_remaining_stream_values_in_sv_iter(
                                sv_mgr, sv_iter, false,
                            );
                        print.streams_kept_alive +=
                            buffer_remaining_stream_values_in_auto_deref_iter(
                                msm,
                                sv_mgr,
                                iter.clone(),
                                usize::MAX,
                                false,
                            );
                        iter.move_to_field_pos(pos);
                        break 'iter;
                    }
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        });
    }
    // TODO: remove this once `sequence` became a reasonable member of society
    while *handled_field_count < batch_size
        && print.current_stream_val.is_none()
    {
        stream.write_fmt(format_args!("{UNDEFINED_STR}\n"))?;
        *handled_field_count += 1;
    }
    fm.store_iter(input_field_id, print.iter_id, iter);
    Ok(())
}

impl Operator for OpPrint {
    fn default_name(&self) -> super::operator::OperatorName {
        "print".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: super::operator::OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: super::operator::OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfPrint {
            // ENHANCE: should we config options for this stuff?
            flush_on_every_print: matches!(
                self.target,
                WritableTarget::Stdout
            ) && std::io::stdout().is_terminal(),
            rationals_print_mode: job
                .job_data
                .get_setting_from_tf_state::<SettingRationalsPrintMode>(
                    tf_state,
                ),
            current_stream_val: None,
            streams_kept_alive: 0,
            iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
            target: self.target.create_writer_hide_error(true),
            opts: self.opts,
        }))
    }
}

impl<'a> Transform<'a> for TfPrint<'a> {
    fn update(&mut self, jd: &mut JobData<'_>, tf_id: TransformId) {
        if self.current_stream_val.is_some() {
            return;
        }
        let (batch_size, ps) = jd
            .tf_mgr
            .claim_batch_with_limit_bump(tf_id, self.streams_kept_alive);

        let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();
        let of_id = jd.tf_mgr.prepare_output_field(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
        );

        let mut handled_field_count = 0;
        let mut outputs_produced = 0;
        let mut last_group_separator = 0;
        let mut output_field = jd.field_mgr.fields[of_id].borrow_mut();
        let string_store = LazyRwLockGuard::new(&jd.session_data.string_store);
        let res = handle_tf_print_raw(
            &mut jd.tf_mgr,
            &jd.field_mgr,
            &jd.match_set_mgr,
            &mut jd.sv_mgr,
            string_store,
            tf_id,
            batch_size,
            self,
            &mut outputs_produced,
            &mut handled_field_count,
            &mut last_group_separator,
            &mut output_field,
        );
        if self.flush_on_every_print {
            self.target.aquire(false).flush().ok();
        }

        match res {
            Ok(()) => {
                if handled_field_count > 0 {
                    let diff = handled_field_count - outputs_produced;
                    output_field.iter_hall.push_null(diff, true);
                    outputs_produced += diff;
                }
            }
            Err(err) => {
                let nsucc = handled_field_count;
                let nfail = batch_size - nsucc;
                if nsucc > 0 {
                    output_field
                        .iter_hall
                        .push_null(nsucc - outputs_produced, true);
                }
                let e =
                    OperatorApplicationError::new_s(err.to_string(), op_id);
                output_field.iter_hall.push_error(e, nfail, false, true);
                outputs_produced += nfail;
            }
        }
        drop(output_field);

        let streams_done = self.current_stream_val.is_none();

        if ps.next_batch_ready && streams_done {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr.submit_batch_ready_for_more(
            tf_id,
            outputs_produced,
            PipelineState {
                input_done: ps.input_done && streams_done,
                ..ps
            },
        );
    }

    fn handle_stream_value_update(
        &mut self,
        jd: &mut JobData<'a>,
        update: StreamValueUpdate,
    ) {
        // we don't use a buffered writer here because stream chunks
        // are large and we want to avoid the copy
        let sv = &mut jd.sv_mgr.stream_values[update.sv_id];
        let run_len = update.custom;
        let mut success_count = run_len;
        let mut error_count = 0;
        let mut err_message = None;

        let mut stream = self.target.aquire(false);

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

        if self.flush_on_every_print {
            let _ = stream.flush();
        }
        let tf = &jd.tf_mgr.transforms[update.tf_id];
        let mut output_field =
            jd.field_mgr.fields[tf.output_field].borrow_mut();
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
            jd.sv_mgr.drop_field_value_subscription(
                update.sv_id,
                Some(update.tf_id),
            );
        }

        self.current_stream_val = None;
        jd.tf_mgr.push_tf_in_ready_stack(update.tf_id);
    }
}
