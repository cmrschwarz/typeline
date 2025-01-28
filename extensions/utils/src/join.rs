use metamatch::metamatch;
use regex::Regex;
use std::{collections::VecDeque, sync::Arc};
use typeline_core::{
    job::Job,
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        operator::{
            Operator, OperatorId, OperatorName, PreboundOutputsMap,
            TransformInstatiation,
        },
        print::typed_slice_zst_str,
        transform::{Transform, TransformId, TransformState},
    },
};

use typeline_core::{
    cli::call_expr::{CallExpr, ParsedArgValue, Span},
    job::{JobData, TransformManager},
    options::chain_settings::{
        RationalsPrintMode, SettingRationalsPrintMode,
        SettingStreamBufferSize, SettingStreamSizeThreshold,
    },
    record_data::{
        action_buffer::ActorId,
        field::FieldRefOffset,
        field_data::{
            FieldData, FieldValueRepr, RunLength, INLINE_STR_MAX_LEN,
        },
        field_value_ref::FieldValueSlice,
        formattable::{
            Formattable, FormattingContext, RealizedFormatKey, TypeReprFormat,
        },
        group_track::GroupTrackIterRef,
        iter::{
            field_iterator::FieldIterator,
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::{
                AutoDerefIter, RefAwareBytesBufferIter,
                RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
                RefAwareInlineTextIter, RefAwareTextBufferIter,
                RefAwareTypedRange,
            },
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataOffset, StreamValueDataType, StreamValueId,
            StreamValueManager, StreamValueUpdate,
        },
        varying_type_inserter::VaryingTypeInserter,
    },
    typeline_error::TypelineError,
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        int_string_conversions::{
            bool_to_str, f64_to_str, i64_to_str, usize_to_str,
        },
        lazy_lock_guard::LazyRwLockGuard,
        maybe_text::{MaybeText, MaybeTextBoxed, MaybeTextCow, MaybeTextRef},
        text_write::MaybeTextWritePanicAdapter,
        universe::{RefHandoutStack, Universe},
    },
};

#[derive(Clone)]
pub struct OpJoin {
    separator: Option<MaybeTextBoxed>,
    join_count: Option<usize>,
    drop_incomplete: bool,
}

#[derive(Debug)]
enum GroupBatchEntryData<'a> {
    Data(StreamValueData<'a>),
    StreamValueId {
        id: StreamValueId,
        streaming_emit: bool,
    },
}
#[derive(Debug)]
struct GroupBatchEntry<'a> {
    data: GroupBatchEntryData<'a>,
    run_length: RunLength,
}

struct GroupBatch<'a> {
    output_stream_value: StreamValueId,
    outstanding_values: VecDeque<GroupBatchEntry<'a>>,
    has_leading_streaming_input_sv: bool,
    is_producer: bool,
}

type GroupBatchId = DebuggableNonMaxUsize;

pub struct TfJoin<'a> {
    separator: Option<MaybeTextRef<'a>>,
    group_capacity: Option<usize>,
    drop_incomplete: bool,
    stream_size_threshold: usize,
    stream_buffer_size: usize,
    rationals_print_mode: RationalsPrintMode,
    input_field_ref_offset: FieldRefOffset,

    group_track_iter_ref: GroupTrackIterRef,
    iter_id: FieldIterId,
    actor_id: ActorId,

    first_record_added: bool,

    stream_value_error: bool,
    current_group_error: Option<OperatorApplicationError>,

    curr_group_len: usize,

    active_stream_value: Option<StreamValueId>,
    active_stream_value_submitted: bool,
    active_stream_value_appended: bool,

    active_group_batch: Option<GroupBatchId>,
    active_group_batch_appended: bool,

    group_batches: Universe<GroupBatchId, GroupBatch<'a>>,
    producing_batches: Vec<GroupBatchId>,
    // temp storage for delayed stream value ref count drops
    svs_to_drop: Vec<StreamValueId>,

    buffer: MaybeText,
}

static ARG_REGEX: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| {
        Regex::new(
            r"^(?:join|j)(?<insert_count>[0-9]+)?(-(?<drop_incomplete>d))?$",
        )
        .unwrap()
    });
pub fn argument_matches_op_join(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_join(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let mut count = None;
    let mut drop_incomplete = false;
    let mut drop_incomplete_span = Span::Generated;
    let mut value = None;
    for arg in expr.parsed_args_iter_with_bounded_positionals(0, 1) {
        let arg = arg?;
        match arg.value {
            ParsedArgValue::Flag(flag) => {
                if flag == "d" || flag == "drop_incomplete" {
                    drop_incomplete = true;
                    drop_incomplete_span = arg.span;
                    continue;
                }
                return Err(expr
                    .error_flag_unsupported(flag, arg.span)
                    .into());
            }
            ParsedArgValue::NamedArg { key, value } => {
                if key == "n" || key == "count" {
                    let value = value
                        .as_maybe_text_ref()
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            expr.error_arg_invalid_utf8(key, arg.span)
                        })?;
                    count = Some(value.parse::<usize>().map_err(|_| {
                        expr.error_arg_invalid_int(key, arg.span)
                    })?);
                    continue;
                }
                return Err(expr
                    .error_named_arg_unsupported(key, arg.span)
                    .into());
            }
            ParsedArgValue::PositionalArg { value: v, .. } => {
                let Some(argv) = v.text_or_bytes() else {
                    return Err(expr
                        .error_positional_arg_not_plaintext(arg.span)
                        .into());
                };
                value = Some(argv);
            }
        }
    }
    if drop_incomplete && count.is_none() {
        return Err(OperatorCreationError::new(
            "drop incomplete (-d) is only available in combination with a fixed join count (-n)",
            drop_incomplete_span,
        ).into());
    }
    Ok(create_op_join(
        value.map(MaybeText::from_bytes_try_str),
        count,
        drop_incomplete,
    ))
}

pub fn create_op_join(
    separator: Option<MaybeText>,
    join_count: Option<usize>,
    drop_incomplete: bool,
) -> Box<dyn Operator> {
    Box::new(OpJoin {
        separator: separator.map(MaybeText::into_boxed),
        join_count,
        drop_incomplete,
    })
}
pub fn create_op_join_str(
    separator: &str,
    join_count: usize,
) -> Box<dyn Operator> {
    let sep = match separator {
        "" => None,
        v => Some(MaybeTextBoxed::from_text(v)),
    };
    Box::new(OpJoin {
        separator: sep,
        join_count: if join_count == 0 {
            None
        } else {
            Some(join_count)
        },
        drop_incomplete: false,
    })
}

fn claim_stream_value(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
) -> StreamValueId {
    debug_assert!(join.active_stream_value.is_none());

    let sv = if join.buffer.is_empty() {
        StreamValue::new_empty(
            Some(StreamValueDataType::MaybeText),
            StreamValueBufferMode::Stream,
        )
    } else {
        let cap = join.buffer.capacity();
        let buf =
            std::mem::replace(&mut join.buffer, MaybeText::with_capacity(cap));

        let data_type = if buf.as_str().is_some() {
            Some(StreamValueDataType::MaybeText)
        } else {
            Some(StreamValueDataType::Bytes)
        };
        StreamValue::from_data(
            data_type,
            StreamValueData::from_maybe_text(buf),
            StreamValueBufferMode::Stream,
            false,
        )
    };

    let sv_id = sv_mgr.claim_stream_value(sv);
    join.active_stream_value = Some(sv_id);
    join.active_stream_value_appended = true;
    sv_id
}

fn get_active_group_batch(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
) -> GroupBatchId {
    if let Some(gbi) = join.active_group_batch {
        return gbi;
    }
    let output_stream_value = if let Some(sv_id) = join.active_stream_value {
        sv_id
    } else {
        claim_stream_value(join, sv_mgr)
    };
    // PERF: we should reuse these..
    let gbi = join.group_batches.claim_with_value(GroupBatch {
        output_stream_value,
        outstanding_values: VecDeque::new(),
        has_leading_streaming_input_sv: false,
        is_producer: false,
    });
    join.active_group_batch = Some(gbi);
    gbi
}

fn write_join_data_to_stream<'a>(
    join: &mut TfJoin<'a>,
    sv_mgr: &mut StreamValueManager<'a>,
    sv_id: StreamValueId,
    data: MaybeTextCow,
    rl: RunLength,
) {
    let sv = &mut sv_mgr.stream_values[sv_id];
    let mut inserter = sv.data_inserter(
        sv_id,
        join.stream_buffer_size,
        !join.active_stream_value_appended,
    );
    // PERF: if these are small (the common case!), we end up appending
    // a lot of tiny buffers. we should just build a big buffer instead,
    // or add a way to the inserter to do that heuristically
    let svd = StreamValueData::from_maybe_text(data.into_owned());
    if let Some(sep) = join.separator {
        let sep_svd = StreamValueData::from_maybe_text_ref(sep);
        if join.first_record_added {
            for _ in 0..rl - 1 {
                inserter.append(sep_svd.clone());
                inserter.append(svd.clone());
            }
            inserter.append(sep_svd);
            inserter.append(svd);
        } else if rl == 1 {
            inserter.append(svd);
        } else {
            for _ in 0..rl - 1 {
                inserter.append(svd.clone());
                inserter.append(sep_svd.clone());
            }
            inserter.append(svd);
        }
    } else {
        for _ in 0..rl {
            inserter.append(svd.clone());
        }
    }
    join.first_record_added = true;
    join.active_stream_value_appended = true;
}

fn push_join_data<'a>(
    join: &mut TfJoin<'a>,
    sv_mgr: &mut StreamValueManager<'a>,
    data: MaybeTextCow,
    mut rl: RunLength,
) {
    if let Some(gbi) = join.active_group_batch {
        let gb = &mut join.group_batches[gbi];
        if !gb.outstanding_values.is_empty() {
            gb.outstanding_values.push_back(GroupBatchEntry {
                data: GroupBatchEntryData::Data(
                    StreamValueData::from_maybe_text(data.into_owned()),
                ),
                run_length: rl,
            });
            join.active_group_batch_appended = true;
            return;
        }
    }

    let data_size = data.len();
    let sep_size = join.separator.map(|s| s.len()).unwrap_or(0);

    if join.active_stream_value.is_none() {
        let mut insert_size = data_size;
        if join.first_record_added {
            insert_size += sep_size;
        }
        if rl > 1 {
            insert_size += (rl as usize - 1) * (data_size + sep_size);
        }
        if join.buffer.len() + insert_size > join.stream_size_threshold {
            claim_stream_value(join, sv_mgr);
        }
    }
    let data_ref = data.as_ref();
    if let Some(sv_id) = join.active_stream_value {
        write_join_data_to_stream(join, sv_mgr, sv_id, data, rl);
        return;
    }

    if let Some(sep) = join.separator {
        if !join.first_record_added {
            join.buffer.extend_with_maybe_text_ref(data_ref);
            rl = rl.saturating_sub(1);
            join.first_record_added = true;
        }
        for _ in 0..rl {
            join.buffer.extend_with_maybe_text_ref(sep);
            join.buffer.extend_with_maybe_text_ref(data_ref);
        }
    } else {
        join.first_record_added = true;
        for _ in 0..rl {
            join.buffer.extend_with_maybe_text_ref(data_ref);
        }
    }
}

fn push_finished_stream_value(
    _join: &mut TfJoin,
    _sv_mgr: &mut StreamValueManager,
    _sv_id: StreamValueId,
    _rl: RunLength,
) -> Result<(), ()> {
    todo!();
}

pub fn drop_group(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    tf_id: TransformId,
) {
    if let Some(gbi) = join.active_group_batch {
        drop_group_batch_sv_subscriptions(sv_mgr, tf_id, join, gbi);
        join.group_batches.release(gbi);
        join.active_group_batch = None;
        join.active_stream_value = None;
        join.active_stream_value_submitted = false;
    } else {
        debug_assert!(join.active_stream_value.is_none());
        join.current_group_error = None;
        join.buffer.clear();
    }
    join.curr_group_len = 0;
    join.first_record_added = false;
}

fn make_group_batch_producer(join: &mut TfJoin, gbi: GroupBatchId) {
    join.producing_batches.push(gbi);
    let gb = &mut join.group_batches[gbi];
    debug_assert!(!gb.is_producer);
    debug_assert!(!gb.has_leading_streaming_input_sv);
    gb.is_producer = true;
}

pub fn emit_group(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    tf_mgr: &mut TransformManager,
    tf_id: TransformId,
    output_inserter: &mut VaryingTypeInserter<&mut FieldData>,
    groups_emitted: &mut usize,
) {
    join.curr_group_len = 0;
    join.first_record_added = false;

    if let Some(sv_id) = join.active_stream_value {
        let mut emitted = true;
        join.active_stream_value = None;

        let mut done = true;

        if let Some(gbi) = join.active_group_batch {
            join.active_group_batch = None;
            let gb = &join.group_batches[gbi];
            done = gb.outstanding_values.is_empty();
            if join.active_group_batch_appended {
                join.active_group_batch_appended = false;
                if !done && !gb.has_leading_streaming_input_sv {
                    tf_mgr.make_stream_producer(tf_id);
                    make_group_batch_producer(join, gbi);
                }
            }
        }
        if done {
            sv_mgr.stream_values[sv_id].mark_done();
        }

        if join.active_stream_value_submitted {
            join.active_stream_value_submitted = false;
            emitted = false;
            if join.active_stream_value_appended || done {
                sv_mgr.inform_stream_value_subscribers(sv_id);
            }
        } else {
            output_inserter.push_stream_value_id(sv_id, 1, true, false);
        }
        join.active_stream_value_appended = false;
        *groups_emitted += usize::from(emitted);
        return;
    }

    *groups_emitted += 1;

    if let Some(err) = join.current_group_error.take() {
        output_inserter.push_error(err, 1, true, false);
        return;
    }

    let len = join.buffer.len();

    if len < INLINE_STR_MAX_LEN {
        output_inserter.push_inline_maybe_text_ref(
            join.buffer.as_ref(),
            1,
            true,
            false,
        );
        join.buffer.clear();
        return;
    }

    let buffer =
        std::mem::replace(&mut join.buffer, MaybeText::with_capacity(len));
    output_inserter.push_maybe_text(buffer, 1, true, false);
}
fn push_error(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    e: OperatorApplicationError,
) {
    if let Some(sv_id) = join.active_stream_value {
        let sv = &mut sv_mgr.stream_values[sv_id];
        sv.set_error(Arc::new(e));
        join.stream_value_error = true;
        join.active_stream_value_appended = true;
    } else {
        join.current_group_error = Some(e);
        join.buffer.clear();
    }
}

fn should_drop_group(join: &mut TfJoin) -> bool {
    let mut emit_incomplete = false;
    // if we dont drop incomplete and there are actual members
    emit_incomplete |= join.curr_group_len > 0 && !join.drop_incomplete;
    // if we join all output, and there is output
    emit_incomplete |=
        join.group_capacity.is_none() && join.curr_group_len > 0;
    // if we join all output, there is potentially no output,
    // but we don't drop incomplete
    emit_incomplete |= join.group_capacity.is_none() && !join.drop_incomplete;
    !emit_incomplete
}

fn try_consume_stream_values<'a>(
    tf_id: TransformId,
    join: &mut TfJoin<'a>,
    sv_mgr: &mut StreamValueManager<'a>,
    range: &RefAwareTypedRange<'_>,
    svs: &[StreamValueId],
) -> Result<(), ()> {
    let sv_iter = FieldValueRangeIter::from_range(range, svs);
    for (&sv_id, rl) in sv_iter {
        let sv = &mut sv_mgr.stream_values[sv_id];
        if let Some(err) = &sv.error {
            if let Some(out_sv_id) = join.active_stream_value {
                let e = err.clone();
                let out_sv = &mut sv_mgr.stream_values[out_sv_id];
                out_sv.set_error(e);
                join.stream_value_error = true;
            } else {
                join.current_group_error = Some((**err).clone());
            }
            return Ok(());
        }
        if sv.done {
            sv.make_buffered();
            push_finished_stream_value(join, sv_mgr, sv_id, rl)?;
            continue;
        }
        push_partial_stream_value_and_sub(join, sv_mgr, tf_id, sv_id, rl);
    }
    Ok(())
}

fn push_partial_stream_value_and_sub<'a>(
    join: &mut TfJoin<'a>,
    sv_mgr: &mut StreamValueManager<'a>,
    tf_id: TransformId,
    sv_id: StreamValueId,
    rl: u32,
) {
    let gbi = get_active_group_batch(join, sv_mgr);
    let gb = &mut join.group_batches[gbi];

    if let Some(GroupBatchEntry {
        data: GroupBatchEntryData::StreamValueId { id, .. },
        run_length,
    }) = gb.outstanding_values.back_mut()
    {
        // This is not a micro optimization, but neccessary for correctness.
        // Due to batching, we might hit the same stream value multiple times
        // directly behind each other. We have to prevent double submits
        // because otherwise the 'leading stream value done' logic in
        // `handle_tf_join_stream_value_update` will fire twice
        if *id == sv_id {
            *run_length += rl;
            return;
        }
    }

    let (sv, out_sv) = sv_mgr
        .stream_values
        .two_distinct_mut(sv_id, gb.output_stream_value);
    let mut buffered = false;
    let streaming_emit = gb.outstanding_values.is_empty();

    if streaming_emit {
        let mut inserter = out_sv.data_inserter(
            gb.output_stream_value,
            join.stream_buffer_size,
            true,
        );
        let mut iter = sv.data_cursor(StreamValueDataOffset::default(), false);

        if join.first_record_added {
            if let Some(sep) = join.separator {
                inserter.append(StreamValueData::from_maybe_text_ref(sep));
            }
        }
        inserter.extend_from_cursor(&mut iter);
        join.active_stream_value_appended = true;
    }
    if !streaming_emit || rl > 1 {
        // PERF: we might be able to avoid this if pending_stream_values == 0?
        sv.make_buffered();
        buffered = true;
    }
    join.first_record_added = true;
    if streaming_emit && gb.outstanding_values.is_empty() {
        gb.has_leading_streaming_input_sv = true;
    }
    gb.outstanding_values.push_back(GroupBatchEntry {
        data: GroupBatchEntryData::StreamValueId {
            id: sv_id,
            streaming_emit,
        },
        run_length: rl,
    });
    sv.subscribe(sv_id, tf_id, gbi.get(), buffered, streaming_emit);
    join.active_group_batch_appended = true;
}

fn drop_group_batch_sv_subscriptions(
    sv_mgr: &mut StreamValueManager,
    tf_id: TransformId,
    join: &mut TfJoin,
    gbi: GroupBatchId,
) {
    let gb = &mut join.group_batches[gbi];
    for v in &gb.outstanding_values {
        match &v.data {
            GroupBatchEntryData::Data(_) => (),
            GroupBatchEntryData::StreamValueId { id, .. } => {
                sv_mgr.drop_field_value_subscription(*id, Some(tf_id));
            }
        }
    }
}

fn handle_group_batch_producer_update<'a>(
    jd: &mut JobData<'a>,
    tf_id: TransformId,
    join: &mut TfJoin<'a>,
    gbi: GroupBatchId,
    make_producer: &mut bool,
) {
    let gb = &mut join.group_batches[gbi];
    let out_sv_id = gb.output_stream_value;

    let mut streams_handout = jd.sv_mgr.stream_values.ref_mut_handout_stack();

    let (mut streams_handout, out_sv) = streams_handout.claim(out_sv_id);

    let mut inserter =
        out_sv.data_inserter(out_sv_id, join.stream_buffer_size, true);

    while let Some(entry) = gb.outstanding_values.front_mut() {
        let mut rl_consumed = 0;
        match &mut entry.data {
            GroupBatchEntryData::Data(data) => {
                while rl_consumed < entry.run_length {
                    if let Some(sep) = join.separator {
                        inserter
                            .append(StreamValueData::from_maybe_text_ref(sep));
                    }
                    inserter.append(data.clone());
                    rl_consumed += 1;
                    if inserter.memory_budget_reached() {
                        break;
                    }
                }
            }
            GroupBatchEntryData::StreamValueId { id, streaming_emit } => {
                let (_, sv) = streams_handout.claim(*id);
                if inserter.propagate_error(&sv.error) {
                    drop(inserter);
                    jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
                    drop_group_batch_sv_subscriptions(
                        &mut jd.sv_mgr,
                        tf_id,
                        join,
                        gbi,
                    );
                    return;
                }
                if !sv.done {
                    // we don't want to set `make_producer` for this case,
                    // as we have to wait on the stream value to proceed
                    break;
                }
                if *streaming_emit {
                    rl_consumed += 1;
                    *streaming_emit = false;
                }
                // PERF: we should make sure to avoid creating the
                // same buffer multiple times here
                while rl_consumed < entry.run_length {
                    if let Some(sep) = join.separator {
                        inserter
                            .append(StreamValueData::from_maybe_text_ref(sep));
                    }
                    rl_consumed += 1;
                    inserter.extend_from_cursor(&mut sv.data_cursor(
                        StreamValueDataOffset::default(),
                        rl_consumed == entry.run_length,
                    ));
                    if inserter.memory_budget_reached() {
                        break;
                    }
                }

                if rl_consumed == entry.run_length {
                    join.svs_to_drop.push(*id);
                }
            }
        }
        if rl_consumed != entry.run_length {
            entry.run_length -= rl_consumed;
            *make_producer = true;
            break;
        }
        gb.outstanding_values.pop_front();
    }
    drop(inserter);

    if gb.outstanding_values.is_empty() {
        if join.active_group_batch == Some(gbi) {
            gb.is_producer = false;
        } else {
            out_sv.mark_done();
        }
    }
    jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);

    for &sv in &join.svs_to_drop {
        jd.sv_mgr.drop_field_value_subscription(sv, Some(tf_id));
    }
    join.svs_to_drop.clear();
}

impl Operator for OpJoin {
    fn default_name(&self) -> OperatorName {
        "join".into()
    }

    fn debug_op_name(&self) -> OperatorName {
        let mut str = String::from("join");
        str.push_str(
            self.join_count
                .map(usize_to_str)
                .unwrap_or_default()
                .as_str(),
        );
        if self.drop_incomplete {
            str.push_str("-d");
        }
        str.into()
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;
        let input_field_ref_offset = jd.field_mgr.register_field_reference(
            tf_state.output_field,
            tf_state.input_field,
        );

        let stream_buffer_size =
            jd.get_setting_from_tf_state::<SettingStreamBufferSize>(tf_state);
        let stream_size_threshold = jd
            .get_setting_from_tf_state::<SettingStreamSizeThreshold>(tf_state);
        let rationals_print_mode = jd
            .get_setting_from_tf_state::<SettingRationalsPrintMode>(tf_state);
        let actor_id = jd.add_actor_for_tf_state(tf_state);
        TransformInstatiation::Single(Box::new(TfJoin {
            separator: self.separator.as_ref().map(|s| s.as_ref()),
            group_capacity: self.join_count,
            drop_incomplete: self.drop_incomplete,
            stream_size_threshold,
            stream_buffer_size,
            rationals_print_mode,
            input_field_ref_offset,
            group_track_iter_ref: jd
                .claim_group_track_iter_for_tf_state(tf_state),
            iter_id: jd.claim_iter_for_tf_state(tf_state),
            actor_id,
            first_record_added: false,
            buffer: MaybeText::default(),
            // TODO: add a separate setting for this
            stream_value_error: false,
            current_group_error: None,
            curr_group_len: 0,
            active_stream_value: None,
            active_group_batch: None,
            active_stream_value_submitted: false,
            active_stream_value_appended: false,
            active_group_batch_appended: false,
            group_batches: Universe::default(),
            producing_batches: Vec::new(),
            svs_to_drop: Vec::new(),
        }))
    }
}

impl<'a> Transform<'a> for TfJoin<'a> {
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        jd.tf_mgr.prepare_output_field(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
        );
        let tf = &jd.tf_mgr.transforms[tf_id];
        let op_id = tf.op_id.unwrap();
        let mut output_field =
            jd.field_mgr.fields[tf.output_field].borrow_mut();

        let mut output_inserter =
            output_field.iter_hall.varying_type_inserter();
        let input_field_id = tf.input_field;
        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
        let base_iter = jd.field_mgr.lookup_iter(
            input_field_id,
            &input_field,
            self.iter_id,
        );
        let field_pos_start = base_iter.get_next_field_pos();
        let mut iter =
            AutoDerefIter::new(&jd.field_mgr, input_field_id, base_iter);

        let mut desired_group_len_rem =
            self.group_capacity.unwrap_or(usize::MAX) - self.curr_group_len;
        let mut groups_emitted = 0;
        let mut batch_size_rem = batch_size;
        let mut last_group_end = field_pos_start;
        let started_with_prebuffered_record = self.first_record_added;
        let mut prebuffered_record = started_with_prebuffered_record;
        let mut string_store =
            LazyRwLockGuard::new(&jd.session_data.string_store);

        let mut record_group_iter =
            jd.group_track_manager.lookup_group_track_iter_mut(
                self.group_track_iter_ref.track_id,
                self.group_track_iter_ref.iter_id,
                &jd.match_set_mgr,
                self.actor_id,
            );

        'iter: loop {
            if self.current_group_error.is_some() || self.stream_value_error {
                let consumed = iter.next_n_fields(
                    desired_group_len_rem
                        .min(batch_size_rem)
                        .min(record_group_iter.group_len_rem()),
                );
                record_group_iter.next_n_fields(consumed);
                desired_group_len_rem -= consumed;
                self.curr_group_len += consumed;
                batch_size_rem -= consumed;
            }
            let end_of_group =
                record_group_iter.is_end_of_group(ps.input_done);
            if end_of_group || desired_group_len_rem == 0 {
                let field_pos = iter.get_next_field_pos();
                let should_drop =
                    desired_group_len_rem != 0 && should_drop_group(self);
                let drop_count = (field_pos - last_group_end
                    + usize::from(prebuffered_record))
                .saturating_sub(usize::from(!should_drop));
                record_group_iter.drop_backwards(drop_count);
                prebuffered_record = false;
                if self.curr_group_len == 0 && !should_drop {
                    record_group_iter
                        .insert_fields(FieldValueRepr::Undefined, 1);
                }
                last_group_end = field_pos;
                if should_drop {
                    drop_group(self, &mut jd.sv_mgr, tf_id);
                } else {
                    emit_group(
                        self,
                        &mut jd.sv_mgr,
                        &mut jd.tf_mgr,
                        tf_id,
                        &mut output_inserter,
                        &mut groups_emitted,
                    );
                }

                if end_of_group {
                    loop {
                        if !record_group_iter.try_next_group() {
                            break;
                        }
                        if !record_group_iter.is_end_of_group(ps.input_done) {
                            break;
                        }
                        record_group_iter
                            .insert_fields(FieldValueRepr::Undefined, 1);
                    }
                }
                desired_group_len_rem =
                    self.group_capacity.unwrap_or(usize::MAX);
            }

            if batch_size_rem != 0
                && self.curr_group_len == 0
                && self.group_capacity.unwrap_or(1) == 1
                && record_group_iter.group_len_rem() == 1
            {
                // optimized case for groups of length 1,
                // where we can just copy straight to output without any
                // buffering or separators

                let count = record_group_iter
                    .skip_single_elem_groups(ps.input_done, batch_size_rem);
                let mut rem = count;
                while rem > 0 {
                    let range =
                        iter.typed_range_fwd(&jd.match_set_mgr, rem).unwrap();
                    rem -= range.base.field_count;
                    output_inserter
                        .extend_from_ref_aware_range_stringified_smart_ref(
                            &jd.field_mgr,
                            &jd.match_set_mgr,
                            &jd.sv_mgr,
                            &mut string_store,
                            range,
                            true,
                            true,
                            true,
                            self.input_field_ref_offset,
                            self.rationals_print_mode,
                        );
                }
                last_group_end = iter.get_next_field_pos();
                groups_emitted += count;
                batch_size_rem -= count;
            }

            let Some(range) = iter.typed_range_fwd(
                &jd.match_set_mgr,
                desired_group_len_rem
                    .min(batch_size_rem)
                    .min(record_group_iter.group_len_rem()),
            ) else {
                break;
            };
            metamatch!(match range.base.data {
                FieldValueSlice::Null(_) | FieldValueSlice::Undefined(_) => {
                    let str = typed_slice_zst_str(&range.base.data);
                    push_error(
                        self,
                        &mut jd.sv_mgr,
                        OperatorApplicationError::new_s(
                            format!("join does not support {str}"),
                            op_id,
                        ),
                    );
                }

                #[expand((REP, ITER, REF) in [
                    (TextInline, RefAwareInlineTextIter, TextRef),
                    (TextBuffer, RefAwareTextBufferIter, TextRef),
                    (BytesInline, RefAwareInlineBytesIter, BytesRef),
                    (BytesBuffer, RefAwareBytesBufferIter, BytesRef),
                ])]
                FieldValueSlice::REP(text) => {
                    for (v, rl, _offs) in ITER::from_range(&range, text) {
                        push_join_data(
                            self,
                            &mut jd.sv_mgr,
                            MaybeTextCow::REF(v),
                            rl,
                        );
                    }
                }
                #[expand((REP, CONV_FN) in [
                    (Bool, bool_to_str(*v)),
                    (Int, &i64_to_str(false, *v)),
                    (Float, &f64_to_str(*v)),
                ])]
                FieldValueSlice::REP(ints) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, ints)
                    {
                        push_join_data(
                            self,
                            &mut jd.sv_mgr,
                            MaybeTextCow::TextRef(CONV_FN),
                            rl,
                        );
                    }
                }
                FieldValueSlice::Custom(custom_data) => {
                    let rfk = RealizedFormatKey::default();
                    for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                        &range,
                        custom_data,
                    ) {
                        let mut res = MaybeText::default();
                        if let Err(e) = v.format_raw(&mut res, &rfk) {
                            push_error(
                                self,
                                &mut jd.sv_mgr,
                                OperatorApplicationError::new_s(
                                    e.to_string(),
                                    op_id,
                                ),
                            );
                        }
                        push_join_data(
                            self,
                            &mut jd.sv_mgr,
                            MaybeTextCow::from_maybe_text(res),
                            rl,
                        )
                    }
                }

                FieldValueSlice::BigInt(_) => {
                    todo!();
                }

                #[expand(REP in [
                    Object, Array, Argument, OpDecl, BigRational
                ])]
                FieldValueSlice::REP(v) => {
                    let mut fc = FormattingContext {
                        ss: Some(&mut string_store),
                        rationals_print_mode: self.rationals_print_mode,
                        fm: Some(&jd.field_mgr),
                        msm: Some(&jd.match_set_mgr),
                        is_stream_value: false,
                        rfk: RealizedFormatKey::with_type_repr(
                            TypeReprFormat::Typed,
                        ),
                    };
                    for (v, rl) in
                        RefAwareFieldValueRangeIter::from_range(&range, v)
                    {
                        // PERF: much allocs, much sadness
                        let mut buff = String::new();
                        v.format(
                            &mut fc,
                            &mut MaybeTextWritePanicAdapter(&mut buff),
                        )
                        .unwrap();
                        push_join_data(
                            self,
                            &mut jd.sv_mgr,
                            MaybeTextCow::Text(buff),
                            rl,
                        );
                    }
                }

                FieldValueSlice::Error(errs) => {
                    push_error(self, &mut jd.sv_mgr, errs[0].clone());
                }

                FieldValueSlice::StreamValueId(svs) => {
                    if try_consume_stream_values(
                        tf_id,
                        self,
                        &mut jd.sv_mgr,
                        &range,
                        svs,
                    )
                    .is_err()
                    {
                        break 'iter;
                    }
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            });
            let fc = range.base.field_count;
            self.curr_group_len += fc;
            desired_group_len_rem -= fc;
            batch_size_rem -= fc;
            record_group_iter.next_n_fields(fc);
        }

        if let Some(sv_id) = self.active_stream_value {
            if !self.active_stream_value_submitted {
                self.active_stream_value_submitted = true;
                output_inserter.push_stream_value_id(sv_id, 1, true, false);
                groups_emitted += 1;
                self.active_stream_value_appended = false;
            }
            if self.active_stream_value_appended {
                jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                self.active_stream_value_appended = false;
            }
            if let Some(gbi) = self.active_group_batch {
                if self.active_group_batch_appended {
                    self.active_group_batch_appended = false;
                    if !self.group_batches[gbi].has_leading_streaming_input_sv
                    {
                        jd.tf_mgr.make_stream_producer(tf_id);
                        make_group_batch_producer(self, gbi);
                    }
                }
            }
        }

        let drop_count = (iter.get_next_field_pos() - last_group_end
            + usize::from(
                started_with_prebuffered_record && groups_emitted == 0,
            ))
        .saturating_sub(1);
        record_group_iter.drop_backwards(drop_count);

        jd.field_mgr.store_iter(input_field_id, self.iter_id, iter);
        record_group_iter.store_iter(self.group_track_iter_ref.iter_id);

        drop(input_field);

        if ps.next_batch_ready {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr.submit_batch(
            tf_id,
            groups_emitted,
            ps.group_to_truncate,
            ps.input_done,
        );
    }

    fn stream_producer_update(
        &mut self,
        jd: &mut JobData<'a>,
        tf_id: TransformId,
    ) {
        let mut make_producer = false;
        for i in 0..self.producing_batches.len() {
            handle_group_batch_producer_update(
                jd,
                tf_id,
                self,
                self.producing_batches[i],
                &mut make_producer,
            );
        }
        if make_producer {
            jd.tf_mgr.make_stream_producer(tf_id);
        }
        let mut gb_producer_idx = self.producing_batches.len();
        while gb_producer_idx > 0 {
            gb_producer_idx -= 1;
            let gbi = self.producing_batches[gb_producer_idx];
            let gb = &mut self.group_batches[gbi];
            if !gb.is_producer {
                self.producing_batches.swap_remove(gb_producer_idx);
            }
        }
    }

    fn handle_stream_value_update(
        &mut self,
        jd: &mut JobData<'a>,
        update: StreamValueUpdate,
    ) {
        let group_batch_id = GroupBatchId::try_from(update.custom).unwrap();
        let in_sv_id = update.sv_id;
        let gb = &mut self.group_batches[group_batch_id];
        let out_sv_id = gb.output_stream_value;

        let (in_sv, out_sv) = jd
            .sv_mgr
            .stream_values
            .two_distinct_mut(in_sv_id, out_sv_id);

        if out_sv.propagate_error(&in_sv.error) {
            if let Some(idx) = self
                .producing_batches
                .iter()
                .position(|gbi| *gbi == group_batch_id)
            {
                debug_assert!(gb.is_producer);
                gb.is_producer = false;
                self.producing_batches.swap_remove(idx);
            }
            drop_group_batch_sv_subscriptions(
                &mut jd.sv_mgr,
                update.tf_id,
                self,
                group_batch_id,
            );
            if self.active_group_batch != Some(group_batch_id) {
                self.group_batches.release(group_batch_id);
            }
            jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
            return;
        }

        let Some(&GroupBatchEntry {
            data:
                GroupBatchEntryData::StreamValueId {
                    id: front_id,
                    streaming_emit,
                },
            run_length: group_batch_run_len,
        }) = gb.outstanding_values.front()
        else {
            return;
        };

        if front_id != in_sv_id {
            return;
        }

        debug_assert!(gb.has_leading_streaming_input_sv);

        if streaming_emit {
            debug_assert!(!out_sv.done);

            let mut inserter =
                out_sv.data_inserter(out_sv_id, self.stream_buffer_size, true);
            inserter.extend_from_cursor(&mut in_sv.data_cursor(
                update.data_offset,
                update.may_consume_data && group_batch_run_len == 1,
            ));
        }

        if in_sv.done {
            gb.has_leading_streaming_input_sv = false;
            if gb.outstanding_values.is_empty() {
                if Some(group_batch_id) != self.active_group_batch {
                    out_sv.mark_done();
                    self.group_batches.release(group_batch_id);
                }
                jd.sv_mgr.drop_field_value_subscription(
                    in_sv_id,
                    Some(update.tf_id),
                );
            } else {
                make_group_batch_producer(self, group_batch_id);
                jd.tf_mgr.make_stream_producer(update.tf_id);
            }
        }

        if streaming_emit {
            jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
        }
    }
}
