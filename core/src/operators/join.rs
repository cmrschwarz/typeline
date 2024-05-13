use std::{collections::VecDeque, sync::Arc};

use regex::Regex;
use smallstr::SmallString;

use crate::{
    job::{JobData, TransformManager},
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorId,
        custom_data::CustomData,
        field::FieldRefOffset,
        field_data::{
            field_value_flags, FieldData, FieldValueRepr, RunLength,
            INLINE_STR_MAX_LEN,
        },
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueSliceIter,
        formattable::RealizedFormatKey,
        group_tracker::GroupListIterRef,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueSliceIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
            RefAwareTypedRange,
        },
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataOffset, StreamValueDataType, StreamValueId,
            StreamValueManager, StreamValueUpdate,
        },
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        int_string_conversions::{i64_to_str, usize_to_str},
        lazy_lock_guard::LazyRwLockGuard,
        maybe_text::{MaybeText, MaybeTextBoxed, MaybeTextCow, MaybeTextRef},
        universe::Universe,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{DefaultOperatorName, OperatorBase, OperatorData, OperatorId},
    print::typed_slice_zst_str,
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpJoin {
    separator: Option<MaybeTextBoxed>,
    join_count: Option<usize>,
    drop_incomplete: bool,
}
impl OpJoin {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        let mut small_str = SmallString::new();
        small_str.push_str("join");
        small_str.push_str(
            self.join_count
                .map(usize_to_str)
                .unwrap_or_default()
                .as_str(),
        );
        if self.drop_incomplete {
            small_str.push_str("-d");
        }
        small_str
    }
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
    stream_len_threshold: usize,
    stream_buffer_size: usize,
    input_field_ref_offset: FieldRefOffset,

    group_list_iter_ref: GroupListIterRef,
    iter_id: IterId,
    actor_id: ActorId,

    first_record_added: bool,
    buffer: MaybeText,

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
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^(?:join|j)(?<insert_count>[0-9]+)?(-(?<drop_incomplete>d))?$").unwrap();
}
pub fn argument_matches_op_join(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_join(
    argument: &str,
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let args = ARG_REGEX.captures(argument).ok_or_else(|| {
        OperatorCreationError::new("invalid argument syntax for join", arg_idx)
    })?;
    let insert_count = args
        .name("insert_count")
        .map(|ic| {
            ic.as_str().parse::<usize>().map_err(|_| {
                OperatorCreationError::new(
                    "failed to parse insertion count as an integer",
                    arg_idx,
                )
            })
        })
        .transpose()?;
    let drop_incomplete = args.name("drop_incomplete").is_some();
    if drop_incomplete && insert_count.is_none() {
        return Err(OperatorCreationError::new(
            "the 'd' option for join is only available in combination with a set size",
            arg_idx,
        ));
    }
    Ok(create_op_join(
        value.map(MaybeText::from_bytes_try_str),
        insert_count,
        drop_incomplete,
    ))
}

pub fn build_tf_join<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpJoin,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let input_field_ref_offset = jd
        .field_mgr
        .register_field_reference(tf_state.output_field, tf_state.input_field);

    let settings = &jd.get_transform_chain_from_tf_state(tf_state).settings;
    TransformData::Join(TfJoin {
        separator: op.separator.as_ref().map(|s| s.as_ref()),
        group_capacity: op.join_count,
        drop_incomplete: op.drop_incomplete,
        stream_len_threshold: settings.stream_size_threshold,
        stream_buffer_size: settings.stream_buffer_size,
        input_field_ref_offset,
        group_list_iter_ref: jd
            .group_tracker
            .claim_group_list_iter_ref(tf_state.input_group_list_id),
        iter_id: jd.field_mgr.claim_iter(
            tf_state.input_field,
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
        ),
        actor_id: jd.add_actor_for_tf_state(tf_state),
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
    })
}

pub fn create_op_join(
    separator: Option<MaybeText>,
    join_count: Option<usize>,
    drop_incomplete: bool,
) -> OperatorData {
    OperatorData::Join(OpJoin {
        separator: separator.map(MaybeText::into_boxed),
        join_count,
        drop_incomplete,
    })
}
pub fn create_op_join_str(separator: &str, join_count: usize) -> OperatorData {
    let sep = match separator {
        "" => None,
        v => Some(MaybeTextBoxed::from_text(v)),
    };
    OperatorData::Join(OpJoin {
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
    let buf = if join.first_record_added {
        let cap = join.buffer.capacity();
        std::mem::replace(&mut join.buffer, MaybeText::with_capacity(cap))
    } else {
        MaybeText::default()
    };
    let sv_id = sv_mgr.claim_stream_value(StreamValue::from_data(
        if buf.as_str().is_some() {
            Some(StreamValueDataType::MaybeText)
        } else {
            Some(StreamValueDataType::Bytes)
        },
        StreamValueData::from_maybe_text(buf),
        StreamValueBufferMode::Stream,
        false,
    ));
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
        if join.buffer.len() + insert_size > join.stream_len_threshold {
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

pub fn reset_group_stats(join: &mut TfJoin) {
    join.curr_group_len = 0;
    join.first_record_added = false;
    join.buffer.clear();
    join.active_stream_value_submitted = false;
    join.active_stream_value = None;
    join.active_group_batch = None;
    join.current_group_error = None;
}

pub fn drop_group(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    tf_id: TransformId,
) {
    if let Some(gbi) = join.active_group_batch {
        drop_group_batch_sv_subscriptions(sv_mgr, tf_id, join, gbi);
        join.group_batches.release(gbi);
    } else {
        debug_assert!(join.active_stream_value.is_none());
    }
    reset_group_stats(join);
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
    let len = join.buffer.len();
    let mut emitted = true;
    if let Some(sv_id) = join.active_stream_value {
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
            emitted = false;
            if join.active_stream_value_appended || done {
                sv_mgr.inform_stream_value_subscribers(sv_id);
            }
        } else {
            output_inserter.push_stream_value_id(sv_id, 1, true, false);
        }
        join.active_stream_value_appended = false;
    } else if let Some(err) = join.current_group_error.take() {
        output_inserter.push_error(err, 1, true, false);
    } else if len < INLINE_STR_MAX_LEN {
        output_inserter.push_inline_maybe_text_ref(
            join.buffer.as_ref(),
            1,
            true,
            false,
        );
        join.buffer.clear();
    } else {
        let buffer =
            std::mem::replace(&mut join.buffer, MaybeText::with_capacity(len));
        output_inserter.push_maybe_text(buffer, 1, true, false);
    }
    reset_group_stats(join);
    *groups_emitted += usize::from(emitted);
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

pub fn handle_tf_join<'a>(
    jd: &mut JobData<'a>,
    tf_id: TransformId,
    join: &mut TfJoin<'a>,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let tf = &jd.tf_mgr.transforms[tf_id];
    let ms_id = tf.match_set_id;
    let op_id = tf.op_id.unwrap();
    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();

    let mut output_inserter = output_field.iter_hall.varying_type_inserter();
    let input_field_id = tf.input_field;
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
    let base_iter =
        jd.field_mgr
            .lookup_iter(input_field_id, &input_field, join.iter_id);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut iter =
        AutoDerefIter::new(&jd.field_mgr, input_field_id, base_iter);

    let mut desired_group_len_rem =
        join.group_capacity.unwrap_or(usize::MAX) - join.curr_group_len;
    let mut groups_emitted = 0;
    let sv_mgr = &mut jd.sv_mgr;
    let mut batch_size_rem = batch_size;
    let mut last_group_end = field_pos_start;
    let mut prebuffered_record = join.first_record_added;
    let mut string_store = LazyRwLockGuard::new(&jd.session_data.string_store);

    let mut groups_iter = jd.group_tracker.lookup_group_list_iter_mut(
        join.group_list_iter_ref.list_id,
        join.group_list_iter_ref.iter_id,
        &jd.match_set_mgr.match_sets[ms_id].action_buffer,
        join.actor_id,
    );

    'iter: loop {
        if join.current_group_error.is_some() || join.stream_value_error {
            let consumed = iter.next_n_fields(
                desired_group_len_rem
                    .min(batch_size_rem)
                    .min(groups_iter.group_len_rem()),
            );
            groups_iter.next_n_fields(consumed);
            desired_group_len_rem -= consumed;
            join.curr_group_len += consumed;
            batch_size_rem -= consumed;
        }
        let end_of_group = groups_iter.is_end_of_group(ps.input_done);
        if end_of_group || desired_group_len_rem == 0 {
            let field_pos = iter.get_next_field_pos();
            let should_drop =
                desired_group_len_rem != 0 && should_drop_group(join);
            let drop_count = (field_pos - last_group_end
                + usize::from(prebuffered_record))
            .saturating_sub(1);
            groups_iter.drop_before(
                groups_iter.field_pos() - drop_count,
                drop_count.saturating_sub(usize::from(!should_drop)),
            );
            prebuffered_record = false;
            if join.curr_group_len == 0 && !should_drop {
                groups_iter.insert_fields(FieldValueRepr::Undefined, 1);
            }
            last_group_end = field_pos;
            if should_drop {
                drop_group(join, sv_mgr, tf_id);
            } else {
                emit_group(
                    join,
                    sv_mgr,
                    &mut jd.tf_mgr,
                    tf_id,
                    &mut output_inserter,
                    &mut groups_emitted,
                );
            }

            if end_of_group {
                loop {
                    if !groups_iter.try_next_group() {
                        break;
                    }
                    if !groups_iter.is_end_of_group(ps.input_done) {
                        break;
                    }
                    groups_iter.insert_fields(FieldValueRepr::Undefined, 1);
                }
            }
            desired_group_len_rem = join.group_capacity.unwrap_or(usize::MAX);
        }

        if batch_size_rem != 0
            && join.curr_group_len == 0
            && join.group_capacity.unwrap_or(1) == 1
            && groups_iter.group_len_rem() == 1
        {
            // optimized case for groups of length 1,
            // where we can just copy straight to output without any buffering
            // or separators

            let count = groups_iter
                .skip_single_elem_groups(ps.input_done, batch_size_rem);
            let mut rem = count;
            while rem > 0 {
                let range = iter
                    .typed_range_fwd(
                        &jd.match_set_mgr,
                        rem,
                        field_value_flags::DEFAULT,
                    )
                    .unwrap();
                rem -= range.base.field_count;
                output_inserter
                    .extend_from_ref_aware_range_stringified_smart_ref(
                        &jd.field_mgr,
                        &jd.match_set_mgr,
                        sv_mgr,
                        &mut string_store,
                        range,
                        true,
                        true,
                        true,
                        join.input_field_ref_offset,
                        true, // TODO: configurable
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
                .min(groups_iter.group_len_rem()),
            field_value_flags::DEFAULT,
        ) else {
            break;
        };
        match range.base.data {
            FieldValueSlice::TextInline(text) => {
                for (v, rl, _offs) in
                    RefAwareInlineTextIter::from_range(&range, text)
                {
                    push_join_data(join, sv_mgr, MaybeTextCow::TextRef(v), rl);
                }
            }
            FieldValueSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    push_join_data(
                        join,
                        sv_mgr,
                        MaybeTextCow::BytesRef(v),
                        rl,
                    );
                }
            }
            FieldValueSlice::TextBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareTextBufferIter::from_range(&range, bytes)
                {
                    push_join_data(join, sv_mgr, MaybeTextCow::TextRef(v), rl);
                }
            }
            FieldValueSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    push_join_data(
                        join,
                        sv_mgr,
                        MaybeTextCow::BytesRef(v),
                        rl,
                    );
                }
            }
            FieldValueSlice::Int(ints) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, ints) {
                    let v = i64_to_str(false, *v);
                    push_join_data(
                        join,
                        sv_mgr,
                        MaybeTextCow::TextRef(&v),
                        rl,
                    );
                }
            }
            FieldValueSlice::Custom(custom_data) => {
                for (v, rl) in RefAwareFieldValueSliceIter::from_range(
                    &range,
                    custom_data,
                ) {
                    push_custom_type(op_id, join, sv_mgr, &**v, rl);
                }
            }
            FieldValueSlice::Error(errs) => {
                push_error(join, sv_mgr, errs[0].clone());
            }
            FieldValueSlice::Null(_) | FieldValueSlice::Undefined(_) => {
                let str = typed_slice_zst_str(&range.base.data);
                push_error(
                    join,
                    sv_mgr,
                    OperatorApplicationError::new_s(
                        format!("join does not support {str}"),
                        op_id,
                    ),
                );
            }
            FieldValueSlice::StreamValueId(svs) => {
                if try_consume_stream_values(tf_id, join, sv_mgr, &range, svs)
                    .is_err()
                {
                    break 'iter;
                }
            }
            FieldValueSlice::BigInt(_)
            | FieldValueSlice::Float(_)
            | FieldValueSlice::Rational(_) => {
                todo!();
            }
            FieldValueSlice::Object(_) => {
                todo!();
            }
            FieldValueSlice::Array(_) => {
                todo!();
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        }
        let fc = range.base.field_count;
        join.curr_group_len += fc;
        desired_group_len_rem -= fc;
        batch_size_rem -= fc;
        groups_iter.next_n_fields(fc);
    }

    jd.field_mgr.store_iter(input_field_id, join.iter_id, iter);
    groups_iter.store_iter(join.group_list_iter_ref.iter_id);

    drop(input_field);

    if let Some(sv_id) = join.active_stream_value {
        if !join.active_stream_value_submitted {
            join.active_stream_value_submitted = true;
            output_inserter.push_stream_value_id(sv_id, 1, true, false);
            groups_emitted += 1;
            join.active_stream_value_appended = false;
        }
        if join.active_stream_value_appended {
            sv_mgr.inform_stream_value_subscribers(sv_id);
            join.active_stream_value_appended = false;
        }
        if let Some(gbi) = join.active_group_batch {
            if join.active_group_batch_appended {
                join.active_group_batch_appended = false;
                if !join.group_batches[gbi].has_leading_streaming_input_sv {
                    jd.tf_mgr.make_stream_producer(tf_id);
                    make_group_batch_producer(join, gbi);
                }
            }
        }
    }

    if ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr.submit_batch(tf_id, groups_emitted, ps.input_done);
}

fn try_consume_stream_values<'a>(
    tf_id: TransformId,
    join: &mut TfJoin<'a>,
    sv_mgr: &mut StreamValueManager<'a>,
    range: &RefAwareTypedRange<'_>,
    svs: &[StreamValueId],
) -> Result<(), ()> {
    let sv_iter = FieldValueSliceIter::from_range(range, svs);
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
    sv_id: usize,
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
}

fn push_custom_type<'a>(
    op_id: OperatorId,
    join: &mut TfJoin<'a>,
    sv_mgr: &mut StreamValueManager<'a>,
    value: &dyn CustomData,
    rl: RunLength,
) {
    // PERF: we could implement this better
    let mut res = MaybeText::default();
    if let Err(e) = value.format_raw(&mut res, &RealizedFormatKey::default()) {
        push_error(
            join,
            sv_mgr,
            OperatorApplicationError::new_s(e.to_string(), op_id),
        );
    }
    push_join_data(join, sv_mgr, MaybeTextCow::from_maybe_text(res), rl)
}

pub fn handle_tf_join_stream_value_update(
    jd: &mut JobData,
    join: &mut TfJoin,
    update: StreamValueUpdate,
) {
    let group_batch_id = GroupBatchId::try_from(update.custom).unwrap();
    let in_sv_id = update.sv_id;
    let gb = &mut join.group_batches[group_batch_id];
    let out_sv_id = gb.output_stream_value;

    let stream_buffer_size = jd
        .get_transform_chain(update.tf_id)
        .settings
        .stream_buffer_size;

    let (in_sv, out_sv) = jd
        .sv_mgr
        .stream_values
        .two_distinct_mut(in_sv_id, out_sv_id);

    if out_sv.propagate_error(&in_sv.error) {
        if let Some(idx) = join
            .producing_batches
            .iter()
            .position(|gbi| *gbi == group_batch_id)
        {
            debug_assert!(gb.is_producer);
            gb.is_producer = false;
            join.producing_batches.swap_remove(idx);
        }
        drop_group_batch_sv_subscriptions(
            &mut jd.sv_mgr,
            update.tf_id,
            join,
            group_batch_id,
        );
        join.group_batches.release(group_batch_id);
        jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
        return;
    }

    let Some(GroupBatchEntry {
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

    if *front_id != in_sv_id {
        return;
    }

    debug_assert!(gb.has_leading_streaming_input_sv);

    if *streaming_emit {
        debug_assert!(!out_sv.done);

        let mut inserter =
            out_sv.data_inserter(out_sv_id, stream_buffer_size, true);
        inserter.extend_from_cursor(&mut in_sv.data_cursor(
            update.data_offset,
            update.may_consume_data && *group_batch_run_len == 1,
        ));
    }

    if in_sv.done {
        gb.has_leading_streaming_input_sv = false;
        if gb.outstanding_values.is_empty() {
            if Some(group_batch_id) != join.active_group_batch {
                out_sv.mark_done();
                join.group_batches.release(group_batch_id);
            }
        } else {
            make_group_batch_producer(join, group_batch_id);
            jd.tf_mgr.make_stream_producer(update.tf_id);
        }
        jd.sv_mgr
            .drop_field_value_subscription(in_sv_id, Some(update.tf_id));
    }
    jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
}

pub fn handle_tf_join_stream_producer_update<'a>(
    jd: &mut JobData<'a>,
    join: &mut TfJoin<'a>,
    tf_id: TransformId,
) {
    let mut make_producer = false;
    for i in 0..join.producing_batches.len() {
        handle_group_batch_producer_update(
            jd,
            tf_id,
            join,
            join.producing_batches[i],
            &mut make_producer,
        );
    }
    if make_producer {
        jd.tf_mgr.make_stream_producer(tf_id);
    }
    let mut gb_producer_idx = join.producing_batches.len().saturating_sub(1);
    while gb_producer_idx > 0 && !join.producing_batches.is_empty() {
        let gbi = join.producing_batches[gb_producer_idx];
        let gb = &mut join.group_batches[gbi];
        if gb.outstanding_values.is_empty()
            && join.active_group_batch != Some(gbi)
        {
            jd.sv_mgr.stream_values[gb.output_stream_value].mark_done();
            jd.sv_mgr
                .inform_stream_value_subscribers(gb.output_stream_value);
            join.group_batches.release(gbi);
            join.producing_batches.swap_remove(gb_producer_idx);
            continue;
        }
        gb_producer_idx -= 1;
    }
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

    let mut streams_handout = jd.sv_mgr.stream_values.multi_ref_handout();

    let out_sv = streams_handout.claim(out_sv_id);

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
                let sv = streams_handout.claim(*id);
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
                    streams_handout.release(*id);
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

    if gb.outstanding_values.is_empty() && join.active_group_batch != Some(gbi)
    {
        out_sv.mark_done();
    }
    jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
}
