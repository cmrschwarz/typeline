use bstr::ByteSlice;
use regex::Regex;
use smallstr::SmallString;
use smallvec::SmallVec;

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
        field_value::{FieldValue, FieldValueKind},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueSliceIter,
        formattable::RealizedFormatKey,
        group_tracker::GroupListIterRef,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        match_set::MatchSetManager,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueSliceIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
            RefAwareTextBufferIter, RefAwareTypedRange,
        },
        stream_value::{StreamValue, StreamValueId, StreamValueManager},
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        int_string_conversions::{i64_to_str, usize_to_str},
        io::PointerWriter,
        text_write::{MaybeTextWriteFlaggedAdapter, TextWriteIoAdapter},
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
    separator: Option<Box<[u8]>>,
    separator_is_valid_utf8: bool,
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

struct GroupBatch {
    output_stream_value: StreamValueId,
    pending_stream_value: Option<StreamValueId>,
    outstanding_values: Vec<(FieldValue, RunLength)>,
}

type GroupBatchId = DebuggableNonMaxUsize;

pub struct TfJoin<'a> {
    separator: Option<&'a [u8]>,
    separator_is_valid_utf8: bool,
    group_capacity: Option<usize>,
    drop_incomplete: bool,
    stream_len_threshold: usize,
    input_field_ref_offset: FieldRefOffset,

    group_list_iter_ref: GroupListIterRef,
    iter_id: IterId,
    actor_id: ActorId,

    first_record_added: bool,
    buffer_is_valid_utf8: bool,
    buffer: Vec<u8>,

    stream_value_error: bool,
    current_group_error: Option<OperatorApplicationError>,

    curr_group_len: usize,

    active_stream_value: Option<StreamValueId>,

    active_group_batch: Option<GroupBatchId>,
    group_batches: Universe<GroupBatchId, GroupBatch>,
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
        value.map(<[_]>::to_owned),
        insert_count,
        drop_incomplete,
    ))
}

pub fn build_tf_join<'a>(
    jd: &mut JobData,
    op_base: &OperatorBase,
    op: &'a OpJoin,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let input_field_ref_offset = jd
        .field_mgr
        .register_field_reference(tf_state.output_field, tf_state.input_field);

    TransformData::Join(TfJoin {
        separator: op.separator.as_deref(),
        separator_is_valid_utf8: op.separator_is_valid_utf8,
        group_capacity: op.join_count,
        drop_incomplete: op.drop_incomplete,
        stream_len_threshold: jd.session_data.chains
            [op_base.chain_id.unwrap() as usize]
            .settings
            .stream_size_threshold,
        input_field_ref_offset,
        group_list_iter_ref: jd.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .group_tracker
            .claim_group_list_iter_ref_for_active(),
        iter_id: jd.field_mgr.claim_iter(
            tf_state.input_field,
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
        ),
        actor_id: jd.add_actor_for_tf_state(tf_state),
        first_record_added: false,
        buffer_is_valid_utf8: true,
        buffer: Vec::new(),
        // TODO: add a separate setting for this
        stream_value_error: false,
        current_group_error: None,
        curr_group_len: 0,
        active_stream_value: None,
        active_group_batch: None,
        group_batches: Universe::default(),
    })
}

pub fn create_op_join(
    separator: Option<Vec<u8>>,
    join_count: Option<usize>,
    drop_incomplete: bool,
) -> OperatorData {
    let separator_is_valid_utf8 =
        separator.as_ref().map_or(true, |v| v.to_str().is_ok());
    let sep = separator.map(Vec::into_boxed_slice);
    OperatorData::Join(OpJoin {
        separator: sep,
        separator_is_valid_utf8,
        join_count,
        drop_incomplete,
    })
}
pub fn create_op_join_str(separator: &str, join_count: usize) -> OperatorData {
    let separator_is_valid_utf8 = true;
    let sep = match separator {
        "" => None,
        v => Some(v.as_bytes().to_owned().into_boxed_slice()),
    };
    OperatorData::Join(OpJoin {
        separator: sep,
        separator_is_valid_utf8,
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
        std::mem::replace(&mut join.buffer, Vec::with_capacity(cap))
    } else {
        Vec::new()
    };
    let sv_id = sv_mgr.stream_values.claim_with_value(StreamValue {
        value: if join.buffer_is_valid_utf8 {
            FieldValue::Text(unsafe { String::from_utf8_unchecked(buf) })
        } else {
            FieldValue::Bytes(buf)
        },
        is_buffered: false,
        done: false,
        subscribers: SmallVec::new(),
        ref_count: 1,
    });
    join.active_stream_value = Some(sv_id);
    sv_id
}

fn claim_group_batch(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
) -> GroupBatchId {
    debug_assert!(join.active_group_batch.is_none());
    let output_stream_value = if let Some(sv_id) = join.active_stream_value {
        sv_id
    } else {
        claim_stream_value(join, sv_mgr)
    };
    // PERF: we should reuse these..
    let gbi = join.group_batches.claim_with_value(GroupBatch {
        output_stream_value,
        pending_stream_value: None,
        outstanding_values: Vec::new(),
    });
    join.active_group_batch = Some(gbi);
    gbi
}

unsafe fn get_join_buffer<'a>(
    join: &'a mut TfJoin,
    sv_mgr: &'a mut StreamValueManager,
    expected_len: usize,
    expect_utf8: bool,
) -> &'a mut Vec<u8> {
    join.buffer_is_valid_utf8 &= expect_utf8;
    if join.active_stream_value.is_none()
        && join.buffer.len() + expected_len > join.stream_len_threshold
    {
        claim_stream_value(join, sv_mgr);
    }

    if let Some(sv_id) = join.active_stream_value {
        let value = &mut sv_mgr.stream_values[sv_id].value;

        if !expect_utf8 {
            if let FieldValue::Text(txt) = value {
                let buf = std::mem::take(txt).into_bytes();
                *value = FieldValue::Bytes(buf);
                let FieldValue::Bytes(bb) = value else {
                    unreachable!()
                };
                return bb;
            }
        }
        match value {
            FieldValue::Bytes(bb) => bb,
            FieldValue::Text(txt) => unsafe { txt.as_mut_vec() },
            _ => unreachable!(),
        }
    } else {
        &mut join.buffer
    }
}
fn update_valid_utf8(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    valid_utf8: bool,
) {
    if !join.buffer_is_valid_utf8 || valid_utf8 {
        return;
    }
    join.buffer_is_valid_utf8 = false;
    let Some(sv_id) = join.active_stream_value else {
        return;
    };

    let value = &mut sv_mgr.stream_values[sv_id].value;

    if let FieldValue::Text(txt) = value {
        let buf = std::mem::take(txt).into_bytes();
        *value = FieldValue::Bytes(buf);
    }
}
unsafe fn push_bytes_raw(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &[u8],
    mut rl: usize,
    expect_utf8: bool,
) {
    let first_record_added = join.first_record_added;
    join.first_record_added = true;
    let sep = join.separator;
    let buf = unsafe {
        get_join_buffer(
            join,
            sv_mgr,
            (data.len() + join.separator.map_or(0, <[_]>::len)) * rl,
            expect_utf8,
        )
    };
    if let Some(sep) = sep {
        if !first_record_added {
            buf.extend_from_slice(data);
            rl = rl.saturating_sub(1);
        }
        for _ in 0..rl {
            buf.extend_from_slice(sep);
            buf.extend_from_slice(data);
        }
    } else {
        for _ in 0..rl {
            buf.extend_from_slice(data);
        }
    }
}
pub fn push_bytes(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &[u8],
    rl: usize,
) {
    unsafe { push_bytes_raw(join, sv_mgr, data, rl, false) };
}
pub fn push_bytes_known_string(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &[u8],
    rl: usize,
) {
    unsafe { push_bytes_raw(join, sv_mgr, data, rl, true) };
}
pub fn push_str(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &str,
    rl: usize,
) {
    unsafe { push_bytes_raw(join, sv_mgr, data.as_bytes(), rl, true) };
}
pub fn drop_group(join: &mut TfJoin) {
    debug_assert!(join.active_stream_value.is_none());
    join.curr_group_len = 0;
    join.first_record_added = false;
    join.buffer.clear();
    join.current_group_error = None;
}
pub fn emit_group(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    output_inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    let len = join.buffer.len();
    let valid_utf8 = join.buffer_is_valid_utf8 && join.separator_is_valid_utf8;
    if let Some(sv_id) = join.active_stream_value {
        // TODO:
        join.active_stream_value = None;
        if let Some(gb_id) = join.active_group_batch {
            join.active_group_batch = None;
        }
    } else if let Some(err) = join.current_group_error.take() {
        output_inserter.push_error(err, 1, true, false);
    } else if len < INLINE_STR_MAX_LEN {
        if valid_utf8 {
            output_inserter.push_inline_str(
                unsafe { std::str::from_utf8_unchecked(&join.buffer) },
                1,
                true,
                false,
            );
        } else {
            output_inserter.push_inline_bytes(&join.buffer, 1, true, false);
        }
        join.buffer.clear();
    } else {
        let buffer =
            std::mem::replace(&mut join.buffer, Vec::with_capacity(len));
        if valid_utf8 {
            output_inserter.push_string(
                unsafe { String::from_utf8_unchecked(buffer) },
                1,
                true,
                false,
            );
        } else {
            output_inserter.push_bytes_buffer(buffer, 1, true, false);
        }
    }
    drop_group(join);
}
fn push_error(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    e: OperatorApplicationError,
) {
    if let Some(sv_id) = join.active_stream_value {
        let sv = &mut sv_mgr.stream_values[sv_id];
        sv.value = FieldValue::Error(e);
        sv.done = true;
        join.stream_value_error = true;
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

pub fn handle_tf_join(
    jd: &mut JobData,
    tf_id: TransformId,
    join: &mut TfJoin,
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
    let mut string_store_ref = None;

    let ms = &jd.match_set_mgr.match_sets[ms_id];
    let mut ab = ms.action_buffer.borrow_mut();

    let mut groups_iter = ms.group_tracker.lookup_group_list_iter_mut(
        join.group_list_iter_ref.list_id,
        join.group_list_iter_ref.iter_id,
        &mut ab,
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
        }
        let end_of_group = groups_iter.is_end_of_group(ps.input_done);
        if end_of_group || desired_group_len_rem == 0 {
            let field_pos = iter.get_next_field_pos();
            let should_drop =
                desired_group_len_rem != 0 && should_drop_group(join);
            let drop_count =
                field_pos - last_group_end + usize::from(prebuffered_record);
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
                drop_group(join);
            } else {
                emit_group(join, sv_mgr, &mut output_inserter);
                groups_emitted += 1;
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

        if join.curr_group_len == 0 {
            // optimized case for groups of length 1,
            // where we can just copy straight to output without any buffering
            // or separators
            if batch_size_rem == 0 {
                break;
            }
            let target_group_len = join
                .group_capacity
                .unwrap_or(2)
                .min(groups_iter.group_len_rem());
            if target_group_len == 1 {
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
                            op_id,
                            &jd.session_data.string_store,
                            &mut string_store_ref,
                            range,
                            true,
                            true,
                            true,
                            join.input_field_ref_offset,
                            true, // TODO: configurable
                        );
                }
                batch_size_rem -= count;
                groups_emitted += count;
                last_group_end = iter.get_next_field_pos();
            }
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
                    push_str(join, sv_mgr, v, rl as usize);
                }
            }
            FieldValueSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    push_bytes(join, sv_mgr, v, rl as usize);
                }
            }
            FieldValueSlice::TextBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareTextBufferIter::from_range(&range, bytes)
                {
                    push_str(join, sv_mgr, v, rl as usize);
                }
            }
            FieldValueSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    push_bytes(join, sv_mgr, v, rl as usize);
                }
            }
            FieldValueSlice::Int(ints) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, ints) {
                    let v = i64_to_str(false, *v);
                    push_str(join, sv_mgr, v.as_str(), rl as usize);
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
                if !try_consume_stream_values(
                    tf_id,
                    join,
                    &mut jd.tf_mgr,
                    &jd.match_set_mgr,
                    sv_mgr,
                    &mut iter,
                    &range,
                    svs,
                ) {
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

    if ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr.submit_batch(tf_id, groups_emitted, ps.input_done);
}

fn try_consume_stream_values<'a>(
    tf_id: TransformId,
    join: &mut TfJoin<'_>,
    tf_mgr: &mut TransformManager,
    msm: &MatchSetManager,
    sv_mgr: &mut StreamValueManager,
    iter: &mut AutoDerefIter<'a, impl FieldIterator<'a>>,
    range: &RefAwareTypedRange<'_>,
    svs: &[StreamValueId],
) -> bool {
    let sv_iter = RefAwareStreamValueIter::from_range(range, svs);
    for (sv_id, offsets, rl) in sv_iter {
        let sv = &mut sv_mgr.stream_values[sv_id];
        match &sv.value {
            FieldValue::Error(err) => {
                let ec = err.clone();
                if let Some(sv_id) = join.active_stream_value {
                    let sv = &mut sv_mgr.stream_values[sv_id];
                    // otherwise this group would have been skipped
                    debug_assert!(!sv.value.is_error());
                    sv.value = FieldValue::Error(ec);
                    sv.done = true;
                    join.stream_value_error = true;
                } else {
                    join.current_group_error = Some(ec);
                }
                return true;
            }
            FieldValue::Bytes(_) | FieldValue::Text(_) => {
                if !sv.done {
                    if let Some(gbi) = join.active_group_batch {
                        let gb = &mut join.group_batches[gbi];
                        if gb.pending_stream_value.is_some() {
                            sv.is_buffered = true;
                            if offsets.is_some() {
                                todo!("spot references");
                            }
                            gb.outstanding_values
                                .push((FieldValue::StreamValueId(sv_id), rl))
                        }
                    } else {
                        let gbi = claim_group_batch(join, sv_mgr);
                        let gb = &mut join.group_batches[gbi];
                        gb.pending_stream_value = Some(sv_id);
                    };
                    continue;
                }
                // SAFETY: this is a buffer on the heap so it
                // will not be affected
                // if the stream values vec is resized in case
                // push_bytes_raw decides
                // to alloc a stream value
                // we have to free this livetime from the
                // sv_mgr here so we can access
                // our (guaranteed to be distinct) target
                // stream value to push data
                assert!(Some(sv_id) != join.active_stream_value);
                let bytes_are_utf8 = sv.value.kind() == FieldValueKind::Text;
                let buf = sv.value.as_ref().as_slice().as_bytes();
                let buf_sliced = offsets.map_or(buf, |o| &buf[o]);
                let buf_laundered = unsafe {
                    std::mem::transmute::<&'_ [u8], &'static [u8]>(buf_sliced)
                };
                unsafe {
                    push_bytes_raw(
                        join,
                        sv_mgr,
                        buf_laundered,
                        rl as usize,
                        bytes_are_utf8,
                    );
                }

                return false;
            }
            _ => todo!(),
        }
    }
    true
}

unsafe fn write_custom_data(
    ptr: &mut *mut u8,
    len: usize,
    v: &dyn CustomData,
    rfk: &RealizedFormatKey,
    valid_utf8: &mut bool,
) {
    const ERR_MSG: &str = "custom stringify failed";
    const LEN_ERR_MSG: &str = "custom type lied about it's `formatted_len`";
    let mut w =
        MaybeTextWriteFlaggedAdapter::new(TextWriteIoAdapter(unsafe {
            PointerWriter::new(*ptr, len)
        }));
    v.format_raw(&mut w, rfk).expect(ERR_MSG);
    *valid_utf8 = w.is_utf8();
    assert!(w.into_inner().remaining_bytes() == 0, "{LEN_ERR_MSG}");
    *ptr = unsafe { ptr.add(len) };
}

fn push_custom_type(
    op_id: OperatorId,
    join: &mut TfJoin<'_>,
    sv_mgr: &mut StreamValueManager,
    value: &dyn CustomData,
    rl: u32,
) {
    let len = match value.formatted_len(&RealizedFormatKey::default()) {
        Ok(len) => len,
        Err(e) => {
            push_error(
                join,
                sv_mgr,
                OperatorApplicationError::new_s(
                    format!(
                        "cannot stringify custom type {}: '{e}'",
                        value.type_name()
                    ),
                    op_id,
                ),
            );
            return;
        }
    };
    let first_record_added = join.first_record_added;
    join.first_record_added = true;
    let sep = join.separator;
    let sep_len = sep.map_or(0, <[_]>::len);
    let target_len = (len + sep_len) * rl as usize;
    let buf = unsafe { get_join_buffer(join, sv_mgr, target_len, true) };
    let mut valid_utf8 = true;
    let start_len = buf.len();
    buf.reserve(target_len);

    let rfk = RealizedFormatKey::default();
    unsafe {
        let start_ptr = buf.as_mut_ptr().add(start_len);
        if let Some(sep) = sep {
            let mut first_target_ptr = start_ptr;
            let mut ptr = start_ptr;
            if first_record_added {
                std::ptr::copy_nonoverlapping(sep.as_ptr(), ptr, sep_len);
                ptr = ptr.add(sep_len);
                first_target_ptr = ptr;
                write_custom_data(&mut ptr, len, value, &rfk, &mut valid_utf8);
            } else {
                write_custom_data(&mut ptr, len, value, &rfk, &mut valid_utf8);
            }

            for _ in 1..rl {
                std::ptr::copy_nonoverlapping(sep.as_ptr(), ptr, sep_len);
                ptr = ptr.add(sep_len);
                std::ptr::copy_nonoverlapping(first_target_ptr, ptr, sep_len);
                ptr = ptr.add(len);
            }
        } else {
            let mut ptr = start_ptr;
            write_custom_data(&mut ptr, len, value, &rfk, &mut valid_utf8);
            for _ in 1..rl {
                ptr = ptr.add(len);
                std::ptr::copy_nonoverlapping(start_ptr, ptr, len);
            }
        }
        buf.set_len(start_len + target_len);
    }
    update_valid_utf8(join, sv_mgr, valid_utf8);
}

pub fn handle_tf_join_stream_value_update(
    jd: &mut JobData,
    tf_id: TransformId,
    join: &mut TfJoin,
    sv_id: StreamValueId,
    custom: usize,
) {
    let group_batch_id = GroupBatchId::try_from(custom).unwrap();
    let gb = &mut join.group_batches[group_batch_id];
    debug_assert!(gb.pending_stream_value == Some(sv_id));
    let (in_sv, out_sv) = jd
        .sv_mgr
        .stream_values
        .two_distinct_mut(sv_id, gb.output_stream_value);

    let mut done = in_sv.done;

    match &in_sv.value {
        FieldValue::Undefined => todo!(),
        FieldValue::Null => todo!(),
        FieldValue::Int(_) => todo!(),
        FieldValue::BigInt(_) => todo!(),
        FieldValue::Float(_) => todo!(),
        FieldValue::Rational(_) => todo!(),
        FieldValue::Text(_) => todo!(),
        FieldValue::Bytes(_) => todo!(),
        FieldValue::Array(_) => todo!(),
        FieldValue::Object(_) => todo!(),
        FieldValue::Custom(_) => todo!(),
        FieldValue::Error(e) => {
            out_sv.value = FieldValue::Error(e.clone());
            done = true;
        }
        FieldValue::StreamValueId(_) => {
            // TODO: maybe support this? could be useful for `join` itself
            todo!("nested stream value")
        }
        FieldValue::FieldReference(_)
        | FieldValue::SlicedFieldReference(_) => unreachable!(),
    }

    if done {
        out_sv.done = true;
        jd.sv_mgr.drop_field_value_subscription(sv_id, Some(tf_id));
    }
    jd.sv_mgr
        .inform_stream_value_subscribers(gb.output_stream_value);
}
