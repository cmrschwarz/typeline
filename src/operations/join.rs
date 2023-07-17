use bstr::{BStr, ByteSlice, ByteVec};
use regex::Regex;
use smallstr::SmallString;

use crate::{
    field_data::{
        field_value_flags, iter_hall::IterId, iters::FieldIterator, push_interface::PushInterface,
        typed::TypedSlice, typed_iters::TypedSliceIter, FieldValueKind, INLINE_STR_MAX_LEN,
    },
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
        RefAwareStreamValueIter,
    },
    stream_value::{StreamValueData, StreamValueId},
    utils::{i64_to_str, usize_to_str},
    worker_thread_session::{Field, JobSession, RecordManager},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorBase, OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    print::{NULL_STR, SUCCESS_STR, UNSET_STR},
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
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        let mut small_str = SmallString::new();
        small_str.push_str("join");
        small_str.push_str(
            self.join_count
                .map(|v| usize_to_str(v))
                .unwrap_or_default()
                .as_str(),
        );
        small_str
    }
}

pub struct TfJoin<'a> {
    current_stream_val: Option<StreamValueId>,
    stream_val_added_len: usize,
    separator: Option<&'a [u8]>,
    separator_is_valid_utf8: bool,
    iter_id: IterId,
    buffer: Vec<u8>,
    buffer_is_valid_utf8: bool,
    first_record_added: bool,
    group_len: usize,
    group_capacity: Option<usize>,
    current_group_contains_errors: bool,
    drop_incomplete: bool,
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^join(?<drop_incomplete>d)?(?<insert_count>[0-9]+)?$").unwrap();
}
pub fn argument_matches_op_join(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_join(
    argument: &str,
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let args = ARG_REGEX.captures(&argument).ok_or_else(|| {
        OperatorCreationError::new("invalid argument syntax for data inserter", arg_idx)
    })?;
    let insert_count = args
        .name("insert_count")
        .map(|ic| {
            ic.as_str().parse::<usize>().map_err(|_| {
                OperatorCreationError::new("failed to parse insertion count as integer", arg_idx)
            })
        })
        .transpose()?;
    let drop_incomplete = args.name("drop_incomplete").is_some();
    Ok(create_op_join(
        value.map(|v| v.as_bytes().to_owned()),
        insert_count,
        drop_incomplete,
    ))
}

pub fn setup_tf_join<'a>(
    sess: &mut JobSession,
    _op_base: &OperatorBase,
    op: &'a OpJoin,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
    TransformData::Join(TfJoin {
        current_stream_val: None,
        stream_val_added_len: 0,
        separator: op.separator.as_deref(),
        separator_is_valid_utf8: op.separator_is_valid_utf8,
        iter_id: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
        buffer: Vec::new(),
        buffer_is_valid_utf8: true,
        first_record_added: false,
        group_len: 0,
        group_capacity: op.join_count,
        current_group_contains_errors: false,
        drop_incomplete: op.drop_incomplete,
    })
}

pub fn create_op_join(
    separator: Option<Vec<u8>>,
    join_count: Option<usize>,
    drop_incomplete: bool,
) -> OperatorData {
    let separator_is_valid_utf8 = separator
        .as_ref()
        .map(|v| std::str::from_utf8(v.as_bytes()).is_ok())
        .unwrap_or(true);
    let sep = match separator {
        Some(v) => Some(v.into_boxed_slice()),
        None => None,
    };
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
pub fn push_str(join: &mut TfJoin, data: &str, rl: usize) {
    push_bytes_raw(join, data.as_bytes(), rl);
}
pub fn push_bytes_raw(join: &mut TfJoin, data: &[u8], mut rl: usize) {
    if let Some(sep) = join.separator {
        if !join.first_record_added {
            join.buffer.push_str(data.as_bytes());
            rl = rl.saturating_sub(1);
            join.first_record_added = true;
        }
        for _ in 0..rl {
            join.buffer.push_str(sep);
            join.buffer.push_str(data.as_bytes());
        }
    } else {
        for _ in 0..rl {
            join.buffer.push_str(data.as_bytes());
        }
    }
}
pub fn push_bytes(join: &mut TfJoin, data: &[u8], rl: usize) {
    join.buffer_is_valid_utf8 = false;
    push_bytes_raw(join, data, rl);
}
pub fn push_bytes_known_string(join: &mut TfJoin, data: &[u8], rl: usize) {
    push_bytes_raw(join, data, rl);
}
pub fn emit_group(join: &mut TfJoin, output_field: &mut Field) {
    let len = join.buffer.len();
    if join.buffer_is_valid_utf8 && join.separator_is_valid_utf8 {
        if len < INLINE_STR_MAX_LEN {
            output_field.field_data.push_inline_str(
                unsafe { std::str::from_utf8_unchecked(&join.buffer) },
                1,
                true,
                false,
            );
            join.buffer.clear();
        } else {
            let buffer = std::mem::replace(&mut join.buffer, Vec::with_capacity(len));
            output_field.field_data.push_string(
                unsafe { String::from_utf8_unchecked(buffer) },
                1,
                true,
                false,
            );
        }
    } else {
        if len < INLINE_STR_MAX_LEN {
            output_field
                .field_data
                .push_inline_bytes(&join.buffer, 1, true, false);
            join.buffer.clear();
        } else {
            let buffer = std::mem::replace(&mut join.buffer, Vec::with_capacity(len));
            output_field
                .field_data
                .push_bytes_buffer(buffer, 1, true, false);
        }
    }
    join.group_len = 0;
    join.first_record_added = false;
}
pub fn handle_tf_join(sess: &mut JobSession<'_>, tf_id: TransformId, join: &mut TfJoin) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let output_field_id = tf.output_field;
    let input_field_id = tf.input_field;
    sess.prepare_for_output(tf_id, &[output_field_id]);
    let input_field = RecordManager::borrow_field_cow(&sess.record_mgr.fields, input_field_id);
    let mut output_field = sess.record_mgr.fields[output_field_id].borrow_mut();
    let base_iter = RecordManager::get_iter_cow_aware(
        &sess.record_mgr.fields,
        input_field_id,
        &input_field,
        join.iter_id,
    )
    .bounded(0, batch_size);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut field_pos = field_pos_start;
    let mut iter = AutoDerefIter::new(&sess.record_mgr.fields, input_field_id, base_iter);

    let mut group_len_rem = join.group_capacity.unwrap_or(usize::MAX) - join.group_len;
    let mut groups_emitted = 0;
    'iter: while let Some(range) = iter.typed_range_fwd(
        &mut sess.record_mgr.match_sets,
        group_len_rem,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    push_str(join, v, rl as usize);
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    push_bytes(join, v, rl as usize);
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    push_bytes(join, v, rl as usize);
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let v = i64_to_str(false, *v);
                    push_str(join, v.as_str(), rl as usize);
                }
            }
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::Null(_) => {
                push_str(join, NULL_STR, range.base.field_count);
            }
            TypedSlice::Error(errs) => {
                output_field
                    .field_data
                    .push_error(errs[0].clone(), 1, true, false);
                join.current_group_contains_errors = true;
            }
            TypedSlice::Unset(_) => {
                push_str(join, UNSET_STR, range.base.field_count);
            }
            TypedSlice::Success(_) => {
                push_str(join, SUCCESS_STR, range.base.field_count);
            }
            TypedSlice::StreamValueId(svs) => {
                let mut pos = field_pos;
                for (sv_id, offsets, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                    pos += rl as usize;
                    let sv = &mut sess.sv_mgr.stream_values[sv_id];
                    match &sv.data {
                        StreamValueData::Dropped => unreachable!(),
                        StreamValueData::Error(err) => {
                            output_field
                                .field_data
                                .push_error(err.clone(), 1, true, false);
                            join.current_group_contains_errors = true;
                            break;
                        }
                        StreamValueData::Bytes(b) => {
                            join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;
                            let b = offsets.as_ref().map(|o| &b[o.clone()]).unwrap_or(&b);
                            if sv.done {
                                push_bytes_raw(join, &b, rl as usize);
                            } else {
                                join.group_len += pos - field_pos;
                                iter.move_to_field_pos(pos);
                                join.current_stream_val = Some(sv_id);
                                if !sv.is_buffered() {
                                    if rl == 1 {
                                        push_bytes_raw(join, b, 1);
                                        debug_assert!(offsets.is_none());
                                        join.stream_val_added_len = b.len();
                                    } else {
                                        sv.promote_to_buffer();
                                    }
                                }
                                sv.subscribe(tf_id, rl as usize, sv.is_buffered());
                                break 'iter;
                            }
                        }
                    }
                }
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                todo!();
            }
        }
        let fc = range.base.field_count;
        join.group_len += fc;
        group_len_rem -= fc;
        field_pos += fc;
        if join.current_group_contains_errors {
            let batch_size_rem = field_pos - field_pos_start;
            let consumed = iter.next_n_fields(group_len_rem.min(batch_size_rem));
            group_len_rem -= consumed;
            if group_len_rem == 0 {
                join.current_group_contains_errors = false;
            } else {
                break;
            }
        }
        if group_len_rem == 0 {
            emit_group(join, &mut output_field);
            groups_emitted += 1;
        }
        group_len_rem = join.group_capacity.unwrap_or(usize::MAX) - join.group_len;
    }
    RecordManager::store_iter_cow_aware(
        &sess.record_mgr.fields,
        input_field_id,
        &input_field,
        join.iter_id,
        iter.into_base_iter(),
    );
    if input_done {
        if !join.drop_incomplete || (join.group_len > 0 && join.group_capacity.is_none()) {
            emit_group(join, &mut output_field);
            groups_emitted += 1;
        }
    }
    let streams_done = join.current_stream_val.is_none();
    drop(input_field);
    drop(output_field);

    if input_done && streams_done {
        sess.unlink_transform(tf_id, groups_emitted);
    } else {
        if streams_done {
            sess.tf_mgr.update_ready_state(tf_id);
        }
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, groups_emitted);
    }
}

pub fn handle_tf_join_stream_value_update(
    sess: &mut JobSession<'_>,
    tf_id: TransformId,
    join: &mut TfJoin,
    sv_id: StreamValueId,
    custom: usize,
) {
    let run_len = custom;
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_done = tf.input_is_done;
    let out_field_id = tf.output_field;
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    match &sv.data {
        StreamValueData::Dropped => unreachable!(),
        StreamValueData::Error(err) => {
            sess.record_mgr.fields[out_field_id]
                .borrow_mut()
                .field_data
                .push_error(err.clone(), 1, true, false);
            if Some(join.group_len) == join.group_capacity {
                join.group_len = 0;
            } else {
                join.current_group_contains_errors = true;
            }
            sess.tf_mgr.update_ready_state(tf_id);
        }
        StreamValueData::Bytes(b) => {
            join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;
            if sv.bytes_are_chunk {
                join.buffer.extend_from_slice(&b);
                join.stream_val_added_len += b.len();
            } else if sv.done {
                join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;
                if run_len == 1 {
                    join.buffer
                        .extend_from_slice(&b[join.stream_val_added_len..]);
                    join.stream_val_added_len = 0;
                } else {
                    push_bytes_raw(join, b, run_len);
                }
            }
            if sv.done {
                join.current_stream_val = None;
                if input_done {
                    sess.tf_mgr.push_tf_in_ready_queue(tf_id);
                } else {
                    sess.tf_mgr.update_ready_state(tf_id);
                }
                if Some(join.group_len) == join.group_capacity {
                    emit_group(
                        join,
                        &mut sess.record_mgr.fields[sess.tf_mgr.transforms[tf_id].output_field]
                            .borrow_mut(),
                    );
                }
            }
        }
    }
}
