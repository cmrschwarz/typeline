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
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::{i64_to_str, usize_to_str},
    worker_thread_session::{Field, JobSession, StreamValueManager},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
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
    output_stream_val: Option<StreamValueId>,
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
    stream_value_error: bool,
    current_group_error: Option<OperatorApplicationError>,
    drop_incomplete: bool,
    stream_len_threshold: usize,
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
    op_base: &OperatorBase,
    op: &'a OpJoin,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
    TransformData::Join(TfJoin {
        current_stream_val: None,
        stream_val_added_len: 0,
        separator: op.separator.as_deref(),
        separator_is_valid_utf8: op.separator_is_valid_utf8,
        iter_id: sess.field_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
        buffer: Vec::new(),
        buffer_is_valid_utf8: true,
        first_record_added: false,
        group_len: 0,
        group_capacity: op.join_count,
        current_group_error: None,
        drop_incomplete: op.drop_incomplete,
        output_stream_val: None,
        //TODO: add a separate setting for this
        stream_len_threshold: sess.session_data.chains[op_base.chain_id as usize]
            .settings
            .stream_size_threshold,
        stream_value_error: false,
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
pub fn push_str(join: &mut TfJoin, sv_mgr: &mut StreamValueManager, data: &str, rl: usize) {
    push_bytes_raw(join, sv_mgr, data.as_bytes(), rl);
}
fn get_join_buffer<'a>(
    join: &'a mut TfJoin,
    sv_mgr: &'a mut StreamValueManager,
    expected_len: usize,
) -> &'a mut Vec<u8> {
    if join.output_stream_val.is_none() {
        if join.buffer.len() + expected_len > join.stream_len_threshold {
            let cap = join.buffer.capacity();
            let sv = sv_mgr.stream_values.claim_with_value(StreamValue {
                data: StreamValueData::Bytes(std::mem::replace(
                    &mut join.buffer,
                    Vec::with_capacity(cap),
                )),
                bytes_are_utf8: join.buffer_is_valid_utf8,
                bytes_are_chunk: true,
                done: false,
                subscribers: Default::default(),
                ref_count: 1,
            });
            join.output_stream_val = Some(sv);
        }
    }
    if let Some(sv_id) = join.output_stream_val {
        if let StreamValueData::Bytes(bb) = &mut sv_mgr.stream_values[sv_id].data {
            bb
        } else {
            unreachable!();
        }
    } else {
        &mut join.buffer
    }
}
fn push_bytes_raw(join: &mut TfJoin, sv_mgr: &mut StreamValueManager, data: &[u8], mut rl: usize) {
    let first_record_added = join.first_record_added;
    join.first_record_added = true;
    let sep = join.separator;
    let buf = get_join_buffer(
        join,
        sv_mgr,
        (data.len() + join.separator.map(|s| s.len()).unwrap_or(0)) * rl,
    );
    if let Some(sep) = sep {
        if !first_record_added {
            buf.push_str(data.as_bytes());
            rl = rl.saturating_sub(1);
        }
        for _ in 0..rl {
            buf.push_str(sep);
            buf.push_str(data.as_bytes());
        }
    } else {
        for _ in 0..rl {
            buf.push_str(data.as_bytes());
        }
    }
}
pub fn push_bytes(join: &mut TfJoin, sv_mgr: &mut StreamValueManager, data: &[u8], rl: usize) {
    join.buffer_is_valid_utf8 = false;
    push_bytes_raw(join, sv_mgr, data, rl);
}
pub fn push_bytes_known_string(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &[u8],
    rl: usize,
) {
    push_bytes_raw(join, sv_mgr, data, rl);
}

pub fn emit_group(join: &mut TfJoin, sv_mgr: &mut StreamValueManager, output_field: &mut Field) {
    let len = join.buffer.len();
    let valid_utf8 = join.buffer_is_valid_utf8 && join.separator_is_valid_utf8;
    if let Some(sv_id) = join.output_stream_val {
        let sv = &mut sv_mgr.stream_values[sv_id];
        sv.done = true;
        output_field
            .field_data
            .push_stream_value_id(sv_id, 1, true, false);
        //TODO: gc old stream values
        sv_mgr.inform_stream_value_subscribers(sv_id);
    } else if let Some(err) = join.current_group_error.take() {
        output_field.field_data.push_error(err, 1, true, false);
    } else if len < INLINE_STR_MAX_LEN {
        if valid_utf8 {
            output_field.field_data.push_inline_str(
                unsafe { std::str::from_utf8_unchecked(&join.buffer) },
                1,
                true,
                false,
            );
        } else {
            output_field
                .field_data
                .push_inline_bytes(&join.buffer, 1, true, false);
        }
        join.buffer.clear();
    } else {
        let buffer = std::mem::replace(&mut join.buffer, Vec::with_capacity(len));
        if valid_utf8 {
            output_field.field_data.push_string(
                unsafe { String::from_utf8_unchecked(buffer) },
                1,
                true,
                false,
            );
        } else {
            output_field
                .field_data
                .push_bytes_buffer(buffer, 1, true, false);
        }
    }
    join.group_len = 0;
    join.first_record_added = false;
    join.output_stream_val = None;
}
pub fn handle_tf_join(sess: &mut JobSession<'_>, tf_id: TransformId, join: &mut TfJoin) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let output_field_id = tf.output_field;
    let input_field_id = tf.input_field;
    sess.prepare_for_output(tf_id, &[output_field_id]);
    let input_field = sess.field_mgr.borrow_field_cow(input_field_id);
    let mut output_field = sess.field_mgr.fields[output_field_id].borrow_mut();
    let base_iter = sess
        .field_mgr
        .get_iter_cow_aware(input_field_id, &input_field, join.iter_id);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut field_pos = field_pos_start;
    let mut iter = AutoDerefIter::new(&sess.field_mgr, input_field_id, base_iter);

    let mut group_len_rem = join.group_capacity.unwrap_or(usize::MAX) - join.group_len;
    let mut groups_emitted = 0;
    let mut sv_mgr = &mut sess.sv_mgr;
    let mut batch_size_rem = batch_size;
    'iter: loop {
        if join.current_group_error.is_some() {
            let consumed = iter.next_n_fields(group_len_rem.min(batch_size_rem));
            group_len_rem -= consumed;
            join.group_len += consumed;
        }
        if group_len_rem == 0 {
            emit_group(join, sv_mgr, &mut output_field);
            groups_emitted += 1;
        }
        group_len_rem = join.group_capacity.unwrap_or(usize::MAX) - join.group_len;
        if let Some(range) = iter.typed_range_fwd(
            &mut sess.match_set_mgr,
            group_len_rem.min(batch_size_rem),
            field_value_flags::BYTES_ARE_UTF8,
        ) {
            match range.base.data {
                TypedSlice::TextInline(text) => {
                    for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                        push_str(join, sv_mgr, v, rl as usize);
                    }
                }
                TypedSlice::BytesInline(bytes) => {
                    for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                        push_bytes(join, sv_mgr, v, rl as usize);
                    }
                }
                TypedSlice::BytesBuffer(bytes) => {
                    for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                        push_bytes(join, sv_mgr, v, rl as usize);
                    }
                }
                TypedSlice::Integer(ints) => {
                    for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                        let v = i64_to_str(false, *v);
                        push_str(join, sv_mgr, v.as_str(), rl as usize);
                    }
                }
                TypedSlice::Reference(_) => unreachable!(),
                TypedSlice::Null(_) => {
                    push_str(join, sv_mgr, NULL_STR, range.base.field_count);
                }
                TypedSlice::Error(errs) => {
                    let ec = errs[0].clone();
                    if let Some(sv_id) = join.output_stream_val {
                        let sv = &mut sv_mgr.stream_values[sv_id];
                        sv.data = StreamValueData::Error(ec);
                        sv.done = true;
                        join.stream_value_error = true;
                    } else {
                        join.current_group_error = Some(ec);
                    }
                }
                TypedSlice::Unset(_) => {
                    push_str(join, sv_mgr, UNSET_STR, range.base.field_count);
                }
                TypedSlice::Success(_) => {
                    push_str(join, sv_mgr, SUCCESS_STR, range.base.field_count);
                }
                TypedSlice::StreamValueId(svs) => {
                    let mut pos = field_pos;
                    for (sv_id, offsets, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                        assert!(Some(sv_id) != join.output_stream_val); //TODO: do some loop error handling
                        pos += rl as usize;
                        let sv = &mut sv_mgr.stream_values[sv_id];
                        match &sv.data {
                            StreamValueData::Dropped => unreachable!(),
                            StreamValueData::Error(err) => {
                                let ec = err.clone();
                                if let Some(sv_id) = join.output_stream_val {
                                    let sv = &mut sv_mgr.stream_values[sv_id];
                                    sv.data = StreamValueData::Error(ec);
                                    sv.done = true;
                                    join.stream_value_error = true;
                                } else {
                                    join.current_group_error = Some(ec);
                                }
                                break;
                            }
                            StreamValueData::Bytes(b) => {
                                join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;
                                let b = offsets.as_ref().map(|o| &b[o.clone()]).unwrap_or(&b);
                                // SAFETY: this is a buffer on the heap so it will not be affected
                                // if the stream values vec is resized in case push_bytes_raw decides
                                // to alloc a stream value
                                // we have to free this livetime from the sv_mgr here so we can access
                                // our (guaranteed to be distinct) target stream value to push data
                                let b_laundered =
                                    unsafe { std::mem::transmute::<&'_ [u8], &'static [u8]>(b) };
                                if sv.done {
                                    sv_mgr = &mut sess.sv_mgr;
                                    push_bytes_raw(join, sv_mgr, &b_laundered, rl as usize);
                                } else {
                                    join.group_len += pos - field_pos;
                                    iter.move_to_field_pos(pos);
                                    join.current_stream_val = Some(sv_id);
                                    let buffered = sv.is_buffered();
                                    sv.subscribe(tf_id, rl as usize, buffered || rl > 1);
                                    if !buffered {
                                        if rl != 1 {
                                            sv.promote_to_buffer();
                                        }
                                        push_bytes_raw(join, sv_mgr, b_laundered, 1);
                                        join.stream_val_added_len = b_laundered.len();
                                        debug_assert!(offsets.is_none());
                                    }

                                    input_field.request_clear_delay();
                                    sess.tf_mgr.unclaim_batch_size(
                                        tf_id,
                                        batch_size - (pos - field_pos_start),
                                    );
                                    break 'iter;
                                }
                            }
                        }
                        sv_mgr = &mut sess.sv_mgr;
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
            batch_size_rem -= fc;
        } else {
            break;
        }
    }

    sess.field_mgr.store_iter_cow_aware(
        input_field_id,
        &input_field,
        join.iter_id,
        iter.into_base_iter(),
    );
    if input_done {
        let mut emit_incomplete = false;
        // if we dont drop incomplete and there are actual members
        emit_incomplete |= join.group_len > 0 && !join.drop_incomplete;
        // if we join all output, and there is output
        emit_incomplete |= join.group_capacity.is_none() && join.group_len > 0;
        // if we join all output, there is potentially no output, but we don't drop incomplete
        emit_incomplete |= join.group_capacity.is_none() && !join.drop_incomplete;
        if emit_incomplete {
            emit_group(join, &mut sess.sv_mgr, &mut output_field);
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
    let mut run_len = custom;
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_done = tf.input_is_done;
    let in_field_id = tf.input_field;
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    let done = sv.done;
    match &sv.data {
        StreamValueData::Dropped => unreachable!(),
        StreamValueData::Error(err) => {
            let ec = err.clone();
            if let Some(sv_id) = join.output_stream_val {
                let sv = &mut sess.sv_mgr.stream_values[sv_id];
                sv.done = true;
                sv.data = StreamValueData::Error(ec);
                join.stream_value_error = true;
            } else {
                join.current_group_error = Some(ec);
            }
        }
        StreamValueData::Bytes(b) => {
            join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;
            if sv.bytes_are_chunk {
                join.buffer.extend_from_slice(&b);
                join.stream_val_added_len += b.len();
            } else if sv.done {
                join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;
                // SAFETY: the assert proves that these buffers
                // don't overlap, so it's safe to have both
                assert!(Some(sv_id) != join.output_stream_val);
                let buf_ref =
                    unsafe { std::mem::transmute::<&'_ [u8], &'static [u8]>(b.as_slice()) };

                let sv_added_len = join.stream_val_added_len;
                if sv_added_len != 0 {
                    join.stream_val_added_len = 0;
                    let buf = get_join_buffer(join, &mut sess.sv_mgr, buf_ref.len());
                    buf.push_str(&buf_ref[sv_added_len..]);
                    join.stream_val_added_len = 0;
                    run_len -= 1;
                }
                push_bytes_raw(join, &mut sess.sv_mgr, buf_ref, run_len);
            }
        }
    }
    if done {
        join.current_stream_val = None;
        if input_done || Some(join.group_len) == join.group_capacity {
            sess.tf_mgr.push_tf_in_ready_queue(tf_id);
        } else {
            sess.tf_mgr.update_ready_state(tf_id);
        }
        sess.field_mgr.fields[in_field_id]
            .borrow()
            .drop_clear_delay_request();
    }
}
