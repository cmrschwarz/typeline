use bstr::ByteSlice;
use regex::Regex;
use smallstr::SmallString;

use crate::{
    job_session::JobData,
    operators::utils::buffer_stream_values::{
        buffer_remaining_stream_values_in_auto_deref_iter,
        buffer_remaining_stream_values_in_sv_iter,
    },
    options::argument::CliArgIdx,
    record_data::{
        custom_data::CustomDataBox,
        field::Field,
        field_data::{field_value_flags, FieldValueRepr, INLINE_STR_MAX_LEN},
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
        },
        stream_value::{
            StreamValue, StreamValueData, StreamValueId, StreamValueManager,
        },
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    utils::{
        int_string_conversions::{i64_to_str, usize_to_str},
        io::PointerWriter,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
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
                    "failed to parse insertion count as integer",
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
        value.map(|v| v.to_owned()),
        insert_count,
        drop_incomplete,
    ))
}

pub fn build_tf_join<'a>(
    sess: &mut JobData,
    op_base: &OperatorBase,
    op: &'a OpJoin,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(FieldValueRepr::BytesInline);
    TransformData::Join(TfJoin {
        current_stream_val: None,
        stream_val_added_len: 0,
        separator: op.separator.as_deref(),
        separator_is_valid_utf8: op.separator_is_valid_utf8,
        iter_id: sess.field_mgr.claim_iter(tf_state.input_field),
        buffer: Vec::new(),
        buffer_is_valid_utf8: true,
        first_record_added: false,
        group_len: 0,
        group_capacity: op.join_count,
        current_group_error: None,
        drop_incomplete: op.drop_incomplete,
        output_stream_val: None,
        // TODO: add a separate setting for this
        stream_len_threshold: sess.session_data.chains
            [op_base.chain_id.unwrap() as usize]
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
        .map(|v| v.to_str().is_ok())
        .unwrap_or(true);
    let sep = separator.map(|v| v.into_boxed_slice());
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
pub fn push_str(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &str,
    rl: usize,
) {
    push_bytes_raw(join, sv_mgr, data.as_bytes(), rl);
}
fn get_join_buffer<'a>(
    join: &'a mut TfJoin,
    sv_mgr: &'a mut StreamValueManager,
    expected_len: usize,
) -> &'a mut Vec<u8> {
    if join.output_stream_val.is_none()
        && join.buffer.len() + expected_len > join.stream_len_threshold
    {
        let cap = join.buffer.capacity();
        let sv = sv_mgr.stream_values.claim_with_value(StreamValue {
            data: StreamValueData::Bytes(std::mem::replace(
                &mut join.buffer,
                Vec::with_capacity(cap),
            )),
            bytes_are_utf8: join.buffer_is_valid_utf8,
            bytes_are_chunk: true,
            drop_previous_chunks: false,
            done: false,
            subscribers: Default::default(),
            ref_count: 1,
        });
        join.output_stream_val = Some(sv);
    }

    if let Some(sv_id) = join.output_stream_val {
        if let StreamValueData::Bytes(bb) =
            &mut sv_mgr.stream_values[sv_id].data
        {
            bb
        } else {
            unreachable!();
        }
    } else {
        &mut join.buffer
    }
}
fn push_bytes_raw(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    data: &[u8],
    mut rl: usize,
) {
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

pub fn emit_group(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    output_field: &mut Field,
) {
    let len = join.buffer.len();
    let valid_utf8 = join.buffer_is_valid_utf8 && join.separator_is_valid_utf8;
    if let Some(sv_id) = join.output_stream_val {
        let sv = &mut sv_mgr.stream_values[sv_id];
        sv.done = true;
        output_field
            .iter_hall
            .push_stream_value_id(sv_id, 1, true, false);
        // TODO: gc old stream values
        sv_mgr.inform_stream_value_subscribers(sv_id);
    } else if let Some(err) = join.current_group_error.take() {
        output_field.iter_hall.push_error(err, 1, true, false);
    } else if len < INLINE_STR_MAX_LEN {
        if valid_utf8 {
            output_field.iter_hall.push_inline_str(
                unsafe { std::str::from_utf8_unchecked(&join.buffer) },
                1,
                true,
                false,
            );
        } else {
            output_field.iter_hall.push_inline_bytes(
                &join.buffer,
                1,
                true,
                false,
            );
        }
        join.buffer.clear();
    } else {
        let buffer =
            std::mem::replace(&mut join.buffer, Vec::with_capacity(len));
        if valid_utf8 {
            output_field.iter_hall.push_string(
                unsafe { String::from_utf8_unchecked(buffer) },
                1,
                true,
                false,
            );
        } else {
            output_field
                .iter_hall
                .push_bytes_buffer(buffer, 1, true, false);
        }
    }
    join.group_len = 0;
    join.first_record_added = false;
    join.output_stream_val = None;
}
fn push_error(
    join: &mut TfJoin,
    sv_mgr: &mut StreamValueManager,
    e: OperatorApplicationError,
) {
    if let Some(sv_id) = join.output_stream_val {
        let sv = &mut sv_mgr.stream_values[sv_id];
        sv.data = StreamValueData::Error(e);
        sv.done = true;
        join.stream_value_error = true;
    } else {
        join.current_group_error = Some(e);
    }
}
pub fn handle_tf_join(
    sess: &mut JobData,
    tf_id: TransformId,
    join: &mut TfJoin,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    sess.tf_mgr.prepare_output_field(
        &mut sess.field_mgr,
        &mut sess.match_set_mgr,
        tf_id,
    );
    let tf = &sess.tf_mgr.transforms[tf_id];
    let mut output_field = sess.field_mgr.fields[tf.output_field].borrow_mut();
    let input_field_id = tf.input_field;
    let input_field = sess
        .field_mgr
        .get_cow_field_ref(&mut sess.match_set_mgr, input_field_id);
    let base_iter =
        sess.field_mgr
            .lookup_iter(input_field_id, &input_field, join.iter_id);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut field_pos = field_pos_start;
    let mut iter =
        AutoDerefIter::new(&sess.field_mgr, input_field_id, base_iter);

    let mut group_len_rem =
        join.group_capacity.unwrap_or(usize::MAX) - join.group_len;
    let mut groups_emitted = 0;
    let mut sv_mgr = &mut sess.sv_mgr;
    let mut batch_size_rem = batch_size;
    'iter: loop {
        if join.current_group_error.is_some() {
            let consumed =
                iter.next_n_fields(group_len_rem.min(batch_size_rem));
            group_len_rem -= consumed;
            join.group_len += consumed;
        }
        if group_len_rem == 0 {
            emit_group(join, sv_mgr, &mut output_field);
            groups_emitted += 1;
        }
        group_len_rem =
            join.group_capacity.unwrap_or(usize::MAX) - join.group_len;
        if let Some(range) = iter.typed_range_fwd(
            &mut sess.match_set_mgr,
            group_len_rem.min(batch_size_rem),
            field_value_flags::BYTES_ARE_UTF8,
        ) {
            match range.base.data {
                TypedSlice::TextInline(text) => {
                    for (v, rl, _offs) in
                        RefAwareInlineTextIter::from_range(&range, text)
                    {
                        push_str(join, sv_mgr, v, rl as usize);
                    }
                }
                TypedSlice::BytesInline(bytes) => {
                    for (v, rl, _offs) in
                        RefAwareInlineBytesIter::from_range(&range, bytes)
                    {
                        push_bytes(join, sv_mgr, v, rl as usize);
                    }
                }
                TypedSlice::BytesBuffer(bytes) => {
                    for (v, rl, _offs) in
                        RefAwareBytesBufferIter::from_range(&range, bytes)
                    {
                        push_bytes(join, sv_mgr, v, rl as usize);
                    }
                }
                TypedSlice::Int(ints) => {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, ints)
                    {
                        let v = i64_to_str(false, *v);
                        push_str(join, sv_mgr, v.as_str(), rl as usize);
                    }
                }
                TypedSlice::Custom(custom_data) => {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, custom_data)
                    {
                        push_custom_type(tf, join, sv_mgr, v, rl);
                    }
                }
                TypedSlice::Error(errs) => {
                    push_error(join, sv_mgr, errs[0].clone());
                }
                TypedSlice::Null(_) | TypedSlice::Undefined(_) => {
                    let str = typed_slice_zst_str(&range.base.data);
                    push_error(
                        join,
                        sv_mgr,
                        OperatorApplicationError::new_s(
                            format!("join does not support {str}"),
                            tf.op_id.unwrap(),
                        ),
                    );
                }
                TypedSlice::StreamValueId(svs) => {
                    let mut pos = field_pos;
                    let mut sv_iter =
                        RefAwareStreamValueIter::from_range(&range, svs);
                    while let Some((sv_id, offsets, rl)) = sv_iter.next() {
                        assert!(Some(sv_id) != join.output_stream_val); // TODO: do some loop error handling
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
                                let b = offsets
                                    .as_ref()
                                    .map(|o| &b[o.clone()])
                                    .unwrap_or(b);
                                // SAFETY: this is a buffer on the heap so it
                                // will not be affected
                                // if the stream values vec is resized in case
                                // push_bytes_raw decides
                                // to alloc a stream value
                                // we have to free this livetime from the
                                // sv_mgr here so we can access
                                // our (guaranteed to be distinct) target
                                // stream value to push data
                                let b_laundered = unsafe {
                                    std::mem::transmute::<
                                        &'_ [u8],
                                        &'static [u8],
                                    >(b)
                                };
                                if sv.done {
                                    sv_mgr = &mut sess.sv_mgr;
                                    push_bytes_raw(
                                        join,
                                        sv_mgr,
                                        b_laundered,
                                        rl as usize,
                                    );
                                } else {
                                    join.group_len += pos - field_pos;
                                    iter.move_to_field_pos(pos);
                                    join.current_stream_val = Some(sv_id);
                                    let buffered = sv.is_buffered();
                                    sv.subscribe(
                                        tf_id,
                                        rl as usize,
                                        buffered || rl > 1,
                                    );
                                    if !buffered {
                                        if rl != 1 {
                                            sv.promote_to_buffer();
                                        }
                                        push_bytes_raw(
                                            join,
                                            sv_mgr,
                                            b_laundered,
                                            1,
                                        );
                                        join.stream_val_added_len =
                                            b_laundered.len();
                                        debug_assert!(offsets.is_none());
                                    }

                                    sess.field_mgr
                                        .request_clear_delay(input_field_id);
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
                                        batch_size_rem - (pos - field_pos),
                                    );
                                    break 'iter;
                                }
                            }
                        }
                        sv_mgr = &mut sess.sv_mgr;
                    }
                }
                TypedSlice::BigInt(_)
                | TypedSlice::Float(_)
                | TypedSlice::Rational(_) => {
                    todo!();
                }
                TypedSlice::Object(_) => {
                    todo!();
                }
                TypedSlice::Array(_) => {
                    todo!();
                }
                TypedSlice::FieldReference(_)
                | TypedSlice::SlicedFieldReference(_) => unreachable!(),
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

    sess.field_mgr
        .store_iter(input_field_id, join.iter_id, iter);
    let streams_done = join.current_stream_val.is_none();
    if input_done && streams_done {
        let mut emit_incomplete = false;
        // if we dont drop incomplete and there are actual members
        emit_incomplete |= join.group_len > 0 && !join.drop_incomplete;
        // if we join all output, and there is output
        emit_incomplete |= join.group_capacity.is_none() && join.group_len > 0;
        // if we join all output, there is potentially no output, but we don't
        // drop incomplete
        emit_incomplete |=
            join.group_capacity.is_none() && !join.drop_incomplete;
        if emit_incomplete {
            emit_group(join, &mut sess.sv_mgr, &mut output_field);
            groups_emitted += 1;
        }
    }

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

fn push_custom_type(
    tf: &TransformState,
    join: &mut TfJoin<'_>,
    sv_mgr: &mut StreamValueManager,
    v: &CustomDataBox,
    rl: u32,
) {
    let Some(len) = v.stringified_len() else {
        push_error(
            join,
            sv_mgr,
            OperatorApplicationError::new_s(
                format!("cannot stringify custom type {}", v.type_name()),
                tf.op_id.unwrap(),
            ),
        );
        return;
    };

    join.buffer_is_valid_utf8 &= v.stringifies_as_valid_utf8();
    let first_record_added = join.first_record_added;
    join.first_record_added = true;
    let sep = join.separator;
    let sep_len = sep.map(|s| s.len()).unwrap_or(0);
    let target_len = (len + sep_len) * rl as usize;
    let buf = get_join_buffer(join, sv_mgr, target_len);
    let start_len = buf.len();
    buf.reserve(target_len);
    const ERR_MSG: &str = "custom stringify failed";
    unsafe {
        let start_ptr = buf.as_mut_ptr().add(start_len);
        if let Some(sep) = sep {
            let mut first_target_ptr = start_ptr;
            let mut ptr = start_ptr;
            if !first_record_added {
                v.stringify_expect_len(len, &mut PointerWriter::new(ptr, len))
                    .expect(ERR_MSG);
                ptr = ptr.add(len);
            } else {
                std::ptr::copy_nonoverlapping(sep.as_ptr(), ptr, sep_len);
                ptr = ptr.add(sep_len);
                first_target_ptr = ptr;
                v.stringify_expect_len(len, &mut PointerWriter::new(ptr, len))
                    .expect(ERR_MSG);
                ptr = ptr.add(len);
            }

            for _ in 1..rl {
                std::ptr::copy_nonoverlapping(sep.as_ptr(), ptr, sep_len);
                ptr = ptr.add(sep_len);
                std::ptr::copy_nonoverlapping(first_target_ptr, ptr, sep_len);
                ptr = ptr.add(len);
            }
        } else {
            let mut ptr = start_ptr;
            v.stringify_expect_len(len, &mut PointerWriter::new(ptr, len))
                .expect(ERR_MSG);
            for _ in 1..rl {
                ptr = ptr.add(len);
                std::ptr::copy_nonoverlapping(start_ptr, ptr, len);
            }
        }
        buf.set_len(start_len + target_len);
    }
}

pub fn handle_tf_join_stream_value_update(
    sess: &mut JobData,
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
            // SAFETY: the assert proves that these buffers
            // don't overlap, so it's safe to have both
            assert!(Some(sv_id) != join.output_stream_val);
            let buf_ref = unsafe {
                std::mem::transmute::<&'_ [u8], &'static [u8]>(b.as_slice())
            };
            let drop_prev_chunks = sv.drop_previous_chunks;
            let mut sv_added_len = join.stream_val_added_len;
            if sv.bytes_are_chunk {
                let buf =
                    get_join_buffer(join, &mut sess.sv_mgr, buf_ref.len());
                if drop_prev_chunks {
                    buf.truncate(buf.len() - sv_added_len);
                }
                buf.extend_from_slice(buf_ref);
                if drop_prev_chunks {
                    join.stream_val_added_len = 0;
                }
                join.stream_val_added_len += buf_ref.len();
            } else if sv.done {
                join.buffer_is_valid_utf8 &= sv.bytes_are_utf8;

                if sv_added_len != 0 {
                    join.stream_val_added_len = 0;
                    let buf =
                        get_join_buffer(join, &mut sess.sv_mgr, buf_ref.len());
                    if drop_prev_chunks {
                        buf.truncate(buf.len() - sv_added_len);
                        sv_added_len = 0;
                    }
                    buf.extend_from_slice(&buf_ref[sv_added_len..]);
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
            sess.tf_mgr.push_tf_in_ready_stack(tf_id);
        } else {
            sess.tf_mgr.update_ready_state(tf_id);
        }
        sess.field_mgr.relinquish_clear_delay(in_field_id);
    }
}
