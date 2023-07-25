use arrayvec::ArrayVec;

use bstr::ByteSlice;
use smallstr::SmallString;

use crate::{
    field_data::{push_interface::PushInterface, FieldValueKind},
    job_session::JobData,
    options::argument::CliArgIdx,
    utils::{i64_to_str, int_units::parse_int_with_units, I64_MAX_DECIMAL_DIGITS},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorBase, OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Copy)]
pub struct SequenceSpec {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

#[derive(Clone)]
pub struct OpSequence {
    pub ss: SequenceSpec,
    pub stop_after_input: bool,
}

impl OpSequence {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self.stop_after_input {
            false => "seq",
            true => "enum",
        }
        .into()
    }
}

pub struct TfSequence {
    ss: SequenceSpec,
    stop_after_input: bool,
}

pub fn setup_tf_sequence<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpSequence,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    // we will forward the whole input in one go and unlink us from the chain
    tf_state.desired_batch_size = usize::MAX;
    TransformData::Sequence(TfSequence {
        ss: op.ss,
        stop_after_input: op.stop_after_input,
    })
}

pub fn increment_int_str(data: &mut ArrayVec<u8, I64_MAX_DECIMAL_DIGITS>) {
    let mut i = data.len() - 1;
    loop {
        if data[i] < '9' as u8 {
            data[i] += 1;
            return;
        }
        data[i] = '0' as u8;
        if i == 0 {
            break;
        }
        i -= 1;
    }
    data.insert(0, '1' as u8);
}

const FAST_SEQ_MAX_STEP: i64 = 200;

pub fn handle_tf_sequence(sess: &mut JobData, tf_id: TransformId, seq: &mut TfSequence) {
    let (mut batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let mut output_field =
        sess.tf_mgr
            .prepare_output_field(&sess.field_mgr, &mut sess.match_set_mgr, tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let succ = &sess.tf_mgr.transforms[tf.successor.unwrap()];
    if batch_size == 0 && !seq.stop_after_input {
        batch_size = succ
            .desired_batch_size
            .saturating_sub(succ.available_batch_size);
    }
    let succ_wants_text = succ.preferred_input_type == Some(FieldValueKind::BytesInline)
        && output_field.names.is_empty();

    let seq_size_rem = (seq.ss.end - seq.ss.start) / seq.ss.step;
    let count = batch_size.min(seq_size_rem as usize);

    //PERF: batch this
    if !succ_wants_text {
        for _ in 0..count {
            output_field
                .field_data
                .push_int(seq.ss.start, 1, true, false);
            seq.ss.start += seq.ss.step;
        }
    } else {
        if seq.ss.start >= 0 && seq.ss.step > 0 && seq.ss.step < FAST_SEQ_MAX_STEP {
            let mut int_str = ArrayVec::new();
            int_str.extend(i64_to_str(false, seq.ss.start).as_bytes().iter().cloned());
            for _ in 0..count {
                output_field.field_data.push_inline_str(
                    unsafe { std::str::from_utf8_unchecked(&int_str) },
                    1,
                    true,
                    false,
                );
                for _ in 0..seq.ss.step {
                    increment_int_str(&mut int_str);
                }
                seq.ss.start += seq.ss.step;
            }
        } else {
            for _ in 0..count {
                output_field
                    .field_data
                    .push_str(&i64_to_str(false, seq.ss.start), 1, true, false);
                seq.ss.start += seq.ss.step;
            }
        }
    }
    let bs_rem = batch_size - count;

    if input_done & seq.stop_after_input {
        drop(output_field);
        sess.unlink_transform(tf_id, count);
        return;
    }
    if seq.ss.start == seq.ss.end {
        sess.tf_mgr.unclaim_batch_size(tf_id, bs_rem);
        drop(output_field);
        sess.unlink_transform(tf_id, count);
        return;
    }
    if input_done {
        sess.tf_mgr.push_tf_in_ready_queue(tf_id);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
    }
    sess.tf_mgr.inform_successor_batch_available(tf_id, count);
}

pub fn parse_op_seq(
    value: Option<&[u8]>,
    stop_after_input: bool,
    natural_number_mode: bool,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if stop_after_input && value.is_none() {
        return create_op_seq_with_cli_arg_idx(
            0 + natural_number_mode as i64,
            i64::MAX,
            1,
            true,
            arg_idx,
        );
    }
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing parameter for sequence", arg_idx))?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "failed to parse sequence parameter (invalid utf-8)",
                arg_idx,
            )
        })?;
    let parts: ArrayVec<&str, 4> = value_str.split(",").take(4).collect();
    if parts.len() == 4 {
        return Err(OperatorCreationError::new(
            "failed to parse sequence parameter, got more than 3 comma separated values",
            arg_idx,
        ));
    }
    let mut start = match parts.len() {
        1 => 0,
        2 | 3 => parse_int_with_units(parts[0]).map_err(|msg| {
            OperatorCreationError::new_s(
                format!("failed to parse sequence start as integer: {msg}"),
                arg_idx,
            )
        })?,
        _ => unreachable!(),
    };
    let step = parts
        .get(2)
        .map(|step| {
            parse_int_with_units(step).map_err(|msg| {
                OperatorCreationError::new_s(
                    format!("failed to parse sequence step size as integer: {msg}"),
                    arg_idx,
                )
            })
        })
        .transpose()?
        .unwrap_or(1);

    let end_str = parts[match parts.len() {
        1 => 0,
        2 | 3 => 1,
        _ => unreachable!(),
    }];

    let mut end = parse_int_with_units(end_str).map_err(|msg| {
        OperatorCreationError::new_s(
            format!("failed to parse sequence end as integer: {msg}"),
            arg_idx,
        )
    })?;
    if natural_number_mode {
        start += 1;
        end += 1;
    }
    create_op_seq_with_cli_arg_idx(start, end, step, stop_after_input, arg_idx)
}

fn create_op_seq_with_cli_arg_idx(
    start: i64,
    mut end: i64,
    step: i64,
    stop_after_input: bool,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if step == 0 {
        return Err(OperatorCreationError::new(
            "sequence step size cannot be zero",
            arg_idx,
        ));
    }
    if step > 0 {
        if end < start {
            return Err(OperatorCreationError::new(
                "end of sequence with positive step size must be at least as large as it's start",
                arg_idx,
            ));
        }
        end += (end - start) % step;
    }
    if step < 0 {
        if end > start {
            return Err(OperatorCreationError::new(
                "end of sequence with negative step size must not be larger than it's start",
                arg_idx,
            ));
        }
        let rem = (start - end) % (-step);
        if rem > 0 {
            end -= -step - rem;
        }
    }
    Ok(OperatorData::Sequence(OpSequence {
        ss: SequenceSpec { start, step, end },
        stop_after_input,
    }))
}

pub fn create_op_sequence(
    start: i64,
    end: i64,
    step: i64,
    stop_after_input: bool,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_seq_with_cli_arg_idx(start, end, step, stop_after_input, None)
}

pub fn create_op_seq(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_seq_with_cli_arg_idx(start, end, step, false, None)
}
pub fn create_op_seqn(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_seq_with_cli_arg_idx(start, end + 1, step, false, None)
}

pub fn create_op_enum(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_seq_with_cli_arg_idx(start, end, step, true, None)
}
