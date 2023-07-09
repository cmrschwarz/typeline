use arrayvec::ArrayVec;
use bstr::{BStr, ByteSlice};
use smallstr::SmallString;

use crate::{
    field_data::{push_interface::PushInterface, FieldValueKind},
    options::argument::CliArgIdx,
    utils::{i64_to_str, I64_MAX_DECIMAL_DIGITS},
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Copy)]
pub struct SequenceSpec {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

pub struct OpSequence {
    ss: SequenceSpec,
    append: bool,
}

impl OpSequence {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        if self.append {
            "+seq".into()
        } else {
            "seq".into()
        }
    }
}

pub struct TfSequence {
    ss: SequenceSpec,
    output_field: FieldId,
}

pub fn setup_tf_sequence<'a>(
    sess: &mut JobData,
    op: &'a OpSequence,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    // we will forward the whole input in one go and unlink us from the chain
    tf_state.desired_batch_size = usize::MAX;
    let output_field = if op.append {
        tf_state.is_appending = true;
        tf_state.input_field
    } else {
        sess.record_mgr.add_field(tf_state.match_set_id, None)
    };
    let data = TransformData::Sequence(TfSequence {
        ss: op.ss,
        output_field,
    });
    (data, output_field)
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

pub fn handle_tf_sequence(sess: &mut JobData<'_>, tf_id: TransformId, seq: &mut TfSequence) {
    sess.prepare_for_output(tf_id, &[seq.output_field]);
    let mut output_field = sess.record_mgr.fields[seq.output_field].borrow_mut();
    output_field.field_data.clear();
    let batch_size;
    let succ_wants_text;
    if let Some(succ) = sess.tf_mgr.transforms[tf_id].successor {
        let s = &mut sess.tf_mgr.transforms[succ];
        batch_size = s.desired_batch_size.saturating_sub(s.available_batch_size);
        succ_wants_text = s.preferred_input_type == Some(FieldValueKind::BytesInline);
    } else {
        batch_size = sess.tf_mgr.transforms[tf_id].desired_batch_size;
        succ_wants_text = false;
    };

    let mut bs_rem = batch_size;

    //PERF: batch this
    if !succ_wants_text {
        while seq.ss.start != seq.ss.end && bs_rem > 0 {
            output_field
                .field_data
                .push_int(seq.ss.start, 1, true, false);
            seq.ss.start += seq.ss.step;
            bs_rem -= 1;
        }
    } else {
        if seq.ss.start >= 0 && seq.ss.step > 0 && seq.ss.step < FAST_SEQ_MAX_STEP {
            let mut int_str = ArrayVec::new();
            int_str.extend(i64_to_str(false, seq.ss.start).as_bytes().iter().cloned());
            while seq.ss.start != seq.ss.end && bs_rem != 0 {
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
                bs_rem -= 1;
            }
        } else {
            while seq.ss.start != seq.ss.end && bs_rem > 0 {
                output_field
                    .field_data
                    .push_str(&i64_to_str(false, seq.ss.start), 1, true, false);
                seq.ss.start += seq.ss.step;
                bs_rem -= 1;
            }
        }
    }

    if seq.ss.start == seq.ss.end {
        sess.tf_mgr.unlink_transform(tf_id, batch_size - bs_rem);
    } else {
        sess.tf_mgr.push_tf_in_ready_queue(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}

pub fn parse_op_seq(
    value: Option<&BStr>,
    append: bool,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing parameter for sequence", arg_idx))?
        .as_bytes()
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
    let start = match parts.len() {
        1 => 0,
        2 | 3 => parts[0].parse::<i64>().map_err(|_| {
            OperatorCreationError::new("failed to parse sequence start as integer", arg_idx)
        })?,
        _ => unreachable!(),
    };
    let step = parts
        .get(2)
        .map(|step| {
            step.parse::<i64>().map_err(|_| {
                OperatorCreationError::new("failed to parse sequence step size as integer", arg_idx)
            })
        })
        .transpose()?
        .unwrap_or(1);

    let end = parts[match parts.len() {
        1 => 0,
        2 | 3 => 1,
        _ => unreachable!(),
    }]
    .parse::<i64>()
    .map_err(|_| OperatorCreationError::new("failed to parse sequence end as integer", arg_idx))?;
    create_op_seq_with_cli_arg_idx(start, end, step, append, arg_idx)
}

fn create_op_seq_with_cli_arg_idx(
    start: i64,
    mut end: i64,
    step: i64,
    append: bool,
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
        append,
    }))
}

pub fn create_op_seq(
    start: i64,
    end: i64,
    step: i64,
    append: bool,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_seq_with_cli_arg_idx(start, end, step, append, None)
}
