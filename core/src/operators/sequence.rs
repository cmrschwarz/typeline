use arrayvec::ArrayVec;

use crate::{
    cli::{parse_arg_value_as_str, ParsedCliArgumentParts},
    context::SessionData,
    job::JobData,
    liveness_analysis::{AccessFlags, LivenessData, NON_STRING_READS_OFFSET},
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field_action::FieldActionKind,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        push_interface::VariableSizeTypeInserter,
    },
    utils::int_string_conversions::{
        i64_to_str, parse_int_with_units, I64_MAX_DECIMAL_DIGITS,
    },
};

use super::{
    errors::OperatorCreationError,
    operator::{DefaultOperatorName, OperatorBase, OperatorData, OperatorId},
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
    ss: SequenceSpec,
    mode: OpSequenceMode,
    non_string_reads: bool,
}

impl OpSequence {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        match self.mode {
            OpSequenceMode::Sequence => "seq",
            OpSequenceMode::Enum => "enum",
            OpSequenceMode::EnumUnbounded => "enum-u",
        }
        .into()
    }
}

#[derive(Clone, Copy)]
pub enum OpSequenceMode {
    Sequence,
    Enum,
    EnumUnbounded,
}

#[derive(Clone, Copy)]
enum TfSequenceMode {
    #[allow(unused)] // TODO
    Sequence {
        actor_id: ActorId,
    },
    Enum,
    EnumUnbounded,
}

pub struct TfSequence {
    pub non_string_reads: bool,
    ss: SequenceSpec,
    mode: TfSequenceMode,
    iter_id: IterId,
}

pub fn build_tf_sequence<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpSequence,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let mode = match op.mode {
        OpSequenceMode::Enum => TfSequenceMode::Enum,
        OpSequenceMode::EnumUnbounded => TfSequenceMode::EnumUnbounded,
        OpSequenceMode::Sequence => {
            let mut ab = jd.match_set_mgr.match_sets[tf_state.match_set_id]
                .action_buffer
                .borrow_mut();
            let actor_id = ab.add_actor();
            let next_actor_id = ActorRef::Unconfirmed(ab.peek_next_actor_id());
            let mut output_field =
                jd.field_mgr.fields[tf_state.output_field].borrow_mut();

            output_field.first_actor = next_actor_id;
            TfSequenceMode::Sequence { actor_id }
        }
    };

    TransformData::Sequence(TfSequence {
        ss: op.ss,
        mode,
        non_string_reads: op.non_string_reads,
        iter_id: jd.field_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .iter_hall
            .claim_iter(IterKind::Transform(
                jd.tf_mgr.transforms.peek_claim_id(),
            )),
    })
}

pub fn setup_op_sequence_concurrent_liveness_data(
    sess: &SessionData,
    op: &mut OpSequence,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let output_id = sess.operator_bases[op_id as usize].outputs_start;
    let stride = ld.op_outputs.len();
    op.non_string_reads = ld.op_outputs_data
        [NON_STRING_READS_OFFSET * stride + output_id as usize];
}

pub fn update_op_sequence_variable_liveness(
    flags: &mut AccessFlags,
    seq: &OpSequence,
) {
    flags.input_accessed = false;
    flags.may_dup_or_drop = matches!(seq.mode, OpSequenceMode::Sequence);
    flags.non_stringified_input_access = false;
}

pub fn increment_int_str(data: &mut ArrayVec<u8, I64_MAX_DECIMAL_DIGITS>) {
    let mut i = data.len() - 1;
    loop {
        if data[i] < b'9' {
            data[i] += 1;
            return;
        }
        data[i] = b'0';
        if i == 0 {
            break;
        }
        i -= 1;
    }
    data.insert(0, b'1');
}

const FAST_SEQ_MAX_STEP: i64 = 200;

pub fn handle_tf_sequence(
    jd: &mut JobData,
    tf_id: TransformId,
    seq: &mut TfSequence,
) {
    let (mut batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];
    let ms_id = tf.match_set_id;
    let no_input = batch_size == 0;
    let input_field_id =
        jd.field_mgr.get_dealiased_field_id(&mut tf.input_field);

    let mut max_count = batch_size;
    let desired_batch_size = if let Some(succ) = tf.successor {
        let bs_max = tf.desired_batch_size;
        let succ = &jd.tf_mgr.transforms[succ];
        succ.desired_batch_size.max(bs_max)
    } else {
        tf.desired_batch_size
    };

    match seq.mode {
        TfSequenceMode::Enum => {
            if no_input && ps.input_done {
                jd.tf_mgr.declare_transform_done(tf_id);
                return;
            }
        }
        TfSequenceMode::EnumUnbounded => {
            if no_input && ps.input_done {
                batch_size = desired_batch_size;
            }
        }
        TfSequenceMode::Sequence { .. } => {
            // TODO: proper foreach
            max_count = desired_batch_size.max(batch_size);
        }
    };
    let seq_size_rem = (seq.ss.end - seq.ss.start) / seq.ss.step;
    let count = max_count.min(usize::try_from(seq_size_rem).unwrap_or(0));

    let of_id = jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let mut output_field = jd.field_mgr.fields[of_id].borrow_mut();

    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&mut jd.match_set_mgr, input_field_id);

    let iter =
        jd.field_mgr
            .lookup_iter(input_field_id, &input_field, seq.iter_id);
    let field_pos = iter.get_next_field_pos();

    if seq.non_string_reads {
        let mut inserter =
            output_field.iter_hall.fixed_size_type_inserter::<i64>();
        inserter.drop_and_reserve(count);
        for _ in 0..count {
            inserter.push(seq.ss.start);
            seq.ss.start += seq.ss.step;
        }
    } else {
        let mut inserter = output_field.iter_hall.inline_str_inserter();
        if seq.ss.start >= 0
            && seq.ss.step > 0
            && seq.ss.step < FAST_SEQ_MAX_STEP
        {
            let mut int_str = ArrayVec::new();
            int_str.extend(
                i64_to_str(false, seq.ss.start).as_bytes().iter().copied(),
            );
            inserter.drop_and_reserve(count, int_str.len());
            for _ in 0..count {
                inserter.push_may_rereserve(unsafe {
                    std::str::from_utf8_unchecked(&int_str)
                });
                for _ in 0..seq.ss.step {
                    increment_int_str(&mut int_str);
                }
                seq.ss.start += seq.ss.step;
            }
        } else {
            for _ in 0..count {
                inserter.push_may_rereserve(&i64_to_str(false, seq.ss.start));
                seq.ss.start += seq.ss.step;
            }
        }
    }
    let bs_rem = batch_size.saturating_sub(count);
    let mut done = ps.input_done && matches!(seq.mode, TfSequenceMode::Enum);
    if seq.ss.start == seq.ss.end {
        if !ps.input_done {
            jd.tf_mgr.unclaim_batch_size(tf_id, bs_rem);
        }
        done = true;
    }
    if !done && (ps.next_batch_ready || ps.input_done) {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    if let TfSequenceMode::Sequence { actor_id } = &mut seq.mode {
        let ab = jd.match_set_mgr.match_sets[ms_id].action_buffer.get_mut();
        ab.begin_action_group(*actor_id);
        if done && count == 0 {
            ab.push_action(FieldActionKind::Drop, field_pos, 1);
        } else {
            ab.push_action(
                FieldActionKind::Dup,
                field_pos,
                count - usize::from(done),
            );
        }
        ab.end_action_group();
    }
    jd.tf_mgr.submit_batch(tf_id, count, done);
}

pub fn parse_op_seq(
    arg: &ParsedCliArgumentParts,
    mode: OpSequenceMode,
    natural_number_mode: bool,
) -> Result<OperatorData, OperatorCreationError> {
    let arg_idx = Some(arg.cli_arg.idx);
    if matches!(mode, OpSequenceMode::Enum | OpSequenceMode::EnumUnbounded)
        && arg.value.is_none()
    {
        return create_op_sequence_with_opts(
            i64::from(natural_number_mode),
            i64::MAX,
            1,
            mode,
            arg_idx,
        );
    }
    let value_str = parse_arg_value_as_str("seq", arg.value, arg_idx)?;
    let parts: ArrayVec<&str, 4> = value_str.split(',').take(4).collect();
    if parts.len() == 4 {
        return Err(OperatorCreationError::new(
            "failed to parse sequence parameter, got more than 3 comma separated values",
            arg_idx,
        ));
    }
    let start = match parts.len() {
        1 => i64::from(natural_number_mode),
        2 | 3 => parse_int_with_units(parts[0]).map_err(|msg| {
            OperatorCreationError::new_s(
                format!("failed to parse sequence start as an integer: {msg}"),
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
                    format!(
                        "failed to parse sequence step size as an integer: {msg}"
                    ),
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
            format!("failed to parse sequence end as an integer: {msg}"),
            arg_idx,
        )
    })?;
    if natural_number_mode {
        end += 1;
    }
    create_op_sequence_with_opts(start, end, step, mode, arg_idx)
}

fn create_op_sequence_with_opts(
    start: i64,
    mut end: i64,
    step: i64,
    mode: OpSequenceMode,
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
        ss: SequenceSpec { start, end, step },
        mode,
        non_string_reads: true,
    }))
}

pub fn create_op_sequence(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_sequence_with_opts(
        start,
        end,
        step,
        OpSequenceMode::Sequence,
        None,
    )
}
pub fn create_op_enum_with_opts(
    start: i64,
    end: i64,
    step: i64,
    unbounded: bool,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_sequence_with_opts(
        start,
        end,
        step,
        if unbounded {
            OpSequenceMode::EnumUnbounded
        } else {
            OpSequenceMode::Enum
        },
        None,
    )
}

pub fn create_op_seq(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_sequence(start, end, step)
}
pub fn create_op_seqn(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_sequence(start, end + 1, step)
}
pub fn create_op_enum(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_enum_with_opts(start, end, step, false)
}
