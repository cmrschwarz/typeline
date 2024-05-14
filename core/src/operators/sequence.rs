use std::cell::RefCell;

use arrayvec::ArrayVec;

use crate::{
    cli::{parse_arg_value_as_str, ParsedCliArgumentParts},
    context::SessionData,
    job::{JobData, PipelineState, TransformManager},
    liveness_analysis::{AccessFlags, LivenessData, NON_STRING_READS_OFFSET},
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::{ActionBuffer, ActorId},
        field::{Field, FieldId, FieldManager},
        field_action::FieldActionKind,
        field_data::FieldValueRepr,
        record_group_tracker::{GroupListIterRef, RecordGroupTracker},
        iter_hall::IterId,
        iters::{DestructuredFieldDataRef, FieldIterator, Iter},
        variable_sized_type_inserter::VariableSizeTypeInserter,
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

const FAST_SEQ_MAX_STEP: i64 = 200;

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
    seq_len_total: u64,
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
    Sequence,
    Enum,
    EnumUnbounded,
}

pub struct TfSequence {
    pub non_string_reads: bool,
    ss: SequenceSpec,
    current_value: i64,
    mode: TfSequenceMode,
    iter_id: IterId,
    actor_id: ActorId,
    seq_len_total: u64,
    group_list_iter_ref: Option<GroupListIterRef>,
}

impl SequenceSpec {
    fn remaining_len(&self, value: i64) -> u64 {
        ((self.end - value) / self.step) as u64
    }
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
        OpSequenceMode::Sequence => TfSequenceMode::Sequence {},
    };

    let group_list_iter_ref = (!matches!(op.mode, OpSequenceMode::Sequence))
        .then(|| {
            jd.record_group_tracker
                .claim_group_list_iter_ref(tf_state.input_group_list_id)
        });

    TransformData::Sequence(TfSequence {
        ss: op.ss,
        current_value: op.ss.start,
        mode,
        non_string_reads: op.non_string_reads,
        iter_id: jd.add_iter_for_tf_state(tf_state),
        actor_id: jd.add_actor_for_tf_state(tf_state),
        seq_len_total: op.seq_len_total,
        group_list_iter_ref,
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
    _seq: &OpSequence,
) {
    flags.input_accessed = true;
    flags.may_dup_or_drop = true;
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

struct SequenceBatchState<'a, 'b> {
    tf_id: TransformId,
    input_field_id: FieldId,
    seq: &'a mut TfSequence,
    ab: &'a RefCell<ActionBuffer>,
    rgt: &'a mut RecordGroupTracker,
    fm: &'a FieldManager,
    tf_mgr: &'a mut TransformManager,
    iter: Iter<'b, DestructuredFieldDataRef<'b>>,
    batch_size: usize,
    desired_batch_size: usize,
    ps: PipelineState,
    output_field: &'a mut Field,
    is_split: bool,
}

pub fn handle_tf_sequence(
    jd: &mut JobData,
    tf_id: TransformId,
    seq: &mut TfSequence,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];
    let ms_id = tf.match_set_id;
    let is_split = tf.is_split;
    let input_field_id =
        jd.field_mgr.get_dealiased_field_id(&mut tf.input_field);

    let desired_batch_size = if let Some(succ) = tf.successor {
        let bs_max = tf.desired_batch_size;
        let succ = &jd.tf_mgr.transforms[succ];
        succ.desired_batch_size.max(bs_max)
    } else {
        tf.desired_batch_size
    };

    let of_id = jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let mut output_field = jd.field_mgr.fields[of_id].borrow_mut();

    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, input_field_id);

    let iter =
        jd.field_mgr
            .lookup_iter(input_field_id, &input_field, seq.iter_id);

    let ms = &mut jd.match_set_mgr.match_sets[ms_id];

    let ss = SequenceBatchState {
        tf_id,
        input_field_id,
        seq,
        ab: &ms.action_buffer,
        rgt: &mut jd.record_group_tracker,
        fm: &jd.field_mgr,
        tf_mgr: &mut jd.tf_mgr,
        iter,
        batch_size,
        desired_batch_size,
        ps,
        output_field: &mut output_field,
        is_split,
    };

    match ss.seq.mode {
        TfSequenceMode::Sequence => {
            let pending_seq_len_claimed = handle_seq_mode(ss);
            if pending_seq_len_claimed > 0 {
                // we partially emitted a sequence.
                // this means that we dup'ed the input element
                // `pending_seq_len_claimed` many times.
                // to keep our iterator pointing at the correct field pos
                // for the next batch, we need to skip those elements
                drop(input_field);
                let input_field = jd
                    .field_mgr
                    .get_cow_field_ref(&jd.match_set_mgr, input_field_id);

                let mut iter = jd.field_mgr.lookup_iter(
                    input_field_id,
                    &input_field,
                    seq.iter_id,
                );
                iter.next_n_fields(pending_seq_len_claimed, true);
                jd.field_mgr.store_iter(input_field_id, seq.iter_id, iter);
            }
        }
        TfSequenceMode::Enum => handle_enum_mode(ss),
        TfSequenceMode::EnumUnbounded => handle_enum_unbounded_mode(ss),
    }
}

// returns the claimed pending sequence len so we can move past it
fn handle_seq_mode(mut sbs: SequenceBatchState) -> usize {
    let mut ab = sbs.ab.borrow_mut();
    ab.begin_action_group(sbs.seq.actor_id);
    let mut field_pos = sbs.iter.get_next_field_pos();
    let mut field_dup_count = 0;
    let field_pos_end = field_pos + sbs.batch_size;
    let mut out_batch_size_rem = sbs.desired_batch_size;

    let seq_len_total = sbs.seq.seq_len_total;
    let seq_len_trunc = usize::try_from(seq_len_total).unwrap_or(0);

    let mut seq_len_rem = sbs.seq.ss.remaining_len(sbs.seq.current_value);

    let mut pending_seq_len_claimed = 0;

    while field_pos != field_pos_end && out_batch_size_rem != 0 {
        if field_pos == field_pos_end || out_batch_size_rem == 0 {
            break;
        }
        if seq_len_rem > out_batch_size_rem as u64
            || seq_len_rem != seq_len_total
        {
            let count = seq_len_rem.min(out_batch_size_rem as u64) as usize;
            let seq_done = count as u64 == seq_len_rem;
            sbs.seq.current_value = advance_sequence(
                sbs.seq,
                sbs.seq.current_value,
                sbs.output_field,
                count,
            );
            let dup_count = count - usize::from(seq_done);
            ab.push_action(
                FieldActionKind::Dup,
                field_pos + field_dup_count,
                dup_count,
            );
            field_dup_count += dup_count;
            out_batch_size_rem -= count;
            if seq_done {
                sbs.iter.next_field();
                sbs.seq.current_value = sbs.seq.ss.start;
                field_pos += 1;
            }
            if out_batch_size_rem == 0 {
                if !seq_done {
                    pending_seq_len_claimed = count;
                }
                break;
            }
            if field_pos == field_pos_end {
                break;
            }
            seq_len_rem = seq_len_total;
        }

        let full_seqs_rem =
            (out_batch_size_rem as u64 / seq_len_total) as usize;
        let field_count = sbs.iter.next_n_fields(
            full_seqs_rem.max(1).min(field_pos_end - field_pos),
            true,
        );

        debug_assert!(field_count > 0 || field_pos == field_pos_end);

        // PERF: we could optimize this to a memcopy for the subsequent
        // ones
        for _ in 0..field_count {
            advance_sequence(
                sbs.seq,
                sbs.seq.ss.start,
                sbs.output_field,
                seq_len_trunc,
            );
        }
        for _ in 0..field_count {
            let dup_count = seq_len_trunc - 1;
            ab.push_action(
                FieldActionKind::Dup,
                field_pos + field_dup_count,
                dup_count,
            );
            field_dup_count += dup_count;
            field_pos += 1;
        }
        out_batch_size_rem -= seq_len_trunc * field_count;
    }
    ab.end_action_group();
    sbs.fm
        .store_iter(sbs.input_field_id, sbs.seq.iter_id, sbs.iter);

    let unclaimed_input = field_pos_end - field_pos;
    sbs.tf_mgr.unclaim_batch_size(sbs.tf_id, unclaimed_input);
    sbs.ps.next_batch_ready |= unclaimed_input > 0;
    sbs.tf_mgr.submit_batch_ready_for_more(
        sbs.tf_id,
        sbs.desired_batch_size - out_batch_size_rem,
        sbs.ps,
    );

    pending_seq_len_claimed
}

fn handle_enum_mode(mut sbs: SequenceBatchState) {
    let mut seq_size_rem = sbs.seq.ss.remaining_len(sbs.seq.current_value);
    let mut out_batch_size = 0;
    let mut drop_count = 0;
    let mut set_done = false;
    let group_iter_ref = sbs.seq.group_list_iter_ref.unwrap();
    let mut group_iter = sbs.rgt.lookup_group_list_iter_mut(
        group_iter_ref.list_id,
        group_iter_ref.iter_id,
        sbs.ab,
        sbs.seq.actor_id,
    );

    loop {
        let input_rem = sbs.batch_size - out_batch_size - drop_count;
        if input_rem == 0 {
            break;
        }
        let field_count = sbs
            .iter
            .next_n_fields(input_rem.min(group_iter.group_len_rem()), true);
        let count = (field_count as u64).min(seq_size_rem) as usize;
        group_iter.next_n_fields_in_group(count);
        sbs.seq.current_value = advance_sequence(
            sbs.seq,
            sbs.seq.current_value,
            sbs.output_field,
            count,
        );
        out_batch_size += count;
        let rem = field_count - count;
        if rem > 0 {
            if sbs.batch_size == out_batch_size
                || (sbs.is_split
                    && sbs.ps.input_done
                    && input_rem == field_count)
            {
                sbs.tf_mgr.unclaim_batch_size(sbs.tf_id, rem);
                break;
            }
            group_iter.drop(rem);
            drop_count += rem;
        }
        seq_size_rem -= count as u64;
        set_done = seq_size_rem == 0;
        if group_iter.is_end_of_group(sbs.ps.input_done) {
            set_done = false;
            sbs.seq.current_value = sbs.seq.ss.start;
            seq_size_rem = sbs.seq.seq_len_total;
            let groups_skipped = group_iter.skip_empty_groups();
            // otherwise we would loop infinitely
            debug_assert!(groups_skipped > 0 || field_count > 0);
        }
    }
    sbs.fm
        .store_iter(sbs.input_field_id, sbs.seq.iter_id, sbs.iter);
    group_iter.store_iter(group_iter_ref.iter_id);
    if sbs.ps.next_batch_ready {
        sbs.tf_mgr.push_successor_in_ready_queue(sbs.tf_id);
    }
    sbs.tf_mgr.submit_batch(
        sbs.tf_id,
        out_batch_size,
        sbs.ps.input_done || sbs.ps.successor_done || set_done,
    );
}

fn handle_enum_unbounded_mode(mut sbs: SequenceBatchState) {
    let field_pos_end = sbs.iter.get_next_field_pos() + sbs.batch_size;
    let mut out_batch_size_rem = sbs.desired_batch_size;

    let seq_len_total = sbs.seq.seq_len_total;
    let seq_len_trunc =
        usize::try_from(seq_len_total).unwrap_or(isize::MAX as usize);

    let mut yield_to_split = false;

    let mut seq_len_rem = sbs.seq.ss.remaining_len(sbs.seq.current_value);

    let group_iter_ref = sbs.seq.group_list_iter_ref.unwrap();
    let mut group_iter = sbs.rgt.lookup_group_list_iter_mut(
        group_iter_ref.list_id,
        group_iter_ref.iter_id,
        sbs.ab,
        sbs.seq.actor_id,
    );

    while out_batch_size_rem != 0 {
        let input_rem = field_pos_end - sbs.iter.get_next_field_pos();
        if input_rem == 0 {
            if seq_len_rem == 0 {
                yield_to_split = sbs.is_split;
                break;
            }
            if !sbs.ps.input_done {
                break;
            }
        }
        let field_count = sbs.iter.next_n_fields(
            out_batch_size_rem
                .min(input_rem)
                .min(group_iter.group_len_rem()),
            true,
        );
        group_iter.next_n_fields(field_count);
        let end_of_group = group_iter.is_end_of_group(sbs.ps.input_done);
        let seq_adv = if end_of_group {
            seq_len_rem.min(out_batch_size_rem as u64) as usize
        } else {
            seq_len_rem.min(field_count as u64) as usize
        };
        sbs.seq.current_value = advance_sequence(
            sbs.seq,
            sbs.seq.current_value,
            sbs.output_field,
            seq_adv,
        );
        seq_len_rem -= seq_adv as u64;

        out_batch_size_rem -= seq_adv;

        let fields_rem = field_count.saturating_sub(seq_adv);
        if fields_rem > 0 {
            if sbs.is_split && sbs.ps.input_done && input_rem == field_count {
                yield_to_split = true;
                break;
            }
            group_iter.drop(fields_rem);
            out_batch_size_rem -= fields_rem;
        }
        let fields_overhang = seq_adv.saturating_sub(field_count);
        if fields_overhang > 0 {
            group_iter
                .insert_fields(FieldValueRepr::Undefined, fields_overhang);
        }

        if !end_of_group {
            continue;
        }
        if !group_iter.try_next_group() {
            break;
        }
        sbs.seq.current_value = sbs.seq.ss.start;
        seq_len_rem = seq_len_total;

        // PERF: we could optimize this to a memcopy for the subsequent
        // ones
        while group_iter.group_len_rem() == 0 {
            advance_sequence(
                sbs.seq,
                sbs.seq.ss.start,
                sbs.output_field,
                seq_len_trunc,
            );
            group_iter.insert_fields(FieldValueRepr::Undefined, seq_len_trunc);
            out_batch_size_rem -= seq_len_trunc;
        }
    }
    let unclaimed_input = field_pos_end - sbs.iter.get_next_field_pos();
    sbs.fm
        .store_iter(sbs.input_field_id, sbs.seq.iter_id, sbs.iter);
    group_iter.store_iter(group_iter_ref.iter_id);
    sbs.tf_mgr.unclaim_batch_size(sbs.tf_id, unclaimed_input);
    sbs.ps.next_batch_ready |= unclaimed_input > 0;
    let seq_unfinished = seq_len_rem != 0 && !yield_to_split;
    if (sbs.ps.next_batch_ready && !yield_to_split)
        || (sbs.ps.input_done && seq_unfinished)
    {
        sbs.tf_mgr.push_tf_in_ready_stack(sbs.tf_id);
    }
    sbs.tf_mgr.submit_batch(
        sbs.tf_id,
        sbs.desired_batch_size - out_batch_size_rem,
        (sbs.ps.input_done || yield_to_split) && !seq_unfinished,
    );
}

fn advance_sequence(
    seq: &TfSequence,
    mut curr: i64,
    output_field: &mut Field,
    count: usize,
) -> i64 {
    if seq.non_string_reads {
        let mut inserter =
            output_field.iter_hall.fixed_size_type_inserter::<i64>();
        inserter.drop_and_reserve(count);
        for _ in 0..count {
            inserter.push(curr);
            curr += seq.ss.step;
        }
    } else {
        let mut inserter = output_field.iter_hall.inline_str_inserter();
        if curr >= 0 && seq.ss.step > 0 && seq.ss.step < FAST_SEQ_MAX_STEP {
            let mut int_str = ArrayVec::new();
            int_str.extend(i64_to_str(false, curr).as_bytes().iter().copied());
            inserter.drop_and_reserve(count, int_str.len());
            for _ in 0..count {
                inserter.push_may_rereserve(unsafe {
                    std::str::from_utf8_unchecked(&int_str)
                });
                for _ in 0..seq.ss.step {
                    increment_int_str(&mut int_str);
                }
                curr += seq.ss.step;
            }
        } else {
            for _ in 0..count {
                inserter.push_may_rereserve(&i64_to_str(false, curr));
                curr += seq.ss.step;
            }
        }
    }
    curr
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
    let ss = SequenceSpec { start, end, step };
    Ok(OperatorData::Sequence(OpSequence {
        ss,
        mode,
        non_string_reads: true,
        seq_len_total: ss.remaining_len(ss.start),
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
    create_op_sequence_with_opts(start, end, step, OpSequenceMode::Enum, None)
}
pub fn create_op_enum_unbounded(
    start: i64,
    end: i64,
    step: i64,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_sequence_with_opts(
        start,
        end,
        step,
        OpSequenceMode::EnumUnbounded,
        None,
    )
}
