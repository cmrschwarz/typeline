use crate::{
    job::{JobData, PipelineState, TransformManager},
    operators::transform::TransformId,
    record_data::{
        action_buffer::ActorId,
        field::{Field, FieldId, FieldManager},
        field_action::FieldActionKind,
        field_data::FieldValueRepr,
        field_data_ref::DestructuredFieldDataRef,
        group_track::{GroupIdxStable, GroupTrackIterRef, GroupTrackManager},
        iter::{field_iter::FieldIter, field_iterator::FieldIterator},
        iter_hall::FieldIterId,
        match_set::{MatchSetId, MatchSetManager},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratorMode {
    Foreach,
    Alongside,
    AlongsideUnbounded,
}

pub trait GeneratorSequence {
    type Inserter<'a>;
    fn seq_len_total(&self) -> u64;
    fn seq_len_rem(&self) -> u64;
    fn reset_sequence(&mut self);
    fn create_inserter<'a>(
        &mut self,
        field: &'a mut Field,
    ) -> Self::Inserter<'a>;
    fn advance_sequence(
        &mut self,
        inserter: &mut Self::Inserter<'_>,
        count: usize,
    );
}

pub struct GeneratorBatchState<'a, 'b, G: GeneratorSequence> {
    generator: &'a mut G,
    tf_id: TransformId,
    input_field_id: FieldId,
    input_iter_id: FieldIterId,
    actor_id: ActorId,
    ms_id: MatchSetId,
    msm: &'a MatchSetManager,
    rgt: &'a mut GroupTrackManager,
    fm: &'a FieldManager,
    tf_mgr: &'a mut TransformManager,
    iter: FieldIter<DestructuredFieldDataRef<'b>>,
    batch_size: usize,
    desired_batch_size: usize,
    group_to_truncate: Option<GroupIdxStable>,
    ps: PipelineState,
    inserter: G::Inserter<'a>,
    is_split: bool,
}

pub fn handle_generator_transform_update<G: GeneratorSequence>(
    jd: &mut JobData,
    tf_id: TransformId,
    input_iter_id: FieldIterId,
    actor_id: ActorId,
    group_iter_ref: GroupTrackIterRef,
    generator: &mut G,
    generator_mode: GeneratorMode,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];

    if ps.successor_done || tf.successor.is_none() {
        // TODO: use `help_out_with_output_done`
        if !tf.done {
            jd.tf_mgr.submit_batch(tf_id, 0, ps.group_to_truncate, true);
        }
        return;
    }

    let ms_id = tf.match_set_id;
    let is_split = tf.is_split;
    let group_to_truncate = ps.group_to_truncate;
    let tf_batch_size = tf.desired_batch_size;
    let input_field_id =
        jd.field_mgr.get_dealiased_field_id(&mut tf.input_field);

    let mut desired_batch_size = batch_size;
    if let Some(succ) = tf.successor {
        let succ = &jd.tf_mgr.transforms[succ];
        desired_batch_size = succ.desired_batch_size.max(tf_batch_size)
    }

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
            .lookup_iter(input_field_id, &input_field, input_iter_id);

    let inserter = generator.create_inserter(&mut output_field);

    let ss = GeneratorBatchState {
        tf_id,
        input_iter_id,
        actor_id,
        ms_id,
        input_field_id,
        generator,
        msm: &jd.match_set_mgr,
        rgt: &mut jd.group_track_manager,
        fm: &jd.field_mgr,
        tf_mgr: &mut jd.tf_mgr,
        group_to_truncate,
        iter,
        batch_size,
        desired_batch_size,
        ps,
        inserter,
        is_split,
    };

    match generator_mode {
        GeneratorMode::Foreach => {
            let pending_seq_len_claimed = handle_seq_mode(ss, group_iter_ref);
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
                    input_iter_id,
                );
                iter.next_n_fields(pending_seq_len_claimed, true);
                jd.field_mgr.store_iter(input_field_id, input_iter_id, iter);
            }
        }
        GeneratorMode::Alongside => handle_enum_mode(ss, group_iter_ref),
        GeneratorMode::AlongsideUnbounded => {
            handle_enum_unbounded_mode(ss, group_iter_ref)
        }
    }
}

// returns the claimed pending sequence len so we can move past it
fn handle_seq_mode<G: GeneratorSequence>(
    mut gbs: GeneratorBatchState<G>,
    group_iter_ref: GroupTrackIterRef,
) -> usize {
    let mut group_iter =
        gbs.rgt.lookup_group_track_iter(group_iter_ref, gbs.msm);

    let mut ab = gbs.msm.match_sets[gbs.ms_id].action_buffer.borrow_mut();
    ab.begin_action_group(gbs.actor_id);
    let mut field_pos = gbs.iter.get_next_field_pos();
    let field_pos_start = field_pos;
    let mut field_dup_count = 0;
    let field_pos_end = field_pos + gbs.batch_size;
    let mut out_batch_size_rem = gbs.desired_batch_size;

    let seq_len_total = gbs.generator.seq_len_total();
    let seq_len_trunc = usize::try_from(seq_len_total).unwrap_or(0);

    let mut seq_len_rem =
        if gbs.group_to_truncate == Some(group_iter.group_idx_stable()) {
            0
        } else {
            gbs.generator.seq_len_rem()
        };

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
            gbs.generator.advance_sequence(&mut gbs.inserter, count);
            let dup_count = count - usize::from(seq_done);
            ab.push_action(
                FieldActionKind::Dup,
                field_pos + field_dup_count,
                dup_count,
            );
            field_dup_count += dup_count;
            out_batch_size_rem -= count;
            if seq_done {
                gbs.iter.next_field();
                gbs.generator.reset_sequence();
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
        if seq_len_total == 0 {
            let bs_rem = field_pos_end - field_pos;
            ab.push_action(
                FieldActionKind::Drop,
                field_pos + field_dup_count,
                bs_rem,
            );
            field_pos += bs_rem;
            break;
        }
        let full_seqs_rem =
            (out_batch_size_rem as u64 / seq_len_total) as usize;
        let field_count = gbs.iter.next_n_fields(
            full_seqs_rem.max(1).min(field_pos_end - field_pos),
            true,
        );

        debug_assert!(field_count > 0 || field_pos == field_pos_end);

        // PERF: we could optimize this to a memcopy for the subsequent
        // ones
        for _ in 0..field_count {
            gbs.generator
                .advance_sequence(&mut gbs.inserter, seq_len_trunc);
            gbs.generator.reset_sequence();
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
    gbs.fm
        .store_iter(gbs.input_field_id, gbs.input_iter_id, gbs.iter);
    let fields_consumed = field_pos - field_pos_start;
    group_iter.next_n_fields(fields_consumed);
    group_iter.store_iter(group_iter_ref.iter_id);

    let unclaimed_input = field_pos_end - field_pos;
    gbs.tf_mgr.unclaim_batch_size(gbs.tf_id, unclaimed_input);
    gbs.ps.next_batch_ready |= unclaimed_input > 0;
    gbs.tf_mgr.submit_batch_ready_for_more(
        gbs.tf_id,
        gbs.desired_batch_size - out_batch_size_rem,
        gbs.ps,
    );

    pending_seq_len_claimed
}

fn handle_enum_mode<G: GeneratorSequence>(
    mut gbs: GeneratorBatchState<G>,
    group_iter_ref: GroupTrackIterRef,
) {
    // TODO: properly implement current_group_done
    let mut seq_size_rem = gbs.generator.seq_len_rem();
    let mut out_batch_size = 0;
    let mut drop_count = 0;
    let mut set_done = false;
    let mut group_iter = gbs.rgt.lookup_group_track_iter_mut(
        group_iter_ref.track_id,
        group_iter_ref.iter_id,
        gbs.msm,
        gbs.actor_id,
    );

    loop {
        let input_rem = gbs.batch_size - out_batch_size - drop_count;
        if input_rem == 0 {
            break;
        }
        let field_count = input_rem.min(group_iter.group_len_rem());
        let advance_count = (field_count as u64).min(seq_size_rem) as usize;
        let count = gbs.iter.next_n_fields(advance_count, true);
        debug_assert_eq!(advance_count, count);
        group_iter.next_n_fields_in_group(count);
        gbs.generator.advance_sequence(&mut gbs.inserter, count);
        out_batch_size += count;
        let rem = field_count - count;
        if rem > 0 {
            if gbs.batch_size == out_batch_size
                || (gbs.is_split
                    && gbs.ps.input_done
                    && input_rem == field_count)
            {
                gbs.tf_mgr.unclaim_batch_size(gbs.tf_id, rem);
                break;
            }
            group_iter.drop(rem);
            drop_count += rem;
        }
        seq_size_rem -= count as u64;
        set_done = seq_size_rem == 0;
        if group_iter.is_end_of_group(gbs.ps.input_done) {
            set_done = false;
            gbs.generator.reset_sequence();
            seq_size_rem = gbs.generator.seq_len_total();
            let groups_skipped = group_iter.skip_empty_groups();
            // otherwise we would loop infinitely
            debug_assert!(groups_skipped > 0 || field_count > 0);
        } else if group_iter.group_len_rem() == 0 {
            break;
        }
    }
    gbs.fm
        .store_iter(gbs.input_field_id, gbs.input_iter_id, gbs.iter);
    group_iter.store_iter(group_iter_ref.iter_id);
    let done = gbs.ps.input_done || gbs.ps.successor_done || set_done;
    if gbs.ps.next_batch_ready && !done {
        gbs.tf_mgr.push_successor_in_ready_queue(gbs.tf_id);
    }
    gbs.tf_mgr.submit_batch(
        gbs.tf_id,
        out_batch_size,
        gbs.group_to_truncate,
        done,
    );
}

fn handle_enum_unbounded_mode<G: GeneratorSequence>(
    mut bgs: GeneratorBatchState<G>,
    group_iter_ref: GroupTrackIterRef,
) {
    let field_pos_end = bgs.iter.get_next_field_pos() + bgs.batch_size;
    let mut out_batch_size_rem = bgs.desired_batch_size;

    let seq_len_total = bgs.generator.seq_len_total();
    let seq_len_trunc =
        usize::try_from(seq_len_total).unwrap_or(isize::MAX as usize);

    let mut group_iter = bgs.rgt.lookup_group_track_iter_mut(
        group_iter_ref.track_id,
        group_iter_ref.iter_id,
        bgs.msm,
        bgs.actor_id,
    );

    // helping out splitcat by yielding early
    let mut yield_to_split = false;

    let mut seq_len_rem =
        if bgs.group_to_truncate == Some(group_iter.group_idx_stable()) {
            0
        } else {
            bgs.generator.seq_len_rem()
        };

    while out_batch_size_rem != 0 {
        let input_rem = field_pos_end - bgs.iter.get_next_field_pos();
        if input_rem == 0 {
            if seq_len_rem == 0 {
                yield_to_split = bgs.is_split;
                break;
            }
            if !bgs.ps.input_done {
                break;
            }
        }
        let field_count = bgs.iter.next_n_fields(
            out_batch_size_rem
                .min(input_rem)
                .min(group_iter.group_len_rem()),
            true,
        );
        group_iter.next_n_fields(field_count);
        let end_of_group = group_iter.is_end_of_group(bgs.ps.input_done);
        let seq_adv = if end_of_group {
            seq_len_rem.min(out_batch_size_rem as u64) as usize
        } else {
            seq_len_rem.min(field_count as u64) as usize
        };
        bgs.generator.advance_sequence(&mut bgs.inserter, seq_adv);
        seq_len_rem -= seq_adv as u64;

        out_batch_size_rem -= seq_adv;

        let fields_rem = field_count.saturating_sub(seq_adv);
        if fields_rem > 0 {
            if bgs.is_split && bgs.ps.input_done && input_rem == field_count {
                yield_to_split = true;
                break;
            }
            group_iter.drop_backwards(fields_rem);
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
        bgs.generator.reset_sequence();
        seq_len_rem = seq_len_total;

        // PERF: we could optimize this to a memcopy for the subsequent
        // ones
        while group_iter.group_len_rem() == 0 {
            bgs.generator
                .advance_sequence(&mut bgs.inserter, seq_len_trunc);
            bgs.generator.reset_sequence();
            group_iter.insert_fields(FieldValueRepr::Undefined, seq_len_trunc);
            out_batch_size_rem -= seq_len_trunc;
        }
    }
    let unclaimed_input = field_pos_end - bgs.iter.get_next_field_pos();
    bgs.fm
        .store_iter(bgs.input_field_id, bgs.input_iter_id, bgs.iter);
    group_iter.store_iter(group_iter_ref.iter_id);
    bgs.tf_mgr.unclaim_batch_size(bgs.tf_id, unclaimed_input);
    bgs.ps.next_batch_ready |= unclaimed_input > 0;
    let seq_unfinished = seq_len_rem != 0 && !yield_to_split;
    if (bgs.ps.next_batch_ready && !yield_to_split)
        || (bgs.ps.input_done && seq_unfinished)
    {
        bgs.tf_mgr.push_tf_in_ready_stack(bgs.tf_id);
    }
    bgs.tf_mgr.submit_batch(
        bgs.tf_id,
        bgs.desired_batch_size - out_batch_size_rem,
        bgs.group_to_truncate,
        (bgs.ps.input_done || yield_to_split) && !seq_unfinished,
    );
}
