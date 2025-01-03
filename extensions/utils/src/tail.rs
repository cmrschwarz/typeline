use typeline_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
        VarId, DYN_VAR_ID,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{
            OffsetInChain, Operator, OperatorId, OutputFieldKind,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{Transform, TransformData, TransformId, TransformState},
    },
    options::session_setup::SessionSetupData,
    record_data::{action_buffer::ActorId, group_track::GroupTrackIterRef},
    typeline_error::TypelineError,
    utils::{
        indexing_type::IndexingType,
        int_string_conversions::parse_int_with_units,
        string_store::StringStoreEntry,
    },
};

#[derive(Default)]
pub struct OpTail {
    count: usize,
    additive_mode: bool,
    accessed_fields_after: Vec<Option<StringStoreEntry>>,
    dyn_var_accessed: bool,
}

pub struct TfTail {
    count: usize,
    actor_id: ActorId,
    group_track_iter: GroupTrackIterRef,
}

pub struct TfTailAdditive {
    skip_count: usize,
    skips_remaining: usize,
    actor_id: ActorId,
    group_track_iter: GroupTrackIterRef,
}

impl Operator for OpTail {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        format!(
            "tail={}{}",
            if self.additive_mode { "+" } else { "" },
            self.count,
        )
        .into()
    }

    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> typeline_core::operators::operator::OutputFieldKind {
        OutputFieldKind::SameAsInput
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        0
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        if self.additive_mode {
            // we don't need liveness information for additive mode
            return;
        }
        let accessed_vars = ld.accessed_names_afterwards(sess, op_id);
        if accessed_vars[DYN_VAR_ID.into_usize()] {
            self.dyn_var_accessed = true;
            return;
        }
        for var_id in accessed_vars.iter_ones().map(VarId::from_usize) {
            self.accessed_fields_after.push(ld.vars[var_id].get_name());
        }
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        output.flags.input_accessed = false;
    }

    fn build_transforms(
        &self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation {
        let actor_id = job.job_data.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .action_buffer
            .borrow_mut()
            .add_actor();

        let group_track_iter =
            job.job_data.claim_group_track_iter_for_tf_state(tf_state);

        let res = if !self.additive_mode {
            Box::new(TfTail {
                count: self.count,
                actor_id,
                group_track_iter,
            }) as TransformData
        } else {
            Box::new(TfTailAdditive {
                skip_count: self.count,
                skips_remaining: self.count,
                actor_id,
                group_track_iter,
            }) as TransformData
        };
        TransformInstatiation::Single(res)
    }
}

impl Transform<'_> for TfTail {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let mut iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                self.group_track_iter,
                &jd.match_set_mgr,
                self.actor_id,
            );

        let mut output_count = 0;
        let mut batch_size_rem = batch_size;

        loop {
            let group_len_rem = iter.group_len_rem();
            let destined_to_drop = group_len_rem.saturating_sub(self.count);
            let droppable = destined_to_drop.min(batch_size_rem);
            iter.drop(droppable);

            if iter.is_last_group() && !ps.input_done {
                batch_size_rem -= droppable;
                break;
            }
            let consumable = group_len_rem.min(batch_size_rem);
            output_count += consumable - droppable;
            batch_size_rem -= consumable;
            if consumable != group_len_rem || !iter.try_next_group() {
                break;
            }
        }
        jd.tf_mgr.unclaim_batch_size(tf_id, batch_size_rem);
        iter.store_iter(self.group_track_iter.iter_id);
        jd.tf_mgr.submit_batch(
            tf_id,
            output_count,
            ps.group_to_truncate,
            ps.input_done,
        );
    }
}

impl Transform<'_> for TfTailAdditive {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let mut iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                self.group_track_iter,
                &jd.match_set_mgr,
                self.actor_id,
            );

        let mut output_count = 0;
        let mut batch_size_rem = batch_size;

        loop {
            let group_len_rem = iter.group_len_rem();
            let consumable = group_len_rem.min(batch_size_rem);
            batch_size_rem -= consumable;
            let acceptable = consumable.saturating_sub(self.skips_remaining);
            let droppable = consumable - acceptable;
            iter.drop(droppable);
            output_count += acceptable;
            if consumable == group_len_rem && iter.try_next_group() {
                self.skips_remaining = self.skip_count;
                continue;
            }
            self.skips_remaining -= droppable;
            iter.next_n_fields_in_group(acceptable);
            break;
        }
        iter.store_iter(self.group_track_iter.iter_id);
        jd.tf_mgr.submit_batch(
            tf_id,
            output_count,
            ps.group_to_truncate,
            ps.input_done,
        );
    }
}

pub fn create_op_tail(count: usize) -> Box<dyn Operator> {
    Box::new(OpTail {
        count,
        additive_mode: false,
        accessed_fields_after: Default::default(),
        dyn_var_accessed: Default::default(),
    })
}

pub fn create_op_tail_add(count: usize) -> Box<dyn Operator> {
    Box::new(OpTail {
        count,
        additive_mode: true,
        accessed_fields_after: Default::default(),
        dyn_var_accessed: Default::default(),
    })
}

pub fn parse_op_tail(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let arg = expr.require_at_most_one_arg()?;
    let Some(arg) = arg else {
        return Ok(create_op_tail(1));
    };
    let arg = arg.stringify_as_text(expr.op_name, sess)?;
    let add_mode = arg.starts_with('+');
    let count = parse_int_with_units::<isize>(arg.trim())
        .map_err(|msg| {
            OperatorCreationError::new_s(
                format!(
                    "failed to parse `tail` argument as an integer: {msg}"
                ),
                expr.span,
            )
        })?
        .abs();
    if add_mode {
        return Ok(create_op_tail_add(count as usize));
    }
    Ok(create_op_tail(count as usize))
}
