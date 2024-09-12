use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect, VarId, DYN_VAR_ID,
    },
    operators::{
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            OutputFieldKind, PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    record_data::{action_buffer::ActorId, group_track::GroupTrackIterRef},
    scr_error::ScrError,
    smallbox,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

#[derive(Default)]
pub struct OpHead {
    count: isize, // negative means all but the last n
    accessed_fields_after: Vec<Option<StringStoreEntry>>,
    dyn_var_accessed: bool,
}

pub struct TfHead {
    retain_total: usize,
    remaining: usize,
    actor_id: ActorId,
    group_track_iter: GroupTrackIterRef,
}

pub struct TfHeadSubtractive {
    drop_count: usize,
    actor_id: ActorId,
    group_track_iter: GroupTrackIterRef,
}

impl Operator for OpHead {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "head".into()
    }

    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> scr_core::operators::operator::OutputFieldKind {
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
        if self.count > 0 {
            // we don't need liveness information for normal mode
            return;
        }
        let accessed_vars = ld.accessed_names_afterwards(sess, op_id);
        if accessed_vars[DYN_VAR_ID.into_usize()] {
            self.dyn_var_accessed = true;
            return;
        }
        for v_id in accessed_vars.iter_ones().map(VarId::from_usize) {
            self.accessed_fields_after.push(ld.vars[v_id].get_name());
        }
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.input_accessed = false;
        None
    }

    fn build_transforms(
        &self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation {
        let jd = &mut job.job_data;
        let actor_id = jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow_mut()
            .add_actor();
        let group_track_iter =
            jd.claim_group_track_iter_for_tf_state(tf_state);

        let res = if self.count < 0 {
            let drop_count = (-self.count) as usize;
            smallbox!(TfHeadSubtractive {
                drop_count,
                actor_id,
                group_track_iter,
            })
        } else {
            let retain_count = self.count as usize;
            smallbox!(TfHead {
                retain_total: retain_count,
                remaining: retain_count,
                actor_id,
                group_track_iter,
            })
        };
        TransformInstatiation::Simple(TransformData::Custom(res))
    }
}

impl Transform<'_> for TfHead {
    fn display_name(&self) -> DefaultTransformName {
        "head".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let mut iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                self.group_track_iter,
                &jd.match_set_mgr,
                self.actor_id,
            );

        let mut batch_size_rem = batch_size;
        let mut output_count = 0;
        loop {
            let group_len_rem = iter.group_len_rem();
            let consumable = group_len_rem.min(batch_size_rem);
            batch_size_rem -= consumable;
            if consumable < self.remaining {
                output_count += consumable;
                if consumable != group_len_rem || !iter.try_next_group() {
                    self.remaining -= consumable;
                    iter.next_n_fields(consumable);
                    break;
                }
                self.remaining = self.retain_total;
                continue;
            }
            iter.next_n_fields(self.remaining);
            output_count += self.remaining;
            if consumable != group_len_rem {
                iter.next_n_fields(consumable);
                self.remaining -= consumable;
                break;
            }
            let overflow = group_len_rem - self.remaining;
            iter.drop(overflow);
            if !iter.try_next_group() {
                self.remaining = 0;
                break;
            }
            self.remaining = self.retain_total;
        }
        iter.store_iter(self.group_track_iter.iter_id);

        jd.tf_mgr.submit_batch(tf_id, output_count, ps.input_done);
    }
}

impl Transform<'_> for TfHeadSubtractive {
    fn display_name(&self) -> DefaultTransformName {
        "head".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let mut iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                self.group_track_iter,
                &jd.match_set_mgr,
                self.actor_id,
            );

        let mut batch_size_rem = batch_size;
        let mut output_count = 0;

        loop {
            let group_len_rem = iter.group_len_rem();
            let consumable = group_len_rem.min(batch_size_rem);
            let pass_count = consumable.saturating_sub(self.drop_count);
            if iter.is_last_group() && !ps.input_done {
                iter.next_n_fields(pass_count);
                batch_size_rem -= pass_count;
                output_count += pass_count;
                break;
            }

            batch_size_rem -= consumable;

            let drop_count = consumable - pass_count;
            iter.next_n_fields(pass_count);
            iter.drop(drop_count);
            if !iter.try_next_group() {
                break;
            }
        }
        iter.store_iter(self.group_track_iter.iter_id);
        jd.tf_mgr.unclaim_batch_size(tf_id, batch_size_rem);
        jd.tf_mgr.submit_batch(tf_id, output_count, ps.input_done);
    }
}

pub fn create_op_head(count: isize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpHead {
        count,
        accessed_fields_after: Default::default(),
        dyn_var_accessed: false
    }))
}

pub fn parse_op_head(expr: &CallExpr) -> Result<OperatorData, ScrError> {
    let count = expr.require_at_most_one_number_arg(false)?.unwrap_or(1);
    Ok(create_op_head(count))
}
