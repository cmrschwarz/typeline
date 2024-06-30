use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect, VarId, DYN_VAR_ID,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    record_data::{action_buffer::ActorId, field_action::FieldActionKind},
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
    remaining: usize,
    actor_id: ActorId,
}

pub struct TfHeadSubtractive {
    drop_count: usize,
    actor_id: ActorId,
}

impl Operator for OpHead {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "head".into()
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
        // TODO: creae a nicer api for this usecase where we don't want
        // an output field
        jd.field_mgr
            .drop_field_refcount(tf_state.output_field, &mut jd.match_set_mgr);
        tf_state.output_field = tf_state.input_field;
        let res = if self.count < 0 {
            smallbox!(TfHeadSubtractive {
                drop_count: (-self.count) as usize,
                actor_id,
            })
        } else {
            smallbox!(TfHead {
                remaining: self.count as usize,
                actor_id
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
        let tf = &jd.tf_mgr.transforms[tf_id];

        if ps.successor_done {
            jd.tf_mgr.help_out_with_output_done(
                &mut jd.match_set_mgr,
                tf_id,
                self.actor_id,
                batch_size,
            );
            return;
        }

        if self.remaining >= batch_size {
            self.remaining -= batch_size;
            jd.tf_mgr
                .submit_batch(tf_id, batch_size, self.remaining == 0);
            return;
        }

        let rows_to_submit = self.remaining;
        let rows_to_drop = batch_size - rows_to_submit;
        self.remaining = 0;

        let mut ab = jd.match_set_mgr.match_sets[tf.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        ab.push_action(FieldActionKind::Drop, rows_to_submit, rows_to_drop);
        ab.end_action_group();
        jd.tf_mgr.submit_batch(tf_id, rows_to_submit, true);
    }
}

impl Transform<'_> for TfHeadSubtractive {
    fn display_name(&self) -> DefaultTransformName {
        "head".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let tf = &mut jd.tf_mgr.transforms[tf_id];
        // TODO: update clear delay for dynamic fields / aliases
        // PERF: e.g. for `head=-10`, we should never have to buffer
        // more than 10 elements, as we can already release the first
        // one if we have 11.
        // we could have two sets of all fields and then swap

        if !tf.predecessor_done {
            return;
        }
        let match_set_id = tf.match_set_id;
        let batch_size = tf.available_batch_size;
        tf.available_batch_size = 0;

        let mut ab = jd.match_set_mgr.match_sets[match_set_id]
            .action_buffer
            .borrow_mut();

        let rows_to_drop = batch_size.min(self.drop_count);
        let rows_to_submit = batch_size - rows_to_drop;

        ab.begin_action_group(self.actor_id);
        ab.push_action(FieldActionKind::Drop, rows_to_submit, rows_to_drop);
        ab.end_action_group();

        jd.tf_mgr.submit_batch(tf_id, rows_to_submit, true);
    }
}

pub fn create_op_head(count: isize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpHead {
        count,
        accessed_fields_after: Default::default(),
        dyn_var_accessed: false
    }))
}

pub fn parse_op_head(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let count = expr.require_at_most_one_number_arg()?.unwrap_or(1);
    Ok(create_op_head(count))
}
