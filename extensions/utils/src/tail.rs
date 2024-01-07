use std::collections::HashMap;

use scr_core::{
    cli::parse_arg_value_as_str,
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx, DYN_VAR_ID,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{Operator, OperatorBase, OperatorData},
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorId, field::FieldId, field_action::FieldActionKind,
    },
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher,
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
    clear_delayed_fields: Vec<FieldId>,
}

pub struct TfTailAdditive {
    skip_count: usize,
    actor_id: ActorId,
}

impl Operator for OpTail {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "tail".into()
    }

    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        0
    }

    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        false
    }

    fn on_liveness_computed(
        &mut self,
        sess: &scr_core::context::SessionData,
        op_id: scr_core::operators::operator::OperatorId,
        ld: &LivenessData,
    ) {
        if self.additive_mode {
            // we don't need liveness information for additive mode
            return;
        }
        let accessed_vars = ld.accessed_names_afterwards(sess, op_id);
        if accessed_vars[DYN_VAR_ID as usize] {
            self.dyn_var_accessed = true;
            return;
        }
        for v_id in accessed_vars.iter_ones() {
            self.accessed_fields_after.push(ld.vars[v_id].get_name());
        }
    }

    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        access_flags: &mut AccessFlags,
    ) {
        access_flags.input_accessed = false;
    }

    fn build_transform(
        &self,
        jd: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData {
        let ab = &mut jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        let actor_id = ab.add_actor();
        jd.field_mgr
            .drop_field_refcount(tf_state.output_field, &mut jd.match_set_mgr);
        tf_state.output_field = tf_state.input_field;
        if !self.additive_mode {
            let mut clear_delayed_fields =
                Vec::with_capacity(self.accessed_fields_after.len());
            for &name in &self.accessed_fields_after {
                let Some(name) = name else {
                    clear_delayed_fields.push(tf_state.input_field);
                    continue;
                };
                if let Some(&field_id) = jd.match_set_mgr.match_sets
                    [tf_state.match_set_id]
                    .field_name_map
                    .get(&name)
                {
                    clear_delayed_fields.push(field_id);
                }
            }
            for &f in &clear_delayed_fields {
                jd.field_mgr.request_clear_delay(f);
            }
            return TransformData::Custom(smallbox!(TfTail {
                count: self.count,
                actor_id,
                clear_delayed_fields
            }));
        }
        TransformData::Custom(smallbox!(TfTailAdditive {
            skip_count: self.count,
            actor_id
        }))
    }
}

impl Transform for TfTail {
    fn display_name(&self) -> DefaultTransformName {
        "tail".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let tf = &mut jd.tf_mgr.transforms[tf_id];
        // TODO: update clear delay for dynamic fields / aliases
        // PERF: just like with the subtractive mode for head
        // this implementation *sucks* and will buffer the whole input

        if !tf.input_is_done {
            return;
        }
        let match_set_id = tf.match_set_id;
        let batch_size = tf.available_batch_size;
        tf.available_batch_size = 0;

        for &f in &self.clear_delayed_fields {
            jd.field_mgr.relinquish_clear_delay(f);
        }

        let ab = &mut jd.match_set_mgr.match_sets[match_set_id].action_buffer;

        let rows_to_submit = batch_size.min(self.count);
        let rows_to_drop = batch_size - rows_to_submit;

        ab.begin_action_group(self.actor_id);
        ab.push_action(FieldActionKind::Drop, 0, rows_to_drop);
        ab.end_action_group();

        jd.tf_mgr.submit_batch(tf_id, rows_to_submit, true);
    }
}

impl Transform for TfTailAdditive {
    fn display_name(&self) -> DefaultTransformName {
        "tail".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];

        if ps.output_batch_done {
            jd.tf_mgr.help_out_with_output_done(
                &mut jd.match_set_mgr,
                tf_id,
                self.actor_id,
                batch_size,
            );
            return;
        }

        if self.skip_count == 0 {
            jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
            return;
        }

        let rows_to_skip = self.skip_count.min(batch_size);
        let rows_to_submit = batch_size - rows_to_skip;
        self.skip_count -= rows_to_skip;

        let ab =
            &mut jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
        ab.begin_action_group(self.actor_id);
        ab.push_action(FieldActionKind::Drop, 0, rows_to_skip);
        ab.end_action_group();
        jd.tf_mgr.submit_batch(tf_id, rows_to_submit, ps.input_done);
    }
}

pub fn create_op_tail(count: usize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpTail {
        count,
        additive_mode: false,
        accessed_fields_after: Default::default(),
        dyn_var_accessed: Default::default()
    }))
}

pub fn create_op_tail_add(count: usize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpTail {
        count,
        additive_mode: true,
        accessed_fields_after: Default::default(),
        dyn_var_accessed: Default::default()
    }))
}

pub fn parse_op_tail(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_none() {
        return Ok(create_op_tail(1));
    };
    let value_str = parse_arg_value_as_str("tail", value, arg_idx)?.trim();
    let add_mode = value_str.starts_with('+');
    let count = parse_int_with_units::<isize>(value_str)
        .map_err(|msg| {
            OperatorCreationError::new_s(
                format!(
                    "failed to parse `tail` parameter as an integer: {msg}"
                ),
                arg_idx,
            )
        })?
        .abs();
    if add_mode {
        return Ok(create_op_tail_add(count as usize));
    }
    Ok(create_op_tail(count as usize))
}
