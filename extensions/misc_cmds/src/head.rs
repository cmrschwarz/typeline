use std::collections::HashMap;

use bstr::ByteSlice;
use scr_core::{
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
pub struct OpHead {
    count: isize,
    accessed_fields_after: Vec<Option<StringStoreEntry>>,
    dyn_var_accessed: bool,
}

pub struct TfHead {
    remaining: usize,
    actor_id: ActorId,
}

impl Operator for OpHead {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "head".into()
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
        if self.count > 0 {
            // we don't need liveness information for normal mode
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
        // TODO: creae a nicer api for this usecase where we don't want
        // an output field
        jd.field_mgr
            .drop_field_refcount(tf_state.output_field, &mut jd.match_set_mgr);
        tf_state.output_field = tf_state.input_field;
        if self.count < 0 {
            unimplemented!("head subtract mode")
        }
        TransformData::Custom(smallbox!(TfHead {
            remaining: self.count as usize,
            actor_id
        }))
    }
}

impl Transform for TfHead {
    fn display_name(&self) -> DefaultTransformName {
        "head".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];

        if ps.output_done {
            // Help out with dropping records if the successor is done.
            // Since we have an action_buffer we might aswell use it.
            let ab = &mut jd.match_set_mgr.match_sets[tf.match_set_id]
                .action_buffer;
            ab.begin_action_group(self.actor_id);
            ab.push_action(FieldActionKind::Drop, 0, batch_size);
            ab.end_action_group();
            jd.tf_mgr.submit_batch(tf_id, 0, true);
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

        let ab =
            &mut jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
        ab.begin_action_group(self.actor_id);
        ab.push_action(FieldActionKind::Drop, rows_to_submit, rows_to_drop);
        ab.end_action_group();
        jd.tf_mgr.submit_batch(tf_id, rows_to_submit, true);
    }
}

pub fn create_op_head(count: isize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpHead {
        count,
        accessed_fields_after: Vec::new(),
        dyn_var_accessed: false
    }))
}

pub fn parse_op_head(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let Some(value) = value else {
        return Ok(create_op_head(1));
    };
    let value_str = value
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "failed to parse `head` parameter (invalid utf-8)",
                arg_idx,
            )
        })?
        .trim();
    let count = parse_int_with_units(value_str).map_err(|msg| {
        OperatorCreationError::new_s(
            format!("failed to parse `head` parameter as integer: {msg}"),
            arg_idx,
        )
    })?;
    Ok(create_op_head(count))
}
