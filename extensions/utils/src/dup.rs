use std::collections::HashMap;

use scr_core::{
    cli::parse_arg_value_as_number,
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
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
        field_data::FieldValueRepr, iters::FieldIterator,
    },
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

#[derive(Default)]
pub struct OpDup {
    count: usize,
}

pub struct TfDup {
    count: usize,
    actor_id: ActorId,
}

impl Operator for OpDup {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "dup".into()
    }

    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        0
    }

    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        false
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &scr_core::context::SessionData,
        _op_id: scr_core::operators::operator::OperatorId,
        _ld: &LivenessData,
    ) {
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
        let actor_id = jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow_mut()
            .add_actor();
        jd.field_mgr
            .drop_field_refcount(tf_state.output_field, &mut jd.match_set_mgr);
        tf_state.output_field = tf_state.input_field;
        TransformData::Custom(smallbox!(TfDup {
            count: self.count,
            actor_id
        }))
    }
}

impl Transform for TfDup {
    fn display_name(&self) -> DefaultTransformName {
        "dup".into()
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
        if self.count == 0 {
            jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
            return;
        }

        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&mut jd.match_set_mgr, tf.input_field);

        let mut iter = input_field.iter();

        let mut ab = jd.match_set_mgr.match_sets[tf.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        let mut field_pos = 0;
        let mut bs_rem = batch_size;
        while bs_rem > 0 {
            let non_gs_records = iter.next_n_fields_with_fmt(
                batch_size,
                [FieldValueRepr::GroupSeparator],
                true,
                0,
                0,
                true,
            );
            for _ in 0..non_gs_records {
                ab.push_action(FieldActionKind::Dup, field_pos, self.count);
                field_pos += self.count + 1;
            }
            bs_rem -= non_gs_records;
            if bs_rem == 0 {
                break;
            }
            let gs_records = iter.next_n_fields_with_fmt(
                batch_size,
                [FieldValueRepr::GroupSeparator],
                false,
                0,
                0,
                true,
            );
            field_pos += gs_records;
            bs_rem -= gs_records;
            // prevent an infinite loop in case of an incorrect batch size
            assert!(non_gs_records > 0 || gs_records > 0);
        }
        ab.end_action_group();
        jd.tf_mgr.submit_batch(tf_id, field_pos, ps.input_done);
    }
}

pub fn create_op_dup(count: usize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpDup { count }))
}

pub fn parse_op_dup(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let count = if value.is_none() {
        2
    } else {
        parse_arg_value_as_number("dup", value, arg_idx)?
    };
    Ok(create_op_dup(count))
}
