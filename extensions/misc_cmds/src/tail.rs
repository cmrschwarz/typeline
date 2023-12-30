use std::collections::HashMap;

use bstr::ByteSlice;
use scr_core::{
    job_session::JobData,
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
    },
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher,
        int_string_conversions::parse_int_with_units,
    },
};

#[derive(Default)]
pub struct OpTail {
    count: usize,
    add_mode: bool,
}

pub struct TfTailAdd {
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
        sess: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData {
        let ab = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        let actor_id = ab.add_actor();
        // TODO: creae a nicer api for this usecase where we don't want
        // an output field
        sess.field_mgr.drop_field_refcount(
            tf_state.output_field,
            &mut sess.match_set_mgr,
        );
        tf_state.output_field = tf_state.input_field;
        if !self.add_mode {
            unimplemented!("tail absolute mode")
        }
        TransformData::Custom(smallbox!(TfTailAdd {
            skip_count: self.count as usize,
            actor_id
        }))
    }
}

impl Transform for TfTailAdd {
    fn display_name(&self) -> DefaultTransformName {
        "tail".into()
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
        add_mode: false
    }))
}

pub fn create_op_tail_add(count: usize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpTail {
        count,
        add_mode: true
    }))
}

pub fn parse_op_tail(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let Some(value) = value else {
        return Ok(create_op_tail(1));
    };
    let value_str = value
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "failed to parse `tail` parameter (invalid utf-8)",
                arg_idx,
            )
        })?
        .trim();
    let add_mode = value_str.chars().next() == Some('+');
    let count = parse_int_with_units::<isize>(value_str)
        .map_err(|msg| {
            OperatorCreationError::new_s(
                format!("failed to parse `tail` parameter as integer: {msg}"),
                arg_idx,
            )
        })?
        .abs();
    if add_mode {
        return Ok(create_op_tail_add(count as usize));
    }
    Ok(create_op_tail(count as usize))
}
