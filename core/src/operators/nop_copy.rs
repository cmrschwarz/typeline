use crate::{
    cli::reject_operator_argument,
    job::JobData,
    liveness_analysis::LivenessData,
    options::argument::CliArgIdx,
    record_data::{
        field::FieldRefOffset, iter_hall::IterId,
        push_interface::PushInterface,
    },
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
    utils::basic_transform_update::{basic_transform_update, BasicUpdateData},
};

// the main purpose of this op is as a helper operation for an aggregation
// at the start of a chain, e.g  `scr seqn=10 fork +int=11 p`

#[derive(Clone, Default)]
pub struct OpNopCopy {
    may_consume_input: bool,
}
pub struct TfNopCopy {
    #[allow(unused)] // TODO
    may_consume_input: bool,
    input_iter_id: IterId,
    input_field_ref_offset: FieldRefOffset,
}

pub fn parse_op_nop_copy(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_argument("nop-c", value, arg_idx)?;
    Ok(create_op_nop_copy())
}
pub fn create_op_nop_copy() -> OperatorData {
    OperatorData::NopCopy(OpNopCopy::default())
}

pub fn on_op_nop_copy_liveness_computed(
    op: &mut OpNopCopy,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    op.may_consume_input = ld.can_consume_nth_access(op_id, 0);
}

pub fn build_tf_nop_copy<'a>(
    jd: &mut JobData,
    op: &OpNopCopy,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let input_field_ref_offset = jd
        .field_mgr
        .register_field_reference(tf_state.output_field, tf_state.input_field);
    let tfc = TfNopCopy {
        may_consume_input: op.may_consume_input,
        input_iter_id: jd.add_iter_for_tf_state(tf_state),
        input_field_ref_offset,
    };
    TransformData::NopCopy(tfc)
}

impl TfNopCopy {
    fn basic_update(&self, bud: &mut BasicUpdateData) -> (usize, bool) {
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            inserter.extend_from_ref_aware_range_smart_ref(
                range,
                true,
                false,
                true,
                self.input_field_ref_offset,
            );
        }
        (bud.batch_size, bud.ps.input_done)
    }
}

pub fn handle_tf_nop_copy(
    jd: &mut JobData,
    tf_id: TransformId,
    nc: &TfNopCopy,
) {
    basic_transform_update(jd, tf_id, [], nc.input_iter_id, |mut bud| {
        nc.basic_update(&mut bud)
    });
}
