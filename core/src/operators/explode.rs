use std::collections::HashMap;

use crate::{
    job_session::JobData,
    liveness_analysis::{BasicBlockId, LivenessData, OpOutputIdx},
    record_data::field::FieldId,
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    operator::{DefaultOperatorName, Operator, OperatorBase},
    transform::{DefaultTransformName, Transform, TransformData, TransformId},
};

pub struct OpExplode {}
#[derive(Default)]
pub struct TfExplode {}

impl Operator for OpExplode {
    fn default_name(&self) -> DefaultOperatorName {
        "explode".into()
    }
    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        1
    }
    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        true
    }
    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        _bb_offset: u32,
        _input_accessed: &mut bool,
        _non_stringified_input_access: &mut bool,
        _may_dup_or_drop: &mut bool,
    ) {
    }

    fn build_transform<'a>(
        &'a self,
        _sess: &mut JobData,
        _op_base: &OperatorBase,
        _tf_state: &mut super::transform::TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        TransformData::Custom(smallbox!(TfExplode::default()))
    }
}

impl Transform for TfExplode {
    fn display_name(&self) -> DefaultTransformName {
        "explode".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        todo!()
    }
}
