use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId, OperatorName,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{Transform, TransformData, TransformId, TransformState},
        utils::basic_transform_update::{
            basic_transform_update, BasicUpdateData,
        },
    },
    record_data::{
        field_value_ref::FieldValueSlice, iter_hall::FieldIterId,
        variable_sized_type_inserter::VariableSizeTypeInserter,
    },
    smallbox,
};

#[derive(Default)]
pub struct OpTypename {}

pub struct TfTypename {
    input_iter_id: FieldIterId,
}

pub fn parse_op_typename(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_typename())
}

pub fn create_op_typename() -> OperatorData {
    OperatorData::Custom(smallbox!(OpTypename::default()))
}

impl Operator for OpTypename {
    fn default_name(&self) -> OperatorName {
        "typename".into()
    }
    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }
    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
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
        output.flags.may_dup_or_drop = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let tfe = TfTypename {
            input_iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
        };
        TransformInstatiation::Single(TransformData::Custom(smallbox!(tfe)))
    }
}

impl Transform<'_> for TfTypename {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.basic_update(bud)
        });
    }
}

impl TfTypename {
    fn basic_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.inline_str_inserter();
        // TODO: get rid of these hardcoded reserves
        inserter.drop_and_reserve(bud.batch_size, 3);
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            match range.base.data {
                FieldValueSlice::Undefined(_)
                | FieldValueSlice::Null(_)
                | FieldValueSlice::Int(_)
                | FieldValueSlice::Float(_)
                | FieldValueSlice::BigInt(_)
                | FieldValueSlice::BigRational(_)
                | FieldValueSlice::TextInline(_)
                | FieldValueSlice::TextBuffer(_)
                | FieldValueSlice::BytesInline(_)
                | FieldValueSlice::BytesBuffer(_)
                | FieldValueSlice::Custom(_)
                | FieldValueSlice::Object(_)
                | FieldValueSlice::Error(_)
                | FieldValueSlice::Argument(_)
                | FieldValueSlice::OpDecl(_)
                | FieldValueSlice::Array(_) => {
                    inserter.push_with_rl(
                        range.base.data.repr().kind().to_str(),
                        range.base.field_count,
                    );
                }
                FieldValueSlice::StreamValueId(_) => {
                    // TODO: handle stream values properly?
                    inserter.push_with_rl(
                        range.base.data.repr().to_str(),
                        range.base.field_count,
                    );
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
        (bud.batch_size, bud.ps.input_done)
    }
}
