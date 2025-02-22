use typeline_core::{
    cli::call_expr::CallExpr,
    job::{Job, JobData},
    operators::{
        errors::OperatorCreationError,
        operator::{
            Operator, OperatorId, OutputFieldKind, PreboundOutputsMap,
            TransformInstatiation,
        },
        transform::{Transform, TransformState},
    },
    record_data::{
        action_buffer::ActorId, field_action::FieldActionKind,
        field_value_ref::FieldValueSlice,
        iter::field_value_slice_iter::FieldValueRangeIter,
        iter_hall::FieldIterId,
    },
};

#[derive(Clone, Default)]
pub struct OpFilter {}

pub struct TfFilter {
    input_iter_id: FieldIterId,
    actor_id: ActorId,
}

impl Operator for OpFilter {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "filter".into()
    }

    fn output_count(
        &self,
        _sess: &typeline_core::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn output_field_kind(
        &self,
        _sess: &typeline_core::context::SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
        OutputFieldKind::SameAsInput
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfFilter {
            actor_id: job
                .job_data
                .add_actor_for_tf_state_ignore_output_field(tf_state),
            input_iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
        }))
    }
}

impl Transform<'_> for TfFilter {
    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: typeline_core::operators::transform::TransformId,
    ) {
        let tf = &jd.tf_mgr.transforms[tf_id];
        let ms_id = jd.tf_mgr.transforms[tf_id].match_set_id;
        let input_field = tf.input_field;
        let mut iter = jd.field_mgr.lookup_auto_deref_iter(
            &jd.match_set_mgr,
            input_field,
            self.input_iter_id,
        );
        let mut ab = jd.match_set_mgr.match_sets[ms_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let mut bs_rem = batch_size;
        let field_pos_start = iter.get_next_field_pos();
        let mut field_pos = field_pos_start;
        while bs_rem > 0 {
            // PERF: we could optimize this but in the interest of supporting
            // by using next_n_fields_with_fmt but we would have to
            // implement that for AutoDerefIter
            let range =
                iter.typed_range_fwd(&jd.match_set_mgr, bs_rem).unwrap();
            let count = range.base.field_count;
            match range.base.data {
                FieldValueSlice::Null(_) | FieldValueSlice::Undefined(_) => {
                    ab.push_action(FieldActionKind::Drop, field_pos, count);
                }
                FieldValueSlice::Bool(vals) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, vals)
                    {
                        let rl = rl as usize;
                        if *v {
                            field_pos += rl;
                        } else {
                            ab.push_action(
                                FieldActionKind::Drop,
                                field_pos,
                                rl,
                            );
                        }
                    }
                }
                FieldValueSlice::Int(_)
                | FieldValueSlice::BigInt(_)
                | FieldValueSlice::Float(_)
                | FieldValueSlice::BigRational(_)
                | FieldValueSlice::TextInline(_)
                | FieldValueSlice::TextBuffer(_)
                | FieldValueSlice::BytesInline(_)
                | FieldValueSlice::BytesBuffer(_)
                | FieldValueSlice::Object(_)
                | FieldValueSlice::Array(_)
                | FieldValueSlice::Custom(_)
                | FieldValueSlice::Error(_)
                | FieldValueSlice::Argument(_)
                | FieldValueSlice::OpDecl(_)
                | FieldValueSlice::StreamValueId(_)
                | FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => {
                    field_pos += count;
                }
            }
            bs_rem -= count;
        }
        ab.end_action_group();
        jd.field_mgr
            .store_iter(input_field, self.input_iter_id, iter);
        jd.tf_mgr.submit_batch_ready_for_more(
            tf_id,
            field_pos - field_pos_start,
            ps,
        );
    }
}

pub fn parse_op_filter(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_filter())
}

pub fn create_op_filter() -> Box<dyn Operator> {
    Box::new(OpFilter {})
}
