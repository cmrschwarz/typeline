use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    operators::{
        errors::OperatorApplicationError,
        operator::{
            Operator, OperatorId, OperatorName, PreboundOutputsMap,
            TransformInstatiation,
        },
        transform::{Transform, TransformData, TransformId, TransformState},
        utils::basic_transform_update::{
            basic_transform_update, BasicUpdateData,
        },
    },
    record_data::{
        action_buffer::ActorId,
        field::FieldRefOffset,
        field_action::FieldActionKind,
        field_value::SlicedFieldReference,
        field_value_ref::FieldValueSlice,
        iter::ref_iter::{
            RefAwareBytesBufferIter, RefAwareFieldValueRangeIter,
            RefAwareInlineBytesIter, RefAwareInlineTextIter,
            RefAwareTextBufferIter,
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
        stream_value::StreamValue,
    },
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use bstr::ByteSlice;

use metamatch::metamatch;

#[derive(Default)]
pub struct OpLines {}

pub struct TfLines {
    input_iter_id: FieldIterId,
    actor_id: ActorId,
    input_field_ref_offset: FieldRefOffset,
    pending_streams: usize,
}

pub fn parse_op_flatten(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, ScrError> {
    expr.reject_args()?;
    Ok(create_op_flatten())
}

pub fn create_op_flatten() -> Box<dyn Operator> {
    Box::new(OpLines::default())
}

impl Operator for OpLines {
    fn default_name(&self) -> OperatorName {
        "lines".into()
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
        _ld: &mut scr_core::liveness_analysis::LivenessData,
        _op_offset_after_last_write: scr_core::operators::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: scr_core::liveness_analysis::BasicBlockId,
        _input_field: scr_core::liveness_analysis::OpOutputIdx,
        output: &mut scr_core::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.non_stringified_input_access = false
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;

        let input_field_ref_offset = jd.field_mgr.register_field_reference(
            tf_state.output_field,
            tf_state.input_field,
        );
        let actor_id = jd.add_actor_for_tf_state(tf_state);
        let input_iter_id = jd.claim_iter_for_tf_state(tf_state);

        let tfe = TfLines {
            actor_id,
            input_iter_id,
            input_field_ref_offset,
            pending_streams: 0,
        };

        TransformInstatiation::Single(TransformData::from_custom(tfe))
    }
}

impl TfLines {
    fn basic_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let op_id = bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap();
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        let mut ab = bud.match_set_mgr.match_sets[bud.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        let mut field_idx = bud.iter.get_next_field_pos();

        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            metamatch!(match range.base.data {
                #[expand_pattern(REP in [
                    Undefined, Null, Int, Float, BigInt,
                    BigRational, Custom, Error, OpDecl, Array, Object, Argument
                ])]
                FieldValueSlice::REP(_) => {
                    field_idx += range.base.field_count;
                    inserter.push_error(
                        OperatorApplicationError::new_s(
                            format!(
                                "expected text or bytes, found `{}`",
                                range.base.data.kind()
                            ),
                            op_id,
                        ),
                        range.base.field_count,
                        true,
                        true,
                    );
                }
                #[expand((REP, ITER, PUSH_FN) in [
                    (TextInline, RefAwareInlineTextIter, push_str),
                    (TextBuffer, RefAwareTextBufferIter, push_str),
                    (BytesInline,RefAwareInlineBytesIter, push_bytes),
                    (BytesBuffer,RefAwareBytesBufferIter, push_bytes),
                ])]
                FieldValueSlice::REP(arguments) => {
                    let field_ref_offset = range
                        .field_ref_offset
                        .unwrap_or(self.input_field_ref_offset);
                    for (v, rl, offsets) in ITER::from_range(&range, arguments)
                    {
                        for _ in 0..rl {
                            let mut count = 0;
                            for line in v.lines() {
                                // TODO: generalize this and make it
                                // configurable
                                if line.len() < 32 {
                                    inserter.PUSH_FN(line, 1, true, false);
                                } else {
                                    let line_offset = unsafe {
                                        line.as_ptr()
                                            .byte_offset_from(v.as_ptr())
                                            as usize
                                    };
                                    let begin =
                                        offsets.from_begin + line_offset;
                                    inserter.push_sliced_field_reference(
                                        SlicedFieldReference {
                                            field_ref_offset,
                                            begin,
                                            end: begin + line.len(),
                                        },
                                        1,
                                        true,
                                        count == 0,
                                    );
                                }
                                count += 1;
                            }
                            ab.push_action(
                                FieldActionKind::Dup,
                                field_idx,
                                count - 1,
                            );
                            field_idx += 1;
                        }
                    }
                }
                FieldValueSlice::StreamValueId(ids) => {
                    for (&sv_id, rl) in
                        RefAwareFieldValueRangeIter::from_range(&range, ids)
                    {
                        if self.pending_streams == 0 {}
                        let output = bud
                            .sv_mgr
                            .claim_stream_value(StreamValue::default());
                        bud.sv_mgr.subscribe_to_stream_value(
                            sv_id,
                            bud.tf_id,
                            output.into_usize(),
                            false,
                            false,
                        );
                        field_idx += rl as usize;
                    }
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            })
        }
        ab.end_action_group();
        (field_idx, bud.ps.input_done)
    }
}

impl Transform<'_> for TfLines {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.basic_update(bud)
        });
    }

    fn handle_stream_value_update(
        &mut self,
        _jd: &mut JobData<'_>,
        _svu: scr_core::record_data::stream_value::StreamValueUpdate,
    ) {
        todo!()
    }
}
