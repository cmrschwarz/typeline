use metamatch::metamatch;
use scr_core::{
    self,
    context::SessionData,
    extension::ExtensionRegistry,
    job::{Job, JobData},
    operators::{
        errors::OperatorApplicationError,
        operator::{
            Operator, OperatorData, OperatorId, PreboundOutputsMap,
            TransformInstatiation,
        },
        transform::{Transform, TransformData, TransformState},
        utils::basic_transform_update::{
            basic_transform_update, BasicUpdateData,
        },
    },
    options::chain_settings::SettingUseFloatingPointMath,
    record_data::{
        field_data::{FieldData, RunLength},
        field_value::FieldValueKind,
        field_value_ref::FieldValueSlice,
        iter::{
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::{
                RefAwareBytesBufferIter, RefAwareInlineBytesIter,
                RefAwareInlineTextIter, RefAwareTextBufferIter,
            },
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataOffset, StreamValueDataType,
        },
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    tyson::parse_tyson,
    utils::indexing_type::IndexingType,
};
use std::io::BufRead;

#[derive(Clone, Default)]
pub struct OpFromTyson {}

pub struct TfFromTyson {
    input_iter_id: FieldIterId,
    use_floating_point_math: bool,
}

impl Operator for OpFromTyson {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "from_tyson".into()
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

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;

        let use_floating_point_math = jd
            .get_setting_from_tf_state::<SettingUseFloatingPointMath>(
                tf_state,
            );

        TransformInstatiation::Single(TransformData::Custom(smallbox!(
            TfFromTyson {
                input_iter_id: jd.claim_iter_for_tf_state(tf_state),
                use_floating_point_math
            }
        )))
    }
}

impl TfFromTyson {
    fn push_as_tyson(
        &self,
        exts: Option<&ExtensionRegistry>,
        inserter: &mut VaryingTypeInserter<&mut FieldData>,
        data: impl BufRead,
        rl: RunLength,
        op_id: OperatorId,
        fpm: bool,
    ) {
        match parse_tyson(data, fpm, exts) {
            Ok(v) => {
                inserter.push_field_value_unpacked(v, rl as usize, true, false)
            }
            Err(e) => inserter.push_error(
                OperatorApplicationError::new_s(e.to_string(), op_id),
                rl as usize,
                true,
                false,
            ),
        }
    }
    fn transform_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let op_id = bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap();
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        let fpm = self.use_floating_point_math;
        let exts = Some(&*bud.session_data.extensions);
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            metamatch!(match range.base.data {
                #[expand((REP, ITER, VAL) in [
                    (TextInline, RefAwareInlineTextIter, v.as_bytes()),
                    (BytesInline, RefAwareInlineBytesIter, v),
                    (TextBuffer, RefAwareTextBufferIter, v.as_bytes()),
                    (BytesBuffer, RefAwareBytesBufferIter, v),
                ])]
                FieldValueSlice::REP(text) => {
                    for (v, rl, _offs) in ITER::from_range(&range, text) {
                        self.push_as_tyson(
                            exts,
                            &mut inserter,
                            VAL,
                            rl,
                            op_id,
                            fpm,
                        );
                    }
                }

                #[expand_pattern(REP in [
                    Undefined, Null, Int, Float, Argument, OpDecl,
                    BigInt, BigRational, Custom, Object, Array, Error
                ])]
                FieldValueSlice::REP(_) => {
                    inserter.push_fixed_size_type(
                        OperatorApplicationError::new_s(
                            format!(
                                "from_tyson can't handle values of type `{}`",
                                range.base.data.repr()
                            ),
                            bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap(),
                        ),
                        range.base.field_count,
                        true,
                        true,
                    );
                }

                FieldValueSlice::StreamValueId(vals) => {
                    for (&sv_id, rl) in
                        FieldValueRangeIter::from_range(&range, vals)
                    {
                        let sv = &mut bud.sv_mgr.stream_values[sv_id];
                        if let Some(err) = &sv.error {
                            inserter.push_error(
                                (**err).clone(),
                                rl as usize,
                                true,
                                true,
                            );
                            continue;
                        }
                        if sv.done {
                            let dt = sv.data_type.unwrap();
                            match dt {
                                StreamValueDataType::Text
                                | StreamValueDataType::MaybeText
                                | StreamValueDataType::Bytes
                                | StreamValueDataType::SingleValue(
                                    FieldValueKind::Text,
                                )
                                | StreamValueDataType::SingleValue(
                                    FieldValueKind::Bytes,
                                ) => (),
                                StreamValueDataType::VariableTypeArray
                                | StreamValueDataType::FixedTypeArray(_)
                                | StreamValueDataType::SingleValue(_) => {
                                    inserter.push_error(
                                        OperatorApplicationError::new_s(
                                            format!(
                                                "cannot parse `{}` as tyson",
                                                dt.kind()
                                            ),
                                            op_id,
                                        ),
                                        rl as usize,
                                        true,
                                        true,
                                    );
                                    continue;
                                }
                            }
                            self.push_as_tyson(
                                exts,
                                &mut inserter,
                                sv.data_iter(StreamValueDataOffset::default()),
                                rl,
                                op_id,
                                fpm,
                            )
                        } else {
                            sv.make_buffered();
                            let out_sv_id = bud
                                .sv_mgr
                                .stream_values
                                .claim_with_value(StreamValue::from_data(
                                    None,
                                    StreamValueData::default(),
                                    StreamValueBufferMode::Stream,
                                    false,
                                ));

                            bud.sv_mgr.subscribe_to_stream_value(
                                sv_id,
                                bud.tf_id,
                                out_sv_id.into_usize(),
                                true,
                                true,
                            )
                        }
                    }
                }

                #[expand_pattern(REP in [FieldReference, SlicedFieldReference])]
                FieldValueSlice::REP(_) => unreachable!(),
            })
        }
        (bud.batch_size, bud.ps.input_done)
    }
}

impl Transform<'_> for TfFromTyson {
    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.transform_update(bud)
        });
    }
}

pub fn create_op_from_tyson() -> OperatorData {
    OperatorData::Custom(smallbox!(OpFromTyson {}))
}
