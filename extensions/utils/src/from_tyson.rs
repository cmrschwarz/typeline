use std::{collections::HashMap, io::BufRead};

use scr_core::{
    extension::ExtensionRegistry,
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, OperatorBase, OperatorData, OperatorId},
        transform::{
            basic_transform_update, BasicUpdateData, DefaultTransformName,
            Transform, TransformData, TransformId, TransformState,
        },
    },
    record_data::{
        action_buffer::ActorRef,
        field::FieldId,
        field_data::{FieldData, RunLength},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueSliceIter,
        iter_hall::{IterId, IterKind},
        push_interface::PushInterface,
        ref_iter::{
            RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
        },
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataOffset, StreamValueDataType, StreamValueId,
        },
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    tyson::parse_tyson,
    utils::identity_hasher::BuildIdentityHasher,
};

#[derive(Clone, Default)]
pub struct OpFromTyson {}

pub struct TfFromTyson {
    input_iter_id: IterId,
}

impl Operator for OpFromTyson {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "from-tyson".into()
    }

    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        1
    }

    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        _access_flags: &mut AccessFlags,
    ) {
    }

    fn build_transform<'a>(
        &'a self,
        jd: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let ab = jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow();

        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        TransformData::Custom(smallbox!(TfFromTyson {
            input_iter_id: jd.field_mgr.claim_iter(
                tf_state.input_field,
                IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id())
            ),
        }))
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
        let fpm = bud.session_data.chains[bud.session_data.operator_bases
            [op_id as usize]
            .chain_id
            .unwrap() as usize]
            .settings
            .floating_point_math;
        let exts = Some(&*bud.session_data.extensions);
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            match range.base.data {
                FieldValueSlice::TextInline(vals) => {
                    for (v, rl, _offset) in
                        RefAwareInlineTextIter::from_range(&range, vals)
                    {
                        self.push_as_tyson(
                            exts,
                            &mut inserter,
                            v.as_bytes(),
                            rl,
                            op_id,
                            fpm,
                        );
                    }
                }
                FieldValueSlice::TextBuffer(vals) => {
                    for (v, rl, _offset) in
                        RefAwareTextBufferIter::from_range(&range, vals)
                    {
                        self.push_as_tyson(
                            exts,
                            &mut inserter,
                            v.as_bytes(),
                            rl,
                            op_id,
                            fpm,
                        );
                    }
                }
                FieldValueSlice::BytesInline(vals) => {
                    for (v, rl, _offset) in
                        RefAwareInlineBytesIter::from_range(&range, vals)
                    {
                        self.push_as_tyson(
                            exts,
                            &mut inserter,
                            v,
                            rl,
                            op_id,
                            fpm,
                        );
                    }
                }
                FieldValueSlice::BytesBuffer(vals) => {
                    for (v, rl, _offset) in
                        RefAwareBytesBufferIter::from_range(&range, vals)
                    {
                        self.push_as_tyson(
                            exts,
                            &mut inserter,
                            v,
                            rl,
                            op_id,
                            fpm,
                        );
                    }
                }
                FieldValueSlice::StreamValueId(vals) => {
                    for (&sv_id, rl) in
                        FieldValueSliceIter::from_range(&range, vals)
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
                                | StreamValueDataType::Bytes => (),
                                StreamValueDataType::VariableTypeArray
                                | StreamValueDataType::FixedTypeArray(_) => {
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
                                sv_id, bud.tf_id, out_sv_id, true, true,
                            )
                        }
                    }
                }
                FieldValueSlice::Undefined(_)
                | FieldValueSlice::Null(_)
                | FieldValueSlice::Int(_)
                | FieldValueSlice::Float(_)
                | FieldValueSlice::BigInt(_)
                | FieldValueSlice::Rational(_)
                | FieldValueSlice::Custom(_)
                | FieldValueSlice::Object(_)
                | FieldValueSlice::Array(_)
                | FieldValueSlice::Error(_) => {
                    inserter.push_fixed_size_type(
                        OperatorApplicationError::new_s(
                            format!(
                                "from-tyson can't handle values of type `{}`",
                                range.base.data.repr()
                            ),
                            bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap(),
                        ),
                        range.base.field_count,
                        true,
                        true,
                    );
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
        (bud.batch_size, bud.ps.input_done)
    }
}

impl Transform for TfFromTyson {
    fn display_name(&self) -> DefaultTransformName {
        "sum".into()
    }

    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.transform_update(bud)
        });
    }

    fn handle_stream_value_update(
        &mut self,
        _jd: &mut JobData,
        _tf_id: TransformId,
        _sv_id: StreamValueId,
        _custom: usize,
    ) {
    }
}

pub fn create_op_from_tyson() -> OperatorData {
    OperatorData::Custom(smallbox!(OpFromTyson {}))
}
