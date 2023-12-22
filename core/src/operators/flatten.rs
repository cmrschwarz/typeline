use std::collections::HashMap;

use crate::{
    cli::reject_operator_argument,
    job_session::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{FieldId, FieldRefOffset},
        field_action::FieldActionKind,
        field_value::{Array, FieldValue, Object},
        iter_hall::IterId,
        push_interface::{PushInterface, VaryingTypeInserter},
        ref_iter::RefAwareTypedSliceIter,
        typed::TypedSlice,
    },
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    errors::OperatorCreationError,
    operator::{DefaultOperatorName, Operator, OperatorBase, OperatorData},
    transform::{
        basic_transform_update, BasicUpdateData, DefaultTransformName,
        Transform, TransformData, TransformId,
    },
};

#[derive(Default)]
pub struct OpFlatten {
    may_consume_input: bool,
}

pub struct TfFlatten {
    #[allow(unused)] // TODO
    may_consume_input: bool,
    input_iter_id: IterId,
    actor_id: ActorId,
    input_field_ref_offset: FieldRefOffset,
}

pub fn parse_op_flatten(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_argument("flatten", value, arg_idx)?;
    Ok(create_op_flatten())
}

pub fn create_op_flatten() -> OperatorData {
    OperatorData::Custom(smallbox!(OpFlatten::default()))
}

impl Operator for OpFlatten {
    fn default_name(&self) -> DefaultOperatorName {
        "flatten".into()
    }
    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        1
    }
    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        true
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &crate::context::Session,
        op_id: super::operator::OperatorId,
        ld: &LivenessData,
    ) {
        self.may_consume_input = ld.can_consume_nth_access(op_id, 0);
    }

    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        _flags: &mut AccessFlags,
    ) {
    }

    fn build_transform<'a>(
        &'a self,
        sess: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut super::transform::TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let cb = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        let input_field_ref_offset = sess.field_mgr.register_field_reference(
            tf_state.output_field,
            tf_state.input_field,
        );
        let tfe = TfFlatten {
            may_consume_input: self.may_consume_input,
            input_iter_id: sess.field_mgr.claim_iter(tf_state.input_field),
            actor_id: cb.add_actor(),
            input_field_ref_offset,
        };
        sess.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(cb.peek_next_actor_id());
        TransformData::Custom(smallbox!(tfe))
    }
}

fn insert_object_entry(
    value: &FieldValue,
    key: &str,
    inserter: &mut VaryingTypeInserter<
        &mut crate::record_data::field_data::FieldData,
    >,
) {
    let arr = if let FieldValue::Text(str) = value {
        Array::String(
            [
                key.to_string().into_boxed_str(),
                str.clone().into_boxed_str(),
            ]
            .to_vec()
            .into_boxed_slice(),
        )
    } else {
        Array::Mixed(
            [FieldValue::Text(key.to_string()), value.clone()]
                .to_vec()
                .into_boxed_slice(),
        )
    };
    inserter.push_array(arr, 1, true, false);
}

impl TfFlatten {
    fn basic_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        bud.match_set_mgr.match_sets[bud.match_set_id]
            .action_buffer
            .begin_action_group(self.actor_id);
        let mut field_idx = bud.iter.get_next_field_pos();
        let mut string_store = None;
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            match range.base.data {
                TypedSlice::Undefined(_)
                | TypedSlice::Null(_)
                | TypedSlice::Int(_)
                | TypedSlice::Float(_)
                | TypedSlice::StreamValueId(_)
                | TypedSlice::BigInt(_)
                | TypedSlice::Rational(_)
                | TypedSlice::BytesInline(_)
                | TypedSlice::TextInline(_)
                | TypedSlice::BytesBuffer(_)
                | TypedSlice::Custom(_)
                | TypedSlice::Error(_) => {
                    field_idx += range.base.field_count;
                    inserter.extend_from_ref_aware_range_smart_ref(
                        range,
                        true,
                        false,
                        true,
                        self.input_field_ref_offset,
                    );
                }
                TypedSlice::Object(objects) => {
                    let ab = &mut bud.match_set_mgr.match_sets
                        [bud.match_set_id]
                        .action_buffer;
                    for (v, rl) in
                        RefAwareTypedSliceIter::from_range(&range, objects)
                    {
                        let rl = rl as usize;
                        let len = v.len();
                        if len == 0 {
                            ab.push_action(
                                FieldActionKind::Drop,
                                field_idx,
                                rl,
                            );
                            continue;
                        }
                        let elem_count = len * rl;
                        if len != 1 {
                            ab.push_action(
                                FieldActionKind::Dup,
                                field_idx,
                                elem_count - rl,
                            );
                        }
                        field_idx += elem_count;
                        match v {
                            Object::KeysStored(d) => {
                                for (k, v) in d.iter() {
                                    insert_object_entry(v, k, &mut inserter);
                                }
                            }
                            Object::KeysInterned(d) => {
                                let ss =
                                    string_store.get_or_insert_with(|| {
                                        bud.session_data
                                            .string_store
                                            .write()
                                            .unwrap()
                                    });
                                for (&k, v) in d.iter() {
                                    insert_object_entry(
                                        v,
                                        ss.lookup(k),
                                        &mut inserter,
                                    );
                                }
                            }
                        }
                    }
                }
                TypedSlice::Array(arrays) => {
                    let ab = &mut bud.match_set_mgr.match_sets
                        [bud.match_set_id]
                        .action_buffer;
                    for (v, rl) in
                        RefAwareTypedSliceIter::from_range(&range, arrays)
                    {
                        let rl = rl as usize;
                        let len = v.len();
                        if len == 0 {
                            ab.push_action(
                                FieldActionKind::Drop,
                                field_idx,
                                rl,
                            );
                            continue;
                        }
                        let elem_count = len * rl;
                        if len != 1 {
                            ab.push_action(
                                FieldActionKind::Dup,
                                field_idx,
                                elem_count - rl,
                            );
                        }
                        field_idx += elem_count;
                        // PERF: we could optimize this for len 1 and for the
                        // zsts
                        for _ in 0..rl {
                            match v {
                                Array::Null(_) => {
                                    inserter.push_null(len, true)
                                }
                                Array::Undefined(_) => {
                                    inserter.push_undefined(len, true)
                                }
                                Array::Int(vals) => inserter.extend(
                                    vals.iter().copied(),
                                    true,
                                    false,
                                ),
                                Array::Bytes(vals) => inserter.extend(
                                    vals.iter().map(|v| v.to_vec()),
                                    true,
                                    false,
                                ),
                                Array::String(vals) => inserter
                                    .extend_from_strings(
                                        vals.iter().map(|v| v.to_string()),
                                        true,
                                        false,
                                    ),
                                Array::Error(vals) => inserter.extend(
                                    vals.iter().cloned(),
                                    true,
                                    false,
                                ),
                                Array::Array(vals) => inserter.extend(
                                    vals.iter().cloned(),
                                    true,
                                    false,
                                ),
                                Array::Object(vals) => inserter.extend(
                                    vals.iter().cloned(),
                                    true,
                                    false,
                                ),
                                Array::FieldReference(vals) => inserter
                                    .extend(vals.iter().cloned(), true, false),
                                Array::SlicedFieldReference(vals) => inserter
                                    .extend(vals.iter().cloned(), true, false),
                                Array::Custom(vals) => inserter.extend(
                                    vals.iter().cloned(),
                                    true,
                                    false,
                                ),
                                Array::Mixed(vals) => {
                                    for v in vals.iter() {
                                        inserter.push_field_value_clone(
                                            v, 1, true, false,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                TypedSlice::FieldReference(_)
                | TypedSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
        bud.match_set_mgr.match_sets[bud.match_set_id]
            .action_buffer
            .end_action_group();
        (field_idx, bud.input_done)
    }
}

impl Transform for TfFlatten {
    fn display_name(&self) -> DefaultTransformName {
        "flatten".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.basic_update(bud)
        });
    }
}
