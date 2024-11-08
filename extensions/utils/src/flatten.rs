use std::{cell::RefMut, ops::ControlFlow};

use scr_core::{
    cli::call_expr::{Argument, CallExpr},
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::LivenessData,
    operators::{
        operator::{
            Operator, OperatorData, OperatorId, OperatorName,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
        utils::basic_transform_update::{
            basic_transform_update, BasicUpdateData,
        },
    },
    record_data::{
        action_buffer::{ActionBuffer, ActorId},
        array::Array,
        field::FieldRefOffset,
        field_action::FieldActionKind,
        field_data::FieldData,
        field_value::{FieldValue, Object},
        field_value_ref::FieldValueSlice,
        iter_hall::IterId,
        push_interface::PushInterface,
        ref_iter::RefAwareFieldValueRangeIter,
        varying_type_inserter::VaryingTypeInserter,
    },
    scr_error::ScrError,
    smallbox,
    utils::{lazy_lock_guard::LazyRwLockGuard, string_store::StringStore},
};

use metamatch::metamatch;

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

pub fn parse_op_flatten(expr: &CallExpr) -> Result<OperatorData, ScrError> {
    expr.reject_args()?;
    Ok(create_op_flatten())
}

pub fn create_op_flatten() -> OperatorData {
    OperatorData::Custom(smallbox!(OpFlatten::default()))
}

impl Operator for OpFlatten {
    fn default_name(&self) -> OperatorName {
        "flatten".into()
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

    fn on_liveness_computed(
        &mut self,
        _sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        self.may_consume_input = ld.can_consume_nth_access(op_id, 0);
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

        let tfe = TfFlatten {
            may_consume_input: self.may_consume_input,
            actor_id,
            input_iter_id,
            input_field_ref_offset,
        };

        TransformInstatiation::Single(TransformData::Custom(smallbox!(tfe)))
    }
}

fn insert_object_entry(
    value: &FieldValue,
    key: &str,
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    let arr = if let FieldValue::Text(str) = value {
        Array::Text(vec![key.to_string(), str.clone()])
    } else {
        Array::Mixed(vec![FieldValue::Text(key.to_string()), value.clone()])
    };
    inserter.push_array(arr, 1, true, false);
}

fn flatten_object(
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ab: &mut RefMut<ActionBuffer>,
    string_store: &mut LazyRwLockGuard<'_, StringStore>,
    field_idx: &mut usize,
    v: &Object,
    rl: u32,
) -> ControlFlow<()> {
    let rl = rl as usize;
    let len = v.len();
    if len == 0 {
        ab.push_action(FieldActionKind::Drop, *field_idx, rl);
        return ControlFlow::Break(());
    }
    let elem_count = len * rl;
    if len != 1 {
        ab.push_action(FieldActionKind::Dup, *field_idx, elem_count - rl);
    }
    *field_idx += elem_count;

    for _ in 0..rl {
        match v {
            Object::KeysStored(d) => {
                for (k, v) in d.iter() {
                    insert_object_entry(v, k, inserter);
                }
            }
            Object::KeysInterned(d) => {
                for (&k, v) in d.iter() {
                    insert_object_entry(
                        v,
                        string_store.get().lookup(k),
                        inserter,
                    );
                }
            }
        }
    }
    ControlFlow::Continue(())
}

fn flatten_array(
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ab: &mut RefMut<ActionBuffer>,
    _string_store: &mut LazyRwLockGuard<'_, StringStore>,
    field_idx: &mut usize,
    v: &Array,
    rl: u32,
) -> ControlFlow<()> {
    let rl = rl as usize;
    let len = v.len();
    if len == 0 {
        ab.push_action(FieldActionKind::Drop, *field_idx, rl);
        return ControlFlow::Continue(());
    }
    let elem_count = len * rl;
    if len != 1 {
        ab.push_action(FieldActionKind::Dup, *field_idx, elem_count - rl);
    }
    *field_idx += elem_count;
    // PERF: we could optimize this for len 1 and for the
    // zsts
    for _ in 0..rl {
        metamatch!(match v {
            Array::Null(_) => {
                inserter.push_null(len, true)
            }
            Array::Undefined(_) => {
                inserter.push_undefined(len, true)
            }

            #[expand(REP in [
                Int, Float, Array, Object, Argument, Macro,
                BigInt, BigRational, Custom,
                FieldReference, SlicedFieldReference,
                StreamValueId, Error,
            ])]
            Array::REP(vals) => {
                inserter.extend(vals.iter().cloned(), true, false)
            }

            #[expand((REP, PUSH_FN) in  [
                (Text, extend_from_strings),
                (Bytes, extend_from_bytes),
            ])]
            Array::REP(vals) => {
                inserter.PUSH_FN(vals.iter().map(|v| &**v), true, false)
            }

            Array::Mixed(vals) => {
                for v in vals.iter() {
                    inserter.push_field_value_unpacked(
                        v.clone(),
                        1,
                        true,
                        false,
                    );
                }
            }
        })
    }
    ControlFlow::Break(())
}

fn flatten_argument(
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ab: &mut RefMut<ActionBuffer>,
    string_store: &mut LazyRwLockGuard<'_, StringStore>,
    field_idx: &mut usize,
    v: &Argument,
    rl: u32,
) -> ControlFlow<()> {
    metamatch!(match &v.value {
        FieldValue::Undefined | FieldValue::Null |
        #[expand_pattern(REP in [
            Int, Float, StreamValueId, BigInt, Macro,
            BigRational, Text, Bytes,Custom, Error,
            FieldReference, SlicedFieldReference
        ])]
        FieldValue::REP(_) => {
            *field_idx += rl as usize;
            inserter.push_field_value_unpacked(
                v.value.clone(),
                rl as usize,
                true,
                false,
            );
            ControlFlow::Continue(())
        }

        FieldValue::Argument(v) => {
            flatten_argument(inserter, ab, string_store, field_idx, v, rl)
        }
        FieldValue::Object(v) => {
            flatten_object(inserter, ab, string_store, field_idx, v, rl)
        }
        FieldValue::Array(v) => flatten_array(
            inserter, ab, string_store, field_idx, v, rl
        ),
    })
}

impl TfFlatten {
    fn basic_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let mut output_field =
            bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        let mut ab = bud.match_set_mgr.match_sets[bud.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        let mut field_idx = bud.iter.get_next_field_pos();
        let mut string_store =
            LazyRwLockGuard::new(&bud.session_data.string_store);
        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            metamatch!(match range.base.data {
                #[expand_pattern(REP in [
                    Undefined, Null, Int, Float, StreamValueId, BigInt,
                    BigRational, TextInline, TextBuffer, BytesInline,
                    BytesBuffer, Custom, Error, Macro
                ])]
                FieldValueSlice::REP(_) => {
                    field_idx += range.base.field_count;
                    inserter.extend_from_ref_aware_range_smart_ref(
                        range,
                        true,
                        false,
                        true,
                        self.input_field_ref_offset,
                    );
                }
                #[expand((REP, FLATTEN_FN) in [
                    (Array, flatten_array),
                    (Object, flatten_object),
                    (Argument, flatten_argument),
                ])]
                FieldValueSlice::REP(arguments) => {
                    for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                        &range, arguments,
                    ) {
                        if let ControlFlow::Break(_) = FLATTEN_FN(
                            &mut inserter,
                            &mut ab,
                            &mut string_store,
                            &mut field_idx,
                            v,
                            rl,
                        ) {
                            break;
                        }
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

impl Transform<'_> for TfFlatten {
    fn display_name(&self) -> DefaultTransformName {
        "flatten".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.basic_update(bud)
        });
    }
}
