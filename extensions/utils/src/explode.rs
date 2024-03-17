use std::{
    cell::{RefCell, RefMut},
    collections::HashMap,
};

use indexmap::{indexmap, IndexMap};

use scr_core::{
    context::SessionData,
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{
            DefaultOperatorName, Operator, OperatorBase, OperatorData,
            OperatorId,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    options::argument::CliArgIdx,
    record_data::{
        field::{FieldId, FieldManager, FieldRefOffset},
        field_data::FieldData,
        field_value::{FieldValue, Object},
        field_value_ref::FieldValueSlice,
        iter_hall::{IterId, IterKind},
        match_set::{MatchSetId, MatchSetManager},
        push_interface::PushInterface,
        ref_iter::RefAwareFieldValueSliceIter,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher, stable_vec::StableVec,
        string_store::StringStoreEntry, temp_vec::BorrowedVec,
    },
};

#[derive(Default)]
pub struct OpExplode {
    may_consume_input: bool,
}

pub enum TargetField {
    Present(FieldId),
    Pending(u32),
}

pub struct TfExplode {
    target_fields: IndexMap<Option<StringStoreEntry>, TargetField>,
    inserters: Vec<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    pending_fields: StableVec<(RefCell<FieldData>, usize)>,
    input_iter_id: IterId,
    input_field_field_ref_offset: FieldRefOffset,
}

// SAFETY: this type is not automatically sync because of pending_fields:
// StableVec but we ensure that the StableVec is never exposed and only used
// by &mut self functions
unsafe impl Sync for TfExplode {}

pub fn parse_op_explode(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(create_op_explode())
}

pub fn create_op_explode() -> OperatorData {
    OperatorData::Custom(smallbox!(OpExplode::default()))
}

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

    fn on_liveness_computed(
        &mut self,
        _sess: &SessionData,
        op_id: OperatorId,
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
        // Counterintuitively, this operator does not impact liveness analysis.
        // LA models the worst case (maximum amount of fields accessed).
        // This op does not *access* any vars other that it's direct input.
        // It may shadow any output, but we have to assume the worst case,
        // which is that it shadowed none. This means that
        // all outputs live before the op stay alive afterwards.
    }

    fn build_transform<'a>(
        &'a self,
        jd: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let input_field_field_ref_offset =
            jd.field_mgr.register_field_reference(
                tf_state.output_field,
                tf_state.input_field,
            );
        let tfe = TfExplode {
            target_fields: indexmap! {
                None => TargetField::Present(tf_state.output_field)
            },
            inserters: Default::default(),
            pending_fields: Default::default(),
            input_iter_id: jd.field_mgr.claim_iter(
                tf_state.input_field,
                IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
            ),
            input_field_field_ref_offset,
        };
        TransformData::Custom(smallbox!(tfe))
    }
}

fn insert_into_key<'a>(
    pending_fields: &'a StableVec<(RefCell<FieldData>, usize)>,
    fm: &'a FieldManager,
    target_fields: &mut IndexMap<Option<StringStoreEntry>, TargetField>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    msm: &mut MatchSetManager,
    match_set_id: MatchSetId,
    key: StringStoreEntry,
    value: &FieldValue,
    run_length: usize,
) {
    use indexmap::map::Entry;
    let inserter_idx = match target_fields.entry(Some(key)) {
        Entry::Occupied(e) => e.index(),
        Entry::Vacant(e) => {
            if let Some(&field_id) =
                msm.match_sets[match_set_id].field_name_map.get(&key)
            {
                inserters.push(fm.get_varying_type_inserter(field_id));
                let idx = e.index();
                e.insert(TargetField::Present(field_id));
                idx
            } else {
                let idx = e.index();
                e.insert(TargetField::Pending(pending_fields.len() as u32));
                pending_fields.push((RefCell::new(FieldData::default()), idx));
                inserters.push(VaryingTypeInserter::new(
                    pending_fields.last().unwrap().0.borrow_mut(),
                ));
                idx
            }
        }
    };
    // PERF: maybe handle stealing?
    inserters[inserter_idx].push_field_value_unpacked(
        value.clone(),
        run_length,
        true,
        false,
    );
}

impl Transform for TfExplode {
    fn display_name(&self) -> DefaultTransformName {
        "explode".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let mut inserters = BorrowedVec::new(&mut self.inserters);
        let present_fields = self.target_fields.values().map(|v| {
            let TargetField::Present(f) = v else {
                unreachable!()
            };
            *f
        });
        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            present_fields.clone(),
        );
        for field_id in present_fields {
            inserters.push(jd.field_mgr.get_varying_type_inserter(field_id));
        }
        let match_set_id = jd.tf_mgr.transforms[tf_id].match_set_id;
        let input_field_id =
            jd.tf_mgr.get_input_field_id(&jd.field_mgr, tf_id);
        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
        let mut iter = jd.field_mgr.get_auto_deref_iter(
            input_field_id,
            &input_field,
            self.input_iter_id,
            batch_size,
        );

        while let Some(range) = iter.next_range(&mut jd.match_set_mgr) {
            match range.base.data {
                FieldValueSlice::Undefined(_)
                | FieldValueSlice::Null(_)
                | FieldValueSlice::Int(_)
                | FieldValueSlice::Float(_)
                | FieldValueSlice::StreamValueId(_)
                | FieldValueSlice::BigInt(_)
                | FieldValueSlice::Rational(_)
                | FieldValueSlice::BytesInline(_)
                | FieldValueSlice::TextInline(_)
                | FieldValueSlice::TextBuffer(_)
                | FieldValueSlice::BytesBuffer(_)
                | FieldValueSlice::Array(_)
                | FieldValueSlice::Custom(_)
                | FieldValueSlice::Error(_) => {
                    inserters[0].extend_from_ref_aware_range_smart_ref(
                        range,
                        true,
                        false,
                        true,
                        self.input_field_field_ref_offset,
                    );
                }
                FieldValueSlice::Object(objects) => {
                    let mut string_store = None;
                    for (v, rl) in RefAwareFieldValueSliceIter::from_range(
                        &range, objects,
                    ) {
                        match v {
                            Object::KeysStored(obj) => {
                                let ss =
                                    string_store.get_or_insert_with(|| {
                                        jd.session_data
                                            .string_store
                                            .write()
                                            .unwrap()
                                    });
                                for (k, v) in obj.iter() {
                                    insert_into_key(
                                        &self.pending_fields,
                                        &jd.field_mgr,
                                        &mut self.target_fields,
                                        &mut inserters,
                                        &mut jd.match_set_mgr,
                                        match_set_id,
                                        ss.intern_cloned(k),
                                        v,
                                        rl as usize,
                                    );
                                }
                            }
                            Object::KeysInterned(obj) => {
                                for (&k, v) in obj.iter() {
                                    insert_into_key(
                                        &self.pending_fields,
                                        &jd.field_mgr,
                                        &mut self.target_fields,
                                        &mut inserters,
                                        &mut jd.match_set_mgr,
                                        match_set_id,
                                        k,
                                        v,
                                        rl as usize,
                                    );
                                }
                            }
                        }
                    }
                    inserters[0].push_null(range.base.field_count, true);
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
        drop(inserters);
        jd.field_mgr
            .store_iter(input_field_id, self.input_iter_id, iter);
        drop(input_field);
        let first_actor =
            jd.field_mgr.fields[input_field_id].borrow().first_actor;
        let mut iter = std::mem::take(&mut self.pending_fields).into_iter();
        for (field, index) in &mut iter {
            jd.field_mgr.add_field_with_data(
                &mut jd.match_set_mgr,
                match_set_id,
                *self.target_fields.get_index(index).unwrap().0,
                first_actor,
                field.take(),
            );
        }
        self.pending_fields = iter.into_empty_vec();

        jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
    }
}
