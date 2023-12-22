use std::{
    cell::{RefCell, RefMut},
    collections::HashMap,
};

use indexmap::{indexmap, IndexMap};

use crate::{
    job_session::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    options::argument::CliArgIdx,
    record_data::{
        field::{FieldId, FieldManager, FieldRefOffset},
        field_data::FieldData,
        field_value::{FieldValue, Object},
        iter_hall::IterId,
        match_set::{MatchSetId, MatchSetManager},
        push_interface::{PushInterface, VaryingTypeInserter},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher, stable_vec::StableVec,
        string_store::StringStoreEntry, temp_vec::BorrowedVec,
    },
};

use super::{
    errors::OperatorCreationError,
    operator::{DefaultOperatorName, Operator, OperatorBase, OperatorData},
    transform::{DefaultTransformName, Transform, TransformData, TransformId},
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

// SAFETY: this type is not automatically sync because of pending_fields: StableVec
// but we ensure that the StableVec is never exposed and only used
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
    OperatorData::Explode(OpExplode::default())
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
        let input_field_field_ref_offset =
            sess.field_mgr.register_field_reference(
                tf_state.output_field,
                tf_state.input_field,
            );
        let tfe = TfExplode {
            target_fields: indexmap! {
                None => TargetField::Present(tf_state.output_field)
            },
            inserters: Default::default(),
            pending_fields: Default::default(),
            input_iter_id: sess.field_mgr.claim_iter(tf_state.input_field),
            input_field_field_ref_offset,
        };
        TransformData::Custom(smallbox!(tfe))
    }
}
fn fn_handle_object_key<'a>(
    target_fields: &mut IndexMap<Option<StringStoreEntry>, TargetField>,
    pending_fields: &'a StableVec<(RefCell<FieldData>, usize)>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    match_set_id: MatchSetId,
    msm: &mut MatchSetManager,
    fm: &'a FieldManager,
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
    inserters[inserter_idx]
        .push_field_value_clone(value, run_length, true, false);
}

impl Transform for TfExplode {
    fn display_name(&self) -> DefaultTransformName {
        "explode".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, input_done) = jd.tf_mgr.claim_batch(tf_id);
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
            .get_cow_field_ref(&mut jd.match_set_mgr, input_field_id);
        let mut iter = jd.field_mgr.get_auto_deref_iter(
            input_field_id,
            &input_field,
            self.input_iter_id,
            batch_size,
        );

        while let Some(range) = iter.next_range(&mut jd.match_set_mgr) {
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
                | TypedSlice::Array(_)
                | TypedSlice::Custom(_)
                | TypedSlice::Error(_) => {
                    inserters[0].extend_from_ref_aware_range_smart_ref(
                        range,
                        true,
                        false,
                        true,
                        self.input_field_field_ref_offset,
                    );
                }
                TypedSlice::Object(objects) => {
                    let mut string_store = None;
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, objects)
                    {
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
                                    fn_handle_object_key(
                                        &mut self.target_fields,
                                        &self.pending_fields,
                                        &mut inserters,
                                        match_set_id,
                                        &mut jd.match_set_mgr,
                                        &jd.field_mgr,
                                        ss.intern_cloned(k),
                                        v,
                                        rl as usize,
                                    );
                                }
                            }
                            Object::KeysInterned(obj) => {
                                for (&k, v) in obj.iter() {
                                    fn_handle_object_key(
                                        &mut self.target_fields,
                                        &self.pending_fields,
                                        &mut inserters,
                                        match_set_id,
                                        &mut jd.match_set_mgr,
                                        &jd.field_mgr,
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
                TypedSlice::FieldReference(_)
                | TypedSlice::SlicedFieldReference(_) => unreachable!(),
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

        if input_done {
            jd.unlink_transform(tf_id, batch_size);
        } else {
            jd.tf_mgr
                .inform_successor_batch_available(tf_id, batch_size);
        }
    }
}
