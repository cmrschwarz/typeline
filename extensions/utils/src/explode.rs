use std::cell::{RefCell, RefMut};

use indexmap::{indexmap, IndexMap};

use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId, OperatorName,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    record_data::{
        field::{FieldId, FieldManager, FieldRefOffset},
        field_data::FieldData,
        field_value::{FieldValue, Object},
        field_value_ref::FieldValueSlice,
        iter_hall::{IterId, IterKind},
        match_set::{MatchSetId, MatchSetManager},
        push_interface::PushInterface,
        ref_iter::RefAwareFieldValueRangeIter,
        scope_manager::ScopeManager,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    utils::{
        stable_vec::StableVec, string_store::StringStoreEntry,
        temp_vec::BorrowedVec,
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

const PENDING_FIELDS_CHUNK_SIZE: usize = 4;

pub struct TfExplode {
    target_fields: IndexMap<Option<StringStoreEntry>, TargetField>,
    // we need a stable vec so we can use references to it in the inserters
    pending_fields:
        StableVec<(RefCell<FieldData>, usize), PENDING_FIELDS_CHUNK_SIZE>,
    inserters: Vec<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    input_iter_id: IterId,
    input_field_field_ref_offset: FieldRefOffset,
}

// SAFETY: this type is not automatically sync because of
// StableVec and RefCell but we ensure that the those fields are
// always cleared between method calls
unsafe impl Send for TfExplode {}

pub fn parse_op_explode(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_explode())
}

pub fn create_op_explode() -> OperatorData {
    OperatorData::Custom(smallbox!(OpExplode::default()))
}

impl Operator for OpExplode {
    fn default_name(&self) -> OperatorName {
        "explode".into()
    }
    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }
    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        true
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        self.may_consume_input = ld.can_consume_nth_access(op_id, 0);
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.may_dup_or_drop = false;
        // Counterintuitively, this operator does not impact liveness analysis,
        // except through it's access of it's direct input.
        // The Liveness Analysis models the worst case (maximum amount of
        // fields accessed). This op does not *access* any vars other
        // that it's direct input. It may shadow any output, but we
        // have to assume the worst case, which is that it shadowed
        // none. This means that all outputs live before the op stay
        // alive afterwards.
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let input_field_field_ref_offset =
            job.job_data.field_mgr.register_field_reference(
                tf_state.output_field,
                tf_state.input_field,
            );
        let tfe = TfExplode {
            target_fields: indexmap! {
                None => TargetField::Present(tf_state.output_field)
            },
            inserters: Default::default(),
            pending_fields: Default::default(),
            input_iter_id: job.job_data.field_mgr.claim_iter_non_cow(
                tf_state.input_field,
                IterKind::Transform(
                    job.job_data.tf_mgr.transforms.peek_claim_id(),
                ),
            ),
            input_field_field_ref_offset,
        };
        TransformInstatiation::Simple(TransformData::Custom(smallbox!(tfe)))
    }
}

fn insert_into_key<'a>(
    pending_fields: &'a StableVec<
        (RefCell<FieldData>, usize),
        PENDING_FIELDS_CHUNK_SIZE,
    >,
    fm: &'a FieldManager,
    target_fields: &mut IndexMap<Option<StringStoreEntry>, TargetField>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    msm: &mut MatchSetManager,
    sm: &mut ScopeManager,
    match_set_id: MatchSetId,
    key: StringStoreEntry,
    value: &FieldValue,
    run_length: usize,
) {
    use indexmap::map::Entry;
    let inserter_idx = match target_fields.entry(Some(key)) {
        Entry::Occupied(e) => e.index(),
        Entry::Vacant(e) => {
            if let Some(field_id) =
                sm.lookup_field(msm.match_sets[match_set_id].active_scope, key)
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

impl Transform<'_> for TfExplode {
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
        let mut iter = jd.field_mgr.get_bounded_auto_deref_iter(
            input_field_id,
            &input_field,
            self.input_iter_id,
            batch_size,
        );

        while let Some(range) = iter.next_range(&jd.match_set_mgr) {
            match range.base.data {
                FieldValueSlice::Undefined(_)
                | FieldValueSlice::Null(_)
                | FieldValueSlice::Int(_)
                | FieldValueSlice::Float(_)
                | FieldValueSlice::StreamValueId(_)
                | FieldValueSlice::BigInt(_)
                | FieldValueSlice::BigRational(_)
                | FieldValueSlice::BytesInline(_)
                | FieldValueSlice::TextInline(_)
                | FieldValueSlice::TextBuffer(_)
                | FieldValueSlice::BytesBuffer(_)
                | FieldValueSlice::Array(_)
                | FieldValueSlice::Argument(_)
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
                    for (v, rl) in RefAwareFieldValueRangeIter::from_range(
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
                                        &mut jd.scope_mgr,
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
                                        &mut jd.scope_mgr,
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
        let first_actor = jd.field_mgr.fields[input_field_id]
            .borrow()
            .first_actor
            .get();
        let mut iter = std::mem::take(&mut self.pending_fields).into_iter();
        for (field, index) in &mut iter {
            jd.field_mgr.add_field_with_data(
                &mut jd.match_set_mgr,
                &mut jd.scope_mgr,
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
