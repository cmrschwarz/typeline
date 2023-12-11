use std::{cell::RefMut, collections::HashMap};

use indexmap::IndexMap;

use crate::{
    job_session::JobData,
    liveness_analysis::{BasicBlockId, LivenessData, OpOutputIdx},
    record_data::{
        field::FieldId,
        field_data::{FieldData, FieldValueRepr},
        iter_hall::IterId,
        push_interface::VaryingTypeInserter,
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
        temp_vec::BorrowedVec,
    },
};

use super::{
    operator::{DefaultOperatorName, Operator, OperatorBase},
    transform::{DefaultTransformName, Transform, TransformData, TransformId},
};

pub struct OpExplode {}
#[derive(Default)]
pub struct TfExplode {
    target_fields:
        IndexMap<Option<StringStoreEntry>, FieldId, BuildIdentityHasher>,
    inserters: Vec<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    input_iter_id: IterId,
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
    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        _bb_offset: u32,
        _input_accessed: &mut bool,
        _non_stringified_input_access: &mut bool,
        _may_dup_or_drop: &mut bool,
    ) {
    }

    fn build_transform<'a>(
        &'a self,
        _sess: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut super::transform::TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let mut tfe = TfExplode::default();
        tfe.target_fields.insert(None, tf_state.output_field);
        TransformData::Custom(smallbox!(tfe))
    }
}

impl Transform for TfExplode {
    fn display_name(&self) -> DefaultTransformName {
        "explode".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, _end) = jd.tf_mgr.claim_batch(tf_id);
        let mut inserters = BorrowedVec::new(&mut self.inserters);
        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            self.target_fields.values().copied(),
        );
        for &sf in self.target_fields.values() {
            inserters
                .push(jd.field_mgr.get_varying_type_inserter(sf, batch_size));
        }
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
        while let Some(range) = iter.next(&mut jd.match_set_mgr) {
            match range.base.data {
                TypedSlice::Undefined(_) => inserters[0].push_zst(
                    FieldValueRepr::Undefined,
                    range.base.field_count,
                ),
                TypedSlice::Null(_) => inserters[0].push_zst(
                    FieldValueRepr::Undefined,
                    range.base.field_count,
                ),
                TypedSlice::Int(ints) => {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, ints)
                    {
                        inserters[0].push_int(*v, rl as usize);
                    }
                }
                TypedSlice::BigInt(ints) => {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, ints)
                    {
                        inserters[0]
                            .push_fixed_sized_type(v.clone(), rl as usize);
                    }
                }
                TypedSlice::Float(_) => todo!(),
                TypedSlice::Rational(_) => todo!(),
                TypedSlice::StreamValueId(_) => {
                    todo!()
                }
                TypedSlice::Error(_) => todo!(),
                TypedSlice::BytesInline(_) => {
                    todo!()
                }
                TypedSlice::TextInline(_) => {
                    todo!()
                }
                TypedSlice::BytesBuffer(_) => {
                    todo!()
                }
                TypedSlice::Object(_) => todo!(),
                TypedSlice::Array(_) => todo!(),
                TypedSlice::Custom(_) => todo!(),
                TypedSlice::FieldReference(_)
                | TypedSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
    }
}
