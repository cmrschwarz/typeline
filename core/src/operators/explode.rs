use std::{cell::RefMut, collections::HashMap};

use indexmap::IndexMap;

use crate::{
    job_session::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    record_data::{
        field::{FieldId, FieldIdOffset},
        field_data::{FieldData, FieldValueRepr},
        field_value::FieldReference,
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

pub struct OpExplode {
    may_consume_input: bool,
}
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
        _bb_offset: u32,
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
        let mut tfe = TfExplode::default();
        tfe.target_fields.insert(None, tf_state.output_field);
        sess.field_mgr.register_field_reference(
            tf_state.output_field,
            tf_state.input_field,
        );
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
                        inserters[0].push_fixed_sized_type(*v, rl as usize);
                    }
                }
                TypedSlice::Float(floats) => {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, floats)
                    {
                        inserters[0].push_fixed_sized_type(*v, rl as usize);
                    }
                }
                TypedSlice::BigInt(_)
                | TypedSlice::Rational(_)
                | TypedSlice::BytesInline(_)
                | TypedSlice::TextInline(_)
                | TypedSlice::BytesBuffer(_)
                | TypedSlice::Array(_)
                | TypedSlice::Custom(_)
                | TypedSlice::Error(_) => {
                    let idx = if let Some(idx) = range.field_id_offset {
                        idx.get() + 1
                    } else {
                        0
                    };
                    let fr = FieldReference {
                        field_id_offset: FieldIdOffset::new(idx).unwrap(),
                    };
                    inserters[0]
                        .push_fixed_sized_type(fr, range.base.field_count);
                }
                TypedSlice::StreamValueId(vals) => {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, vals)
                    {
                        inserters[0].push_fixed_sized_type(*v, rl as usize);
                    }
                }
                TypedSlice::Object(_) => {}
                TypedSlice::FieldReference(_)
                | TypedSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
    }
}
