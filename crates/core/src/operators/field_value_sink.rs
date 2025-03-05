use crate::{
    job::JobData,
    record_data::{
        field_data::FieldData,
        field_value::FieldValue,
        field_value_ref::FieldValueSlice,
        iter::{
            field_iterator::FieldIterator,
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::{
                AutoDerefIter, RefAwareBytesBufferIter,
                RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
                RefAwareInlineTextIter, RefAwareTextBufferIter,
            },
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
    },
};
use indexland_utils::counted_universe::CountedUniverse;
use metamatch::metamatch;
use std::sync::{Arc, Mutex, MutexGuard};

use super::{
    operator::{Operator, TransformInstatiation},
    transform::{Transform, TransformId, TransformState},
};

#[derive(Clone)]
pub enum FieldValueDataStorage {
    Rle(FieldData),
    Flat(Vec<FieldValue>),
}

#[derive(Clone)]
pub struct FieldValueSinkHandle {
    data: Arc<Mutex<FieldValueDataStorage>>,
}

impl FieldValueSinkHandle {
    pub fn new_rle() -> Self {
        Self {
            data: Arc::new(Mutex::new(FieldValueDataStorage::new_rle())),
        }
    }
    pub fn new_flat() -> Self {
        Self {
            data: Arc::new(Mutex::new(FieldValueDataStorage::new_flat())),
        }
    }
    pub fn get(&self) -> MutexGuard<FieldValueDataStorage> {
        self.data.lock().unwrap()
    }
}

#[derive(Clone)]
pub struct OpFieldValueSink {
    pub handle: FieldValueSinkHandle,
}

pub fn create_op_field_value_sink(
    handle: FieldValueSinkHandle,
) -> Box<dyn Operator> {
    Box::new(OpFieldValueSink { handle })
}

struct StreamValueHandle {
    start_idx: usize,
    run_len: usize,
}

pub struct TfFieldValueSink<'a> {
    handle: &'a Mutex<FieldValueDataStorage>,
    batch_iter: FieldIterId,
    stream_value_handles: CountedUniverse<usize, StreamValueHandle>,
}

impl Default for FieldValueDataStorage {
    fn default() -> Self {
        FieldValueDataStorage::Rle(FieldData::default())
    }
}

impl FieldValueDataStorage {
    pub fn new_rle() -> Self {
        FieldValueDataStorage::Rle(FieldData::default())
    }
    pub const fn new_flat() -> Self {
        FieldValueDataStorage::Flat(Vec::new())
    }
    pub fn flat(&self) -> Option<&Vec<FieldValue>> {
        match self {
            FieldValueDataStorage::Rle(_) => None,
            FieldValueDataStorage::Flat(v) => Some(v),
        }
    }
    pub fn rle(&self) -> Option<&FieldData> {
        match self {
            FieldValueDataStorage::Rle(v) => Some(v),
            FieldValueDataStorage::Flat(_) => None,
        }
    }
    pub fn flat_mut(&mut self) -> Option<&mut Vec<FieldValue>> {
        match self {
            FieldValueDataStorage::Rle(_) => None,
            FieldValueDataStorage::Flat(v) => Some(v),
        }
    }
    pub fn rle_mut(&mut self) -> Option<&mut FieldData> {
        match self {
            FieldValueDataStorage::Rle(v) => Some(v),
            FieldValueDataStorage::Flat(_) => None,
        }
    }
    pub fn field_count(&self) -> usize {
        match self {
            FieldValueDataStorage::Rle(v) => v.field_count(),
            FieldValueDataStorage::Flat(v) => v.len(),
        }
    }

    fn extend(&mut self, iter: impl IntoIterator<Item = FieldValue>) {
        match self {
            FieldValueDataStorage::Rle(v) => {
                v.extend_from_field_values_upacked(iter, true, true);
            }
            FieldValueDataStorage::Flat(v) => v.extend(iter),
        }
    }

    fn push(&mut self, v: FieldValue) {
        match self {
            FieldValueDataStorage::Rle(storage) => {
                storage.push_field_value_unpacked(v, 1, true, true);
            }
            FieldValueDataStorage::Flat(storage) => storage.push(v),
        }
    }
    pub fn take(&mut self) -> FieldValueDataStorage {
        match self {
            FieldValueDataStorage::Rle(_) => std::mem::replace(
                self,
                FieldValueDataStorage::Rle(FieldData::default()),
            ),
            FieldValueDataStorage::Flat(_) => std::mem::replace(
                self,
                FieldValueDataStorage::Flat(Vec::new()),
            ),
        }
    }
    pub fn take_flat(&mut self) -> Vec<FieldValue> {
        match self {
            FieldValueDataStorage::Rle(data) => {
                let mut iter = data.iter(false);
                // no deads in here
                debug_assert!(iter.is_next_valid_alive());
                let mut res = Vec::new();
                while let Some(v) = iter.typed_field_fwd(1) {
                    res.push(v.value.to_field_value());
                }
                data.clear();
                res
            }
            FieldValueDataStorage::Flat(v) => std::mem::take(v),
        }
    }
    pub fn take_rle(&mut self) -> FieldData {
        match self {
            FieldValueDataStorage::Rle(data) => std::mem::take(data),
            FieldValueDataStorage::Flat(data) => {
                let mut res = FieldData::default();
                res.extend_from_field_values_upacked(
                    std::mem::take(data),
                    true,
                    true,
                );
                res
            }
        }
    }
}

fn push_field_values(
    fvs: &mut FieldValueDataStorage,
    v: FieldValue,
    run_len: usize,
) {
    fvs.extend(std::iter::repeat_with(|| v.clone()).take(run_len - 1));
    fvs.push(v);
}

impl Operator for OpFieldValueSink {
    fn default_name(&self) -> super::operator::OperatorName {
        "field_value_sink".into()
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> super::operator::OutputFieldKind {
        super::operator::OutputFieldKind::SameAsInput
    }
    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: super::operator::OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: super::operator::OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfFieldValueSink {
            handle: &self.handle.data,
            batch_iter: job.job_data.claim_iter_for_tf_state(tf_state),
            stream_value_handles: CountedUniverse::default(),
        }))
    }
}

impl<'a> Transform<'a> for TfFieldValueSink<'a> {
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &mut jd.tf_mgr.transforms[tf_id];
        let input_field_id = tf.input_field;
        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);
        let base_iter = jd
            .field_mgr
            .lookup_iter(tf.input_field, &input_field, self.batch_iter)
            .bounded(0, batch_size);
        let starting_pos = base_iter.get_next_field_pos();
        let mut iter =
            AutoDerefIter::new(&jd.field_mgr, tf.input_field, base_iter);
        let mut fvs = self.handle.lock().unwrap();
        let mut field_pos = fvs.field_count();
        while let Some(range) =
            iter.typed_range_fwd(&jd.match_set_mgr, usize::MAX)
        {
            metamatch!(match range.base.data {
                #[expand(REP in [Null, Undefined])]
                FieldValueSlice::REP(_) => {
                    push_field_values(
                        &mut fvs,
                        FieldValue::REP,
                        range.base.field_count,
                    );
                }

                #[expand((REP, KIND, ITER, CONV) in [
                    (TextInline, Text, RefAwareInlineTextIter, v.to_string()),
                    (BytesInline, Bytes, RefAwareInlineBytesIter, v.to_vec()),
                    (TextBuffer, Text, RefAwareTextBufferIter, v.to_string()),
                    (BytesBuffer, Bytes, RefAwareBytesBufferIter, v.to_vec()),
                ])]
                FieldValueSlice::REP(text) => {
                    for (v, rl, _offs) in ITER::from_range(&range, text) {
                        push_field_values(
                            &mut fvs,
                            FieldValue::KIND(CONV),
                            rl as usize,
                        );
                    }
                }

                #[expand((REP, ITER, CONV) in [
                    (Bool, FieldValueRangeIter, *v),
                    (Int, FieldValueRangeIter, *v),
                    (Float, FieldValueRangeIter, *v),
                    (BigInt, RefAwareFieldValueRangeIter, Box::new(v.clone())),
                    (BigRational, RefAwareFieldValueRangeIter, Box::new(v.clone())),
                    (Argument, RefAwareFieldValueRangeIter, Box::new(v.clone())),
                    (OpDecl, RefAwareFieldValueRangeIter, v.clone()),
                    (Array, RefAwareFieldValueRangeIter, v.clone()),
                    (Object, RefAwareFieldValueRangeIter, Box::new(v.clone())),
                    (Custom, RefAwareFieldValueRangeIter, v.clone()),
                ])]
                FieldValueSlice::REP(text) => {
                    for (v, rl) in ITER::from_range(&range, text) {
                        push_field_values(
                            &mut fvs,
                            FieldValue::REP(CONV),
                            rl as usize,
                        );
                    }
                }

                FieldValueSlice::Error(errs) => {
                    for (v, rl) in
                        RefAwareFieldValueRangeIter::from_range(&range, errs)
                    {
                        push_field_values(
                            &mut fvs,
                            FieldValue::Error(v.clone()),
                            rl as usize,
                        );
                    }
                }

                FieldValueSlice::StreamValueId(sv_ids) => {
                    let mut pos = field_pos;
                    for (svid, rl) in
                        FieldValueRangeIter::from_range(&range, sv_ids)
                    {
                        let start_idx = pos;
                        let run_len = rl as usize;
                        pos += run_len;
                        let sv = &mut jd.sv_mgr.stream_values[*svid];
                        if !sv.done {
                            let handle_id =
                                self.stream_value_handles.claim_with_value(
                                    StreamValueHandle { start_idx, run_len },
                                );
                            sv.subscribe(*svid, tf_id, handle_id, true, true);
                            continue;
                        }
                        push_field_values(
                            &mut fvs,
                            sv.to_field_value(),
                            run_len,
                        );
                    }
                }

                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            });
            field_pos += range.base.field_count;
        }
        let base_iter = iter.into_base_iter();
        let consumed_fields = base_iter.get_next_field_pos() - starting_pos;
        // TODO: get rid of this once we removed the short fields mechanism
        // from sequence
        if consumed_fields < batch_size {
            push_field_values(
                &mut fvs,
                FieldValue::Undefined,
                batch_size - consumed_fields,
            );
        }
        jd.field_mgr
            .store_iter(input_field_id, self.batch_iter, base_iter);
        drop(input_field);
        let streams_done = self.stream_value_handles.is_empty();
        if streams_done && ps.next_batch_ready {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr.submit_batch(
            tf_id,
            batch_size,
            ps.group_to_truncate,
            ps.input_done,
        );
    }

    fn handle_stream_value_update(
        &mut self,
        jd: &mut JobData<'a>,
        svu: crate::record_data::stream_value::StreamValueUpdate,
    ) {
        let mut fvs = self.handle.lock().unwrap();
        let svh = &mut self.stream_value_handles[svu.custom];
        let sv = &mut jd.sv_mgr.stream_values[svu.sv_id];
        debug_assert!(sv.done);
        match &mut *fvs {
            FieldValueDataStorage::Rle(_field_data) => {
                todo!("implement value updating in FieldData");
            }
            FieldValueDataStorage::Flat(vec) => {
                for fv in &mut vec[svh.start_idx..svh.start_idx + svh.run_len]
                {
                    *fv = sv.to_field_value();
                }
            }
        }

        jd.sv_mgr.drop_field_value_subscription(svu.sv_id, None);
        self.stream_value_handles.release(svu.custom);
        if self.stream_value_handles.is_empty() {
            jd.tf_mgr.push_tf_in_ready_stack(svu.tf_id);
        }
    }
}
