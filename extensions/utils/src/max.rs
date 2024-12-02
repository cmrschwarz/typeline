use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        operator::{
            Operator, OperatorData, OperatorId, PreboundOutputsMap,
            TransformInstatiation,
        },
        transform::{Transform, TransformData, TransformState},
        utils::any_number::AnyNumberRef,
    },
    record_data::{
        action_buffer::ActorId,
        field_action::FieldActionKind,
        field_value_ref::{FieldValueBlock, FieldValueSlice},
        group_track::GroupTrackIterRef,
        iter::{
            field_iterator::FieldIterOpts,
            field_value_slice_iter::FieldValueRangeIter,
        },
        iter_hall::FieldIterId,
    },
    utils::max_index::{max_index_f64, max_index_i64},
};

use scr_core::operators::utils::any_number::AnyNumber;

#[derive(Clone, Default)]
pub struct OpMax {}

pub struct TfMax {
    input_iter_id: FieldIterId,
    group_track_iter: GroupTrackIterRef,
    curr_max_value: Option<AnyNumber>,
    current_group_error_type: Option<OperatorApplicationError>,
    actor_id: ActorId,
}

impl Operator for OpMax {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "max".into()
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
        TransformInstatiation::Single(TransformData::from_custom(TfMax {
            actor_id: job.job_data.add_actor_for_tf_state(tf_state),
            group_track_iter: job
                .job_data
                .claim_group_track_iter_for_tf_state(tf_state),
            input_iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
            curr_max_value: None,
            current_group_error_type: None,
        }))
    }
}

fn merge_max_val(curr: &mut Option<AnyNumber>, new: AnyNumberRef) -> bool {
    let Some(curr) = curr.as_mut() else {
        *curr = Some(new.to_owned());
        return true;
    };
    if new > curr.as_ref() {
        *curr = new.to_owned();
        return true;
    }
    false
}

impl Transform<'_> for TfMax {
    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        jd.tf_mgr.prepare_output_field(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
        );
        let tf = &jd.tf_mgr.transforms[tf_id];
        let op_id = tf.op_id.unwrap();
        let ms_id = jd.tf_mgr.transforms[tf_id].match_set_id;
        let input_field = tf.input_field;
        let mut inserter =
            jd.field_mgr.get_varying_type_inserter(tf.output_field);
        let mut iter = jd.field_mgr.lookup_auto_deref_iter(
            &jd.match_set_mgr,
            input_field,
            self.input_iter_id,
        );
        let mut group_track_iter = jd
            .group_track_manager
            .lookup_group_track_iter(self.group_track_iter, &jd.match_set_mgr);

        let mut ab = jd.match_set_mgr.match_sets[ms_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let mut bs_rem = batch_size;
        let mut field_idx = iter.get_next_field_pos();
        let mut group_start = field_idx;
        let mut fields_produced = 0;
        let mut new_max_idx_found = None;
        let mut had_max_value = self.curr_max_value.is_some();
        loop {
            let mut gs_rem = group_track_iter.group_len_rem();

            let next_available = if gs_rem == 0 {
                let next = group_track_iter.try_next_group();
                if next {
                    group_track_iter.skip_empty_groups();
                    gs_rem = group_track_iter.group_len_rem();
                }
                next
            } else {
                false
            };

            if next_available || bs_rem == 0 {
                let hmv = usize::from(had_max_value);
                if let Some(new_max_id) = new_max_idx_found {
                    let leading_drop = (new_max_id - group_start) + hmv;
                    ab.push_action(
                        FieldActionKind::Drop,
                        group_start - hmv,
                        leading_drop,
                    );
                    ab.push_action(
                        FieldActionKind::Drop,
                        new_max_id + 1 - leading_drop,
                        ((field_idx - new_max_id) as usize).saturating_sub(1),
                    );
                    field_idx -= field_idx - group_start - (1 - hmv);
                } else {
                    ab.push_action(
                        FieldActionKind::Drop,
                        group_start,
                        field_idx - group_start,
                    );
                    field_idx -= field_idx - group_start;
                }
                new_max_idx_found = None;
            }
            if next_available || (bs_rem == 0 && ps.input_done) {
                if let Some(max) = self.curr_max_value.take() {
                    max.push(&mut inserter, true, false);
                    fields_produced += 1;
                }
                group_start = field_idx;
                had_max_value = false;
            }
            if bs_rem == 0 {
                break;
            }

            let range = iter
                .typed_range_fwd(
                    &jd.match_set_mgr,
                    gs_rem.min(bs_rem),
                    FieldIterOpts::default(),
                )
                .unwrap();
            group_track_iter.next_n_fields(range.base.field_count);
            bs_rem -= range.base.field_count;

            if self.current_group_error_type.is_some() {
                field_idx += range.base.field_count;
                continue;
            }
            let mut res_idx = field_idx;

            // declare these up here so they live long enough
            let mut max_val_f64;
            let mut max_val_i64;

            let res_val;
            match range.base.data {
                FieldValueSlice::Int(vals) => {
                    max_val_i64 = i64::MIN;
                    let mut iter =
                        FieldValueRangeIter::from_range(&range, vals);
                    while let Some(b) = iter.next_block() {
                        match b {
                            FieldValueBlock::Plain(v) => {
                                if let Some(idx) = max_index_i64(v) {
                                    if v[idx] > max_val_i64 {
                                        max_val_i64 = v[idx];
                                        res_idx = field_idx + idx;
                                    }
                                }
                                field_idx += v.len();
                            }
                            FieldValueBlock::WithRunLength(v, rl) => {
                                if *v > max_val_i64 {
                                    res_idx = field_idx;
                                    max_val_i64 = *v;
                                }
                                field_idx += rl as usize;
                            }
                        }
                    }
                    res_val = AnyNumberRef::Int(&max_val_i64);
                }
                FieldValueSlice::Float(vals) => {
                    let mut iter =
                        FieldValueRangeIter::from_range(&range, vals);

                    let (&v, rl) = iter.next().unwrap();
                    max_val_f64 = v;
                    let range_start = field_idx;
                    field_idx += rl as usize;
                    if max_val_f64.is_nan() {
                        while let Some((&v, rl)) = iter.next() {
                            if !v.is_nan() {
                                max_val_f64 = v;
                                res_idx = field_idx;
                                break;
                            }
                            field_idx += rl as usize;
                        }
                        if max_val_f64.is_nan() {
                            res_idx = range_start;
                        }
                    }

                    while let Some(b) = iter.next_block() {
                        match b {
                            FieldValueBlock::Plain(v) => {
                                if let Some(idx) = max_index_f64(v) {
                                    if v[idx] > max_val_f64 {
                                        max_val_f64 = v[idx];
                                        res_idx = field_idx + idx;
                                    }
                                }
                                field_idx += v.len();
                            }
                            FieldValueBlock::WithRunLength(v, rl) => {
                                if *v > max_val_f64 {
                                    res_idx = field_idx;
                                    max_val_f64 = *v;
                                }
                                field_idx += rl as usize;
                            }
                        }
                    }
                    res_val = AnyNumberRef::Float(&max_val_f64);
                }
                FieldValueSlice::BigInt(vals) => {
                    let mut iter =
                        FieldValueRangeIter::from_range(&range, vals);
                    let (v, rl) = iter.next().unwrap();
                    let mut max_val_ref_bi = v;
                    field_idx += rl as usize;
                    while let Some((v, rl)) = iter.next() {
                        if v > max_val_ref_bi {
                            max_val_ref_bi = v;
                            res_idx = field_idx;
                        }
                        field_idx += rl as usize;
                    }
                    res_val = AnyNumberRef::BigInt(max_val_ref_bi);
                }
                FieldValueSlice::BigRational(vals) => {
                    let mut iter =
                        FieldValueRangeIter::from_range(&range, vals);
                    let (v, rl) = iter.next().unwrap();
                    let mut max_val_ref_br = v;
                    field_idx += rl as usize;
                    while let Some((v, rl)) = iter.next() {
                        if v > max_val_ref_br {
                            max_val_ref_br = v;
                            res_idx = field_idx;
                        }
                        field_idx += rl as usize;
                    }
                    res_val = AnyNumberRef::BigRational(max_val_ref_br);
                }
                FieldValueSlice::Null(_)
                | FieldValueSlice::Undefined(_)
                | FieldValueSlice::TextInline(_)
                | FieldValueSlice::TextBuffer(_)
                | FieldValueSlice::BytesInline(_)
                | FieldValueSlice::BytesBuffer(_)
                | FieldValueSlice::Object(_)
                | FieldValueSlice::Array(_)
                | FieldValueSlice::Custom(_)
                | FieldValueSlice::Error(_)
                | FieldValueSlice::Argument(_)
                | FieldValueSlice::OpDecl(_)
                | FieldValueSlice::StreamValueId(_)
                | FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => {
                    self.current_group_error_type =
                        Some(OperatorApplicationError::new_s(
                            format!(
                                "operator `max` does not support type `{}`",
                                range.base.data.kind()
                            ),
                            op_id,
                        ));
                    field_idx += range.base.field_count;
                    continue;
                }
            }
            if merge_max_val(&mut self.curr_max_value, res_val) {
                new_max_idx_found = Some(res_idx);
            }
        }
        ab.end_action_group();
        group_track_iter.store_iter(self.group_track_iter.iter_id);
        jd.field_mgr
            .store_iter(input_field, self.input_iter_id, iter);

        jd.tf_mgr
            .submit_batch_ready_for_more(tf_id, fields_produced, ps);
    }
}

pub fn parse_op_max(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_max())
}

pub fn create_op_max() -> OperatorData {
    OperatorData::from_custom(OpMax {})
}
