use std::fmt::Write;

use crate::{
    chain::ChainId,
    cli::call_expr::Span,
    context::SessionData,
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{LivenessData, OpOutputIdx, OperatorLivenessOutput},
    options::session_setup::SessionSetupData,
    record_data::{
        action_buffer::ActorId, field_action::FieldActionKind,
        iter::field_iterator::FieldIterator, iter_hall::FieldIterId,
    },
    scr_error::ScrError,
    utils::{index_vec::IndexVec, indexing_type::IndexingType},
};

use super::{
    nop_copy::create_op_nop_copy,
    operator::{
        OffsetInAggregation, Operator, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain, PreboundOutputsMap,
        TransformInstatiation,
    },
    transform::{Transform, TransformId, TransformState},
};

pub struct OpAggregator {
    pub sub_ops_from_user: Vec<(Box<dyn Operator>, Span)>,
    pub sub_ops: IndexVec<OffsetInAggregation, OperatorId>,
}

pub const AGGREGATOR_DEFAULT_NAME: &str = "aggregator";

// TODO: rename this primitive to splitcat and support whole subchains
// Gives input records to the first subchain that accepts it, concatenates the
// results. Basic approach:
// - Sandwich subchains between a header and a trailer transform
// - Start by giving records to the first subchain.
// - Once a subchain proclaims `done`, move to the next one, including any
//   `available_batch_size` still remaining in the previous chain.
// - If the last subchain proclaims `done`, ignore it and keep feeding it
//   records.
// - If input records are available, always give each subchain at least one
//   record (TODO: make this configurable).
// - To achieve this, keep the last record received so far buffered and don't
//   hand it out to the current subchain.
// - Once `input_done` is received, hand out the last record but `dup` it
//   beforehand for any following subchains in case it is consumed.
pub struct TfAggregatorHeader {
    pub curr_sub_tf_idx: usize,
    pub sub_tfs: Vec<TransformId>,
    pub elem_buffered: bool,
    pub last_elem_multiplied: bool,
    pub actor_id: ActorId,
    pub iter_id: FieldIterId,
    pub trailer_tf_id: TransformId,
}

pub struct TfAggregatorTrailer {
    header_tf_id: TransformId,
}

pub fn create_op_aggregate(
    sub_ops: impl IntoIterator<Item = Box<dyn Operator>>,
) -> Box<dyn Operator> {
    Box::new(OpAggregator {
        sub_ops_from_user: sub_ops
            .into_iter()
            .map(|v| (v, Span::Generated))
            .collect(),
        sub_ops: IndexVec::new(),
    })
}

pub fn create_op_aggregate_appending(
    sub_ops: impl IntoIterator<Item = Box<dyn Operator>>,
) -> Box<dyn Operator> {
    create_op_aggregate(std::iter::once(create_op_nop_copy()).chain(sub_ops))
}

impl Operator for OpAggregator {
    fn default_name(&self) -> super::operator::OperatorName {
        AGGREGATOR_DEFAULT_NAME.into()
    }

    fn debug_op_name(&self) -> super::operator::OperatorName {
        let mut n = self.default_name().to_string();
        n.push('<');
        for (i, &so) in self.sub_ops.iter().enumerate() {
            if i > 0 {
                n.push_str(", ");
            }
            n.write_fmt(format_args!("{so}")).unwrap();
        }
        n.push('>');
        n.into()
    }

    fn output_count(&self, sess: &SessionData, _op_id: OperatorId) -> usize {
        let mut op_count = 1;
        // TODO: do this properly, merging field names etc.
        for &sub_op in &self.sub_ops {
            op_count += sess.operator_data[sess.op_data_id(sub_op)]
                .output_count(sess, sub_op)
                .saturating_sub(1);
        }
        op_count
    }

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        sess: &SessionData,
        _op_id: OperatorId,
    ) {
        for &sub_op in &self.sub_ops {
            ld.setup_op_vars(sess, sub_op);
        }
    }

    fn has_dynamic_outputs(
        &self,
        sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        for &sub_op in &self.sub_ops {
            if sess.operator_data[sess.op_data_id(sub_op)]
                .has_dynamic_outputs(sess, sub_op)
            {
                return true;
            }
        }
        false
    }

    fn update_variable_liveness(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        bb_id: crate::liveness_analysis::BasicBlockId,
        input_field: OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
        output.flags.input_accessed = false;
        for &sub_op_id in &self.sub_ops {
            let sub_op = &sess.operator_bases[sub_op_id];
            let outputs_start = sub_op.outputs_start;
            let outputs_end = sub_op.outputs_end;
            let mut sub_op_output =
                OperatorLivenessOutput::with_defaults(outputs_start);
            sess.operator_data[sess.op_data_id(sub_op_id)]
                .update_variable_liveness(
                    sess,
                    ld,
                    op_offset_after_last_write,
                    sub_op_id,
                    bb_id,
                    input_field,
                    &mut sub_op_output,
                );
            output.flags = output.flags.or(&sub_op_output.flags);

            if outputs_start != outputs_end {
                ld.op_outputs[output.primary_output]
                    .field_references
                    .push(outputs_start);
            }
        }
    }

    fn assign_op_outputs(
        &mut self,
        sess: &mut SessionData,
        ld: &mut LivenessData,
        op_id: OperatorId,
        output_count: &mut crate::liveness_analysis::OpOutputIdx,
    ) {
        let outputs_before = *output_count;
        // for the aggregation column
        ld.append_op_outputs(1, op_id);
        *output_count += OpOutputIdx::one();
        for &op_id in &self.sub_ops {
            sess.with_mut_op_data(op_id, |sess, op| {
                op.assign_op_outputs(sess, ld, op_id, output_count)
            });
        }
        let outputs_after = *output_count;
        let op_base = &mut sess.operator_bases[op_id];
        op_base.outputs_start = outputs_before;
        op_base.outputs_end = outputs_after;
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        _op_id: OperatorId,
    ) {
        for &op_id in &self.sub_ops {
            sess.with_mut_op_data(op_id, |sess, op_data| {
                op_data.on_liveness_computed(sess, ld, op_id)
            });
        }
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

        for (op_data, span) in std::mem::take(&mut self.sub_ops_from_user) {
            let op_id = sess.setup_op_from_data(
                op_data,
                chain_id,
                OperatorOffsetInChain::AggregationMember(
                    op_id,
                    self.sub_ops.next_idx(),
                ),
                span,
            )?;
            self.sub_ops.push(op_id);
        }

        Ok(op_id)
    }

    fn aggregation_member(
        &self,
        agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        self.sub_ops.get(agg_offset).copied()
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let op_count = self.sub_ops.len();
        let in_fid = tf_state.input_field;
        let out_fid = tf_state.output_field;
        let ms_id = tf_state.match_set_id;
        let desired_batch_size = tf_state.desired_batch_size;
        job.job_data
            .field_mgr
            .inc_field_refcount(in_fid, op_count + 1);
        job.job_data
            .field_mgr
            .inc_field_refcount(out_fid, op_count + 1);

        let active_group_track = tf_state.input_group_track_id;
        let actor_id = job.job_data.add_actor_for_tf_state(tf_state);
        let iter_id = job.job_data.claim_iter_for_tf_state(tf_state);

        tf_state.output_field = in_fid;

        let header_tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            tf_state.clone(),
            Box::new(TfAggregatorHeader {
                sub_tfs: Vec::new(),
                curr_sub_tf_idx: 0,
                elem_buffered: false,
                last_elem_multiplied: false,
                actor_id,
                iter_id,
                trailer_tf_id: TransformId::MAX_VALUE,
            }),
        );
        let mut sub_tfs = Vec::with_capacity(op_count);
        for (i, &sub_op_id) in self.sub_ops.iter().enumerate() {
            let mut sub_tf_state = TransformState::new(
                in_fid,
                out_fid,
                ms_id,
                desired_batch_size,
                Some(sub_op_id),
                active_group_track,
            );
            sub_tf_state.is_split = i + 1 != self.sub_ops.len();
            let instantiation = job.job_data.session_data.operator_data
                [job.job_data.session_data.op_data_id(sub_op_id)]
            .build_transforms_expand_single(
                job,
                sub_tf_state,
                sub_op_id,
                prebound_outputs,
            )
            .expect("aggregator on zero tf not supported");
            assert!(
                instantiation.next_match_set == ms_id,
                "aggregator does not support changing match sets"
            );
            sub_tfs.push(instantiation.tfs_begin);
        }
        let trailer_tf_state = TransformState::new(
            out_fid,
            out_fid,
            ms_id,
            desired_batch_size,
            Some(op_id),
            active_group_track,
        );
        let trailer_tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            trailer_tf_state,
            Box::new(TfAggregatorTrailer { header_tf_id }),
        );
        for &sub_tf_id in &sub_tfs {
            job.job_data.tf_mgr.transforms[sub_tf_id].successor =
                Some(trailer_tf_id);
        }
        let header = job.transform_data[header_tf_id]
            .downcast_mut::<TfAggregatorHeader>()
            .unwrap();
        header.trailer_tf_id = trailer_tf_id;

        header.sub_tfs = sub_tfs;
        job.job_data.tf_mgr.transforms[header_tf_id].successor =
            Some(header.sub_tfs.first().copied().unwrap_or(trailer_tf_id));

        TransformInstatiation::Multiple(OperatorInstantiation {
            tfs_begin: header_tf_id,
            tfs_end: trailer_tf_id,
            next_match_set: ms_id,
            next_input_field: out_fid,
            next_group_track: active_group_track,
        })
    }
    fn update_bb_for_op(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        _op_id: OperatorId,
        op_n: super::operator::OffsetInChain,
        cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        for &sub_op in &self.sub_ops {
            ld.update_bb_for_op(sess, sub_op, op_n, cn, bb_id);
        }
        false
    }
}

impl<'a> Transform<'a> for TfAggregatorHeader {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        "aggregator_header".into()
    }

    fn collect_out_fields(
        &self,
        _jd: &JobData,
        tf_state: &TransformState,
        fields: &mut Vec<crate::record_data::field::FieldId>,
    ) {
        fields.push(tf_state.output_field)
    }

    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (mut batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let sub_tf_count = self.sub_tfs.len();
        if sub_tf_count == self.curr_sub_tf_idx
            || (!ps.next_batch_ready && !ps.input_done && batch_size == 0)
        {
            return;
        }

        let tf = &mut jd.tf_mgr.transforms[tf_id];
        let prev_sub_tf_id = tf.successor.unwrap();
        let sub_tf_id = self.sub_tfs[self.curr_sub_tf_idx];
        let iter_id = self.iter_id;
        let input_field_id = tf.input_field;
        let ms_id = tf.match_set_id;
        let actor_id = self.actor_id;
        let sub_tfs_after = sub_tf_count - self.curr_sub_tf_idx - 1;
        let last_sc = sub_tfs_after == 0;

        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, input_field_id);

        let mut iter = jd.field_mgr.lookup_iter(
            input_field_id,
            &input_field,
            self.iter_id,
        );

        if prev_sub_tf_id != sub_tf_id {
            tf.successor = Some(sub_tf_id);
            // in case the previous op left behind unclaimed records,
            // we reclaim them and give them to the next
            let prev_tf = &mut jd.tf_mgr.transforms[prev_sub_tf_id];
            batch_size += prev_tf
                .available_batch_size
                .saturating_sub(usize::from(self.last_elem_multiplied));
            prev_tf.available_batch_size = 0;
        }

        if !ps.input_done {
            if last_sc && self.elem_buffered {
                batch_size += 1;
                self.elem_buffered = false;
            }
            if !last_sc && !self.elem_buffered && batch_size > 0 {
                batch_size -= 1;
                // TODO: don't buffer unneccessarily. think of the streams. dup
                // instead. // HACK
                self.elem_buffered = true;
            }
            iter.next_n_fields(batch_size, true);
            jd.field_mgr.store_iter(input_field_id, iter_id, iter);
            jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
            return;
        }

        if self.elem_buffered {
            self.elem_buffered = false;
            batch_size += 1;
        }
        if self.last_elem_multiplied {
            batch_size += 1;
        }

        if !last_sc && !self.last_elem_multiplied && batch_size > 0 {
            let pos = iter.get_next_field_pos() + batch_size - 1;
            let mut ab = jd.match_set_mgr.match_sets[ms_id]
                .action_buffer
                .borrow_mut();
            ab.begin_action_group(actor_id);
            ab.push_action(FieldActionKind::Dup, pos, sub_tfs_after);
            ab.end_action_group();

            self.last_elem_multiplied = true;
            iter.next_n_fields(batch_size - 1, true);
            jd.field_mgr.store_iter(input_field_id, iter_id, iter);
            drop(ab);
            drop(input_field);
            // this implicitly applies the dup so when we move we don't skip over
            // the dup'ed field entirely, but just over one instance
            jd.field_mgr.move_iter(
                &mut jd.match_set_mgr,
                input_field_id,
                iter_id,
                1,
            );
        } else {
            iter.next_n_fields(batch_size, true);
            jd.field_mgr.store_iter(input_field_id, iter_id, iter);
        }
        if !last_sc {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr
            .inform_transform_batch_available(sub_tf_id, batch_size, true);
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

impl<'a> Transform<'a> for TfAggregatorTrailer {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        "aggregator_trailer".into()
    }
    fn pre_update_required(&self) -> bool {
        true
    }
    fn pre_update(
        &mut self,
        _ctx: Option<&std::sync::Arc<crate::context::ContextData>>,
        job: &mut Job<'a>,
        tf_id: TransformId,
    ) -> Result<(), crate::context::VentureDescription> {
        let (batch_size, mut ps) = job.job_data.tf_mgr.claim_all(tf_id);
        if ps.input_done {
            let header_tf_id = self.header_tf_id;

            let agg_h = job.transform_data[header_tf_id]
                .downcast_mut::<TfAggregatorHeader>()
                .unwrap();
            agg_h.curr_sub_tf_idx += 1;
            job.job_data.tf_mgr.transforms[tf_id].predecessor_done = false;
            ps.input_done = agg_h.curr_sub_tf_idx == agg_h.sub_tfs.len();
        }
        job.job_data.tf_mgr.submit_batch(
            tf_id,
            batch_size,
            ps.group_to_truncate,
            ps.input_done,
        );
        Ok(())
    }
    fn update(&mut self, _jd: &mut JobData<'a>, _tf_id: TransformId) {}
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}
