use std::iter;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::call_expr::{Argument, CallExpr, Span},
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::OperatorCallEffect,
    operators::operator::TransformInstatiation,
    options::session_setup::SessionSetupData,
    record_data::{
        group_track::{GroupTrackIterId, GroupTrackIterRef},
        iter_hall::IterKind,
    },
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use super::{
    foreach_unique::parse_op_foreach_unique,
    nop::create_op_nop,
    operator::{
        Operator, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
    transform::{Transform, TransformData, TransformId, TransformState},
};

pub struct OpForeach {
    pub subchain: Vec<(Box<dyn Operator>, Span)>,
    pub subchain_idx: SubchainIndex,
}
pub struct TfForeachHeader {
    parent_group_track_iter: GroupTrackIterId,
    unrealized_group_skips: usize,
}
pub struct TfForeachTrailer {}

impl Operator for OpForeach {
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
        self.subchain_idx = sess.chains[chain_id].subchains.next_idx();
        sess.setup_subchain(chain_id, std::mem::take(&mut self.subchain))?;
        Ok(op_id)
    }

    fn default_name(&self) -> super::operator::OperatorName {
        "foreach".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        super::operator::OutputFieldKind::Unconfigured
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.input_accessed = false;
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
        output.call_effect = OperatorCallEffect::Diverge;
    }

    fn update_bb_for_op(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        _op_id: OperatorId,
        op_n: super::operator::OffsetInChain,
        cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        ld.basic_blocks[bb_id]
            .calls
            .push(cn.subchains[self.subchain_idx].into_bb_id());
        ld.split_bb_at_call(sess, bb_id, op_n);
        false
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let chain_id =
            job.job_data.session_data.operator_bases[op_id].chain_id;
        let subchain_id = job.job_data.session_data.chains[chain_id].subchains
            [self.subchain_idx];
        let sc_start_op_id = job.job_data.session_data.chains[subchain_id]
            .operators
            .first();
        let ms_id = tf_state.match_set_id;
        let desired_batch_size = tf_state.desired_batch_size;
        let input_field = tf_state.input_field;

        let ms = &mut job.job_data.match_set_mgr.match_sets[ms_id];
        let next_actor_id = ms.action_buffer.borrow().next_actor_ref();
        let parent_group_track = tf_state.input_group_track_id;

        let header_tf_id_peek = job.job_data.tf_mgr.transforms.peek_claim_id();

        let parent_group_track_iter =
            job.job_data.group_track_manager.claim_group_track_iter(
                parent_group_track,
                next_actor_id.get_id(),
                IterKind::Transform(header_tf_id_peek),
            );
        let group_track = job.job_data.group_track_manager.add_group_track(
            &job.job_data.match_set_mgr,
            Some(parent_group_track),
            ms_id,
            next_actor_id,
        );
        tf_state.output_group_track_id = group_track;

        let mut trailer_output_field = input_field;

        let header_tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            tf_state.clone(),
            TransformData::from_custom(TfForeachHeader {
                parent_group_track_iter,
                unrealized_group_skips: 0,
            }),
        );
        debug_assert!(header_tf_id_peek == header_tf_id);

        let mut out_tf_id = header_tf_id;
        let mut out_ms_id = ms_id;
        let mut out_group_track = group_track;

        if let Some(&op_id) = sc_start_op_id {
            let instantiation = job.setup_transforms_from_op(
                ms_id,
                op_id,
                input_field,
                group_track,
                None,
                prebound_outputs,
            );
            trailer_output_field = instantiation.next_input_field;
            job.job_data.tf_mgr.transforms[header_tf_id].successor =
                Some(instantiation.tfs_begin);

            out_tf_id = instantiation.tfs_end;
            out_ms_id = instantiation.next_match_set;
            out_group_track = instantiation.next_group_track;
        }

        job.job_data
            .field_mgr
            .inc_field_refcount(trailer_output_field, 2);

        let mut trailer_tf_state = TransformState::new(
            trailer_output_field,
            trailer_output_field,
            out_ms_id,
            desired_batch_size,
            Some(op_id),
            out_group_track,
        );

        let parent_group_track_parent =
            job.job_data.group_track_manager.group_tracks[parent_group_track]
                .borrow()
                .parent_group_track_id();
        let next_actor_ref = job.job_data.match_set_mgr.match_sets[out_ms_id]
            .action_buffer
            .borrow()
            .next_actor_ref();

        let continuation_group_track =
            job.job_data.group_track_manager.add_group_track(
                &job.job_data.match_set_mgr,
                parent_group_track_parent,
                out_ms_id,
                next_actor_ref,
            );
        trailer_tf_state.output_group_track_id = continuation_group_track;

        #[cfg(feature = "debug_state")]
        {
            job.job_data.group_track_manager.group_tracks
                [trailer_tf_state.output_group_track_id]
                .borrow_mut()
                .corresponding_header = Some(group_track);
        }

        let trailer_tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            trailer_tf_state,
            TransformData::from_custom(TfForeachTrailer {}),
        );
        job.job_data.tf_mgr.transforms[out_tf_id].successor =
            Some(trailer_tf_id);

        TransformInstatiation::Multiple(OperatorInstantiation {
            tfs_begin: header_tf_id,
            tfs_end: trailer_tf_id,
            next_input_field: trailer_output_field,
            next_group_track: continuation_group_track,
            next_match_set: out_ms_id,
        })
    }
}

impl Transform<'_> for TfForeachHeader {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        "foreach_header".into()
    }

    fn update(&mut self, jd: &mut JobData<'_>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        if batch_size == 0 {
            jd.tf_mgr.submit_batch(
                tf_id,
                batch_size,
                ps.group_to_truncate,
                ps.input_done,
            );
            return;
        }
        let tf = &jd.tf_mgr.transforms[tf_id];

        let in_group_track_id = tf.input_group_track_id;
        let out_group_track_id = tf.output_group_track_id;

        let mut group_track = jd
            .group_track_manager
            .borrow_group_track_mut(out_group_track_id);

        group_track.apply_field_actions(&jd.match_set_mgr);
        let mut parent_record_group_iter =
            jd.group_track_manager.lookup_group_track_iter(
                GroupTrackIterRef {
                    track_id: in_group_track_id,
                    iter_id: self.parent_group_track_iter,
                },
                &jd.match_set_mgr,
            );

        let mut size_rem = batch_size;
        loop {
            let mut gs_rem =
                parent_record_group_iter.group_len_rem().min(size_rem);
            if gs_rem == 0 {
                self.unrealized_group_skips +=
                    parent_record_group_iter.skip_empty_groups();
                if parent_record_group_iter.is_end(true) {
                    break;
                }
                gs_rem =
                    parent_record_group_iter.group_len_rem().min(size_rem);
                debug_assert!(gs_rem != 0);
            }

            parent_record_group_iter.next_n_fields(gs_rem);

            group_track
                .parent_group_advancement
                .push_back(self.unrealized_group_skips);
            self.unrealized_group_skips = 0;
            group_track
                .parent_group_advancement
                .extend_truncated(iter::repeat(0).take(gs_rem - 1));
            group_track
                .group_lengths
                .extend_truncated(iter::repeat(1).take(gs_rem));
            size_rem -= gs_rem;

            if size_rem == 0 {
                break;
            }
            parent_record_group_iter.next_group();
            self.unrealized_group_skips = 1;
        }
        parent_record_group_iter.store_iter(self.parent_group_track_iter);

        jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);

        #[cfg(feature = "debug_logging_output_fields")]
        {
            eprintln!(
                "foreach header (tf {tf_id}) set up group list {}: {}",
                out_group_track_id, group_track
            );
        }
    }
}

impl Transform<'_> for TfForeachTrailer {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        "foreach_trailer".into()
    }

    fn update(&mut self, jd: &mut JobData<'_>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let tf = &jd.tf_mgr.transforms[tf_id];

        let in_group_track_id = tf.input_group_track_id;
        let out_group_track_id = tf.output_group_track_id;

        jd.group_track_manager.pass_on_leading_groups_to_parent(
            &jd.match_set_mgr,
            in_group_track_id,
            batch_size,
            ps.input_done,
            out_group_track_id,
        );

        jd.tf_mgr.submit_batch(
            tf_id,
            batch_size,
            ps.group_to_truncate,
            ps.input_done,
        );
    }
}

pub fn create_op_foreach_with_spans(
    subchain: impl IntoIterator<Item = (Box<dyn Operator>, Span)>,
) -> Box<dyn Operator> {
    let mut subchain = subchain.into_iter().collect::<Vec<_>>();
    if subchain.is_empty() {
        subchain.push((create_op_nop(), Span::Generated));
    }
    Box::new(OpForeach {
        subchain,
        subchain_idx: SubchainIndex::MAX_VALUE,
    })
}

pub fn create_op_foreach(
    subchain: impl IntoIterator<Item = Box<dyn Operator>>,
) -> Box<dyn Operator> {
    create_op_foreach_with_spans(
        subchain.into_iter().map(|v| (v, Span::Generated)),
    )
}

pub fn parse_op_foreach(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<Box<dyn Operator>, ScrError> {
    let mut subchain = Vec::new();
    let expr = CallExpr::from_argument(&arg)?;
    if let (Some(flags), _) = expr.split_flags_arg(false) {
        if flags.contains_key("-u") {
            return parse_op_foreach_unique(sess, arg);
        }
        return Err(expr.error_flags_unsupported(arg.span).into());
    }
    for arg in std::mem::take(arg.expect_arg_array_mut()?)
        .into_iter()
        .skip(1)
    {
        let span = arg.span;
        let op = sess.parse_argument(arg)?;
        subchain.push((op, span));
    }

    Ok(create_op_foreach_with_spans(subchain))
}
