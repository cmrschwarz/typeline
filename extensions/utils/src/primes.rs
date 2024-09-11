use primes::PrimeSet;
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
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
        utils::generator_transform_update::{
            handle_generator_transform_update, GeneratorMode,
            GeneratorSequence,
        },
    },
    record_data::{
        action_buffer::ActorId,
        field::Field,
        fixed_sized_type_inserter::FixedSizeTypeInserter,
        group_track::GroupTrackIterRef,
        iter_hall::{IterId, IterKind},
    },
    smallbox,
};

#[derive(Default)]
pub struct OpPrimes {}

pub struct TfPrimes {
    sieve: primes::Sieve,
    count: usize,
    actor_id: ActorId,
    iter_id: IterId,
    group_iter: GroupTrackIterRef,
}

impl Operator for OpPrimes {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "primes".into()
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
        access_flags.input_accessed = false;
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;
        let actor_id =
            jd.add_actor_for_tf_state(tf_state);
        let iter_id = jd.claim_iter_for_tf_state(tf_state);
        let iter_kind =
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id());
        let group_iter = jd.group_track_manager.claim_group_track_iter_ref(
            tf_state.input_group_track_id,
            iter_kind,
        );

        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfPrimes {
                sieve: primes::Sieve::new(),
                count: 0,
                actor_id,
                iter_id,
                group_iter
            }
        )))
    }
}

impl GeneratorSequence for TfPrimes {
    type Inserter<'a> = FixedSizeTypeInserter<'a, i64>;
    fn seq_len_total(&self) -> u64 {
        u64::MAX
    }

    fn seq_len_rem(&self) -> u64 {
        u64::MAX
    }

    fn reset_sequence(&mut self) {
        todo!()
    }

    fn create_inserter<'a>(
        &mut self,
        field: &'a mut Field,
    ) -> Self::Inserter<'a> {
        field.iter_hall.fixed_size_type_inserter()
    }

    fn advance_sequence(
        &mut self,
        inserter: &mut Self::Inserter<'_>,
        count: usize,
    ) {
        inserter.extend(
            self.sieve
                .iter()
                .skip(self.count)
                .map(|v| v as i64)
                .take(count),
        );
        self.count += count;
    }
}

impl Transform<'_> for TfPrimes {
    fn display_name(&self) -> DefaultTransformName {
        "primes".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        handle_generator_transform_update(
            jd,
            tf_id,
            self.iter_id,
            self.actor_id,
            Some(self.group_iter),
            self,
            GeneratorMode::AlongsideUnbounded,
        )
    }
}

pub fn create_op_primes() -> OperatorData {
    OperatorData::Custom(smallbox!(OpPrimes {}))
}

pub fn parse_op_primes(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_primes())
}
