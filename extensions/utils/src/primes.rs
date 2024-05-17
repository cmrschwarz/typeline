use primes::PrimeSet;
use scr_core::{
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        operator::{
            Operator, OperatorData, OperatorId, OperatorOffsetInChain,
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
        action_buffer::{ActorId, ActorRef},
        field::Field,
        fixed_sized_type_inserter::FixedSizeTypeInserter,
        iter_hall::{IterId, IterKind},
        record_group_tracker::RecordGroupListIterRef,
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
    group_iter: RecordGroupListIterRef,
}

impl Operator for OpPrimes {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
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
        _op_offset_after_last_write: OperatorOffsetInChain,
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
        let mut ab = jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow_mut();
        let actor_id = ab.add_actor();
        let group_iter = jd
            .record_group_tracker
            .claim_group_list_iter_ref(tf_state.input_group_list_id);
        let iter_id = jd.field_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .iter_hall
            .claim_iter(IterKind::Transform(
                jd.tf_mgr.transforms.peek_claim_id(),
            ));
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
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
