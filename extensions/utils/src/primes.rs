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
    },
    record_data::action_buffer::ActorRef,
    smallbox,
};

#[derive(Default)]
pub struct OpPrimes {}

pub struct TfPrimes {
    sieve: primes::Sieve,
    count: usize,
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
        let actor_id = job.job_data.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .action_buffer
            .borrow()
            .peek_next_actor_id();
        job.job_data.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(actor_id);
        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfPrimes {
                sieve: primes::Sieve::new(),
                count: 0
            }
        )))
    }
}

impl Transform for TfPrimes {
    fn display_name(&self) -> DefaultTransformName {
        "primes".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let output_field = tf.output_field;
        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            [output_field],
        );
        let mut primes_to_produce = batch_size;
        let mut done = false;
        if ps.successor_done {
            if ps.input_done {
                return;
            }
            done = true;
            primes_to_produce = 0;
        } else if ps.input_done {
            if let Some(succ) = jd.tf_mgr.transforms[tf_id].successor {
                primes_to_produce = primes_to_produce
                    .max(jd.tf_mgr.transforms[succ].desired_batch_size);
            } else {
                done = true;
                primes_to_produce = 0;
            }
        }
        let mut output_field = jd.field_mgr.fields[output_field].borrow_mut();
        let mut inserter =
            output_field.iter_hall.fixed_size_type_inserter::<i64>();

        inserter.extend(
            self.sieve
                .iter()
                .skip(self.count)
                .map(|v| v as i64)
                .take(primes_to_produce),
        );
        self.count += primes_to_produce;

        if ps.next_batch_ready || (ps.input_done && !done) {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr.submit_batch(
            tf_id,
            primes_to_produce,
            ps.input_done && done,
        );
    }
}

pub fn create_op_primes() -> OperatorData {
    OperatorData::Custom(smallbox!(OpPrimes {}))
}
