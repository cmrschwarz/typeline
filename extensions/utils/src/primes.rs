use std::collections::HashMap;

use primes::PrimeSet;
use scr_core::{
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    operators::{
        operator::{Operator, OperatorBase, OperatorData},
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    record_data::{action_buffer::ActorRef, field::FieldId},
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
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

    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        1
    }

    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        access_flags: &mut AccessFlags,
    ) {
        access_flags.input_accessed = false;
    }

    fn build_transform<'a>(
        &'a self,
        jd: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let ab = &mut jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        TransformData::Custom(smallbox!(TfPrimes {
            sieve: primes::Sieve::new(),
            count: 0
        }))
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
        if ps.output_done {
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
        jd.tf_mgr.submit_batch(tf_id, primes_to_produce, done);
    }
}

pub fn create_op_primes() -> OperatorData {
    OperatorData::Custom(smallbox!(OpPrimes {}))
}
