use std::{
    borrow::Cow,
    collections::HashMap,
    ops::{Add, AddAssign, Mul},
};

use num::ToPrimitive;
use scr_core::{
    job_session::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    num_bigint::BigInt,
    num_rational::BigRational,
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, OperatorBase},
        transform::{
            basic_transform_update, BasicUpdateData, DefaultTransformName,
            Transform, TransformData, TransformState,
        },
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        field_action::FieldActionKind::Drop,
        field_data::RunLength,
        iter_hall::IterId,
        iters::{BoundedIter, DestructuredFieldDataRef, Iter},
        push_interface::PushInterface,
        ref_iter::AutoDerefIter,
        typed::TypedValue,
    },
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

#[derive(Clone, Default)]
pub struct OpSum {}

#[derive(Clone)]
enum Aggregate {
    Int(i64),
    BigInt(scr_core::num_bigint::BigInt),
    Float(f64),
    Rational(BigRational),
}

impl Aggregate {
    fn push(self, tgt: &mut impl PushInterface) {
        match self {
            Aggregate::Int(v) => tgt.push_fixed_size_type(v, 1, false, false),
            Aggregate::BigInt(v) => {
                tgt.push_fixed_size_type(v, 1, false, false)
            }
            Aggregate::Float(v) => {
                tgt.push_fixed_size_type(v, 1, false, false)
            }
            Aggregate::Rational(v) => {
                tgt.push_fixed_size_type(v, 1, false, false)
            }
        }
    }
    // PERF: this whole thing is slow and stupid
    fn add_int(&mut self, v: i64, rl: RunLength) {
        let mut v_x_rl = v;
        if rl != 1 {
            let Some(r) = v.checked_mul(rl as i64) else {
                match self {
                    Aggregate::Int(i) => {
                        *self =
                            Aggregate::BigInt(BigInt::from(v).mul(rl).add(*i));
                    }
                    Aggregate::BigInt(i) => {
                        i.add_assign(BigInt::from(v).mul(rl));
                    }
                    Aggregate::Float(f) => {
                        *f = (v as f64).mul_add(rl as f64, *f);
                    }
                    Aggregate::Rational(r) => {
                        r.add_assign(BigInt::from(v).mul(rl))
                    }
                }
                return;
            };
            v_x_rl = r;
        }
        match self {
            Aggregate::Int(i) => {
                if let Some(r) = i.checked_add(v_x_rl) {
                    *self = Aggregate::Int(r);
                    return;
                }
                *self = Aggregate::BigInt(BigInt::from(*i).add(v_x_rl));
            }
            Aggregate::BigInt(v) => v.add_assign(v_x_rl),
            Aggregate::Float(v) => v.add_assign(v_x_rl as f64),
            Aggregate::Rational(v) => v.add_assign(BigInt::from(v_x_rl)),
        }
    }
    fn add_big_int(&mut self, v: &BigInt, rl: RunLength) {
        let v_x_rl = {
            if rl == 1 {
                Cow::Borrowed(v)
            } else {
                Cow::Owned(v.clone().mul(rl))
            }
        };
        match self {
            Aggregate::Int(i) => {
                *self = Aggregate::BigInt(v_x_rl.into_owned().add(*i));
            }
            Aggregate::BigInt(i) => i.add_assign(&*v_x_rl),
            // unwrap is fine, implementation of to_f64 never returns None
            Aggregate::Float(v) => v.add_assign(v_x_rl.to_f64().unwrap()),

            Aggregate::Rational(v) => v.add_assign(&*v_x_rl),
        }
    }
    fn add_float(&mut self, v: f64, rl: RunLength) {
        let curr = match self {
            Aggregate::Int(i) => *i as f64,
            Aggregate::BigInt(i) => i.to_f64().unwrap(),
            Aggregate::Float(f) => *f,
            Aggregate::Rational(r) => r.to_f64().unwrap(),
        };
        *self = Aggregate::Float(v.mul_add(rl as f64, curr));
    }
    fn add_rational(&mut self, v: &BigRational, rl: RunLength) {
        let v_x_rl = {
            if rl == 1 {
                Cow::Borrowed(v)
            } else {
                Cow::Owned(v.clone().mul(BigInt::from(rl)))
            }
        };
        match self {
            Aggregate::Int(i) => {
                *self = Aggregate::Rational(
                    v_x_rl.into_owned().add(BigInt::from(*i)),
                );
            }
            Aggregate::BigInt(i) => {
                *self = Aggregate::Rational(v_x_rl.into_owned().add(&*i));
            }
            Aggregate::Float(v) => v.add_assign(v_x_rl.to_f64().unwrap()),
            Aggregate::Rational(v) => v.add_assign(&*v_x_rl),
        }
    }
}

pub struct TfSum {
    input_iter_id: IterId,
    aggregate: Aggregate,
    error_occured: bool,
    actor_id: ActorId,
}

impl Operator for OpSum {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "sum".into()
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
        _bb_offset: u32,
        _access_flags: &mut AccessFlags,
    ) {
    }

    fn build_transform<'a>(
        &'a self,
        sess: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let ab = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        let actor_id = ab.add_actor();
        sess.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        TransformData::Custom(smallbox!(TfSum {
            input_iter_id: sess.field_mgr.claim_iter(tf_state.input_field),
            aggregate: Aggregate::Int(0),
            actor_id,
            error_occured: false
        }))
    }
}

impl TfSum {
    fn transform_update<'a>(
        &mut self,
        bud: BasicUpdateData,
        iter: &mut AutoDerefIter<
            'a,
            BoundedIter<'a, Iter<'a, DestructuredFieldDataRef<'a>>>,
        >,
    ) -> usize {
        let mut res = 0;
        while let (Some((v, rl, _)), false) =
            (iter.next_value(bud.match_set_mgr), self.error_occured)
        {
            match v {
                TypedValue::Int(v) => self.aggregate.add_int(*v, rl),
                TypedValue::BigInt(v) => self.aggregate.add_big_int(v, rl),
                TypedValue::Float(v) => self.aggregate.add_float(*v, rl),
                TypedValue::Rational(v) => self.aggregate.add_rational(v, rl),
                TypedValue::Null(_)
                | TypedValue::Undefined(_)
                | TypedValue::BytesInline(_)
                | TypedValue::TextInline(_)
                | TypedValue::BytesBuffer(_)
                | TypedValue::Array(_)
                | TypedValue::Object(_)
                | TypedValue::Custom(_)
                | TypedValue::StreamValueId(_)
                | TypedValue::Error(_)
                | TypedValue::FieldReference(_)
                | TypedValue::SlicedFieldReference(_) => {
                    bud.field_mgr
                        .borrow_field_dealiased_mut(bud.output_field_id)
                        .iter_hall
                        .push_error(
                            OperatorApplicationError::new_s(
                                format!(
                                    "cannot sum over type `{}`",
                                    v.as_slice().kind()
                                ),
                                bud.tf_mgr.transforms[bud.tf_id]
                                    .op_id
                                    .unwrap(),
                            ),
                            1,
                            false,
                            false,
                        );
                    res = 1;
                    self.error_occured = true;
                }
            }
        }
        if bud.input_done && !self.error_occured {
            let of = &mut bud
                .field_mgr
                .borrow_field_dealiased_mut(bud.output_field_id)
                .iter_hall;
            std::mem::replace(&mut self.aggregate, Aggregate::Int(0)).push(of);
            res = 1;
        }
        let ab =
            &mut bud.match_set_mgr.match_sets[bud.match_set_id].action_buffer;
        ab.begin_action_group(self.actor_id);
        ab.push_action(Drop, 0, bud.batch_size - res);
        ab.end_action_group();
        res
    }
}

impl Transform for TfSum {
    fn display_name(&self) -> DefaultTransformName {
        "sum".into()
    }

    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        basic_transform_update(
            jd,
            tf_id,
            [],
            self.input_iter_id,
            |bud, iter| self.transform_update(bud, iter),
        );
    }
}
