use std::{
    borrow::Cow,
    ops::{Add, AddAssign, Mul, MulAssign},
};

use crate::record_data::{
    field_data::RunLength, push_interface::PushInterface,
};
use num::{BigInt, BigRational, FromPrimitive, ToPrimitive};

#[derive(Clone)]
pub enum AnyNumber {
    Int(i64),
    BigInt(num::BigInt),
    Float(f64),
    Rational(BigRational),
}

impl Default for AnyNumber {
    fn default() -> Self {
        AnyNumber::Int(0)
    }
}

// ENHANCE: use this to add max / min / avg functions
impl AnyNumber {
    pub fn push(
        self,
        tgt: &mut impl PushInterface,
        header_rle: bool,
        data_rle: bool,
    ) {
        match self {
            AnyNumber::Int(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::BigInt(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::Float(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::Rational(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
        }
    }
    // PERF: this whole thing is slow and stupid
    pub fn add_int(&mut self, v: i64, rl: RunLength, fpm: bool) {
        let mut v_x_rl = v;
        if rl != 1 {
            let Some(res) = v.checked_mul(i64::from(rl)) else {
                match self {
                    AnyNumber::Int(i) => {
                        *self =
                            AnyNumber::BigInt(BigInt::from(v).mul(rl).add(*i));
                    }
                    AnyNumber::BigInt(i) => {
                        i.add_assign(BigInt::from(v).mul(rl));
                    }
                    AnyNumber::Float(f) => {
                        if fpm {
                            #[allow(clippy::cast_precision_loss)]
                            let v_f = v as f64;
                            *f = v_f.mul_add(f64::from(rl), *f);
                        } else if !f.is_nan() && !f.is_infinite() {
                            *self = AnyNumber::Rational(
                                BigRational::from_f64(*f)
                                    .unwrap()
                                    .add(BigInt::from(v).mul(rl)),
                            );
                        }
                    }
                    AnyNumber::Rational(r) => {
                        r.add_assign(BigInt::from(v).mul(rl))
                    }
                }
                return;
            };
            v_x_rl = res;
        }
        match self {
            AnyNumber::Int(i) => {
                if let Some(r) = i.checked_add(v_x_rl) {
                    *self = AnyNumber::Int(r);
                    return;
                }
                *self = AnyNumber::BigInt(BigInt::from(*i).add(v_x_rl));
            }
            AnyNumber::BigInt(v) => v.add_assign(v_x_rl),
            AnyNumber::Float(f) => {
                if fpm {
                    #[allow(clippy::cast_precision_loss)]
                    f.add_assign(v_x_rl as f64);
                } else if !f.is_nan() && !f.is_infinite() {
                    *self = AnyNumber::Rational(
                        BigRational::from_f64(*f)
                            .unwrap()
                            .add(BigInt::from(v).mul(rl)),
                    );
                }
            }
            AnyNumber::Rational(v) => v.add_assign(BigInt::from(v_x_rl)),
        }
    }
    pub fn add_big_int(&mut self, v: &BigInt, rl: RunLength, fpm: bool) {
        let v_x_rl = {
            if rl == 1 {
                Cow::Borrowed(v)
            } else {
                Cow::Owned(v.clone().mul(rl))
            }
        };
        match self {
            AnyNumber::Int(i) => {
                *self = AnyNumber::BigInt(v_x_rl.into_owned().add(*i));
            }
            AnyNumber::BigInt(i) => i.add_assign(&*v_x_rl),
            AnyNumber::Float(f) => {
                if fpm {
                    f.add_assign(v_x_rl.to_f64().unwrap());
                } else if !f.is_nan() && !f.is_infinite() {
                    *self = AnyNumber::Rational(
                        BigRational::from_f64(*f).unwrap().add(&*v_x_rl),
                    );
                }
            }
            AnyNumber::Rational(v) => v.add_assign(&*v_x_rl),
        }
    }
    pub fn add_float(&mut self, v: f64, rl: RunLength, fpm: bool) {
        if fpm {
            let curr = match self {
                #[allow(clippy::cast_precision_loss)]
                AnyNumber::Int(i) => *i as f64,
                AnyNumber::BigInt(i) => i.to_f64().unwrap(),
                AnyNumber::Float(f) => *f,
                AnyNumber::Rational(r) => r.to_f64().unwrap(),
            };
            // floating point add is commutative, so this order is fine
            // rle somewhat breaks order, you have to disable
            // that if you care
            *self = AnyNumber::Float(v.mul_add(f64::from(rl), curr));
            return;
        }
        if v.is_infinite() || v.is_nan() {
            *self = AnyNumber::Float(v);
            return;
        }
        let curr = match std::mem::take(self) {
            AnyNumber::Int(i) => BigRational::from_i64(i).unwrap(),
            AnyNumber::BigInt(i) => {
                BigRational::new(i, BigInt::from_u8(1).unwrap())
            }
            AnyNumber::Float(f) => {
                if f.is_infinite() || f.is_nan() {
                    *self = AnyNumber::Float(f);
                    return;
                }
                BigRational::from_f64(f).unwrap()
            }
            AnyNumber::Rational(r) => r,
        };
        let mut v_x_rl = BigRational::from_f64(v).unwrap();
        if rl != 1 {
            v_x_rl.mul_assign(BigInt::from_u32(rl).unwrap());
        }
        *self = AnyNumber::Rational(curr.add(v_x_rl));
    }
    pub fn add_rational(&mut self, v: &BigRational, rl: RunLength, fpm: bool) {
        let v_x_rl = {
            if rl == 1 {
                Cow::Borrowed(v)
            } else {
                Cow::Owned(v.clone().mul(BigInt::from(rl)))
            }
        };
        match self {
            AnyNumber::Int(i) => {
                *self = AnyNumber::Rational(
                    v_x_rl.into_owned().add(BigInt::from(*i)),
                );
            }
            AnyNumber::BigInt(i) => {
                *self = AnyNumber::Rational(v_x_rl.into_owned().add(&*i));
            }
            AnyNumber::Float(v) => {
                if fpm {
                    if v.is_infinite() || v.is_nan() {
                        return;
                    }
                    *self = AnyNumber::Rational(
                        BigRational::from_f64(*v).unwrap().add(&*v_x_rl),
                    );
                } else {
                    v.add_assign(v_x_rl.to_f64().unwrap());
                }
            }
            AnyNumber::Rational(v) => v.add_assign(&*v_x_rl),
        }
    }
}
