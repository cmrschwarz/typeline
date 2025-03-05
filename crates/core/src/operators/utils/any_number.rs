use std::{
    borrow::Cow,
    cmp::Ordering,
    ops::{Add, AddAssign, Mul, MulAssign},
};

use crate::{
    record_data::{field_data::RunLength, push_interface::PushInterface},
    utils::{
        compare_i64_bigint::compare_i64_bigint, integer_sum::try_integer_sum,
    },
};
use num::{BigInt, BigRational, FromPrimitive, ToPrimitive};
use num_order::NumOrd;

#[derive(Clone)]
pub enum AnyNumber {
    Bool(bool),
    Int(i64),
    BigInt(num::BigInt),
    Float(f64),
    BigRational(BigRational),
}

#[derive(Clone, Copy)]
pub enum AnyNumberRef<'a> {
    Bool(&'a bool),
    Int(&'a i64),
    BigInt(&'a num::BigInt),
    Float(&'a f64),
    BigRational(&'a BigRational),
}

impl PartialEq for AnyNumber {
    fn eq(&self, other: &AnyNumber) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for AnyNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().partial_cmp(&other.as_ref())
    }
}

impl<'b> PartialEq<AnyNumberRef<'b>> for AnyNumberRef<'_> {
    fn eq(&self, other: &AnyNumberRef<'b>) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl<'b> PartialOrd<AnyNumberRef<'b>> for AnyNumberRef<'_> {
    fn partial_cmp(&self, other: &AnyNumberRef<'b>) -> Option<Ordering> {
        match (self, other) {
            (AnyNumberRef::Bool(lhs), rhs) => {
                AnyNumberRef::Int(&i64::from(**lhs)).partial_cmp(rhs)
            }
            (lhs, AnyNumberRef::Bool(rhs)) => {
                lhs.partial_cmp(&AnyNumberRef::Int(&i64::from(**rhs)))
            }
            (AnyNumberRef::Int(lhs), AnyNumberRef::Int(rhs)) => {
                Some(lhs.cmp(rhs))
            }
            (AnyNumberRef::Float(lhs), AnyNumberRef::Float(rhs)) => {
                lhs.partial_cmp(rhs)
            }
            (AnyNumberRef::BigInt(lhs), AnyNumberRef::BigInt(rhs)) => {
                Some(lhs.cmp(rhs))
            }
            (
                AnyNumberRef::BigRational(lhs),
                AnyNumberRef::BigRational(rhs),
            ) => Some(lhs.cmp(rhs)),

            (AnyNumberRef::Int(lhs), AnyNumberRef::BigInt(rhs)) => {
                Some(compare_i64_bigint(**lhs, rhs))
            }
            (AnyNumberRef::BigInt(lhs), AnyNumberRef::Int(rhs)) => {
                Some(compare_i64_bigint(**rhs, lhs).reverse())
            }

            (AnyNumberRef::Int(lhs), AnyNumberRef::Float(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }
            (AnyNumberRef::Float(lhs), AnyNumberRef::Int(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }

            (AnyNumberRef::Int(lhs), AnyNumberRef::BigRational(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }
            (AnyNumberRef::BigRational(lhs), AnyNumberRef::Int(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }

            (AnyNumberRef::BigInt(lhs), AnyNumberRef::Float(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }
            (AnyNumberRef::Float(lhs), AnyNumberRef::BigInt(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }

            (AnyNumberRef::BigInt(lhs), AnyNumberRef::BigRational(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }
            (AnyNumberRef::BigRational(lhs), AnyNumberRef::BigInt(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }

            (AnyNumberRef::Float(lhs), AnyNumberRef::BigRational(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }
            (AnyNumberRef::BigRational(lhs), AnyNumberRef::Float(rhs)) => {
                lhs.num_partial_cmp(*rhs)
            }
        }
    }
}

impl Default for AnyNumber {
    fn default() -> Self {
        AnyNumber::Int(0)
    }
}

impl AnyNumber {
    pub fn as_ref(&self) -> AnyNumberRef {
        match self {
            AnyNumber::Bool(v) => AnyNumberRef::Bool(v),
            AnyNumber::Int(v) => AnyNumberRef::Int(v),
            AnyNumber::BigInt(v) => AnyNumberRef::BigInt(v),
            AnyNumber::Float(v) => AnyNumberRef::Float(v),
            AnyNumber::BigRational(v) => AnyNumberRef::BigRational(v),
        }
    }
}

impl AnyNumberRef<'_> {
    pub fn to_owned(&self) -> AnyNumber {
        match *self {
            AnyNumberRef::Bool(v) => AnyNumber::Bool(*v),
            AnyNumberRef::Int(v) => AnyNumber::Int(*v),
            AnyNumberRef::BigInt(v) => AnyNumber::BigInt(v.clone()),
            AnyNumberRef::BigRational(v) => AnyNumber::BigRational(v.clone()),
            AnyNumberRef::Float(v) => AnyNumber::Float(*v),
        }
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
            AnyNumber::Bool(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::Int(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::BigInt(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::Float(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
            AnyNumber::BigRational(v) => {
                tgt.push_fixed_size_type(v, 1, header_rle, data_rle)
            }
        }
    }

    pub fn add_int(&mut self, value: i64, fpm: bool) {
        match self {
            AnyNumber::Bool(v) => {
                *self = AnyNumber::Int(i64::from(*v));
                self.add_int(value, fpm);
            }
            AnyNumber::Int(i) => {
                if let Some(r) = i.checked_add(value) {
                    *self = AnyNumber::Int(r);
                    return;
                }
                *self = AnyNumber::BigInt(BigInt::from(*i).add(value));
            }
            AnyNumber::BigInt(v) => v.add_assign(value),
            AnyNumber::Float(f) => {
                if fpm {
                    #[allow(clippy::cast_precision_loss)]
                    f.add_assign(value as f64);
                } else if !f.is_nan() && !f.is_infinite() {
                    *self = AnyNumber::BigRational(
                        BigRational::from_f64(*f)
                            .unwrap()
                            .add(BigInt::from(value)),
                    );
                }
            }
            AnyNumber::BigRational(r) => r.add_assign(BigInt::from(value)),
        }
    }
    pub fn add_bool(&mut self, value: bool, fpm: bool) {
        self.add_int(i64::from(value), fpm);
    }
    pub fn add_bool_with_rl(&mut self, value: bool, rl: RunLength, fpm: bool) {
        self.add_int_with_rl(i64::from(value), rl, fpm);
    }
    pub fn div_usize(&mut self, value: usize) {
        match self {
            // TODO: figure out somethign smarter
            AnyNumber::Bool(v) => {
                *self = AnyNumber::BigRational(
                    BigRational::from_i64(i64::from(*v)).unwrap()
                        / BigRational::from_usize(value).unwrap(),
                );
            }
            AnyNumber::Int(v) => {
                *self = AnyNumber::BigRational(
                    BigRational::from_i64(*v).unwrap()
                        / BigRational::from_usize(value).unwrap(),
                );
            }
            AnyNumber::BigInt(v) => {
                *self = AnyNumber::BigInt(
                    std::mem::take(v) / BigInt::from_usize(value).unwrap(),
                );
            }
            AnyNumber::Float(v) => {
                // HACK
                #[allow(clippy::cast_precision_loss)]
                {
                    *self = AnyNumber::Float(*v / value as f64);
                }
            }
            AnyNumber::BigRational(v) => {
                *self = AnyNumber::BigRational(
                    std::mem::take(v)
                        / BigRational::from_usize(value).unwrap(),
                );
            }
        }
    }
    pub fn add_int_with_rl(&mut self, v: i64, rl: RunLength, fpm: bool) {
        if rl == 1 {
            self.add_int(v, fpm);
            return;
        }
        if let Some(v_x_rl) = v.checked_mul(i64::from(rl)) {
            self.add_int(v_x_rl, fpm);
            return;
        }
        match self {
            AnyNumber::Bool(b) => {
                *self = AnyNumber::BigInt(
                    BigInt::from(v).mul(rl).add(i64::from(*b)),
                );
            }
            AnyNumber::Int(i) => {
                *self = AnyNumber::BigInt(BigInt::from(v).mul(rl).add(*i));
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
                    *self = AnyNumber::BigRational(
                        BigRational::from_f64(*f)
                            .unwrap()
                            .add(BigInt::from(v).mul(rl)),
                    );
                }
            }
            AnyNumber::BigRational(r) => r.add_assign(BigInt::from(v).mul(rl)),
        }
    }
    pub fn add_ints(&mut self, v: &[i64], fpm: bool) {
        match try_integer_sum(v) {
            Some(res) => self.add_int(res, fpm),
            None => {
                for &i in v {
                    self.add_int(i, fpm);
                }
            }
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
            AnyNumber::Bool(b) => {
                *self =
                    AnyNumber::BigInt(v_x_rl.into_owned().add(i64::from(*b)));
            }
            AnyNumber::Int(i) => {
                *self = AnyNumber::BigInt(v_x_rl.into_owned().add(*i));
            }
            AnyNumber::BigInt(i) => i.add_assign(&*v_x_rl),
            AnyNumber::Float(f) => {
                if fpm {
                    f.add_assign(v_x_rl.to_f64().unwrap());
                } else if !f.is_nan() && !f.is_infinite() {
                    *self = AnyNumber::BigRational(
                        BigRational::from_f64(*f).unwrap().add(&*v_x_rl),
                    );
                }
            }
            AnyNumber::BigRational(v) => v.add_assign(&*v_x_rl),
        }
    }
    pub fn add_float(&mut self, v: f64, rl: RunLength, fpm: bool) {
        if fpm {
            #[allow(clippy::cast_precision_loss)]
            let curr = match self {
                AnyNumber::Bool(b) => i64::from(*b) as f64,
                AnyNumber::Int(i) => *i as f64,
                AnyNumber::BigInt(i) => i.to_f64().unwrap(),
                AnyNumber::Float(f) => *f,
                AnyNumber::BigRational(r) => r.to_f64().unwrap(),
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
            AnyNumber::Bool(b) => BigRational::from_i64(i64::from(b)).unwrap(),
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
            AnyNumber::BigRational(r) => r,
        };
        let mut v_x_rl = BigRational::from_f64(v).unwrap();
        if rl != 1 {
            v_x_rl.mul_assign(BigInt::from_u32(rl).unwrap());
        }
        *self = AnyNumber::BigRational(curr.add(v_x_rl));
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
            AnyNumber::Bool(b) => {
                *self = AnyNumber::BigRational(
                    v_x_rl.into_owned().add(BigInt::from(i64::from(*b))),
                );
            }
            AnyNumber::Int(i) => {
                *self = AnyNumber::BigRational(
                    v_x_rl.into_owned().add(BigInt::from(*i)),
                );
            }
            AnyNumber::BigInt(i) => {
                *self = AnyNumber::BigRational(v_x_rl.into_owned().add(&*i));
            }
            AnyNumber::Float(v) => {
                if fpm {
                    if v.is_infinite() || v.is_nan() {
                        return;
                    }
                    *self = AnyNumber::BigRational(
                        BigRational::from_f64(*v).unwrap().add(&*v_x_rl),
                    );
                } else {
                    v.add_assign(v_x_rl.to_f64().unwrap());
                }
            }
            AnyNumber::BigRational(v) => v.add_assign(&*v_x_rl),
        }
    }
}
