use std::cmp::Ordering;

use num::{bigint::Sign, BigInt, BigRational, FromPrimitive};

use super::int_string_conversions::{
    I64_AS_F64_MAX_LOSLESS, I64_AS_F64_MIN_LOSLESS,
};

pub fn compare_i64_bigint(lhs: i64, rhs: &BigInt) -> Ordering {
    // If signs differ, we can return immediately
    match rhs.sign() {
        Sign::Minus => {
            // +lhs/0 > -rhs
            if lhs >= 0 {
                return Ordering::Greater;
            }
        }
        Sign::Plus => {
            // -lhs/0 < +rhs
            if lhs <= 0 {
                return Ordering::Less;
            }
        }
        // otherwise the rhs will have zero i64 sections
        // which would fail below
        Sign::NoSign => return lhs.cmp(&0),
    }

    let rhs_bits = rhs.bits();

    if rhs_bits > 64 {
        return if lhs > 0 {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }

    let rhs_mag = rhs.iter_u64_digits().next().unwrap();

    if lhs >= 0 {
        return (lhs as u64).cmp(&rhs_mag);
    }

    rhs_mag.cmp(&lhs.unsigned_abs())
}

pub fn try_convert_bigint_to_i64(bi: &BigInt) -> Option<i64> {
    if bi.sign() == Sign::NoSign {
        return Some(0);
    }

    if bi.bits() > 64 {
        return None;
    }

    let mag = bi.iter_u64_digits().next().unwrap();

    if bi.sign() == Sign::Minus {
        const I64_MIN_UNSIGNED: u64 = i64::MAX as u64 + 1;
        if mag > I64_MIN_UNSIGNED {
            return None;
        }
        return Some(-(mag as i64));
    }

    if mag > i64::MAX as u64 {
        return None;
    }
    Some(mag as i64)
}

pub fn convert_int_to_float(v: i64) -> Result<f64, BigRational> {
    if (I64_AS_F64_MIN_LOSLESS..=I64_AS_F64_MAX_LOSLESS).contains(&v) {
        #[allow(clippy::cast_precision_loss)]
        Ok(v as f64)
    } else {
        Err(BigRational::from_i64(v).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use num::One;

    use super::*;

    #[test]
    fn test_equal_values() {
        assert_eq!(compare_i64_bigint(42, &BigInt::from(42)), Ordering::Equal);
    }

    #[test]
    fn test_positive_greater() {
        assert_eq!(
            compare_i64_bigint(100, &BigInt::from(42)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_positive_less() {
        assert_eq!(compare_i64_bigint(42, &BigInt::from(100)), Ordering::Less);
    }

    #[test]
    fn test_negative_greater() {
        assert_eq!(
            compare_i64_bigint(-42, &BigInt::from(-100)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_negative_less() {
        assert_eq!(
            compare_i64_bigint(-100, &BigInt::from(-42)),
            Ordering::Less
        );
    }

    #[test]
    fn test_opposite_signs_positive() {
        assert_eq!(
            compare_i64_bigint(42, &BigInt::from(-42)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_opposite_signs_negative() {
        assert_eq!(compare_i64_bigint(-42, &BigInt::from(42)), Ordering::Less);
    }

    #[test]
    fn test_i64_min_equal() {
        assert_eq!(
            compare_i64_bigint(i64::MIN, &BigInt::from(i64::MIN)),
            Ordering::Equal
        );
    }

    #[test]
    fn test_i64_min_greater() {
        assert_eq!(
            compare_i64_bigint(
                i64::MIN,
                &(BigInt::from(i64::MIN) - BigInt::one())
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn test_i64_min_less() {
        assert_eq!(
            compare_i64_bigint(i64::MIN, &BigInt::from(i64::MIN + 1)),
            Ordering::Less
        );
    }

    #[test]
    fn test_i64_max_equal() {
        assert_eq!(
            compare_i64_bigint(i64::MAX, &BigInt::from(i64::MAX)),
            Ordering::Equal
        );
    }

    #[test]
    fn test_i64_max_greater() {
        assert_eq!(
            compare_i64_bigint(i64::MAX, &BigInt::from(i64::MAX - 1)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_i64_max_less() {
        let just_over_max = BigInt::from(i64::MAX) + 1;
        assert_eq!(
            compare_i64_bigint(i64::MAX, &just_over_max),
            Ordering::Less
        );
    }

    #[test]
    fn test_against_large_bigint() {
        let large = BigInt::from(2).pow(65);
        assert_eq!(compare_i64_bigint(i64::MAX, &large), Ordering::Less);
        assert_eq!(compare_i64_bigint(i64::MIN, &large), Ordering::Less);
    }

    #[test]
    fn test_against_very_small_bigint() {
        let very_small = -BigInt::from(2).pow(65);
        assert_eq!(
            compare_i64_bigint(i64::MAX, &very_small),
            Ordering::Greater
        );
        assert_eq!(
            compare_i64_bigint(i64::MIN, &very_small),
            Ordering::Greater
        );
    }

    #[test]
    fn test_zero_cases() {
        assert_eq!(compare_i64_bigint(0, &BigInt::from(0)), Ordering::Equal);
        assert_eq!(compare_i64_bigint(0, &BigInt::from(1)), Ordering::Less);
        assert_eq!(
            compare_i64_bigint(0, &BigInt::from(-1)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_small_numbers() {
        assert_eq!(compare_i64_bigint(1, &BigInt::from(1)), Ordering::Equal);
        assert_eq!(compare_i64_bigint(-1, &BigInt::from(-1)), Ordering::Equal);
        assert_eq!(
            compare_i64_bigint(1, &BigInt::from(-1)),
            Ordering::Greater
        );
        assert_eq!(compare_i64_bigint(-1, &BigInt::from(1)), Ordering::Less);
    }
}
