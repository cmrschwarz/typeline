use arrayvec::ArrayString;
use bstr::ByteSlice;
use num_traits::{FromPrimitive, PrimInt};
use smallstr::SmallString;
use std::{
    borrow::Cow,
    fmt::{Display, Write},
    str::FromStr,
};

const MSG_EMPTY_STR: &str = "found emtpy string when expecting number";
const MSG_INVALID_DIGIT: &str = "invalid digit";

pub fn parse_int_with_units<
    I: PrimInt + Display + FromPrimitive + FromStr<Err = std::num::ParseIntError>,
>(
    v: &str,
) -> Result<I, Cow<'static, str>> {
    fn msg_too_large<I: PrimInt + Display>() -> String {
        format!("too large (> {})", I::max_value())
    }
    fn msg_too_small<I: PrimInt + Display>() -> String {
        format!("too small (< {})", I::min_value())
    }

    let mut number_end = 0;
    let v = v.trim_start();
    while number_end < v.len()
        && "-+0123456789"
            .as_bytes()
            .find_byte(v.as_bytes()[number_end])
            .is_some()
    {
        number_end += 1;
    }
    let unit_str = v[number_end..].trim();
    let number = if number_end == 0 && !unit_str.is_empty() {
        I::one()
    } else {
        v[0..number_end].parse::<I>().map_err(|e| match e.kind() {
            std::num::IntErrorKind::Empty => MSG_EMPTY_STR.to_string(),
            std::num::IntErrorKind::InvalidDigit => {
                MSG_INVALID_DIGIT.to_string()
            }
            std::num::IntErrorKind::PosOverflow => msg_too_large::<I>(),
            std::num::IntErrorKind::NegOverflow => msg_too_small::<I>(),
            std::num::IntErrorKind::Zero => unreachable!(),
            _ => todo!(),
        })?
    };

    let mut unit = SmallString::<[u8; 8]>::from(unit_str);
    unit.make_ascii_lowercase();
    if unit.ends_with('b') {
        unit.pop();
    }
    let unit_mult: usize = match unit.as_str() {
        "ei" => 1152921504606846976,
        "e" => 1000000000000000000,
        "pi" => 1125899906842624,
        "p" => 1000000000000000,
        "ti" => 1099511627776,
        "t" => 1000000000000,
        "gi" => 1073741824,
        "g" => 1000000000,
        "mi" => 1048576,
        "m" => 1000000,
        "ki" => 1024,
        "k" => 1000,
        "" => 1,
        _ => return Err(format!("unknown integer unit '{unit_str}'").into()),
    };
    match I::from_usize(unit_mult).and_then(|v| v.checked_mul(&number)) {
        Some(v) => Ok(v),
        None => Err(if number >= I::zero() {
            msg_too_large::<I>()
        } else {
            msg_too_small::<I>()
        }
        .into()),
    }
}

pub fn parse_int_with_units_from_bytes<
    I: PrimInt + Display + FromPrimitive + FromStr<Err = std::num::ParseIntError>,
>(
    v: &[u8],
) -> Result<I, Cow<'static, str>> {
    let v = match v.to_str() {
        Ok(v) => v,
        Err(_) => return Err("invalid UTF-8".into()),
    };
    parse_int_with_units(v)
}

pub const USIZE_MAX_DECIMAL_DIGITS: usize = usize::MAX.ilog10() as usize + 1;

pub const I64_MAX_DECIMAL_DIGITS: usize = i64::MAX.ilog10() as usize + 1;
pub const U64_MAX_DECIMAL_DIGITS: usize = u64::MAX.ilog10() as usize + 1;
pub fn usize_to_str(val: usize) -> ArrayString<USIZE_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    res.write_fmt(format_args!("{val}")).unwrap();
    res
}

pub fn u64_to_str(
    display_plus: bool,
    val: u64,
) -> ArrayString<U64_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    if display_plus {
        res.write_fmt(format_args!("{val:+}")).unwrap();
    } else {
        res.write_fmt(format_args!("{val}")).unwrap();
    }
    res
}
pub fn i64_to_str(
    display_plus: bool,
    val: i64,
) -> ArrayString<I64_MAX_DECIMAL_DIGITS> {
    let mut res = ArrayString::new();
    if display_plus {
        res.write_fmt(format_args!("{val:+}")).unwrap();
    } else {
        res.write_fmt(format_args!("{val}")).unwrap();
    }
    res
}

pub fn i64_digits(display_plus_sign: bool, mut v: i64) -> usize {
    let sign_len = if v < 0 {
        v = -v;
        1
    } else {
        display_plus_sign as usize
    };
    sign_len + v.checked_ilog10().unwrap_or(0) as usize + 1
}
