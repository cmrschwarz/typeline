use std::arch::x86_64::{
    __m256d, __m256i, _mm256_and_si256, _mm256_andnot_si256,
    _mm256_blendv_epi8, _mm256_castpd_si256, _mm256_cmpeq_epi64,
    _mm256_cmpgt_epi64, _mm256_max_epi32, _mm256_or_si256, _mm256_set1_epi64x,
    _mm256_set1_pd, _mm256_slli_epi64, _mm256_sllv_epi64, _mm256_srli_epi64,
    _mm256_srlv_epi64, _mm256_sub_epi64, _mm256_xor_si256,
};

// roughly based on an idea from https://stackoverflow.com/a/68176624/7204912
// returns the integer as well as a mask indicating an overflow
pub unsafe fn f64_to_i64(v: __m256d) -> (__m256i, __m256i) {
    unsafe {
        // Constants
        let c_minus_one = _mm256_set1_epi64x(-1);
        let c_zero = _mm256_set1_epi64x(0);
        let c_one = _mm256_set1_epi64x(1);
        let c_51 = _mm256_set1_epi64x(51);
        let c_62 = _mm256_set1_epi64x(62);
        let c_7ff = _mm256_set1_epi64x(0x7FF);
        let c_1024 = _mm256_set1_epi64x(1024);
        let c_mantissa_one_bit = _mm256_slli_epi64(c_one, 52);
        let c_52bit_mask = _mm256_sub_epi64(c_mantissa_one_bit, c_one);
        let c_i64_max = _mm256_set1_epi64x(i64::MAX);

        #[allow(clippy::cast_precision_loss)]
        let i64_min_pd = _mm256_set1_pd(i64::MIN as f64);

        // Bitcast to integer registers
        let vi = _mm256_castpd_si256(v);

        // Extract exponents (bits 52-62), subtract 0x3ff bias
        let biased_exp = _mm256_and_si256(_mm256_srli_epi64(vi, 52), c_7ff);
        let exp = _mm256_sub_epi64(biased_exp, c_1024);

        // Check if exponent < c_62
        let in_range = _mm256_cmpgt_epi64(c_62, exp);

        // Calculate shift amounts
        let shift_pos_raw = _mm256_sub_epi64(exp, c_51);
        let shift_pos = _mm256_max_epi32(shift_pos_raw, c_zero);
        let shift_neg_raw = _mm256_sub_epi64(c_51, exp);
        let shift_neg = _mm256_max_epi32(shift_neg_raw, c_zero);

        // Extract mantissa and add implicit one bit
        let mantissa_raw = _mm256_and_si256(vi, c_52bit_mask);
        let mantissa = _mm256_or_si256(mantissa_raw, c_mantissa_one_bit);

        let mantissa_shifted = _mm256_srlv_epi64(
            _mm256_sllv_epi64(mantissa, shift_pos),
            shift_neg,
        );

        // Broadcast sign bit
        let sign_mask = _mm256_andnot_si256(
            _mm256_cmpgt_epi64(vi, c_minus_one),
            c_minus_one,
        );

        // get the 64 bit integer limit flipped based on the sign
        // example in 8 bit: 0x7F - 0xFF = 0x10; 0x7F - 0 = 0x7F
        // the two's complement below will revert the sign
        let limit_flipped = _mm256_sub_epi64(c_i64_max, sign_mask);

        // Select between (unsigned) converted value and the flipped limit
        let magnitude =
            _mm256_blendv_epi8(limit_flipped, mantissa_shifted, in_range);

        // Apply two's complement for negative values
        let flipped = _mm256_xor_si256(magnitude, sign_mask);
        let converted = _mm256_sub_epi64(flipped, sign_mask);

        // special case: -2**63 is not an overflow despite having an exp of 63
        let is_neg_2_pow63 = _mm256_cmpeq_epi64(
            _mm256_castpd_si256(i64_min_pd),
            _mm256_castpd_si256(v),
        );

        let overflow = _mm256_xor_si256(
            _mm256_or_si256(is_neg_2_pow63, in_range),
            c_minus_one,
        );

        (converted, overflow)
    }
}

#[cfg(test)]
mod test {
    use std::arch::x86_64::_mm256_setr_pd;

    use super::*;

    #[track_caller]
    fn test_f64_to_i64(
        inputs: [f64; 4],
        expected: [i64; 4],
        expect_overflow: [bool; 4],
    ) {
        let mut result_arr = [0i64; 4];
        let mut overflow_arr = [0i64; 4];

        unsafe {
            let input_v =
                _mm256_setr_pd(inputs[0], inputs[1], inputs[2], inputs[3]);
            let (result, overflow) = f64_to_i64(input_v);

            std::ptr::copy_nonoverlapping(
                std::ptr::from_ref(&result).cast(),
                result_arr.as_mut_ptr(),
                4,
            );

            std::ptr::copy_nonoverlapping(
                std::ptr::from_ref(&overflow).cast(),
                overflow_arr.as_mut_ptr(),
                4,
            );
        }

        let mut expected_overflow_arr = [0i64; 4];
        for i in 0..4 {
            if expect_overflow[i] {
                expected_overflow_arr[i] = -1;
            }
        }

        assert_eq!(&result_arr, &expected, "wrong resut ");
        assert_eq!(
            &overflow_arr, &expected_overflow_arr,
            "wrong overflow value"
        );
    }

    #[test]
    fn test_simple_integers() {
        test_f64_to_i64(
            [1.0, 2.0, 3.0, 4.0],
            [1, 2, 3, 4],
            [false, false, false, false],
        );
    }

    #[test]
    fn test_negative_integers() {
        test_f64_to_i64(
            [-1.0, -2.0, -3.0, -4.0],
            [-1, -2, -3, -4],
            [false, false, false, false],
        );
    }

    #[test]
    fn test_limits() {
        #[allow(clippy::cast_precision_loss)]
        test_f64_to_i64(
            [i64::MAX as f64, i64::MIN as f64, 0.0, -0.0],
            [i64::MAX, i64::MIN, 0, 0],
            [true, false, false, false],
        );
    }

    #[test]
    fn test_overflow() {
        test_f64_to_i64(
            [1e20, -1e20, f64::INFINITY, f64::NEG_INFINITY],
            [i64::MAX, i64::MIN, i64::MAX, i64::MIN],
            [true, true, true, true],
        );
    }

    #[test]
    fn test_fractional() {
        test_f64_to_i64(
            [1.5, 2.7, -1.5, -2.7],
            [1, 2, -1, -2],
            [false, false, false, false],
        );
    }

    #[test]
    fn test_special_values() {
        test_f64_to_i64(
            [f64::NAN, -0.0, f64::EPSILON, -f64::EPSILON],
            [i64::MAX, 0, 0, 0],
            [true, false, false, false],
        );
    }

    #[test]
    fn test_mantissa_bounds() {
        // Test values around 2^51 to 2^53
        let p51 = 2.0f64.powi(51);
        let p52 = 2.0f64.powi(52);

        test_f64_to_i64(
            [p51, p51 + 1.0, p52, p52 + 1.0],
            [p51 as i64, (p51 + 1.0) as i64, p52 as i64, p52 as i64 + 1],
            [false, false, false, false],
        );
    }

    #[test]
    fn test_mantissa_bounds_1() {
        let p51 = 2.0f64.powi(51);
        let p53 = 2.0f64.powi(53);

        test_f64_to_i64(
            [p53, p53 + 1.0, -p51, -(p51 + 1.0)],
            [
                p53 as i64,
                p53 as i64, // this overflows the 52 bit mantissa
                -(p51 as i64),
                -((p51 + 1.0) as i64),
            ],
            [false, false, false, false],
        );
    }

    #[test]
    fn test_mantissa_bounds_2() {
        let p52 = 2.0f64.powi(52);

        test_f64_to_i64(
            [p52 - 1.0, p52 - 0.5, p52 + 0.5, p52 + 2.0],
            [
                (p52 - 1.0) as i64,
                (p52 - 0.5) as i64,
                p52 as i64,
                p52 as i64 + 2,
            ],
            [false, false, false, false],
        );
    }

    #[test]
    fn test_negative_mantissa_bounds() {
        let p52 = 2.0f64.powi(52);
        let p53 = 2.0f64.powi(53);

        test_f64_to_i64(
            [-p52, -(p52 + 1.0), -p53, -(p53 + 1.0)],
            [
                -(p52 as i64),
                -(p52 as i64 + 1),
                -(p53 as i64),
                -(p53 as i64), // overflows
            ],
            [false, false, false, false],
        );
    }
}
