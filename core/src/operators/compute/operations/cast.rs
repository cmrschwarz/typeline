use std::arch::x86_64::{
    __m256d, __m256i, _mm256_add_epi64, _mm256_and_si256, _mm256_andnot_si256,
    _mm256_blendv_epi8, _mm256_castpd_si256, _mm256_cmp_pd,
    _mm256_cmpgt_epi64, _mm256_or_si256, _mm256_set1_epi64x, _mm256_set1_pd,
    _mm256_slli_epi64, _mm256_sllv_epi64, _mm256_srli_epi64,
    _mm256_srlv_epi64, _mm256_sub_epi64, _mm256_xor_si256, _CMP_NEQ_OQ,
};

// based on an idea from https://stackoverflow.com/a/68176624/7204912
// returns the integer as well as a mask indicating an overflow
pub unsafe fn f64_to_i64(v: __m256d) -> (__m256i, __m256i) {
    unsafe {
        // Constants
        let kneg1 = _mm256_set1_epi64x(-1);
        let k0 = _mm256_set1_epi64x(0);
        let k1 = _mm256_set1_epi64x(1);
        let k51 = _mm256_set1_epi64x(51);
        let mask_7ff = _mm256_set1_epi64x(0x7FF);
        let mask_3ff = _mm256_set1_epi64x(0x3FF);
        let mask_52bits = _mm256_set1_epi64x((1 << 52) - 1);
        let i64_max = _mm256_set1_epi64x(i64::MAX);

        #[allow(clippy::cast_precision_loss)]
        let i64_min_pd = _mm256_set1_pd(i64::MIN as f64);

        // Bitcast to integer registers
        let vi = _mm256_castpd_si256(v);

        // Extract exponents (bits 52-62)
        let biased_exp = _mm256_and_si256(_mm256_srli_epi64(vi, 52), mask_7ff);
        let exp = _mm256_sub_epi64(biased_exp, mask_3ff);

        // Check if exponent < 63
        let k63 = _mm256_set1_epi64x(63);
        let in_range = _mm256_cmpgt_epi64(k63, exp);

        // Calculate shift amounts
        let exp_n = _mm256_sub_epi64(k51, exp);
        let exp_p = _mm256_sub_epi64(exp, k51);
        let shift_mnt =
            _mm256_blendv_epi8(k0, exp_n, _mm256_cmpgt_epi64(exp_n, k0));
        let shift_int =
            _mm256_blendv_epi8(k0, exp_p, _mm256_cmpgt_epi64(exp_p, k0));

        // Extract mantissa and add implicit bit
        let mantissa = _mm256_and_si256(vi, mask_52bits);
        let implicit_bit =
            _mm256_or_si256(mantissa, _mm256_slli_epi64(k1, 52));
        let int52 =
            _mm256_srlv_epi64(implicit_bit, _mm256_add_epi64(shift_mnt, k1));

        // Shift to create integer part
        let shifted = _mm256_sllv_epi64(int52, shift_int);

        // Restore lost bit (conditional shift)
        let shift_restore = _mm256_sub_epi64(shift_int, k1);
        let restore_bits =
            _mm256_sllv_epi64(_mm256_and_si256(mantissa, k1), shift_restore);
        let restored = _mm256_or_si256(shifted, restore_bits);

        // Broadcast sign bit
        let sign_mask =
            _mm256_andnot_si256(_mm256_cmpgt_epi64(vi, kneg1), kneg1);

        // Select between converted value and limit
        let limit = _mm256_sub_epi64(i64_max, sign_mask);
        let magnitude = _mm256_blendv_epi8(limit, restored, in_range);

        // Apply two's complement for negative values
        let flipped = _mm256_xor_si256(magnitude, sign_mask);

        // special case: -2**63 is not an overflow despite having an exp of 63:
        let is_not_neg_2_pow63 = _mm256_cmp_pd(i64_min_pd, v, _CMP_NEQ_OQ);

        (
            _mm256_sub_epi64(flipped, sign_mask),
            _mm256_andnot_si256(
                in_range,
                _mm256_castpd_si256(is_not_neg_2_pow63),
            ),
        )
    }
}
