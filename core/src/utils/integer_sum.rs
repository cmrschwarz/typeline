#[cfg(target_feature = "avx2")]
const AVX2_MIN_LEN: usize = 8;

pub fn add_with_overflow_check(a: i64, b: i64) -> (i64, bool) {
    let sum = a.wrapping_add(b);

    // We have an overflow iff the sign of sum is not the same
    // as the sign of either of the inputs.
    // - mixed sign inputs never overflow
    // - if both inputs are positive but the sum is negative -> of
    // - if both inputs are negative but the sum is positive -> of
    // We can therefore calculate the overflow in the sign bit unsing
    // (a ^ res) & (b ^ res)
    let overflow = (sum ^ a) & (sum ^ b);

    (sum, overflow.is_negative())
}

pub fn integer_sum_with_overflow_baseline(nums: &[i64]) -> (i64, bool) {
    let mut sum = 0i64;
    let mut overflow = 0i64;
    for &b in nums {
        let a = sum;
        sum = a.wrapping_add(b);
        // same logic as in `add_with_overflow_check`
        overflow |= (sum ^ a) & (sum ^ b);
    }
    (sum, overflow.is_negative())
}

#[cfg(target_feature = "avx2")]
fn integer_sum_with_overflow_avx2(nums: &[i64]) -> (i64, bool) {
    use std::arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_and_si256, _mm256_loadu_si256,
        _mm256_or_si256, _mm256_setzero_si256, _mm256_srli_epi64,
        _mm256_storeu_si256, _mm256_testz_si256, _mm256_xor_si256,
    };

    const ELEMS_PER_V: usize = 4;

    let mut i = 0;
    let of_vectored;
    let mut res_arr = [0i64; ELEMS_PER_V];

    unsafe {
        let mut res_v = _mm256_setzero_si256();
        let mut overflow_v = _mm256_setzero_si256();

        while i + ELEMS_PER_V <= nums.len() {
            let a_v = res_v;
            #[allow(clippy::cast_ptr_alignment)]
            let b_v = _mm256_loadu_si256(nums[i..].as_ptr().cast::<__m256i>());
            res_v = _mm256_add_epi64(a_v, b_v);

            // same logic as in `add_with_overflow_check`
            let sign_diff_a_v = _mm256_xor_si256(res_v, a_v);
            let sign_diff_b_v = _mm256_xor_si256(res_v, b_v);
            let of_v = _mm256_and_si256(sign_diff_a_v, sign_diff_b_v);

            overflow_v = _mm256_or_si256(overflow_v, of_v);
            i += 4;
        }
        let overflow_bit_v =
            _mm256_srli_epi64(overflow_v, i64::BITS as i32 - 1);
        of_vectored = _mm256_testz_si256(overflow_bit_v, overflow_bit_v) == 0;

        #[allow(clippy::cast_ptr_alignment)]
        _mm256_storeu_si256(res_arr.as_mut_ptr().cast::<__m256i>(), res_v);
    }

    let (sum_agg, of_agg) = integer_sum_with_overflow_baseline(&res_arr);
    let (sum_rem, of_rem) = integer_sum_with_overflow_baseline(&nums[i..]);
    let (sum, of_final) = add_with_overflow_check(sum_agg, sum_rem);
    (sum, of_vectored | of_agg | of_rem | of_final)
}

pub fn integer_sum_with_overflow_check(v: &[i64]) -> (i64, bool) {
    #[cfg(target_feature = "avx2")]
    if v.len() >= AVX2_MIN_LEN {
        return integer_sum_with_overflow_avx2(v);
    }

    integer_sum_with_overflow_baseline(v)
}

pub fn try_integer_sum(nums: &[i64]) -> Option<i64> {
    let (sum, of) = integer_sum_with_overflow_check(nums);
    if of {
        return None;
    }
    Some(sum)
}

#[cfg(test)]
mod test {
    use crate::utils::integer_sum::integer_sum_with_overflow_check;

    #[test]
    fn integer_overflow_in_avx() {
        let arr = [i64::MAX, 0, 0, 0, 1, 0, 0, 0];
        assert_eq!(integer_sum_with_overflow_check(&arr), (i64::MIN, true));
    }

    #[test]
    fn integer_underflow_in_avx() {
        let arr = [i64::MIN, 0, 0, 0, -1, 0, 0, 0];
        assert_eq!(integer_sum_with_overflow_check(&arr), (i64::MAX, true));
    }

    #[test]
    fn integer_overflow_in_overhang() {
        let arr = [0, 0, 0, 0, 0, 0, 0, 0, i64::MAX, 1];
        assert_eq!(integer_sum_with_overflow_check(&arr), (i64::MIN, true));
    }

    #[test]
    fn integer_overflow_in_final_merge() {
        let arr = [0, 0, 0, i64::MAX, 0, 0, 0, 0, 0, 1];
        assert_eq!(integer_sum_with_overflow_check(&arr), (i64::MIN, true));
    }
}
