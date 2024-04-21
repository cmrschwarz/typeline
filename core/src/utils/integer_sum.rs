#[cfg(target_feature = "avx2")]
fn integer_sum_with_overflow_avx2(nums: &[i64]) -> Option<i64> {
    use std::arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_and_si256, _mm256_loadu_si256,
        _mm256_or_si256, _mm256_setzero_si256, _mm256_srli_epi64,
        _mm256_storeu_si256, _mm256_testz_si256, _mm256_xor_si256,
    };

    let mut i = 0;
    let overflow;
    let mut res_arr = [0i64; 4];

    unsafe {
        let mut res_v = _mm256_setzero_si256();
        let mut overflow_v = _mm256_setzero_si256();

        while i + 4 <= nums.len() {
            #[allow(clippy::cast_ptr_alignment)]
            let nums_v =
                _mm256_loadu_si256(nums[i..].as_ptr().cast::<__m256i>());
            let sum_v = _mm256_add_epi64(res_v, nums_v);
            let of_a_v = _mm256_xor_si256(sum_v, res_v);
            let of_b_v = _mm256_xor_si256(sum_v, nums_v);
            let of_v = _mm256_and_si256(of_a_v, of_b_v);
            overflow_v = _mm256_or_si256(overflow_v, of_v);
            res_v = sum_v;
            i += 4;
        }
        let overflow_bit_v = _mm256_srli_epi64(overflow_v, 63);
        overflow = _mm256_testz_si256(overflow_bit_v, overflow_bit_v) == 0;

        #[allow(clippy::cast_ptr_alignment)]
        _mm256_storeu_si256(res_arr.as_mut_ptr().cast::<__m256i>(), res_v);
    }
    if overflow {
        return None;
    }
    let res = res_arr.iter().copied().try_fold(0, i64::checked_add)?;
    nums[i..].iter().copied().try_fold(res, i64::checked_add)
}

pub fn integer_sum_with_overflow(v: &[i64]) -> Option<i64> {
    #[cfg(target_feature = "avx2")]
    return integer_sum_with_overflow_avx2(v);

    #[cfg(not(target_feature = "avx2"))]
    {
        let mut sum = 0i64;
        let mut overflow = 0i64;
        for &i in v {
            let res = sum.wrapping_add(i);
            overflow |= (sum ^ res) & (i ^ res);
            sum = res;
        }
        if overflow.is_negative() {
            return None;
        }
        Some(sum)
    }
}
