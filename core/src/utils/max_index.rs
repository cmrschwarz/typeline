#[cfg(target_feature = "avx2")]
const AVX2_MIN_LEN: usize = 8;

#[cfg(target_feature = "avx2")]
pub fn max_index_i64_avx2(arr: &[i64]) -> Option<usize> {
    use std::arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_blendv_epi8, _mm256_cmpgt_epi64,
        _mm256_loadu_si256, _mm256_set1_epi64x, _mm256_set_epi64x,
        _mm256_storeu_si256,
    };
    if arr.is_empty() {
        return None;
    }

    let len = arr.len();

    let mut max_vals = [i64::MIN; 4];
    let mut max_idxs = [0i64; 4];

    let mut i = 0;

    unsafe {
        let mut current_indices = _mm256_set_epi64x(3, 2, 1, 0);
        let mut max_vector = _mm256_set1_epi64x(i64::MIN);
        let mut max_indices = current_indices;
        let idx_stride = _mm256_set1_epi64x(4);

        while i + 4 <= len {
            let values =
                _mm256_loadu_si256(arr.as_ptr().add(i) as *const __m256i);

            let cmp_mask = _mm256_cmpgt_epi64(values, max_vector);

            max_vector = _mm256_blendv_epi8(max_vector, values, cmp_mask);
            max_indices =
                _mm256_blendv_epi8(max_indices, current_indices, cmp_mask);

            i += 4;
            current_indices = _mm256_add_epi64(current_indices, idx_stride);
        }

        _mm256_storeu_si256(max_vals.as_mut_ptr() as *mut __m256i, max_vector);
        _mm256_storeu_si256(
            max_idxs.as_mut_ptr() as *mut __m256i,
            max_indices,
        );
    }

    let mut max_val = max_vals[0];
    let mut max_idx = max_idxs[0];

    for j in 1..4 {
        // we want to make sure we return the earliest possible index as the
        // maximum, so if two indices have the same value we pick the smaller
        // one
        if max_vals[j] < max_val {
            continue;
        }
        if max_vals[j] == max_val && max_idx < max_idxs[j] {
            continue;
        }
        max_val = max_vals[j];
        max_idx = max_idxs[j];
    }

    let mut max_idx = max_idx as usize;

    // Handle remaining elements
    for j in i..len {
        if arr[j] > max_val {
            max_val = arr[j];
            max_idx = j;
        }
    }

    Some(max_idx)
}

#[cfg(target_feature = "avx2")]
pub fn max_index_f64_avx2(arr: &[f64]) -> Option<usize> {
    use std::arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_blendv_epi8, _mm256_blendv_pd,
        _mm256_cmp_pd, _mm256_loadu_pd, _mm256_set1_epi64x, _mm256_set1_pd,
        _mm256_set_epi64x, _mm256_storeu_pd, _mm256_storeu_si256, _CMP_GT_OQ,
    };
    if arr.is_empty() {
        return None;
    }

    let len = arr.len();

    let mut max_vals = [f64::NEG_INFINITY; 4];
    let mut max_idxs = [0i64; 4];

    let mut i = 0;

    unsafe {
        let mut current_indices = _mm256_set_epi64x(3, 2, 1, 0);
        let mut max_vector = _mm256_set1_pd(f64::NEG_INFINITY);
        let mut max_indices = current_indices;
        let idx_stride = _mm256_set1_epi64x(4);

        while i + 4 <= len {
            let values = _mm256_loadu_pd(arr.as_ptr().add(i) as *const _);

            let cmp_mask = _mm256_cmp_pd(values, max_vector, _CMP_GT_OQ);

            max_vector = _mm256_blendv_pd(max_vector, values, cmp_mask);
            max_indices = _mm256_blendv_epi8(
                max_indices,
                current_indices,
                std::mem::transmute(cmp_mask),
            );

            i += 4;
            current_indices = _mm256_add_epi64(current_indices, idx_stride);
        }

        _mm256_storeu_pd(max_vals.as_mut_ptr(), max_vector);
        _mm256_storeu_si256(
            max_idxs.as_mut_ptr() as *mut __m256i,
            max_indices,
        );
    }

    let mut max_idx = max_idxs[0] as usize;
    let mut max_val = arr[max_idx];

    let mut j = 1;
    // In case all values were `NaN`, we will still have `-inf` in the
    // max value slot. So we start by searching for the first non NaN value
    // within the actual data.
    while j < 4 && max_val.is_nan() {
        max_idx = max_idxs[j] as usize;
        max_val = arr[max_idx];
        j += 1;
    }
    if j == 4 {
        max_idx = 0; // all were `NaN`, pick 0
        debug_assert_eq!(max_idxs[0], 0);
    } else {
        for j in j..4 {
            // we want to make sure we return the earliest possible index as
            // the maximum, so if two indices have the same value
            // we pick the smaller one (which might be in the later
            // slot due to the vectorization)

            let v = max_vals[j];
            let idx = max_idxs[j] as usize;

            if v < max_val {
                continue;
            }
            if v == max_val && max_idx < idx {
                continue;
            }
            max_val = v;
            max_idx = idx;
        }
    }

    // if max_val is *still* `NaN`, search for the first `NaN`
    // in the overflow to pick as max value
    if max_val.is_nan() {
        loop {
            if i == len {
                return Some(0); // the entire array way `NaN`, so return 0
            }
            max_val = arr[i];
            if !max_val.is_nan() {
                max_idx = i;
                i += 1;
                break;
            }
            i += 1;
        }
    }

    for j in i..len {
        if arr[j] > max_val {
            max_val = arr[j];
            max_idx = j;
        }
    }

    Some(max_idx)
}

pub fn max_index_i64(nums: &[i64]) -> Option<usize> {
    #[cfg(target_feature = "avx2")]
    if nums.len() >= AVX2_MIN_LEN {
        return max_index_i64_avx2(nums);
    }
    if nums.is_empty() {
        return None;
    }
    let mut max_val = nums[0];
    let mut max_idx = 0;
    for i in 1..nums.len() {
        let v = nums[i];
        if v > max_val {
            max_idx = i;
            max_val = v;
        }
    }
    Some(max_idx)
}

pub fn max_index_f64(nums: &[f64]) -> Option<usize> {
    let len = nums.len();

    #[cfg(target_feature = "avx2")]
    if len >= AVX2_MIN_LEN {
        return max_index_f64_avx2(nums);
    }
    if len == 0 {
        return None;
    }

    let mut max_val;
    let mut i = 0;

    loop {
        max_val = nums[i];
        if !max_val.is_nan() {
            break;
        }
        i += 1;
        if i == len {
            return Some(0);
        }
    }

    let mut max_idx = i;

    for i in i + 1..len {
        let v = nums[i];
        if v > max_val {
            max_idx = i;
            max_val = v;
        }
    }
    Some(max_idx)
}

#[cfg(test)]
mod test {
    use core::f64;

    use crate::utils::max_index::{max_index_f64, max_index_i64_avx2};

    #[test]
    fn i64_max_value() {
        let arr = [i64::MAX, 0, 0, 0, 1, 0, 0, -1];
        assert_eq!(max_index_i64_avx2(&arr), Some(0));
    }

    #[test]
    fn i64_take_first_if_same() {
        let arr = [-1, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(max_index_i64_avx2(&arr), Some(1));
    }

    #[test]
    fn f64_max_value() {
        let arr = [f64::MAX, 0., 0., 0., 1., 0., 0., -1.];
        assert_eq!(max_index_f64(&arr), Some(0));
    }

    #[test]
    fn f64_take_first_if_same() {
        let arr = [-1.0, 0., 0., 0., 0., 0., 0., 0.];
        assert_eq!(max_index_f64(&arr), Some(1));
    }

    #[test]
    fn f64_take_non_nan() {
        let arr = [f64::NAN, 0., 0., 0., f64::NAN, 0., 0., 0.];
        assert_eq!(max_index_f64(&arr), Some(1));
    }

    #[test]
    fn f64_take_non_nan_small() {
        let arr = [f64::NAN, 0., 0.];
        assert_eq!(max_index_f64(&arr), Some(1));
    }

    #[test]
    fn f64_take_neg_inf_over_nan() {
        let arr = Vec::from_iter(
            std::iter::once(f64::NAN)
                .chain(std::iter::repeat_n(f64::NEG_INFINITY, 7)),
        );

        assert_eq!(max_index_f64(&arr), Some(1));
    }

    #[test]
    fn f64_take_neg_inf_over_nan_small() {
        let arr = [f64::NAN, f64::NEG_INFINITY, f64::NEG_INFINITY];
        assert_eq!(max_index_f64(&arr), Some(1));
    }

    #[test]
    fn f64_return_zero_if_all_nan() {
        let arr = Vec::from_iter(std::iter::repeat_n(f64::NAN, 10));

        assert_eq!(max_index_f64(&arr), Some(0));
    }

    #[test]
    fn f64_accept_positive_inf() {
        let arr = [f64::NEG_INFINITY, 0., 0., 0., f64::INFINITY, 0., 0., 0.];
        assert_eq!(max_index_f64(&arr), Some(4));
    }
}
