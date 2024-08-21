use std::{
    arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_and_si256, _mm256_castsi256_pd,
        _mm256_loadu_si256, _mm256_movemask_pd, _mm256_set1_epi64x,
        _mm256_storeu_si256, _mm256_sub_epi64, _mm256_xor_si256,
    },
    mem::MaybeUninit,
};

use super::binary_ops::{
    BinOpAdd, BinOpSub, OverflowingBinOp, AVX2_I64_ELEM_COUNT,
};

fn get_i64_add_overflow_mask(
    res_v: __m256i,
    lhs_v: __m256i,
    rhs_v: __m256i,
) -> i32 {
    unsafe {
        // We have an overflow iff the sign of sum is not the same
        // as the sign of either of the inputs.
        // - mixed sign inputs never overflow
        // - if both inputs are positive but the sum is negative -> of
        // - if both inputs are negative but the sum is positive -> of
        // We can therefore calculate the overflow in the sign bit using
        // (a ^ res) & (b ^ res)
        let sign_diff_a_v = _mm256_xor_si256(res_v, lhs_v);
        let sign_diff_b_v = _mm256_xor_si256(res_v, rhs_v);
        let of_v = _mm256_and_si256(sign_diff_a_v, sign_diff_b_v);
        _mm256_movemask_pd(_mm256_castsi256_pd(of_v))
    }
}

pub fn integer_add_stop_on_overflow_avx2(
    lhs: &[i64],
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
) -> usize {
    let len_min = lhs.len().min(rhs.len()).min(res.len());
    let lhs_p = lhs.as_ptr();
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs_p.add(i).cast::<__m256i>());

            let res_v = _mm256_add_epi64(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            let of_mask = get_i64_add_overflow_mask(res_v, lhs_v, rhs_v);
            if of_mask != 0 {
                return i + of_mask.leading_zeros() as usize;
            }

            i += 4;
        }
    }

    i + BinOpAdd::calc_until_overflow_baseline(
        &lhs[i..],
        &rhs[i..],
        &mut res[i..],
    )
}

fn get_i64_sub_overflow_mask(
    lhs_v: __m256i,
    rhs_v: __m256i,
    res_v: __m256i,
) -> i32 {
    unsafe {
        // We have an overflow iff we have different signed inputs and the
        // sign of the first input does not equal the sign of the output
        // - same sign inputs never overflows
        // - of if a is positive, b is negative and the result is negative
        // - of if a is negative, b is positive and the result is positive
        // We can therefore calculate the overflow in the sign bit using
        // (a ^ b) & (a ^ res)
        let sign_diff_inputs = _mm256_xor_si256(lhs_v, rhs_v);
        let sign_diff_a_res = _mm256_xor_si256(lhs_v, res_v);
        let of_v = _mm256_and_si256(sign_diff_inputs, sign_diff_a_res);
        _mm256_movemask_pd(_mm256_castsi256_pd(of_v))
    }
}

pub fn integer_sub_stop_on_overflow_avx2(
    lhs: &[i64],
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
) -> usize {
    let len_min = lhs.len().min(rhs.len()).min(res.len());
    let lhs_p = lhs.as_ptr();
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs_p.add(i).cast::<__m256i>());

            let res_v = _mm256_sub_epi64(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            let of_mask = get_i64_sub_overflow_mask(lhs_v, rhs_v, res_v);
            if of_mask != 0 {
                return i + of_mask.leading_zeros() as usize;
            }

            i += 4;
        }
    }

    i + BinOpSub::calc_until_overflow_baseline(
        &lhs[i..],
        &rhs[i..],
        &mut res[i..],
    )
}

pub fn integer_add_immediate_stop_on_overflow_avx2(
    lhs: &[i64],
    rhs: i64,
    res: &mut [MaybeUninit<i64>],
) -> usize {
    let len_min = lhs.len().min(res.len());
    let lhs_p = lhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let rhs_v = _mm256_set1_epi64x(rhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            let res_v = _mm256_add_epi64(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            let of_mask = get_i64_add_overflow_mask(res_v, lhs_v, rhs_v);
            if of_mask != 0 {
                return i + of_mask.leading_zeros() as usize;
            }

            i += 4;
        }
    }

    i + BinOpAdd::calc_until_overflow_rhs_immediate_baseline(
        &lhs[i..],
        rhs,
        &mut res[i..],
    )
}

pub fn integer_sub_immediate_stop_on_overflow_avx2(
    lhs: &[i64],
    rhs: i64,
    res: &mut [MaybeUninit<i64>],
) -> usize {
    let len_min = lhs.len().min(res.len());
    let lhs_p = lhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let rhs_v = _mm256_set1_epi64x(rhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            let res_v = _mm256_add_epi64(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            let of_mask = get_i64_sub_overflow_mask(lhs_v, rhs_v, res_v);
            if of_mask != 0 {
                return i + of_mask.leading_zeros() as usize;
            }

            i += 4;
        }
    }

    i + BinOpSub::calc_until_overflow_rhs_immediate_baseline(
        &lhs[i..],
        rhs,
        &mut res[i..],
    )
}

pub fn integer_sub_from_immediate_stop_on_overflow_avx2(
    lhs: i64,
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
) -> usize {
    let len_min = rhs.len().min(res.len());
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let lhs_v = _mm256_set1_epi64x(lhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs_p.add(i).cast::<__m256i>());

            let res_v = _mm256_add_epi64(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            let of_mask = get_i64_sub_overflow_mask(lhs_v, rhs_v, res_v);
            if of_mask != 0 {
                return i + of_mask.leading_zeros() as usize;
            }

            i += 4;
        }
    }

    i + BinOpSub::calc_until_overflow_lhs_immediate_baseline(
        lhs,
        &rhs[i..],
        &mut res[i..],
    )
}
