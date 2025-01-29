use std::{
    arch::x86_64::{
        __m256d, __m256i, _mm256_add_pd, _mm256_castpd_si256,
        _mm256_castps256_ps128, _mm256_castsi256_ps, _mm256_cmp_pd,
        _mm256_cmpeq_epi64, _mm256_cvtepi32_pd, _mm256_extractf128_ps,
        _mm256_loadu_pd, _mm256_loadu_si256, _mm256_movemask_epi8,
        _mm256_mul_pd, _mm256_or_pd, _mm256_round_pd, _mm256_set1_pd,
        _mm256_sub_pd, _mm_and_si128, _mm_castps_si128, _mm_set1_epi32,
        _mm_shuffle_ps, _mm_srli_epi32, _CMP_EQ_OQ, _CMP_NEQ_OQ,
        _MM_FROUND_NO_EXC, _MM_FROUND_TO_ZERO,
    },
    convert::Infallible,
    marker::PhantomData,
    mem::MaybeUninit,
};

use crate::{
    operators::compute::operations::avx2::mask_to_bool_array_neg,
    record_data::field_data::FixedSizeFieldValueType,
};

use super::{
    avx2::{
        calc_cmp_avx2_rhs_immediate, calc_cmp_f64_avx2_lhs_immediate,
        mm256d_to_bool_array, mm256i_to_bool_array, BinaryOpAvx2Adapter,
        BinaryOpAvx2Aware, BinaryOpCmpF64Avx2Adapter,
        BinaryOpCmpF64F64Avx2Aware, BinaryOpCmpI64Avx2Adapter,
        BinaryOpCmpI64I64Avx2Aware, AVX2_I64_ELEM_COUNT,
    },
    BinaryOp, BinaryOpCommutationWrapper,
};
use num_order::NumOrd;

pub type BinaryOpEqI64I64 = BinaryOpCmpI64Avx2Adapter<BinaryOpEqI64I64Avx2>;
pub struct BinaryOpEqI64I64Avx2;
impl BinaryOpCmpI64I64Avx2Aware for BinaryOpEqI64I64Avx2 {
    fn cmp_single(lhs: &i64, rhs: &i64) -> bool {
        *lhs == *rhs
    }
    fn cmp_avx2(lhs: __m256i, rhs: __m256i) -> [bool; AVX2_I64_ELEM_COUNT] {
        mm256i_to_bool_array(unsafe { _mm256_cmpeq_epi64(lhs, rhs) })
    }
}

pub type BinaryOpEqF64F64 = BinaryOpCmpF64Avx2Adapter<BinaryOpEqF64F64Avx2>;
pub struct BinaryOpEqF64F64Avx2;
impl BinaryOpCmpF64F64Avx2Aware for BinaryOpEqF64F64Avx2 {
    fn cmp_single(lhs: &f64, rhs: &f64) -> bool {
        #[allow(clippy::float_cmp)]
        {
            *lhs == *rhs
        }
    }
    fn cmp_avx2(lhs: __m256d, rhs: __m256d) -> [bool; AVX2_I64_ELEM_COUNT] {
        mm256d_to_bool_array(unsafe { _mm256_cmp_pd(lhs, rhs, _CMP_EQ_OQ) })
    }
}

pub type BinaryOpEqI64F64 = BinaryOpAvx2Adapter<BinaryOpEqI64F64Avx2>;
pub struct BinaryOpEqI64F64Avx2;
unsafe impl BinaryOpAvx2Aware for BinaryOpEqI64F64Avx2 {
    type Lhs = i64;
    type Rhs = f64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs.num_eq(rhs))
    }

    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        cmp_eq_i64_f64_avx2(lhs, rhs, res)
    }

    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = lhs.len().min(res.len());
        #[allow(clippy::cast_precision_loss, clippy::float_cmp)]
        if rhs.trunc() != *rhs
            || rhs.is_infinite()
            || *rhs > i64::MAX as f64
            || *rhs < i64::MIN as f64
        {
            res[0..len].fill(MaybeUninit::new(false));
            return (len, None);
        }
        calc_cmp_avx2_rhs_immediate(
            lhs,
            &(*rhs as i64),
            res,
            |lhs, rhs| {
                mm256i_to_bool_array(unsafe { _mm256_cmpeq_epi64(lhs, rhs) })
            },
            NumOrd::num_eq,
        );
        (len, None)
    }

    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = rhs.len().min(res.len());
        #[allow(clippy::cast_precision_loss)]
        if ((*lhs as f64) as i64) != *lhs {
            res[0..len].fill(MaybeUninit::new(false));
            return (len, None);
        }
        #[allow(clippy::cast_precision_loss)]
        calc_cmp_f64_avx2_lhs_immediate(
            &(*lhs as f64),
            rhs,
            res,
            |lhs, rhs| {
                mm256d_to_bool_array(unsafe {
                    _mm256_cmp_pd::<_CMP_EQ_OQ>(lhs, rhs)
                })
            },
            NumOrd::num_eq,
        );
        (len, None)
    }
}

pub type BinaryOpEqF64I64 = BinaryOpCommutationWrapper<BinaryOpEqI64F64>;

pub struct BasicBinaryOpEq<Lhs, Rhs>(PhantomData<(Lhs, Rhs)>);
unsafe impl<
        Lhs: NumOrd<Rhs> + FixedSizeFieldValueType,
        Rhs: FixedSizeFieldValueType,
    > BinaryOp for BasicBinaryOpEq<Lhs, Rhs>
{
    type Lhs = Lhs;
    type Rhs = Rhs;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(lhs.num_eq(rhs))
    }
}

fn cmp_eq_i64_f64_avx2(
    ints: &[i64],
    floats: &[f64],
    res: &mut [MaybeUninit<bool>],
) -> (usize, Option<Infallible>) {
    let len_min = ints.len().min(floats.len()).min(res.len());
    let ints_p = ints.as_ptr();
    let floats_p = floats.as_ptr();
    let res_p = res.as_mut_ptr();

    #[allow(clippy::identity_op)]
    const SHUF_EXTRACT_LOW: i32 = 0 | 2 << 2 | 0 << 4 | 2 << 6;

    const SHUF_EXTRACT_HI: i32 = 1 | 3 << 2 | 1 << 4 | 3 << 6;

    let mut i = 0;

    unsafe {
        let ints_sign_mask = _mm_set1_epi32(0x7FFF_FFFF);
        let ints_sign_bit_shift = _mm256_set1_pd(f64::from(0x8000_0000u32)); // 2^31
        let ints_hi_shift = _mm256_set1_pd(4_294_967_296.0); // 2^32
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            let ints_v = _mm256_loadu_si256(ints_p.add(i).cast());
            let floats_v = _mm256_loadu_pd(floats_p.add(i));

            let ints_ps = _mm256_castsi256_ps(ints_v);
            let ints_12 = _mm256_castps256_ps128(ints_ps);
            let ints_34 = _mm256_extractf128_ps::<1>(ints_ps);

            let ints_lo = _mm_castps_si128(_mm_shuffle_ps(
                ints_12,
                ints_34,
                SHUF_EXTRACT_LOW,
            ));
            let ints_lo_no_sign = _mm_and_si128(ints_lo, ints_sign_mask);
            let ints_lo_bit32 = _mm_srli_epi32::<31>(ints_lo);

            let ints_hi_ps = _mm_shuffle_ps(ints_12, ints_34, SHUF_EXTRACT_HI);
            let ints_lo_f64_no_sign = _mm256_cvtepi32_pd(ints_lo_no_sign);
            let ints_lo_f64_bit_32 = _mm256_mul_pd(
                _mm256_cvtepi32_pd(ints_lo_bit32),
                ints_sign_bit_shift,
            );
            let ints_lo_f64 =
                _mm256_add_pd(ints_lo_f64_no_sign, ints_lo_f64_bit_32);

            let ints_hi_f64 = _mm256_cvtepi32_pd(_mm_castps_si128(ints_hi_ps));

            let ints_f64 = _mm256_add_pd(
                _mm256_mul_pd(ints_hi_f64, ints_hi_shift),
                ints_lo_f64,
            );

            let floats_inf_or_nan = _mm256_sub_pd(floats_v, floats_v);
            // let floats_not_int = _mm256_cmp_pd(
            //    floats_v,
            //    _mm256_round_pd::<{ _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC
            // }>(        floats_v,
            //    ),
            //    _CMP_NEQ_OQ,
            //);
            let not_eq_raw = _mm256_cmp_pd(floats_v, ints_f64, _CMP_NEQ_OQ);
            let not_eq = _mm256_or_pd(floats_inf_or_nan, not_eq_raw);
            // let not_eq = _mm256_or_pd(
            //    _mm256_or_pd(floats_inf_or_nan, not_eq_raw),
            //    floats_not_int,
            //);

            let mask =
                _mm256_movemask_epi8(_mm256_castpd_si256(not_eq)) as u32;

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() =
                mask_to_bool_array_neg(mask);

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(ints[i].num_eq(&floats[i]));
            i += 1;
        }
    }
    (i, None)
}

#[cfg(test)]
#[allow(clippy::cast_precision_loss)]
mod test {
    use std::mem::MaybeUninit;

    use super::cmp_eq_i64_f64_avx2;

    unsafe fn assume_slice_init<T>(s: &[MaybeUninit<T>]) -> &[T] {
        unsafe { &*(std::ptr::from_ref(s) as *const [T]) }
    }

    #[track_caller]
    fn check_eq_i64_f64(ints: &[i64], floats: &[f64], expected: &[bool]) {
        let mut res = vec![MaybeUninit::uninit(); ints.len()];
        cmp_eq_i64_f64_avx2(ints, floats, &mut res);
        assert_eq!(unsafe { assume_slice_init(&res) }, expected);
    }

    #[test]
    fn test_cmp_eq_i64_f64_avx2() {
        let ints = [1, 2, 3, 4, 5, 6, 7, 8];
        let floats = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        check_eq_i64_f64(&ints, &floats, &[true; 8]);
    }

    #[test]
    fn test_cmp_eq_i64_f64_avx2_above_2pow53() {
        let x = 9_007_199_254_740_992; // 2**53
        let xf = x as f64;
        let ints = [0, 0, x, x];
        let floats = [0., 1., xf, xf + 1.0];
        check_eq_i64_f64(&ints, &floats, &[true, false, true, false]);
    }

    #[test]
    fn test_cmp_eq_i64_f64_avx2_nan() {
        let ints = [1, 2, 3, 4];
        let floats = [1.0, f64::NAN, 3.0, f64::NAN];
        check_eq_i64_f64(&ints, &floats, &[true, false, true, false]);
    }

    #[test]
    fn test_cmp_eq_i64_f64_avx2_infinity() {
        let ints = [1, 2, 3, 4];
        let floats = [1.0, f64::INFINITY, 3.0, f64::NEG_INFINITY];
        check_eq_i64_f64(&ints, &floats, &[true, false, true, false]);
    }

    #[test]
    fn test_cmp_eq_i64_f64_avx2_negative() {
        let ints = [-1, -2, -3, -4];
        let floats = [-1.0, -2.0, -3.0, -4.0];
        check_eq_i64_f64(&ints, &floats, &[true; 4]);
    }

    #[test]
    fn test_cmp_eq_i64_f64_avx2_mixed() {
        let ints = [0, i64::MAX, i64::MIN, 42];
        let floats = [0.00001, i64::MAX as f64, i64::MIN as f64, 42.5];
        check_eq_i64_f64(&ints, &floats, &[false; 4]);
    }
}
