use std::{
    arch::x86_64::{
        __m256d, __m256i, _mm256_loadu_pd, _mm256_loadu_si256,
        _mm256_movemask_epi8, _mm256_movemask_pd, _mm256_set1_epi64x,
        _mm256_set1_pd, _mm256_storeu_si256,
    },
    convert::Infallible,
    mem::MaybeUninit,
};

use crate::{
    operators::compute::HwInfo,
    record_data::field_data::FixedSizeFieldValueType,
};
use std::fmt::Debug;

use super::{
    calc_until_error_baseline, calc_until_error_lhs_immediate_baseline,
    calc_until_error_rhs_immediate_baseline, BinaryOp,
    ErrorToOperatorApplicationError,
};

// avx2 -> 256 bit registers -> 4 i64 elements
pub const AVX2_I64_ELEM_COUNT: usize = 4;

pub fn calc_infallible_avx2(
    lhs: &[i64],
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
    operation: impl Fn(__m256i, __m256i) -> __m256i,
    base_case: impl Fn(&i64, &i64) -> i64,
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

            let res_v = operation(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(&lhs[i], &rhs[i]));
            i += 1;
        }

        i
    }
}

pub fn calc_infallible_avx2_lhs_immediate(
    lhs: &i64,
    rhs: &[i64],
    res: &mut [MaybeUninit<i64>],
    operation: impl Fn(__m256i, __m256i) -> __m256i,
    base_case: impl Fn(&i64, &i64) -> i64,
) -> usize {
    let len_min = rhs.len().min(res.len());
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let lhs_v = _mm256_set1_epi64x(*lhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs_p.add(i).cast::<__m256i>());

            let res_v = operation(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(lhs, &rhs[i]));
            i += 1;
        }

        i
    }
}

pub fn calc_infallible_avx2_rhs_immediate(
    lhs: &[i64],
    rhs: &i64,
    res: &mut [MaybeUninit<i64>],
    operation: impl Fn(__m256i, __m256i) -> __m256i,
    base_case: impl Fn(&i64, &i64) -> i64,
) -> usize {
    let len_min = lhs.len().min(res.len());
    let lhs_p = lhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let rhs_v = _mm256_set1_epi64x(*rhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            let res_v = operation(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(&lhs[i], rhs));
            i += 1;
        }

        i
    }
}

pub fn calc_until_error_avx2<'a, Error>(
    lhs: &[i64],
    rhs: &[i64],
    res: &'a mut [MaybeUninit<i64>],
    operation: impl Fn(__m256i, __m256i) -> (__m256i, Result<(), (usize, Error)>),
    base_case: impl Fn(&i64, &i64) -> Result<i64, Error>,
) -> (usize, Option<Error>) {
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

            let (res_v, e) = operation(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            if let Err((err_idx, err)) = e {
                return (i + err_idx, Some(err));
            }

            i += 4;
        }

        while i < len_min {
            match base_case(&lhs[i], &rhs[i]) {
                Ok(v) => res[i] = MaybeUninit::new(v),
                Err(e) => return (i, Some(e)),
            }
            i += 1;
        }

        (i, None)
    }
}

pub fn calc_until_error_avx2_lhs_immediate<'a, Error>(
    lhs: &i64,
    rhs: &[i64],
    res: &'a mut [MaybeUninit<i64>],
    operation: impl Fn(__m256i, __m256i) -> (__m256i, Result<(), (usize, Error)>),
    base_case: impl Fn(&i64, &i64) -> Result<i64, Error>,
) -> (usize, Option<Error>) {
    let len_min = rhs.len().min(res.len());
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let lhs_v = _mm256_set1_epi64x(*lhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs_p.add(i).cast::<__m256i>());

            let (res_v, e) = operation(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            if let Err((err_idx, err)) = e {
                return (i + err_idx, Some(err));
            }

            i += 4;
        }

        while i < len_min {
            match base_case(lhs, &rhs[i]) {
                Ok(v) => res[i] = MaybeUninit::new(v),
                Err(e) => return (i, Some(e)),
            }
            i += 1;
        }

        (i, None)
    }
}

pub fn calc_until_error_avx2_rhs_immediate<'a, Error>(
    lhs: &[i64],
    rhs: &i64,
    res: &'a mut [MaybeUninit<i64>],
    operation: impl Fn(__m256i, __m256i) -> (__m256i, Result<(), (usize, Error)>),
    base_case: impl Fn(&i64, &i64) -> Result<i64, Error>,
) -> (usize, Option<Error>) {
    let len_min = lhs.len().min(res.len());
    let lhs_p = lhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let rhs_v = _mm256_set1_epi64x(*rhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            let (res_v, e) = operation(lhs_v, rhs_v);

            #[allow(clippy::cast_ptr_alignment)]
            _mm256_storeu_si256(res_p.add(i).cast::<__m256i>(), res_v);

            if let Err((err_idx, err)) = e {
                return (i + err_idx, Some(err));
            }

            i += 4;
        }

        while i < len_min {
            match base_case(&lhs[i], rhs) {
                Ok(v) => res[i] = MaybeUninit::new(v),
                Err(e) => return (i, Some(e)),
            }
            i += 1;
        }
        (i, None)
    }
}

pub fn mask_to_bool_array(mask: u32) -> [bool; AVX2_I64_ELEM_COUNT] {
    [
        (mask & 0x0000_00FF) != 0,
        (mask & 0x0000_FF00) != 0,
        (mask & 0x00FF_0000) != 0,
        (mask & 0xFF00_0000) != 0,
    ]
}

pub fn mask_to_bool_array_neg(mask: u32) -> [bool; AVX2_I64_ELEM_COUNT] {
    #[allow(clippy::verbose_bit_mask)]
    [
        (mask & 0x0000_00FF) == 0,
        (mask & 0x0000_FF00) == 0,
        (mask & 0x00FF_0000) == 0,
        (mask & 0xFF00_0000) == 0,
    ]
}

pub fn mm256d_to_bool_array(vec: __m256d) -> [bool; AVX2_I64_ELEM_COUNT] {
    mask_to_bool_array(unsafe { _mm256_movemask_pd(vec) } as u32)
}

pub fn mm256i_to_bool_array(vec: __m256i) -> [bool; AVX2_I64_ELEM_COUNT] {
    mask_to_bool_array(unsafe { _mm256_movemask_epi8(vec) } as u32)
}

pub fn mm256i_to_bool_array_neg(vec: __m256i) -> [bool; AVX2_I64_ELEM_COUNT] {
    mask_to_bool_array_neg(unsafe { _mm256_movemask_epi8(vec) } as u32)
}

pub fn calc_cmp_avx2(
    lhs: &[i64],
    rhs: &[i64],
    res: &mut [MaybeUninit<bool>],
    operation: impl Fn(__m256i, __m256i) -> [bool; AVX2_I64_ELEM_COUNT],
    base_case: impl Fn(&i64, &i64) -> bool,
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

            let res_v = operation(lhs_v, rhs_v);

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() = res_v;

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(&lhs[i], &rhs[i]));
            i += 1;
        }

        i
    }
}

pub fn calc_cmp_avx2_lhs_immediate(
    lhs: &i64,
    rhs: &[i64],
    res: &mut [MaybeUninit<bool>],
    operation: impl Fn(__m256i, __m256i) -> [bool; AVX2_I64_ELEM_COUNT],
    base_case: impl Fn(&i64, &i64) -> bool,
) -> usize {
    let len_min = rhs.len().min(res.len());
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let lhs_v = _mm256_set1_epi64x(*lhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_si256(rhs_p.add(i).cast::<__m256i>());

            let res = operation(lhs_v, rhs_v);

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() = res;

            i += AVX2_I64_ELEM_COUNT;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(lhs, &rhs[i]));
            i += 1;
        }

        i
    }
}

pub fn calc_cmp_avx2_rhs_immediate(
    lhs: &[i64],
    rhs: &i64,
    res: &mut [MaybeUninit<bool>],
    operation: impl Fn(__m256i, __m256i) -> [bool; AVX2_I64_ELEM_COUNT],
    base_case: impl Fn(&i64, &i64) -> bool,
) -> usize {
    let len_min = lhs.len().min(res.len());
    let lhs_p = lhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let rhs_v = _mm256_set1_epi64x(*rhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_si256(lhs_p.add(i).cast::<__m256i>());

            let res = operation(lhs_v, rhs_v);

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() = res;

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(&lhs[i], rhs));
            i += 1;
        }

        i
    }
}

pub fn calc_cmp_f64_avx2(
    lhs: &[f64],
    rhs: &[f64],
    res: &mut [MaybeUninit<bool>],
    operation: impl Fn(__m256d, __m256d) -> [bool; AVX2_I64_ELEM_COUNT],
    base_case: impl Fn(&f64, &f64) -> bool,
) -> usize {
    let len_min = lhs.len().min(rhs.len()).min(res.len());
    let lhs_p = lhs.as_ptr();
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            let lhs_v = _mm256_loadu_pd(lhs_p.add(i));
            let rhs_v = _mm256_loadu_pd(rhs_p.add(i));

            let res = operation(lhs_v, rhs_v);

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() = res;

            i += 4;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(&lhs[i], &rhs[i]));
            i += 1;
        }

        i
    }
}

pub fn calc_cmp_f64_avx2_lhs_immediate(
    lhs: &f64,
    rhs: &[f64],
    res: &mut [MaybeUninit<bool>],
    operation: impl Fn(__m256d, __m256d) -> [bool; AVX2_I64_ELEM_COUNT],
    base_case: impl Fn(&f64, &f64) -> bool,
) -> usize {
    let len_min = rhs.len().min(res.len());
    let rhs_p = rhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let lhs_v = _mm256_set1_pd(*lhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let rhs_v = _mm256_loadu_pd(rhs_p.add(i));

            let res_v = operation(lhs_v, rhs_v);

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() = res_v;

            i += AVX2_I64_ELEM_COUNT;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(lhs, &rhs[i]));
            i += 1;
        }

        i
    }
}

pub fn calc_cmp_f64_avx2_rhs_immediate(
    lhs: &[f64],
    rhs: &f64,
    res: &mut [MaybeUninit<bool>],
    operation: impl Fn(__m256d, __m256d) -> [bool; AVX2_I64_ELEM_COUNT],
    base_case: impl Fn(&f64, &f64) -> bool,
) -> usize {
    let len_min = lhs.len().min(res.len());
    let lhs_p = lhs.as_ptr();
    let res_p = res.as_mut_ptr();

    let mut i = 0;

    unsafe {
        let rhs_v = _mm256_set1_pd(*rhs);
        while i + AVX2_I64_ELEM_COUNT <= len_min {
            #[allow(clippy::cast_ptr_alignment)]
            let lhs_v = _mm256_loadu_pd(lhs_p.add(i));

            let res_v = operation(lhs_v, rhs_v);

            *res_p.add(i).cast::<[bool; AVX2_I64_ELEM_COUNT]>() = res_v;

            i += AVX2_I64_ELEM_COUNT;
        }

        while i < len_min {
            res[i] = MaybeUninit::new(base_case(&lhs[i], rhs));
            i += 1;
        }

        i
    }
}

pub unsafe trait BinaryOpAvx2Aware {
    type Lhs: FixedSizeFieldValueType;
    type Rhs: FixedSizeFieldValueType;
    type Output: FixedSizeFieldValueType;
    type Error: Debug + ErrorToOperatorApplicationError;

    const AVX2_MIN_ELEM_COUNT: usize = 4;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error>;

    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>);

    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>);

    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>);
}

pub struct BinaryOpAvx2Adapter<OP: BinaryOpAvx2Aware>(OP);
unsafe impl<OP: BinaryOpAvx2Aware> BinaryOp for BinaryOpAvx2Adapter<OP> {
    type Lhs = OP::Lhs;

    type Rhs = OP::Rhs;

    type Output = OP::Output;

    type Error = OP::Error;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        OP::try_calc_single(lhs, rhs)
    }

    fn calc_until_error<'a>(
        hwinfo: HwInfo,
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        if hwinfo.avx2() && res.len() >= OP::AVX2_MIN_ELEM_COUNT {
            OP::calc_until_error_avx2(lhs, rhs, res)
        } else {
            calc_until_error_baseline(lhs, rhs, res, OP::try_calc_single)
        }
    }

    fn calc_until_error_rhs_immediate<'a>(
        hwinfo: HwInfo,
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        if hwinfo.avx2() && res.len() >= OP::AVX2_MIN_ELEM_COUNT {
            OP::calc_until_error_rhs_immediate_avx2(lhs, rhs, res)
        } else {
            calc_until_error_rhs_immediate_baseline(
                lhs,
                rhs,
                res,
                OP::try_calc_single,
            )
        }
    }

    fn calc_until_error_lhs_immediate<'a>(
        hwinfo: HwInfo,
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        if hwinfo.avx2() && res.len() >= OP::AVX2_MIN_ELEM_COUNT {
            OP::calc_until_error_lhs_immediate_avx2(lhs, rhs, res)
        } else {
            calc_until_error_lhs_immediate_baseline(
                lhs,
                rhs,
                res,
                OP::try_calc_single,
            )
        }
    }
}

pub trait BinaryOpCmpI64I64Avx2Aware {
    fn cmp_single(lhs: &i64, rhs: &i64) -> bool;
    fn cmp_avx2(lhs: __m256i, rhs: __m256i) -> [bool; AVX2_I64_ELEM_COUNT];
}

pub type BinaryOpCmpI64Avx2Adapter<OP> =
    BinaryOpAvx2Adapter<BinaryOpCmpI64Avx2AdapterInternal<OP>>;

pub struct BinaryOpCmpI64Avx2AdapterInternal<OP: BinaryOpCmpI64I64Avx2Aware>(
    OP,
);

unsafe impl<Op: BinaryOpCmpI64I64Avx2Aware> BinaryOpAvx2Aware
    for BinaryOpCmpI64Avx2AdapterInternal<Op>
{
    type Lhs = i64;
    type Rhs = i64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(Op::cmp_single(lhs, rhs))
    }

    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_avx2(lhs, rhs, res, Op::cmp_avx2, Op::cmp_single);
        (len, None)
    }

    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_avx2_rhs_immediate(
            lhs,
            rhs,
            res,
            Op::cmp_avx2,
            Op::cmp_single,
        );
        (len, None)
    }

    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_avx2_lhs_immediate(
            lhs,
            rhs,
            res,
            Op::cmp_avx2,
            Op::cmp_single,
        );
        (len, None)
    }
}

pub trait BinaryOpCmpF64F64Avx2Aware {
    fn cmp_single(lhs: &f64, rhs: &f64) -> bool;
    fn cmp_avx2(lhs: __m256d, rhs: __m256d) -> [bool; AVX2_I64_ELEM_COUNT];
}
pub type BinaryOpCmpF64Avx2Adapter<OP> =
    BinaryOpAvx2Adapter<BinaryOpCmpF64Avx2AdapterInternal<OP>>;

pub struct BinaryOpCmpF64Avx2AdapterInternal<OP: BinaryOpCmpF64F64Avx2Aware>(
    OP,
);

unsafe impl<Op: BinaryOpCmpF64F64Avx2Aware> BinaryOpAvx2Aware
    for BinaryOpCmpF64Avx2AdapterInternal<Op>
{
    type Lhs = f64;
    type Rhs = f64;
    type Output = bool;
    type Error = Infallible;

    fn try_calc_single(
        lhs: &Self::Lhs,
        rhs: &Self::Rhs,
    ) -> Result<Self::Output, Self::Error> {
        Ok(Op::cmp_single(lhs, rhs))
    }

    fn calc_until_error_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len =
            calc_cmp_f64_avx2(lhs, rhs, res, Op::cmp_avx2, Op::cmp_single);
        (len, None)
    }

    fn calc_until_error_rhs_immediate_avx2<'a>(
        lhs: &[Self::Lhs],
        rhs: &Self::Rhs,
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_f64_avx2_rhs_immediate(
            lhs,
            rhs,
            res,
            Op::cmp_avx2,
            Op::cmp_single,
        );
        (len, None)
    }

    fn calc_until_error_lhs_immediate_avx2<'a>(
        lhs: &Self::Lhs,
        rhs: &[Self::Rhs],
        res: &'a mut [MaybeUninit<Self::Output>],
    ) -> (usize, Option<Self::Error>) {
        let len = calc_cmp_f64_avx2_lhs_immediate(
            lhs,
            rhs,
            res,
            Op::cmp_avx2,
            Op::cmp_single,
        );
        (len, None)
    }
}
