use std::{
    mem::ManuallyDrop,
    sync::{RwLock, RwLockReadGuard},
};

use num::{BigInt, BigRational};

use super::{
    bytes_insertion_stream::{
        BytesInsertionStream, MaybeTextInsertionStream, TextInsertionStream,
    },
    custom_data::CustomDataBox,
    field::{FieldManager, FieldRefOffset},
    field_data::{
        field_value_flags, FieldData, FieldValueFlags, FieldValueFormat,
        FieldValueHeader, FieldValueRepr, FieldValueSize, FieldValueType,
        RunLength,
    },
    field_value::{
        format_rational, Array, FieldReference, FieldValue, FormattingContext,
        Object, SlicedFieldReference, RATIONAL_DIGITS,
    },
    field_value_ref::FieldValueSlice,
    field_value_slice_iter::FieldValueSliceIter,
    formattable::RealizedFormatKey,
    match_set::MatchSetManager,
    ref_iter::{
        AnyRefSliceIter, RefAwareFieldValueSliceIter, RefAwareInlineBytesIter,
        RefAwareInlineTextIter, RefAwareTypedRange,
    },
    stream_value::{StreamValueId, StreamValueManager},
};
use crate::{
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::field_data::{
        field_value_flags::{DELETED, SHARED_VALUE},
        INLINE_STR_MAX_LEN,
    },
    utils::{
        as_u8_slice,
        int_string_conversions::{f64_to_str, i64_to_str},
        string_store::StringStore,
        text_write::TextWrite,
    },
    NULL_STR, UNDEFINED_STR,
};

pub unsafe trait PushInterface {
    // SAFETY: this is highly unsafe, we leave a slot of uninitialized
    // data in our data buffer (it's not a standard Vec, so that's fine ;))
    // the caller must ensure that that buffer is filled before it is ever read
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueRepr,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8;
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: FieldValueRepr,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    );
    unsafe fn push_fixed_size_type_unchecked<T: PartialEq + FieldValueType>(
        &mut self,
        repr: FieldValueRepr,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    );
    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    );
    fn bytes_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> BytesInsertionStream;
    fn text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> TextInsertionStream;
    fn maybe_text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> MaybeTextInsertionStream;
    fn push_inline_bytes(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(data.len() < INLINE_STR_MAX_LEN);
        unsafe {
            self.push_variable_sized_type_unchecked(
                FieldValueRepr::BytesInline,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_inline_str(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(data.len() < INLINE_STR_MAX_LEN);
        unsafe {
            self.push_variable_sized_type_unchecked(
                FieldValueRepr::TextInline,
                data.as_bytes(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_fixed_size_type<T: PartialEq + FieldValueType>(
        &mut self,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(!T::DST && !T::ZST);
        unsafe {
            self.push_fixed_size_type_unchecked(
                T::REPR,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_zst(
        &mut self,
        kind: FieldValueRepr,
        run_length: usize,
        try_header_rle: bool,
    ) {
        assert!(kind.is_zst());
        unsafe {
            self.push_zst_unchecked(
                kind,
                field_value_flags::DEFAULT,
                run_length,
                try_header_rle,
            );
        }
    }

    // string / bytes convenience wrappers
    fn push_str_as_buffer(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type_unchecked(
                FieldValueRepr::TextBuffer,
                data.to_string(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_bytes_as_buffer(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type_unchecked(
                FieldValueRepr::BytesBuffer,
                data.to_vec(),
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_str(
        &mut self,
        data: &str,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if data.len() <= INLINE_STR_MAX_LEN {
            self.push_inline_str(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        } else {
            self.push_str_as_buffer(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_bytes(
        &mut self,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if data.len() <= INLINE_STR_MAX_LEN {
            self.push_inline_bytes(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        } else {
            self.push_bytes_as_buffer(
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }

    // fixed sized types / ZST convenience wrappers
    fn push_string(
        &mut self,
        data: String,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.push_fixed_size_type_unchecked(
                FieldValueRepr::TextBuffer,
                data,
                run_length,
                try_header_rle,
                try_data_rle,
            );
        }
    }
    fn push_bytes_buffer(
        &mut self,
        data: Vec<u8>,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_text_buffer(
        &mut self,
        data: String,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }

    fn push_custom(
        &mut self,
        data: CustomDataBox,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_int(
        &mut self,
        data: i64,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_big_int(
        &mut self,
        data: BigInt,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_float(
        &mut self,
        data: f64,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_rational(
        &mut self,
        data: BigRational,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            data,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_stream_value_id(
        &mut self,
        id: StreamValueId,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            id,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_error(
        &mut self,
        err: OperatorApplicationError,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            err,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_object(
        &mut self,
        v: Object,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(v, run_length, try_header_rle, try_data_rle);
    }
    fn push_array(
        &mut self,
        v: Array,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(v, run_length, try_header_rle, try_data_rle);
    }
    fn push_sliced_field_reference(
        &mut self,
        reference: SlicedFieldReference,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            reference,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_field_reference(
        &mut self,
        reference: FieldReference,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_fixed_size_type(
            reference,
            run_length,
            try_header_rle,
            try_data_rle,
        );
    }
    fn push_null(&mut self, run_length: usize, try_header_rle: bool) {
        self.push_zst(FieldValueRepr::Null, run_length, try_header_rle);
    }
    fn push_undefined(&mut self, run_length: usize, try_header_rle: bool) {
        self.push_zst(FieldValueRepr::Undefined, run_length, try_header_rle);
    }
    fn push_field_value_unpacked(
        &mut self,
        v: FieldValue,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        match v {
            FieldValue::Null => self.push_null(run_length, try_header_rle),
            FieldValue::Undefined => {
                self.push_undefined(run_length, try_header_rle)
            }
            FieldValue::Int(v) => {
                self.push_int(v, run_length, try_header_rle, try_data_rle)
            }
            FieldValue::BigInt(v) => self.push_fixed_size_type(
                v.clone(),
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Float(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Rational(v) => self.push_fixed_size_type(
                *v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Text(v) => {
                self.push_string(v, run_length, try_header_rle, try_data_rle)
            }
            FieldValue::Bytes(v) => self.push_bytes_buffer(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Array(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Object(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Custom(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::Error(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::FieldReference(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::SlicedFieldReference(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
            FieldValue::StreamValueId(v) => self.push_fixed_size_type(
                v,
                run_length,
                try_header_rle,
                try_data_rle,
            ),
        }
    }
    fn extend_from_ref_aware_range(
        &mut self,
        range: RefAwareTypedRange,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        // PERF: this sucks
        let fc = range.base.field_count;
        match range.base.data {
            FieldValueSlice::Null(_) => self.push_null(fc, try_header_rle),
            FieldValueSlice::Undefined(_) => {
                self.push_undefined(fc, try_header_rle)
            }
            FieldValueSlice::Int(vals) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, vals) {
                    self.push_int(
                        *v,
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::BigInt(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_big_int(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Float(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_float(
                        *v,
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Rational(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_rational(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::BytesInline(vals) => {
                // we can ignore the offset here because we copy
                for (v, rl, _offset) in
                    RefAwareInlineBytesIter::from_range(&range, vals)
                {
                    self.push_inline_bytes(
                        v,
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::TextInline(vals) => {
                // we can ignore the offset here because we copy
                for (v, rl, _offset) in
                    RefAwareInlineTextIter::from_range(&range, vals)
                {
                    self.push_inline_str(
                        v,
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::TextBuffer(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_text_buffer(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::BytesBuffer(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_bytes_buffer(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Object(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_object(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Array(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_array(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Custom(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_custom(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Error(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_error(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::StreamValueId(vals) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, vals) {
                    self.push_stream_value_id(
                        *v,
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        }
    }
    fn extend_from_ref_aware_range_smart_ref(
        &mut self,
        range: RefAwareTypedRange,
        try_header_rle: bool,
        try_data_rle: bool,
        try_ref_data_rle: bool,
        input_field_ref_offset: FieldRefOffset,
    ) {
        match range.base.data {
            FieldValueSlice::Undefined(_)
            | FieldValueSlice::Null(_)
            | FieldValueSlice::Int(_)
            | FieldValueSlice::Float(_)
            | FieldValueSlice::StreamValueId(_) => {
                self.extend_from_ref_aware_range(
                    range,
                    try_header_rle,
                    try_data_rle,
                );
            }
            FieldValueSlice::BigInt(_)
            | FieldValueSlice::Rational(_)
            | FieldValueSlice::TextBuffer(_)
            | FieldValueSlice::BytesBuffer(_)
            | FieldValueSlice::Array(_)
            | FieldValueSlice::Custom(_)
            | FieldValueSlice::Error(_)
            | FieldValueSlice::Object(_) => {
                self.push_field_reference(
                    FieldReference::new(
                        range
                            .field_ref_offset
                            .unwrap_or(input_field_ref_offset),
                    ),
                    range.base.field_count,
                    try_header_rle,
                    try_ref_data_rle,
                );
            }
            FieldValueSlice::TextInline(vals) => {
                match range.refs {
                    Some(AnyRefSliceIter::SlicedFieldRef(_)) => {
                        // in this case we need to create sliced field refs
                        // aswell to respect the offset
                        for (v, rl, offset) in
                            RefAwareInlineTextIter::from_range(&range, vals)
                        {
                            self.push_sliced_field_reference(
                                SlicedFieldReference::new(
                                    range.field_ref_offset.unwrap(),
                                    offset,
                                    offset + v.len(),
                                ),
                                rl as usize,
                                try_header_rle,
                                try_data_rle,
                            )
                        }
                    }
                    Some(AnyRefSliceIter::FieldRef(_)) | None => {
                        self.push_field_reference(
                            FieldReference::new(
                                range
                                    .field_ref_offset
                                    .unwrap_or(input_field_ref_offset),
                            ),
                            range.base.field_count,
                            try_header_rle,
                            try_ref_data_rle,
                        );
                    }
                }
            }
            FieldValueSlice::BytesInline(vals) => {
                match range.refs {
                    Some(AnyRefSliceIter::SlicedFieldRef(_)) => {
                        // in this case we need to create sliced field refs
                        // aswell to respect the offset
                        for (v, rl, offset) in
                            RefAwareInlineBytesIter::from_range(&range, vals)
                        {
                            self.push_sliced_field_reference(
                                SlicedFieldReference::new(
                                    range.field_ref_offset.unwrap(),
                                    offset,
                                    offset + v.len(),
                                ),
                                rl as usize,
                                try_header_rle,
                                try_data_rle,
                            )
                        }
                    }
                    Some(AnyRefSliceIter::FieldRef(_)) | None => {
                        self.push_field_reference(
                            FieldReference::new(
                                range
                                    .field_ref_offset
                                    .unwrap_or(input_field_ref_offset),
                            ),
                            range.base.field_count,
                            try_header_rle,
                            try_ref_data_rle,
                        );
                    }
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        }
    }
    #[allow(clippy::fn_params_excessive_bools)]
    fn extend_from_ref_aware_range_stringified_smart_ref<'a>(
        &mut self,
        fm: &FieldManager,
        msm: &MatchSetManager,
        sv_mgr: &StreamValueManager,
        op_id: OperatorId,
        string_store: &'a RwLock<StringStore>,
        string_store_ref: &mut Option<RwLockReadGuard<'a, StringStore>>,
        range: RefAwareTypedRange,
        try_header_rle: bool,
        try_data_rle: bool,
        try_ref_data_rle: bool,
        input_field_ref_offset: FieldRefOffset,
        print_rationals_raw: bool,
    ) {
        let field_count = range.base.field_count;
        match range.base.data {
            FieldValueSlice::BytesInline(_)
            | FieldValueSlice::TextInline(_)
            | FieldValueSlice::TextBuffer(_)
            | FieldValueSlice::BytesBuffer(_) => {
                self.extend_from_ref_aware_range_smart_ref(
                    range,
                    try_header_rle,
                    try_data_rle,
                    try_ref_data_rle,
                    input_field_ref_offset,
                );
            }
            FieldValueSlice::Null(_) => self.push_inline_str(
                NULL_STR,
                field_count,
                try_header_rle,
                try_data_rle,
            ),
            FieldValueSlice::Undefined(_) => self.push_inline_str(
                UNDEFINED_STR,
                field_count,
                try_header_rle,
                try_data_rle,
            ),
            FieldValueSlice::Int(vals) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, vals) {
                    self.push_inline_str(
                        &i64_to_str(false, *v),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::BigInt(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    let mut stream = self.text_insertion_stream(rl as usize);
                    stream.write_text_fmt(format_args!("{v}")).unwrap();
                }
            }
            FieldValueSlice::Float(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_str(
                        &f64_to_str(*v),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::Rational(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    let mut stream = self.text_insertion_stream(rl as usize);
                    format_rational(&mut stream, v, RATIONAL_DIGITS).unwrap();
                }
            }

            FieldValueSlice::Object(vals) => {
                let ss = string_store_ref
                    .get_or_insert_with(|| string_store.read().unwrap());
                let fc = FormattingContext {
                    ss,
                    fm,
                    msm,
                    print_rationals_raw,
                    rfk: RealizedFormatKey::default(),
                };
                for (o, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    o.format(
                        &mut self.text_insertion_stream(rl as usize),
                        &fc,
                    )
                    .unwrap();
                }
            }
            FieldValueSlice::Array(vals) => {
                let ss = string_store_ref
                    .get_or_insert_with(|| string_store.read().unwrap());
                let fc = FormattingContext {
                    ss,
                    fm,
                    msm,
                    print_rationals_raw,
                    rfk: RealizedFormatKey::default(),
                };
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    let mut stream = self.text_insertion_stream(rl as usize);
                    v.format(&mut stream, &fc).unwrap();
                }
            }
            FieldValueSlice::Custom(vals) => {
                let rfk = RealizedFormatKey::default();
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    let mut stream = self.text_insertion_stream(rl as usize);
                    v.format(&mut stream, &rfk).unwrap();
                }
            }
            FieldValueSlice::Error(vals) => {
                for (v, rl) in
                    RefAwareFieldValueSliceIter::from_range(&range, vals)
                {
                    self.push_error(
                        v.clone(),
                        rl as usize,
                        try_header_rle,
                        try_data_rle,
                    );
                }
            }
            FieldValueSlice::StreamValueId(vals) => {
                for (v, rl) in FieldValueSliceIter::from_range(&range, vals) {
                    let sv = &sv_mgr.stream_values[*v];
                    match &sv.value {
                        FieldValue::Undefined
                        | FieldValue::Null
                        | FieldValue::Int(_)
                        | FieldValue::BigInt(_)
                        | FieldValue::Float(_)
                        | FieldValue::Rational(_)
                        | FieldValue::Error(_) => {
                            debug_assert!(sv.done);
                            self.push_field_value_unpacked(
                                sv.value.clone(),
                                rl as usize,
                                try_header_rle,
                                try_data_rle,
                            )
                        }
                        FieldValue::Text(_) | FieldValue::Bytes(_) => self
                            .push_stream_value_id(
                                *v,
                                rl as usize,
                                try_header_rle,
                                try_data_rle,
                            ),
                        // TODO: mechanism for having a stream value
                        // update that triggers a simple function instead of
                        // a transform so we can stringifiy these
                        FieldValue::Array(_) => todo!(),
                        FieldValue::Object(_) => todo!(),
                        FieldValue::Custom(v) => {
                            let mut stream =
                                self.maybe_text_insertion_stream(rl as usize);
                            if let Err(e) = v.format_raw(
                                &mut stream,
                                &RealizedFormatKey::default(),
                            ) {
                                stream.abort();
                                self.push_error(
                                    e.as_operator_application_error(
                                        op_id,
                                        &v.type_name(),
                                    ),
                                    rl as usize,
                                    try_header_rle,
                                    try_data_rle,
                                );
                            }
                        }
                        FieldValue::FieldReference(_)
                        | FieldValue::SlicedFieldReference(_)
                        | FieldValue::StreamValueId(_) => {
                            unreachable!()
                        }
                    }
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        }
    }
    unsafe fn extend_unchecked<T: FieldValueType + Sized>(
        &mut self,
        repr: FieldValueRepr,
        iter: impl Iterator<Item = T>,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        if T::ZST {
            self.push_zst(T::REPR, iter.count(), try_header_rle);
            return;
        }
        // implementers of this trait would do well to specialize this
        for v in iter {
            unsafe {
                self.push_fixed_size_type_unchecked(
                    repr,
                    v,
                    1,
                    try_header_rle,
                    try_data_rle,
                );
            }
        }
    }
    fn extend_from_strings(
        &mut self,
        iter: impl Iterator<Item = String>,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.extend_unchecked(
                FieldValueRepr::TextBuffer,
                iter.map(String::into_bytes),
                try_header_rle,
                try_data_rle,
            )
        }
    }
    fn extend<T: FieldValueType + Sized>(
        &mut self,
        iter: impl Iterator<Item = T>,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        unsafe {
            self.extend_unchecked(T::REPR, iter, try_header_rle, try_data_rle)
        }
    }
    fn extend_with_variable_sized_types<
        'a,
        T: FieldValueType + ?Sized + 'a,
    >(
        &mut self,
        iter: impl Iterator<Item = &'a T>,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        // implementers of this trait would do well to specialize this
        for v in iter {
            unsafe {
                self.push_variable_sized_type_unchecked(
                    T::REPR,
                    std::slice::from_raw_parts(
                        std::ptr::from_ref(v).cast(),
                        std::mem::size_of_val(v),
                    ),
                    1,
                    try_header_rle,
                    try_data_rle,
                );
            }
        }
    }
}
impl FieldData {
    #[inline(always)]
    pub unsafe fn push_header_raw(
        &mut self,
        fmt: FieldValueFormat,
        mut run_length: usize,
    ) {
        debug_assert!(run_length > 0);
        while run_length > RunLength::MAX as usize {
            self.headers.push_back(FieldValueHeader {
                fmt,
                run_length: RunLength::MAX,
            });
            run_length -= RunLength::MAX as usize;
        }
        self.headers.push_back(FieldValueHeader {
            fmt,
            run_length: run_length as RunLength,
        });
    }
    pub unsafe fn push_header_raw_same_value_after_first(
        &mut self,
        mut fmt: FieldValueFormat,
        run_length: usize,
    ) {
        debug_assert!(run_length > 0);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.headers.push_back(FieldValueHeader {
            fmt,
            run_length: rl_to_push as RunLength,
        });
        if rl_to_push != run_length {
            fmt.set_same_value_as_previous(true);
            unsafe { self.push_header_raw(fmt, run_length - rl_to_push) };
        }
    }
    pub unsafe fn add_header_for_single_value(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        header_rle: bool,
        data_rle: bool,
    ) {
        debug_assert!(fmt.shared_value());
        if !header_rle && !data_rle {
            unsafe {
                self.push_header_raw_same_value_after_first(fmt, run_length)
            };
            return;
        }
        // safe to unwrap here, otherwise we would have gone into the
        // branch above since header rle only makes sense with a previous
        // header
        let last_header =
            unsafe { self.headers.back_mut().unwrap_unchecked() };
        if last_header.run_length == 1 {
            last_header.set_shared_value(data_rle);
        }
        if last_header.shared_value() || run_length > 1 {
            if data_rle {
                let rl_rem = last_header.run_len_rem() as usize;
                if rl_rem > run_length {
                    last_header.run_length += run_length as RunLength;
                    return;
                }
                last_header.run_length = RunLength::MAX;
                run_length -= rl_rem;
                fmt.set_same_value_as_previous(true);
                unsafe {
                    self.push_header_raw(fmt, run_length);
                }
                return;
            }
            unsafe {
                self.push_header_raw_same_value_after_first(fmt, run_length);
            }
            return;
        }
        if data_rle {
            // guaranteed to be at least two, otherwise we would have gone into
            // the shared value branch
            last_header.run_length -= 1;
            unsafe {
                self.push_header_raw_same_value_after_first(
                    fmt,
                    run_length + 1,
                );
            }
            return;
        }
        if header_rle
            && last_header.run_length as usize + run_length
                < RunLength::MAX as usize
        {
            last_header.run_length += run_length as RunLength;
            return;
        }
        unsafe {
            self.push_header_raw_same_value_after_first(fmt, run_length);
        }
    }
    pub unsafe fn add_header_padded_for_single_value(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        padding: usize,
    ) {
        debug_assert!(fmt.shared_value());
        fmt.set_leading_padding(padding);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.headers.push_back(FieldValueHeader {
            fmt,
            run_length: rl_to_push as RunLength,
        });
        if run_length == rl_to_push {
            return;
        }
        run_length -= rl_to_push;
        fmt.set_leading_padding(0);
        fmt.set_same_value_as_previous(true);
        unsafe {
            self.push_header_raw(fmt, run_length);
        }
    }
    pub unsafe fn add_header_padded_for_multiple_values(
        &mut self,
        mut fmt: FieldValueFormat,
        mut run_length: usize,
        padding: usize,
    ) {
        debug_assert!(!fmt.shared_value());
        fmt.set_leading_padding(padding);
        let rl_to_push = run_length.min(RunLength::MAX as usize);
        self.headers.push_back(FieldValueHeader {
            fmt,
            run_length: rl_to_push as RunLength,
        });
        if run_length == rl_to_push {
            return;
        }
        run_length -= rl_to_push;
        fmt.set_leading_padding(0);
        unsafe {
            self.push_header_raw(fmt, run_length);
        }
    }

    pub unsafe fn add_header_for_multiple_values(
        &mut self,
        fmt: FieldValueFormat,
        mut run_length: usize,
        format_flags_mask: FieldValueFlags,
    ) {
        debug_assert!(!fmt.shared_value());
        match self.headers.back_mut() {
            None => (),
            Some(last_header) => {
                if last_header.repr == fmt.repr
                    && last_header.size == fmt.size
                    && last_header.flags & format_flags_mask
                        == fmt.flags & format_flags_mask
                {
                    if last_header.run_length == 1 {
                        last_header.set_shared_value(false);
                    }
                    if !last_header.shared_value() {
                        let rl_rem = last_header.run_len_rem();
                        if rl_rem as usize > run_length {
                            last_header.run_length += run_length as RunLength;
                            return;
                        }
                        last_header.run_length = RunLength::MAX;
                        run_length -= rl_rem as usize;
                    }
                }
            }
        }
        unsafe {
            self.push_header_raw(fmt, run_length);
        }
    }

    pub fn dup_last_value(&mut self, run_length: usize) {
        if run_length == 0 {
            return;
        }
        let last_header = self.headers.back_mut().unwrap();
        // command buffer should clear data after last non deleted
        debug_assert!(!last_header.deleted());
        self.field_count += run_length;
        unsafe {
            if last_header.run_length > 1 && !last_header.shared_value() {
                last_header.run_length -= 1;
                let mut fmt = last_header.fmt;
                fmt.set_shared_value(true);
                self.push_header_raw(fmt, run_length + 1);
            } else {
                last_header.set_shared_value(true);
                let rl_rem = last_header.run_len_rem();
                if last_header.run_len_rem() as usize > run_length {
                    last_header.run_length += run_length as RunLength;
                } else {
                    last_header.run_length = RunLength::MAX;
                    let fmt = last_header.fmt;
                    self.push_header_raw(fmt, run_length - rl_rem as usize);
                }
            }
        }
    }
    pub fn drop_last_value(&mut self, mut run_length: usize) {
        self.field_count -= run_length;
        loop {
            if run_length == 0 {
                return;
            }
            let last_header = self.headers.back_mut().unwrap();
            if last_header.run_length as usize > run_length {
                if !last_header.deleted() {
                    last_header.run_length -= run_length as RunLength;
                }
                if !last_header.shared_value() {
                    self.data.truncate(
                        self.data.len()
                            - last_header.size as usize * run_length,
                    );
                }
                return;
            }
            if !last_header.deleted() {
                run_length -= last_header.run_length as usize;
            }
            if !last_header.same_value_as_previous() {
                self.data
                    .truncate(self.data.len() - last_header.total_size());
            }
            self.headers.pop_back();
        }
    }
}

unsafe impl PushInterface for FieldData {
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        repr: FieldValueRepr,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        debug_assert!(repr.is_variable_sized_type());
        debug_assert!(data_len <= INLINE_STR_MAX_LEN);
        if run_length == 0 {
            return std::ptr::null_mut();
        }
        self.field_count += run_length;
        let fmt = FieldValueFormat {
            repr,
            flags: SHARED_VALUE,
            size: data_len as FieldValueSize,
        };

        let mut header_rle = false;

        if try_header_rle {
            if let Some(h) = self.headers.back_mut() {
                if h.repr == repr && h.size == fmt.size && !h.deleted() {
                    header_rle = true;
                }
            }
        }

        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, false,
            );
        }
        self.data.reserve_contiguous(data_len, 0);
        let res = self.data.tail_ptr_mut();
        unsafe {
            // in debug mode, we initialize the memory with all ones
            // to make it easier to detect in the debugger
            #[cfg(debug_assertions)]
            std::ptr::write_bytes(res, 0xFF, data_len);

            self.data.set_len(self.data.len() + data_len);
        };

        res
    }
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: FieldValueRepr,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        debug_assert!(kind.is_variable_sized_type());
        debug_assert!(data.len() <= INLINE_STR_MAX_LEN);
        if run_length == 0 {
            return;
        }
        self.field_count += run_length;
        let size = data.len() as FieldValueSize;

        let mut header_rle = false;
        let mut data_rle = false;

        if try_header_rle || try_data_rle {
            if let Some(h) = self.headers.back_mut() {
                if h.repr == kind && h.size == size && !h.deleted() {
                    header_rle = true;
                    if try_data_rle {
                        let len = h.size as usize;
                        let prev_data = unsafe {
                            std::slice::from_raw_parts(
                                self.data
                                    .ptr_from_index(self.data.len() - len),
                                len,
                            )
                        };
                        data_rle = prev_data == data;
                    }
                }
            }
        }
        let fmt = FieldValueFormat {
            repr: kind,
            flags: SHARED_VALUE,
            size,
        };
        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, data_rle,
            );
        }
        if !data_rle {
            self.data.extend_from_slice(data);
        }
    }
    unsafe fn push_fixed_size_type_unchecked<T: PartialEq + FieldValueType>(
        &mut self,
        repr: FieldValueRepr,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        assert!(repr == T::REPR);
        debug_assert!(repr.is_fixed_size_type());
        if run_length == 0 {
            return;
        }
        self.field_count += run_length;
        let mut data_rle = false;
        let mut header_rle = false;
        let fmt = FieldValueFormat {
            repr,
            flags: SHARED_VALUE,
            size: std::mem::size_of::<T>() as FieldValueSize,
        };
        if repr.needs_alignment() {
            let align = unsafe { self.pad_to_align() };
            if align != 0 {
                unsafe {
                    self.add_header_padded_for_single_value(
                        fmt, run_length, align,
                    );
                }
                if !data_rle {
                    let data = ManuallyDrop::new(data);
                    self.data.extend_from_slice(unsafe { as_u8_slice(&data) });
                }
                return;
            }
        }
        if try_header_rle || try_data_rle {
            if let Some(h) = self.headers.back_mut() {
                if h.repr == repr && !h.deleted() {
                    header_rle = true;
                    if try_data_rle {
                        data_rle = unsafe {
                            data == *self
                                .data
                                .ptr_from_index(
                                    self.data.len() - std::mem::size_of::<T>(),
                                )
                                .cast::<T>()
                        };
                    }
                }
            }
        }
        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, data_rle,
            );
        }
        if !data_rle {
            let data = ManuallyDrop::new(data);
            self.data.extend_from_slice(unsafe { as_u8_slice(&data) });
        }
    }
    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        const MUST_MATCH_HEADER_FLAGS: FieldValueFlags = DELETED;
        debug_assert!(kind.is_zst());
        if run_length == 0 {
            return;
        }
        self.field_count += run_length;
        let fmt = FieldValueFormat {
            repr: kind,
            flags: flags | SHARED_VALUE,
            size: 0,
        };
        let mut header_rle = false;
        if try_header_rle {
            if let Some(h) = self.headers.back_mut() {
                header_rle = h.repr == kind
                    && h.flags & MUST_MATCH_HEADER_FLAGS
                        == flags & MUST_MATCH_HEADER_FLAGS;
            }
        }
        // when we have header_rle, that implies data_rle here, because
        // the type has no value
        unsafe {
            self.add_header_for_single_value(
                fmt, run_length, header_rle, header_rle,
            );
        }
    }

    fn bytes_insertion_stream(
        &mut self,
        run_len: usize,
    ) -> BytesInsertionStream {
        BytesInsertionStream::new(self, run_len)
    }
    fn text_insertion_stream(
        &mut self,
        run_len: usize,
    ) -> TextInsertionStream {
        TextInsertionStream::new(self, run_len)
    }
    fn maybe_text_insertion_stream(
        &mut self,
        run_len: usize,
    ) -> MaybeTextInsertionStream {
        MaybeTextInsertionStream::new(self, run_len)
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::{
        field_data::FieldData, push_interface::PushInterface,
    };

    #[test]
    fn no_header_rle_for_distinct_shared_values() {
        let mut fd = FieldData::default();
        fd.push_int(1, 2, true, false);
        fd.push_int(2, 2, true, false);
        assert!(fd.headers.len() == 2);
    }
}
