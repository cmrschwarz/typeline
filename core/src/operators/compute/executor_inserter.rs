use std::cell::RefMut;

use metamatch::metamatch;

use crate::record_data::{
    field_data::FieldData, push_interface::PushInterface,
    varying_type_inserter::VaryingTypeInserter,
};

#[allow(clippy::large_enum_variant)]
pub enum ExecutorInserter<'a> {
    Output(VaryingTypeInserter<&'a mut FieldData>),
    TempField(VaryingTypeInserter<RefMut<'a, FieldData>>),
}

unsafe impl PushInterface for ExecutorInserter<'_> {
    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: crate::record_data::field_data::FieldValueRepr,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => unsafe {
                    inserter.push_variable_sized_type_uninit(
                        kind,
                        data_len,
                        run_length,
                        try_header_rle
                    )
                }
            }
        }
    }

    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: crate::record_data::field_data::FieldValueRepr,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => unsafe {
                    inserter.push_variable_sized_type_unchecked(
                        kind,
                        data,
                        run_length,
                        try_header_rle,
                        try_data_rle,
                    )
                }
            }
        }
    }

    unsafe fn push_fixed_size_type_unchecked<
        T: PartialEq + crate::record_data::field_data::FieldValueType,
    >(
        &mut self,
        repr: crate::record_data::field_data::FieldValueRepr,
        data: T,
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => unsafe {
                    inserter.push_fixed_size_type_unchecked(
                        repr,
                        data,
                        run_length,
                        try_header_rle,
                        try_data_rle,
                    )
                }
            }
        }
    }

    unsafe fn push_zst_unchecked(
        &mut self,
        kind: crate::record_data::field_data::FieldValueRepr,
        flags: crate::record_data::field_data::FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => unsafe {
                    inserter.push_zst_unchecked(
                        kind,
                        flags,
                        run_length,
                        try_header_rle,
                    )
                }
            }
        }
    }

    fn bytes_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> crate::record_data::bytes_insertion_stream::BytesInsertionStream {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => {
                    inserter.bytes_insertion_stream(
                        run_length
                    )
                }
            }
        }
    }

    fn text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> crate::record_data::bytes_insertion_stream::TextInsertionStream {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => {
                    inserter.text_insertion_stream(
                        run_length
                    )
                }
            }
        }
    }

    fn maybe_text_insertion_stream(
        &mut self,
        run_length: usize,
    ) -> crate::record_data::bytes_insertion_stream::MaybeTextInsertionStream
    {
        metamatch! {
            match self {
                #[expand(T in [Output, TempField])]
                ExecutorInserter::T(inserter) => {
                    inserter.maybe_text_insertion_stream(
                        run_length
                    )
                }
            }
        }
    }
}
