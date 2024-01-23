use smallvec::{smallvec, SmallVec};

use crate::utils::string_store::StringStoreEntry;

use super::{
    field_data::{FieldData, FieldValueFlags, FieldValueRepr, FieldValueType},
    push_interface::PushInterface,
};

#[derive(Default, Clone)]
pub struct NamedField {
    pub name: Option<StringStoreEntry>,
    pub data: FieldData,
}

#[derive(Clone)]
pub struct RecordSet {
    pub fields: SmallVec<[NamedField; 1]>,
}

impl Default for RecordSet {
    fn default() -> Self {
        Self {
            fields: smallvec![NamedField::default()],
        }
    }
}

unsafe impl PushInterface for RecordSet {
    unsafe fn push_variable_sized_type_unchecked(
        &mut self,
        kind: FieldValueRepr,
        data: &[u8],
        run_length: usize,
        try_header_rle: bool,
        try_data_rle: bool,
    ) {
        self.push_null_to_secondary_cols(run_length);
        unsafe {
            self.fields
                .first_mut()
                .unwrap()
                .data
                .push_variable_sized_type_unchecked(
                    kind,
                    data,
                    run_length,
                    try_header_rle,
                    try_data_rle,
                );
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
        self.push_null_to_secondary_cols(run_length);
        unsafe {
            self.fields
                .first_mut()
                .unwrap()
                .data
                .push_fixed_size_type_unchecked(
                    repr,
                    data,
                    run_length,
                    try_header_rle,
                    try_data_rle,
                );
        }
    }

    unsafe fn push_zst_unchecked(
        &mut self,
        kind: FieldValueRepr,
        flags: FieldValueFlags,
        run_length: usize,
        try_header_rle: bool,
    ) {
        self.push_null_to_secondary_cols(run_length);
        unsafe {
            self.fields.first_mut().unwrap().data.push_zst_unchecked(
                kind,
                flags,
                run_length,
                try_header_rle,
            );
        }
    }

    unsafe fn push_variable_sized_type_uninit(
        &mut self,
        kind: FieldValueRepr,
        data_len: usize,
        run_length: usize,
        try_header_rle: bool,
    ) -> *mut u8 {
        self.push_null_to_secondary_cols(run_length);
        unsafe {
            self.fields
                .first_mut()
                .unwrap()
                .data
                .push_variable_sized_type_uninit(
                    kind,
                    data_len,
                    run_length,
                    try_header_rle,
                )
        }
    }
}

impl RecordSet {
    pub fn adjust_field_lengths(&mut self) -> usize {
        let max_field_len = self
            .fields
            .iter()
            .map(|f| f.data.field_count())
            .max()
            .unwrap_or(0);
        for f in &mut self.fields {
            let len = f.data.field_count();
            if len < max_field_len {
                f.data.push_null(max_field_len - len, true);
            }
        }
        max_field_len
    }
    fn push_null_to_secondary_cols(&mut self, run_length: usize) {
        for f in self.fields.iter_mut().skip(1) {
            f.data.push_null(run_length, true);
        }
    }
}
