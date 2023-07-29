use std::{cell::Cell, collections::HashMap};

use bitvec::slice::BitSlice;

use crate::{
    liveness_analysis::{LivenessData, Var, READS_OFFSET, WRITES_OFFSET},
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

#[derive(Clone, Default, PartialEq, Eq)]
pub enum FieldAccessMode {
    #[default]
    Read,
    WriteHeaders,
    WriteData,
}

#[derive(Clone, Default)]
pub struct FieldAccessMappings {
    pub input_field: Option<FieldAccessMode>,
    pub fields:
        HashMap<StringStoreEntry, FieldAccessMode, BuildIdentityHasher>,
}

impl FieldAccessMappings {
    pub fn from_var_data(
        ld: &LivenessData,
        var_data: &BitSlice<Cell<usize>>,
    ) -> Self {
        let mut mappings = FieldAccessMappings::default();
        let var_count = ld.vars.len();
        for var_id in var_data
            [var_count * READS_OFFSET..var_count * (READS_OFFSET + 1)]
            .iter_ones()
        {
            // TODO: handle data writes through append
            let writes = var_data[var_count * WRITES_OFFSET + var_id];
            let mode = if writes {
                FieldAccessMode::WriteHeaders
            } else {
                FieldAccessMode::Read
            };
            match ld.vars[var_id] {
                Var::Named(name) => {
                    mappings.fields.insert(name, mode);
                }
                Var::BBInput => {
                    mappings.input_field = Some(mode);
                }
                Var::UnreachableDummyVar => (),
            }
        }
        mappings
    }
    pub fn iter_name_opt<'a>(
        &'a self,
    ) -> impl Iterator<Item = (Option<StringStoreEntry>, FieldAccessMode)> + 'a
    {
        std::iter::once((None, self.input_field.clone().unwrap_or_default()))
            .take(if self.input_field.is_some() { 1 } else { 0 })
            .chain(
                self.fields
                    .iter()
                    .map(|(name, mode)| (Some(*name), mode.clone())),
            )
    }
}
