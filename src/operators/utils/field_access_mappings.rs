use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
};

use bitvec::slice::BitSlice;

use crate::{
    liveness_analysis::{LivenessData, Var, READS_OFFSET, WRITES_OFFSET},
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldAccessMode {
    #[default]
    Read,
    WriteHeaders,
    WriteData,
}

pub trait AccessType: Clone + Default {
    type ContextType: Copy;
    fn from_field_access_mode(
        ctx: Self::ContextType,
        fam: FieldAccessMode,
    ) -> Self;
    fn append_field_access_mode(
        &mut self,
        ctx: Self::ContextType,
        fam: FieldAccessMode,
    );
}

impl AccessType for FieldAccessMode {
    type ContextType = ();
    fn from_field_access_mode(_ctx: (), fam: FieldAccessMode) -> Self {
        fam
    }

    fn append_field_access_mode(&mut self, _ctx: (), fam: FieldAccessMode) {
        let res = match self {
            FieldAccessMode::Read => fam,
            FieldAccessMode::WriteHeaders => match fam {
                FieldAccessMode::Read => *self,
                FieldAccessMode::WriteHeaders => *self,
                FieldAccessMode::WriteData => fam,
            },
            FieldAccessMode::WriteData => *self,
        };
        *self = res;
    }
}

#[derive(Clone, Default)]
pub struct WriteCountingFieldAccessType {
    write_count: u32,
    last_writing_subchain_id: u32,
}

impl AccessType for WriteCountingFieldAccessType {
    type ContextType = u32;
    fn from_field_access_mode(subchain_id: u32, fam: FieldAccessMode) -> Self {
        match fam {
            FieldAccessMode::Read => WriteCountingFieldAccessType {
                write_count: 0,
                last_writing_subchain_id: 0,
            },
            FieldAccessMode::WriteHeaders => WriteCountingFieldAccessType {
                write_count: 1,
                last_writing_subchain_id: subchain_id,
            },
            FieldAccessMode::WriteData => WriteCountingFieldAccessType {
                write_count: 0,
                last_writing_subchain_id: 0,
            },
        }
    }

    fn append_field_access_mode(
        &mut self,
        subchain_id: u32,
        fam: FieldAccessMode,
    ) {
        match fam {
            FieldAccessMode::WriteHeaders => {
                self.last_writing_subchain_id = subchain_id;
                self.write_count += 1;
            }
            FieldAccessMode::Read => (),
            FieldAccessMode::WriteData => (),
        }
    }
}

#[derive(Clone, Default)]
pub struct AccessMappings<AT: AccessType> {
    pub input_field: Option<AT>,
    pub fields: HashMap<StringStoreEntry, AT, BuildIdentityHasher>,
}

pub type FieldAccessMappings = AccessMappings<FieldAccessMode>;
pub type WriteCountingAccessMappings =
    AccessMappings<WriteCountingFieldAccessType>;

impl<AT: AccessType> AccessMappings<AT> {
    pub fn append_var_data(
        &mut self,
        ctx: AT::ContextType,
        ld: &LivenessData,
        var_data: &BitSlice<Cell<usize>>,
    ) {
        let var_count = ld.vars.len();
        let reads = &var_data
            [var_count * READS_OFFSET..var_count * (READS_OFFSET + 1)];
        let writes = &var_data
            [var_count * WRITES_OFFSET..var_count * (WRITES_OFFSET + 1)];
        self.fields.reserve(reads.count_ones());
        for var_id in reads.iter_ones() {
            // TODO: handle data writes through append
            let mode = if writes[var_id] {
                FieldAccessMode::WriteHeaders
            } else {
                FieldAccessMode::Read
            };
            match ld.vars[var_id] {
                Var::Named(name) => match self.fields.entry(name) {
                    Entry::Occupied(mut e) => {
                        e.get_mut().append_field_access_mode(ctx, mode);
                    }
                    Entry::Vacant(e) => {
                        e.insert(AT::from_field_access_mode(ctx, mode));
                    }
                },
                Var::BBInput => {
                    if let Some(at) = &mut self.input_field {
                        at.append_field_access_mode(ctx, mode);
                    } else {
                        self.input_field =
                            Some(AT::from_field_access_mode(ctx, mode));
                    }
                }
                Var::UnreachableDummyVar => (),
            }
        }
    }
    pub fn from_var_data(
        ctx: AT::ContextType,
        ld: &LivenessData,
        var_data: &BitSlice<Cell<usize>>,
    ) -> Self {
        let mut mappings = Self::default();
        mappings.append_var_data(ctx, ld, var_data);
        mappings
    }
    pub fn iter_name_opt<'a>(
        &'a self,
    ) -> impl Iterator<Item = (Option<StringStoreEntry>, AT)> + 'a {
        std::iter::once((None, self.input_field.clone().unwrap_or_default()))
            .take(if self.input_field.is_some() { 1 } else { 0 })
            .chain(
                self.fields
                    .iter()
                    .map(|(name, mode)| (Some(*name), mode.clone())),
            )
    }
}
