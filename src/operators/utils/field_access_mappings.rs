use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
};

use bitvec::slice::BitSlice;

use crate::{
    liveness_analysis::{
        LivenessData, Var, DATA_WRITES_OFFSET, HEADER_WRITES_OFFSET,
        READS_OFFSET,
    },
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct FieldAccessMode {
    pub header_writes: bool,
    pub data_writes: bool,
}

impl FieldAccessMode {
    pub fn any_writes(&self) -> bool {
        self.header_writes || self.data_writes
    }
}

pub trait AccessKind: Clone + Default {
    type ContextType;
    fn from_field_access_mode(
        ctx: &mut Self::ContextType,
        fam: FieldAccessMode,
    ) -> Self;
    fn append_field_access_mode(
        &mut self,
        ctx: &mut Self::ContextType,
        fam: FieldAccessMode,
    );
}

impl AccessKind for FieldAccessMode {
    type ContextType = ();
    fn from_field_access_mode(_ctx: &mut (), fam: FieldAccessMode) -> Self {
        fam
    }

    fn append_field_access_mode(
        &mut self,
        _ctx: &mut (),
        fam: FieldAccessMode,
    ) {
        self.header_writes |= fam.header_writes;
        self.data_writes |= fam.data_writes;
    }
}

#[derive(Clone, Default)]
pub struct WriteCountingFieldAccessType {
    pub header_write_count: u32,
    pub last_header_writing_sc: u32,
    pub data_write_count: u32,
    pub last_data_writing_sc: u32,
    pub access_count: u32,
    pub last_accessing_sc: u32,
}

impl WriteCountingFieldAccessType {
    pub fn total_write_count(&self) -> u32 {
        self.header_write_count + self.data_write_count
    }
    pub fn any_writes(&self) -> bool {
        self.total_write_count() > 0
    }
}

impl AccessKind for WriteCountingFieldAccessType {
    type ContextType = u32; // subchain id / number
    fn from_field_access_mode(
        subchain_id: &mut u32,
        fam: FieldAccessMode,
    ) -> Self {
        let mut res = WriteCountingFieldAccessType {
            header_write_count: 0,
            last_header_writing_sc: 0,
            data_write_count: 0,
            last_data_writing_sc: 0,
            access_count: 1,
            last_accessing_sc: *subchain_id,
        };
        if fam.header_writes {
            res.header_write_count = 1;
            res.last_header_writing_sc = *subchain_id;
        }
        if fam.data_writes {
            res.data_write_count = 1;
            res.last_data_writing_sc = *subchain_id;
        }
        res
    }

    fn append_field_access_mode(
        &mut self,
        subchain_id: &mut u32,
        fam: FieldAccessMode,
    ) {
        if fam.header_writes {
            self.header_write_count += 1;
            self.last_header_writing_sc = *subchain_id;
        }
        if fam.data_writes {
            self.data_write_count += 1;
            self.last_data_writing_sc = *subchain_id;
        }
        self.access_count += 1;
        self.last_accessing_sc = *subchain_id;
    }
}

#[derive(Clone, Default)]
pub struct AccessMappings<AT: AccessKind> {
    pub input_field: Option<AT>,
    // PERF: we should have two types here, one of them using a vector
    // instread of a map for cases where we never need to append
    pub fields: HashMap<StringStoreEntry, AT, BuildIdentityHasher>,
}

pub type FieldAccessMappings = AccessMappings<FieldAccessMode>;
pub type WriteCountingAccessMappings =
    AccessMappings<WriteCountingFieldAccessType>;

impl<AT: AccessKind> AccessMappings<AT> {
    pub fn append_var_data(
        &mut self,
        ctx: &mut AT::ContextType,
        ld: &LivenessData,
        var_data: &BitSlice<Cell<usize>>,
    ) {
        let var_count = ld.vars.len();
        let reads = &var_data
            [var_count * READS_OFFSET..var_count * (READS_OFFSET + 1)];
        let header_writes = &var_data[var_count * HEADER_WRITES_OFFSET
            ..var_count * (HEADER_WRITES_OFFSET + 1)];
        let data_writes = &var_data[var_count * DATA_WRITES_OFFSET
            ..var_count * (DATA_WRITES_OFFSET + 1)];
        self.fields.reserve(reads.count_ones());
        for var_id in reads.iter_ones() {
            let mode = FieldAccessMode {
                data_writes: data_writes[var_id],
                header_writes: header_writes[var_id],
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
                Var::UnreachableDummyVar | Var::BBOutput => (),
            }
        }
    }
    pub fn from_var_data(
        ctx: &mut AT::ContextType,
        ld: &LivenessData,
        var_data: &BitSlice<Cell<usize>>,
    ) -> Self {
        let mut mappings = Self::default();
        mappings.append_var_data(ctx, ld, var_data);
        mappings
    }
    pub fn iter_name_opt(
        &self,
    ) -> impl Iterator<Item = (Option<StringStoreEntry>, AT)> + '_ {
        std::iter::once((None, self.input_field.clone().unwrap_or_default()))
            .take(if self.input_field.is_some() { 1 } else { 0 })
            .chain(
                self.fields
                    .iter()
                    .map(|(name, mode)| (Some(*name), mode.clone())),
            )
    }
    pub fn get(&self, key: Option<StringStoreEntry>) -> Option<&AT> {
        match key {
            Some(name) => self.fields.get(&name),
            None => self.input_field.as_ref(),
        }
    }
}
