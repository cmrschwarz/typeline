use crate::{match_value::MatchValueKind, sync_variant::SyncVariantImpl};

//if the u32 overflows we just split into two values
pub type RunLength = u32;

#[allow(dead_code)] //TODO
struct FieldValueHeader {
    kind: MatchValueKind,
    sync_variant: SyncVariantImpl,
    flags: u8, // is_stream, value_shared_with_next,
    run_length: RunLength,
}

#[derive(Default)]
pub struct FieldData {
    header: Vec<FieldValueHeader>,
    data: Vec<u8>,
}

impl FieldData {
    pub fn clear(&mut self) {
        self.header.clear();
        self.data.clear();
    }
}

pub type EntryId = usize;

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
