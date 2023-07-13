use std::{borrow::Cow, cmp::min, collections::HashMap, mem::transmute, num::NonZeroU32};

pub type StringStoreEntry = NonZeroU32;

pub const INVALID_STRING_STORE_ENTRY: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(u32::MAX) };

pub struct StringStore {
    arena: Vec<Vec<u8>>,
    existing_strings: Vec<Vec<Box<str>>>,
    table_idx_to_str: Vec<&'static str>,
    table_str_to_idx: HashMap<&'static str, StringStoreEntry>,
}

impl Default for StringStore {
    fn default() -> Self {
        Self {
            table_str_to_idx: Default::default(),
            table_idx_to_str: Default::default(),
            arena: vec![Vec::with_capacity(1024)],
            existing_strings: vec![Vec::with_capacity(8)],
        }
    }
}

impl StringStore {
    pub fn claim_id_without_lookup(&mut self, entry: &'static str) -> StringStoreEntry {
        self.table_idx_to_str.push(entry);
        let id = StringStoreEntry::try_from(self.table_idx_to_str.len() as u32).unwrap();
        assert!(id < INVALID_STRING_STORE_ENTRY);
        self.table_str_to_idx.insert(entry, id);
        id
    }
    pub fn intern_cloned(&mut self, entry: &str) -> StringStoreEntry {
        if let Some(key) = self.table_str_to_idx.get(entry) {
            return *key;
        }
        let len = entry.len();
        let mut bucket = self.arena.last_mut().unwrap();
        let mut bucket_cap = bucket.capacity();
        let mut bucket_len = bucket.len();
        if bucket_cap - bucket_len < len {
            bucket_cap *= 2;
            self.arena
                .push(Vec::with_capacity(min(bucket_cap, len.next_power_of_two())));
            bucket = self.arena.last_mut().unwrap();
            bucket_len = 0;
        }
        bucket.extend_from_slice(entry.as_bytes());
        let str_ref = &bucket[bucket_len..bucket_len + len];
        // SAFETY: this is fine because these never get handed out
        let str_ref_static = unsafe { transmute::<&[u8], &'static str>(str_ref) };
        self.claim_id_without_lookup(str_ref_static)
    }

    pub fn intern_moved(&mut self, entry: String) -> StringStoreEntry {
        if let Some(key) = self.table_str_to_idx.get(entry.as_str()) {
            return *key;
        }
        let mut bucket = self.existing_strings.last_mut().unwrap();
        let mut bucket_cap = bucket.capacity();
        let bucket_len = bucket.len();
        if bucket_cap == bucket_len {
            bucket_cap *= 2;
            self.existing_strings.push(Vec::with_capacity(bucket_cap));
            bucket = self.existing_strings.last_mut().unwrap();
        }
        bucket.push(entry.into_boxed_str());
        let str_ref = &**bucket.last().unwrap();
        // SAFETY: this is fine because these never get handed out
        let str_ref_static = unsafe { transmute::<&str, &'static str>(str_ref) };
        self.claim_id_without_lookup(str_ref_static)
    }
    pub fn intern_cow(&mut self, cow: Cow<'static, str>) -> StringStoreEntry {
        match cow {
            Cow::Borrowed(s) => self.intern_cloned(s),
            Cow::Owned(s) => self.intern_moved(s),
        }
    }
    pub fn lookup(&self, entry: StringStoreEntry) -> &str {
        &self.table_idx_to_str[u32::from(entry) as usize - 1]
    }
}
