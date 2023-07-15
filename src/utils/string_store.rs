use std::{borrow::Cow, cmp::min, collections::HashMap, mem::transmute, num::NonZeroU32};

pub type StringStoreEntry = NonZeroU32;

pub const INVALID_STRING_STORE_ENTRY: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(u32::MAX) };
pub const INVALID_STRING_STORE_ENTRY_2: NonZeroU32 =
    unsafe { NonZeroU32::new_unchecked(u32::MAX - 1) };

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

impl Clone for StringStore {
    fn clone(&self) -> Self {
        let mut res = Self {
            arena: self.arena.clone(),
            existing_strings: self.existing_strings.clone(),
            table_idx_to_str: Vec::with_capacity(self.table_idx_to_str.capacity()),
            table_str_to_idx: HashMap::with_capacity(self.table_str_to_idx.capacity()),
        };

        for ex_str_buf in self.existing_strings.iter() {
            res.existing_strings
                .push(Vec::with_capacity(ex_str_buf.capacity()));
        }
        for arena_buf in self.arena.iter() {
            res.arena.push(Vec::with_capacity(arena_buf.capacity()));
        }

        let mut arena_outer_idx = 0;
        let mut arena_inner_idx = 0;

        let mut existing_strs_outer_idx = 0;
        let mut existing_strs_inner_idx = 0;
        for i in 0..self.table_idx_to_str.len() {
            let idx = StringStoreEntry::try_from(i as u32 + 1).unwrap();
            let str = self.table_idx_to_str[i];
            let str_ptr = str.as_ptr();
            let arena_str_start_ptr = &self.arena[arena_outer_idx][arena_inner_idx] as *const u8;
            if arena_str_start_ptr == str.as_ptr() {
                res.arena[arena_outer_idx].extend(str.as_bytes());
                let str_clone = &res.arena[arena_outer_idx].as_slice()
                    [arena_inner_idx..arena_inner_idx + str.len()];
                // SAFETY: same argument as for normal interning, we are just cloning here
                let str_ref_static: &'static str = unsafe { std::mem::transmute(str_clone) };
                res.table_str_to_idx.insert(str_ref_static, idx);
                res.table_idx_to_str.push(str_ref_static);
                arena_inner_idx += str.len();
                if arena_inner_idx == self.arena[arena_outer_idx].len() {
                    arena_outer_idx += 1;
                    arena_inner_idx = 0;
                }
            } else {
                let ex_str =
                    &self.existing_strings[existing_strs_outer_idx][existing_strs_inner_idx];
                assert!(ex_str.as_ptr() == str_ptr);
                let str_clone = ex_str.clone();
                // SAFETY: same argument as for normal interning, we are just cloning here
                let str_ref_static: &'static str = unsafe { std::mem::transmute(ex_str.as_ref()) };
                res.existing_strings[existing_strs_outer_idx].push(str_clone);
                res.table_idx_to_str[i] = str_ref_static;
                res.table_str_to_idx.insert(str_ref_static, idx);
                existing_strs_inner_idx += 1;
                if existing_strs_inner_idx == self.arena[existing_strs_outer_idx].len() {
                    existing_strs_outer_idx += 1;
                    existing_strs_inner_idx = 0;
                }
            }
        }

        res
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
        let str = entry.into_boxed_str();
        let str_ref = str.as_ref();
        // SAFETY: this is fine because these never get handed out
        let str_ref_static: &'static str = unsafe { transmute::<&str, &'static str>(str_ref) };
        bucket.push(str);
        let idx = self.claim_id_without_lookup(str_ref_static);
        idx
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
