use std::{
    alloc::Layout, borrow::Cow, cmp::min, collections::HashMap, hash::Hash,
    num::NonZeroU32,
};

pub type StringStoreEntry = NonZeroU32;

#[derive(Clone, Copy)]
struct StrPtr {
    data: *const u8,
    len: usize,
}

impl StrPtr {
    fn from_str(v: &str) -> Self {
        StrPtr {
            data: v.as_ptr(),
            len: v.len(),
        }
    }
    fn as_str(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.data, self.len,
            ))
        }
    }
}

struct OwnedStrPtr(StrPtr);

impl Clone for OwnedStrPtr {
    fn clone(&self) -> Self {
        let data;
        unsafe {
            data = std::alloc::alloc(Layout::from_size_align_unchecked(
                self.0.len,
                std::mem::align_of::<u8>(),
            ));
            std::ptr::copy_nonoverlapping(self.0.data, data, self.0.len);
        };
        Self(StrPtr {
            data,
            len: self.0.len,
        })
    }
}
impl Drop for OwnedStrPtr {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(
                self.0.data as *mut u8,
                Layout::from_size_align_unchecked(
                    self.0.len,
                    std::mem::align_of::<u8>(),
                ),
            )
        }
    }
}

impl PartialEq for StrPtr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for StrPtr {}
impl Hash for StrPtr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

pub struct StringStore {
    arena: Vec<Vec<u8>>,
    existing_strings: Vec<Vec<OwnedStrPtr>>,
    table_idx_to_str: Vec<StrPtr>,
    table_str_to_idx: HashMap<StrPtr, StringStoreEntry>,
}

unsafe impl Send for StringStore {}
unsafe impl Sync for StringStore {}

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
            table_idx_to_str: Vec::with_capacity(
                self.table_idx_to_str.capacity(),
            ),
            table_str_to_idx: HashMap::with_capacity(
                self.table_str_to_idx.capacity(),
            ),
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
            let arena_str_start_ptr =
                &self.arena[arena_outer_idx][arena_inner_idx] as *const u8;
            if arena_str_start_ptr == str.data {
                res.arena[arena_outer_idx].extend(str.as_str().as_bytes());
                let str_clone = &res.arena[arena_outer_idx].as_slice()
                    [arena_inner_idx..arena_inner_idx + str.len];
                // SAFETY: same argument as for normal interning, we are just
                // cloning here
                let str_ptr = StrPtr::from_str(unsafe {
                    std::str::from_utf8_unchecked(str_clone)
                });
                res.table_str_to_idx.insert(str_ptr, idx);
                res.table_idx_to_str.push(str_ptr);
                arena_inner_idx += str.len;
                if arena_inner_idx == self.arena[arena_outer_idx].len() {
                    arena_outer_idx += 1;
                    arena_inner_idx = 0;
                }
            } else {
                let ex_str = &self.existing_strings[existing_strs_outer_idx]
                    [existing_strs_inner_idx];
                assert!(ex_str.0.data == str.data);
                let str_clone = ex_str.clone();
                // SAFETY: same argument as for normal interning, we are just
                // cloning here
                res.table_idx_to_str[i] = str_clone.0;
                res.table_str_to_idx.insert(str_clone.0, idx);
                res.existing_strings[existing_strs_outer_idx].push(str_clone);
                existing_strs_inner_idx += 1;
                if existing_strs_inner_idx
                    == self.arena[existing_strs_outer_idx].len()
                {
                    existing_strs_outer_idx += 1;
                    existing_strs_inner_idx = 0;
                }
            }
        }

        res
    }
}

impl StringStore {
    fn claim_id_for_str_ptr(&mut self, str_ptr: StrPtr) -> StringStoreEntry {
        self.table_idx_to_str.push(str_ptr);
        let id =
            StringStoreEntry::try_from(self.table_idx_to_str.len() as u32)
                .unwrap();
        self.table_str_to_idx.insert(str_ptr, id);
        id
    }
    pub fn claim_id_without_lookup(
        &mut self,
        entry: &'static str,
    ) -> StringStoreEntry {
        self.claim_id_for_str_ptr(StrPtr::from_str(entry))
    }
    pub fn intern_cloned(&mut self, entry: &str) -> StringStoreEntry {
        if let Some(key) = self.table_str_to_idx.get(&StrPtr::from_str(entry))
        {
            return *key;
        }
        let len = entry.len();
        let mut bucket = self.arena.last_mut().unwrap();
        let mut bucket_cap = bucket.capacity();
        let mut bucket_len = bucket.len();
        if bucket_cap - bucket_len < len {
            bucket_cap *= 2;
            self.arena.push(Vec::with_capacity(min(
                bucket_cap,
                len.next_power_of_two(),
            )));
            bucket = self.arena.last_mut().unwrap();
            bucket_len = 0;
        }
        bucket.extend_from_slice(entry.as_bytes());
        let str_ptr = StrPtr::from_str(unsafe {
            std::str::from_utf8_unchecked(
                &bucket[bucket_len..bucket_len + len],
            )
        });
        self.claim_id_for_str_ptr(str_ptr)
    }

    pub fn intern_moved(&mut self, entry: String) -> StringStoreEntry {
        if let Some(key) =
            self.table_str_to_idx.get(&StrPtr::from_str(entry.as_str()))
        {
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
        let len = entry.len();
        let data_ptr =
            std::boxed::Box::into_raw(entry.into_bytes().into_boxed_slice());
        let str_ptr = StrPtr {
            data: data_ptr as *const u8,
            len,
        };
        bucket.push(OwnedStrPtr(str_ptr));
        self.claim_id_for_str_ptr(str_ptr)
    }
    pub fn intern_cow(&mut self, cow: Cow<'static, str>) -> StringStoreEntry {
        match cow {
            Cow::Borrowed(s) => self.intern_cloned(s),
            Cow::Owned(s) => self.intern_moved(s),
        }
    }
    pub fn lookup(&self, entry: StringStoreEntry) -> &str {
        self.table_idx_to_str[u32::from(entry) as usize - 1].as_str()
    }
    pub fn lookup_str(&self, entry: &str) -> Option<StringStoreEntry> {
        self.table_str_to_idx.get(&StrPtr::from_str(entry)).copied()
    }
}
