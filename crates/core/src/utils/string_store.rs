use std::{alloc::Layout, borrow::Cow, collections::HashMap, hash::Hash};

use indexland::NewtypeIdx;

use indexland::{index_vec::IndexVec, nonmax::NonMaxU32, Idx};

#[derive(NewtypeIdx)]
pub struct StringStoreEntry(NonMaxU32);

pub const INVALID_STRING_STORE_ENTRY: StringStoreEntry = StringStoreEntry::MAX;

const MIN_BUCKET_CAP: usize = 64;

#[derive(Default)]
pub struct StringStore {
    arena: Vec<Vec<u8>>,
    existing_strings: Vec<OwnedStrPtr>,
    table_idx_to_str: IndexVec<StringStoreEntry, StrPtr>,
    table_str_to_idx: HashMap<StrPtr, StringStoreEntry>,
}

unsafe impl Send for StringStore {}
unsafe impl Sync for StringStore {}

#[derive(Clone, Copy)]
struct StrPtr {
    data: *const u8,
    len: usize,
}

struct OwnedStrPtr(StrPtr);

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

impl Clone for OwnedStrPtr {
    fn clone(&self) -> Self {
        if self.0.len == 0 {
            return Self(self.0);
        }
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
        if self.0.len == 0 {
            return;
        }
        unsafe {
            std::alloc::dealloc(
                self.0.data.cast_mut(),
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

impl Clone for StringStore {
    fn clone(&self) -> Self {
        let mut res = Self {
            arena: self.arena.clone(),
            existing_strings: Vec::with_capacity(self.existing_strings.len()),
            table_idx_to_str: IndexVec::with_capacity(
                self.table_idx_to_str.capacity(),
            ),
            table_str_to_idx: HashMap::with_capacity(
                self.table_str_to_idx.capacity(),
            ),
        };

        for arena_buf in &self.arena {
            res.arena.push(Vec::with_capacity(arena_buf.capacity()));
        }

        let mut arena_outer_idx = 0;
        let mut arena_inner_idx = 0;

        let mut existing_strs_idx = 0;
        for i in self.table_idx_to_str.indices() {
            let str = self.table_idx_to_str[i];
            let arena_str_start_ptr = std::ptr::addr_of!(
                self.arena[arena_outer_idx][arena_inner_idx]
            );
            if arena_str_start_ptr == str.data {
                res.arena[arena_outer_idx].extend(str.as_str().as_bytes());
                let str_clone = &res.arena[arena_outer_idx].as_slice()
                    [arena_inner_idx..arena_inner_idx + str.len];
                // SAFETY: same argument as for normal interning, we are just
                // cloning here
                let str_ptr = StrPtr::from_str(unsafe {
                    std::str::from_utf8_unchecked(str_clone)
                });
                res.table_str_to_idx.insert(str_ptr, i);
                res.table_idx_to_str.push(str_ptr);
                arena_inner_idx += str.len;
                if arena_inner_idx == self.arena[arena_outer_idx].len() {
                    arena_outer_idx += 1;
                    arena_inner_idx = 0;
                }
                continue;
            }
            let ex_str = &self.existing_strings[existing_strs_idx];
            if ex_str.0.data != str.data {
                //'static strs
                continue;
            }
            let str_clone = ex_str.clone();
            // SAFETY: same argument as for normal interning, we are just
            // cloning here
            res.table_idx_to_str[i] = str_clone.0;
            res.table_str_to_idx.insert(str_clone.0, i);
            res.existing_strings.push(str_clone);
            existing_strs_idx += 1;
        }

        res
    }
}

impl StringStore {
    fn claim_id_for_str_ptr(&mut self, str_ptr: StrPtr) -> StringStoreEntry {
        let id = StringStoreEntry::from_usize(self.table_idx_to_str.len());
        self.table_idx_to_str.push(str_ptr);
        self.table_str_to_idx.insert(str_ptr, id);
        id
    }
    pub fn claim_id_without_lookup(
        &mut self,
        entry: &'static str,
    ) -> StringStoreEntry {
        self.claim_id_for_str_ptr(StrPtr::from_str(entry))
    }
    pub fn intern_static(&mut self, entry: &'static str) -> StringStoreEntry {
        if let Some(key) = self.table_str_to_idx.get(&StrPtr::from_str(entry))
        {
            return *key;
        }
        let str_ptr = StrPtr::from_str(entry);
        self.claim_id_for_str_ptr(str_ptr)
    }
    pub fn intern_cloned(&mut self, entry: &str) -> StringStoreEntry {
        if let Some(key) = self.table_str_to_idx.get(&StrPtr::from_str(entry))
        {
            return *key;
        }
        let len = entry.len();
        let bucket = if let Some(bucket) = self.arena.last_mut() {
            let mut bucket_cap = bucket.capacity();
            let bucket_len = bucket.len();
            if bucket_cap - bucket_len < len {
                bucket_cap *= 2;
                if entry.len() > bucket_cap {
                    bucket_cap = entry.len().next_power_of_two();
                }
                self.arena.push(Vec::with_capacity(bucket_cap));
                self.arena.last_mut().unwrap()
            } else {
                bucket
            }
        } else {
            let mut bucket_cap = MIN_BUCKET_CAP;
            if entry.len() > bucket_cap {
                bucket_cap = entry.len().next_power_of_two();
            }
            self.arena.push(Vec::with_capacity(bucket_cap));
            &mut self.arena[0]
        };

        bucket.extend_from_slice(entry.as_bytes());
        let str_ptr = StrPtr::from_str(unsafe {
            std::str::from_utf8_unchecked(&bucket[bucket.len() - len..])
        });
        self.claim_id_for_str_ptr(str_ptr)
    }

    pub fn intern_moved(&mut self, entry: String) -> StringStoreEntry {
        if let Some(key) =
            self.table_str_to_idx.get(&StrPtr::from_str(entry.as_str()))
        {
            return *key;
        }
        let len = entry.len();
        if len == 0 {
            let str_ptr = StrPtr::from_str("");
            return self.claim_id_for_str_ptr(str_ptr);
        }
        let data_ptr =
            std::boxed::Box::into_raw(entry.into_bytes().into_boxed_slice());
        let str_ptr = StrPtr {
            data: data_ptr as *const u8,
            len,
        };
        self.existing_strings.push(OwnedStrPtr(str_ptr));
        self.claim_id_for_str_ptr(str_ptr)
    }
    pub fn intern_cow_static(
        &mut self,
        cow: Cow<'static, str>,
    ) -> StringStoreEntry {
        match cow {
            Cow::Borrowed(s) => self.intern_static(s),
            Cow::Owned(s) => self.intern_moved(s),
        }
    }
    pub fn intern_cow(&mut self, cow: Cow<str>) -> StringStoreEntry {
        match cow {
            Cow::Borrowed(s) => self.intern_cloned(s),
            Cow::Owned(s) => self.intern_moved(s),
        }
    }
    pub fn lookup(&self, entry: StringStoreEntry) -> &str {
        self.table_idx_to_str[entry].as_str()
    }
    pub fn lookup_str(&self, entry: &str) -> Option<StringStoreEntry> {
        self.table_str_to_idx.get(&StrPtr::from_str(entry)).copied()
    }
}
