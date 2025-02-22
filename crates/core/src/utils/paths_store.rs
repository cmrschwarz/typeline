#![allow(unused)]
#![allow(unused_variables)]
use std::{collections::HashMap, num::NonZeroU32};

use super::string_store::StringStoreEntry;

type PathStoreEntry = NonZeroU32;

type Path = [StringStoreEntry];

pub struct PathStore {
    indices: Vec<&'static Path>,
    table: HashMap<&'static Path, PathStoreEntry>,
    arena: Vec<Vec<StringStoreEntry>>,
}

impl PathStore {
    fn add_path(path: &Path) -> &'static Path {
        todo!()
    }
    pub fn intern(path: &Path) -> PathStoreEntry {
        todo!()
    }
    pub fn try_lookup_entry(path: &Path) -> Option<PathStoreEntry> {
        todo!()
    }
    pub fn lookup(path: &Path) -> PathStoreEntry {
        todo!()
    }
}
