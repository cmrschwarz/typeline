use indexmap::IndexMap;

use crate::{
    record_data::field_value::FieldValue,
    utils::string_store::StringStoreEntry,
};

pub type ObjectKeysStored = IndexMap<String, FieldValue>;
pub type ObjectKeysInterned = IndexMap<StringStoreEntry, FieldValue>;

#[derive(Debug, Clone, PartialEq)]
pub enum Object {
    KeysStored(ObjectKeysStored),
    KeysInterned(ObjectKeysInterned),
}

impl Default for Object {
    fn default() -> Self {
        Object::KeysStored(IndexMap::default())
    }
}

impl Object {
    pub fn new_keys_stored() -> Object {
        Object::KeysStored(IndexMap::default())
    }
    pub fn len(&self) -> usize {
        match self {
            Object::KeysStored(d) => d.len(),
            Object::KeysInterned(d) => d.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        match self {
            Object::KeysStored(o) => o.clear(),
            Object::KeysInterned(o) => o.clear(),
        }
    }
}

impl FromIterator<(String, FieldValue)> for Object {
    fn from_iter<I: IntoIterator<Item = (String, FieldValue)>>(
        iter: I,
    ) -> Self {
        Object::KeysStored(IndexMap::from_iter(iter))
    }
}

#[derive(Default)]
pub struct ObjectKeysInternedBuilder {
    obj: ObjectKeysInterned,
    run_lengths: Vec<usize>,
    drained_indices: Vec<usize>,
    run_len_available: usize,
}

impl ObjectKeysInternedBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_empty(&self) -> bool {
        self.obj.is_empty()
    }
    pub fn push_entry(
        &mut self,
        key: StringStoreEntry,
        value: FieldValue,
        rl: usize,
    ) {
        debug_assert!(rl != 0);
        let prev = self.obj.insert(key, value);
        debug_assert!(prev.is_none());
        self.run_lengths.push(rl);
        if self.run_lengths.len() == 1 {
            self.run_len_available = rl;
        } else {
            self.run_len_available = self.run_len_available.min(rl);
        }
    }
    pub fn get_drained_idx(&mut self) -> Option<usize> {
        self.drained_indices.last().copied()
    }
    pub fn replenish_drained_value(&mut self, v: FieldValue, rl: usize) {
        let idx = self.drained_indices.pop().unwrap();
        self.run_lengths[idx] = rl;
        *self.obj.get_index_mut(idx).unwrap().1 = v;
        if self.drained_indices.is_empty() {
            self.run_len_available = rl;
        }
    }
    pub fn build(&self) -> Object {
        Object::KeysInterned(self.obj.clone())
    }
    pub fn take(&mut self) -> Object {
        self.drained_indices.clear();
        self.run_lengths.clear();
        self.run_len_available = 0;
        Object::KeysInterned(std::mem::take(&mut self.obj))
    }
    pub fn available_len(&self) -> usize {
        self.run_len_available
    }
    pub fn consume_len(&mut self, len: usize) {
        let mut rla = usize::MAX;
        for (idx, rl) in self.run_lengths.iter_mut().enumerate() {
            *rl -= len;
            rla = rla.min(*rl);
            if *rl == 0 {
                self.drained_indices.push(idx);
            }
        }
        self.run_len_available = rla;
    }
}
