use std::ops::{Index, IndexMut};

#[derive(Clone)]
pub struct Universe<I: From<usize> + Into<usize>, T> {
    data: Vec<T>,
    unused_ids: Vec<I>,
}

impl<I: From<usize> + Into<usize>, T> Default for Universe<I, T> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            unused_ids: Default::default(),
        }
    }
}

impl<I: From<usize> + Into<usize>, T> Universe<I, T> {
    pub fn push(&mut self, val: T) -> I {
        if let Some(id) = self.unused_ids.pop() {
            let index = id.into();
            self.data[index] = val;
            index.into()
        } else {
            let id = self.data.len().into();
            self.data.push(val);
            id
        }
    }
    pub fn release(&mut self, id: I) {
        let index = id.into();
        if self.data.len() == index + 1 {
            self.data.pop();
            return;
        }
        self.unused_ids.push(index.into());
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn clear(&mut self) {
        self.unused_ids.clear();
        self.data.clear();
    }
}

impl<I: From<usize> + Into<usize>, T> Index<I> for Universe<I, T> {
    type Output = T;

    fn index(&self, index: I) -> &Self::Output {
        &self.data[index.into()]
    }
}

impl<I: From<usize> + Into<usize>, T> IndexMut<I> for Universe<I, T> {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.data[index.into()]
    }
}

impl<I: From<usize> + Into<usize>, T: Default> Universe<I, T> {
    pub fn claim(&mut self) -> I {
        if let Some(id) = self.unused_ids.pop() {
            id
        } else {
            let id = self.data.len();
            self.data.push(Default::default());
            id.into()
        }
    }
}
