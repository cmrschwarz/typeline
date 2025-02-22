use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub enum LazyRwLockReadGuard<'a, T> {
    Unlocked(&'a RwLock<T>),
    Read(RwLockReadGuard<'a, T>),
}

pub enum LazyRwLockWriteGuard<'a, T> {
    Unlocked(&'a RwLock<T>),
    Write(RwLockWriteGuard<'a, T>),
    Plain(&'a mut T),
}

pub enum LazyRwLockGuard<'a, T> {
    Read {
        // this field must come first to not confuse the compiler's niche
        // optimizations (as of Rust Version 1.77)
        guard: RwLockReadGuard<'a, T>,
        lock: &'a RwLock<T>, // neccessary to promote to write
    },
    // we use a subenum instead of a flat layout for better memory layout
    // optimizations
    NonRead(LazyRwLockWriteGuard<'a, T>),
}

impl<'a, T> LazyRwLockReadGuard<'a, T> {
    pub fn new(lock: &'a RwLock<T>) -> Self {
        LazyRwLockReadGuard::Unlocked(lock)
    }
    pub fn get(&mut self) -> &T {
        match self {
            LazyRwLockReadGuard::Unlocked(l) => {
                *self = LazyRwLockReadGuard::Read(l.read().unwrap());
                self.get()
            }
            LazyRwLockReadGuard::Read(w) => w,
        }
    }
}

impl<'a, T> LazyRwLockWriteGuard<'a, T> {
    pub fn new(lock: &'a RwLock<T>) -> Self {
        LazyRwLockWriteGuard::Unlocked(lock)
    }
    pub fn get(&mut self) -> &T {
        match self {
            LazyRwLockWriteGuard::Unlocked(l) => {
                *self = LazyRwLockWriteGuard::Write(l.write().unwrap());
                self.get()
            }
            LazyRwLockWriteGuard::Write(w) => w,
            LazyRwLockWriteGuard::Plain(v) => v,
        }
    }
    pub fn get_mut(&mut self) -> &mut T {
        match self {
            LazyRwLockWriteGuard::Unlocked(l) => {
                *self = LazyRwLockWriteGuard::Write(l.write().unwrap());
                self.get_mut()
            }
            LazyRwLockWriteGuard::Write(w) => w,
            LazyRwLockWriteGuard::Plain(v) => v,
        }
    }
}

impl<'a, T> LazyRwLockGuard<'a, T> {
    pub fn new(lock: &'a RwLock<T>) -> Self {
        LazyRwLockGuard::NonRead(LazyRwLockWriteGuard::Unlocked(lock))
    }
    pub fn get(&mut self) -> &T {
        match self {
            LazyRwLockGuard::NonRead(LazyRwLockWriteGuard::Unlocked(l)) => {
                *self = LazyRwLockGuard::Read {
                    lock: l,
                    guard: l.read().unwrap(),
                };
                self.get()
            }
            LazyRwLockGuard::Read { guard, .. } => guard,
            LazyRwLockGuard::NonRead(LazyRwLockWriteGuard::Write(w)) => w,
            LazyRwLockGuard::NonRead(LazyRwLockWriteGuard::Plain(v)) => v,
        }
    }
    pub fn get_mut(&mut self) -> &mut T {
        match self {
            LazyRwLockGuard::Read { lock, .. } => {
                *self = LazyRwLockGuard::NonRead(
                    LazyRwLockWriteGuard::Unlocked(lock),
                );
                self.get_mut()
            }
            LazyRwLockGuard::NonRead(nr) => nr.get_mut(),
        }
    }
}
