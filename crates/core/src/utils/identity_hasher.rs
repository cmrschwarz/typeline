use std::hash::{BuildHasherDefault, Hasher};

pub type BuildIdentityHasher = BuildHasherDefault<IdentityHasher>;

#[derive(Clone, Copy, Debug, Default)]
pub struct IdentityHasher {
    hash: u64,
    #[cfg(debug_assertions)]
    accessed: bool,
}

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        // cfg needed because self.accessed doesn't exist in release mode
        #[cfg(debug_assertions)]
        debug_assert!(
            self.accessed,
            "IdentityHasher: finish() called before writing"
        );
        self.hash
    }
    fn write(&mut self, _: &[u8]) {
        panic!("IdentityHasher: attempted to write a byte slice")
    }
    fn write_u64(&mut self, n: u64) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                !self.accessed,
                "IdentityHasher: attempted to write a second time"
            );
            self.accessed = true;
        }
        self.hash = n
    }
    fn write_u8(&mut self, n: u8) {
        self.write_u64(u64::from(n))
    }
    fn write_u16(&mut self, n: u16) {
        self.write_u64(u64::from(n))
    }
    fn write_u32(&mut self, n: u32) {
        self.write_u64(u64::from(n))
    }
    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64)
    }
    fn write_i8(&mut self, n: i8) {
        self.write_u64(n as u64)
    }
    fn write_i16(&mut self, n: i16) {
        self.write_u64(n as u64)
    }
    fn write_i32(&mut self, n: i32) {
        self.write_u64(n as u64)
    }
    fn write_i64(&mut self, n: i64) {
        self.write_u64(n as u64)
    }
    fn write_isize(&mut self, n: isize) {
        self.write_u64(n as u64)
    }
}
