pub struct PointerWriter {
    ptr: *mut u8,
    max_len: usize,
}

impl PointerWriter {
    pub unsafe fn new(ptr: *mut u8, max_len: usize) -> Self {
        Self { ptr, max_len }
    }
}

impl std::io::Write for PointerWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        self.max_len -= self
            .max_len
            .checked_sub(len)
            .expect("buffer overrun in PointerWriter");
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), self.ptr, len);
            self.ptr = self.ptr.add(len);
        };
        Ok(len)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.write(buf).map(|_| ())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
