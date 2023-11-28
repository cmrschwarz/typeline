pub struct PointerWriter {
    ptr: *mut u8,
}

impl PointerWriter {
    pub unsafe fn new(ptr: *mut u8) -> Self {
        Self { ptr }
    }
}

impl std::io::Write for PointerWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
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
