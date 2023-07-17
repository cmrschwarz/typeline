use std::io::Read;

#[derive(Clone)]
pub struct SliceReader<'a> {
    pub data: &'a [u8],
}

impl<'a> SliceReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Read for SliceReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.data.len();
        if buf.len() >= len {
            buf[0..len].copy_from_slice(self.data);
            self.data = &[];
            return Ok(len);
        }
        let (lhs, rhs) = self.data.split_at(buf.len());
        buf.copy_from_slice(lhs);
        self.data = rhs;
        Ok(lhs.len())
    }
}

#[derive(Clone)]
pub struct TricklingStream<'a> {
    pub total_size: usize,
    pub data_to_repeat: &'a [u8],
    pub data_pos: usize,
}

impl<'a> Read for TricklingStream<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.total_size == 0 || buf.len() == 0 {
            return Ok(0);
        }
        if self.data_pos == self.data_to_repeat.len() {
            self.data_pos = 0;
        }
        buf[0] = self.data_to_repeat[self.data_pos];
        self.total_size -= 1;
        self.data_pos += 1;
        return Ok(1);
    }
}

impl<'a> TricklingStream<'a> {
    pub fn new(data_to_repeat: &'a [u8], total_size: usize) -> Self {
        Self {
            data_to_repeat,
            total_size,
            data_pos: 0,
        }
    }
}

#[derive(Clone)]
pub struct ErroringStream<R> {
    base: R,
    error_after: usize,
}
impl<R: Read> ErroringStream<R> {
    pub fn new(error_after: usize, base: R) -> Self {
        Self { base, error_after }
    }
}

impl<'a, R: Read> Read for ErroringStream<R> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        if self.error_after == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ErroringStream: Expected Debug Error",
            ));
        }
        if buf.len() > self.error_after {
            buf = &mut buf[0..self.error_after];
        }
        let read_len = self.base.read(buf)?;
        self.error_after -= read_len;
        Ok(read_len)
    }
}
