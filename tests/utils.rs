use std::io::Read;

pub struct SliceReader<'a> {
    pub data: &'a [u8],
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
