use std::{io::Write, marker::PhantomData};

use super::pointer_writer::PointerWriter;

pub struct SliceWriter<'a> {
    pw: PointerWriter,
    phantom: PhantomData<&'a [u8]>,
}

impl<'a> SliceWriter<'a> {
    pub fn new(slice: &'a mut [u8]) -> Self {
        unsafe {
            Self {
                pw: PointerWriter::new(slice.as_mut_ptr(), slice.len()),
                phantom: PhantomData,
            }
        }
    }
}

impl<'a> Write for SliceWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pw.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
