use std::ops::{Deref, DerefMut};

use bstr::ByteSlice;

use super::{
    is_utf8_continuation_byte,
    text_write::{
        MaybeTextWriteFlaggedAdapter, TextWrite, TextWriteIoAdapter,
    },
    valid_utf8_codepoint_begins,
};

#[derive(Clone, Copy, Default)]
pub struct LengthCountingWriter {
    pub len: usize,
}
impl std::fmt::Write for LengthCountingWriter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.len += s.len();
        Ok(())
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.len += c.len_utf8();
        Ok(())
    }
}
impl std::io::Write for LengthCountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.len += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl TextWrite for LengthCountingWriter {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        self.len += buf.len();
        Ok(buf.len())
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        self.len += buf.len();
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct LengthAndCharsCountingWriter {
    pub len: usize,
    pub char_count: usize,
}
impl std::fmt::Write for LengthAndCharsCountingWriter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.len += s.len();
        self.char_count += s.chars().count();
        Ok(())
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.len += c.len_utf8();
        self.char_count += 1;
        Ok(())
    }

    fn write_fmt(
        mut self: &mut Self,
        args: std::fmt::Arguments<'_>,
    ) -> std::fmt::Result {
        std::fmt::write(&mut self, args)
    }
}

impl std::io::Write for LengthAndCharsCountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.len += buf.len();
        self.char_count += buf.chars().count();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl TextWrite for LengthAndCharsCountingWriter {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        unsafe { self.write_all_text_unchecked(buf).unwrap() }
        Ok(buf.len())
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        self.len += buf.len();
        self.char_count += valid_utf8_codepoint_begins(buf);
        Ok(())
    }

    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        self.len += buf.len();
        self.char_count = buf.chars().count();
        Ok(())
    }

    fn write_text_fmt(
        &mut self,
        args: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        std::fmt::write(self, args).unwrap();
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct CharLimitedLengthAndCharsCountingWriter {
    pub len: usize,
    pub char_count: usize,
    pub max_char_count: usize,
}
impl CharLimitedLengthAndCharsCountingWriter {
    pub fn new(max_char_count: usize) -> Self {
        Self {
            max_char_count,
            ..Default::default()
        }
    }
}

impl std::fmt::Write for CharLimitedLengthAndCharsCountingWriter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        for c in s.chars() {
            self.write_char(c)?;
        }
        Ok(())
    }

    fn write_char(&mut self, c: char) -> std::fmt::Result {
        if self.char_count == self.max_char_count {
            return Err(std::fmt::Error);
        }
        self.len += c.len_utf8();
        self.char_count += 1;
        Ok(())
    }

    fn write_fmt(
        mut self: &mut Self,
        args: std::fmt::Arguments<'_>,
    ) -> std::fmt::Result {
        std::fmt::write(&mut self, args)
    }
}
impl std::io::Write for CharLimitedLengthAndCharsCountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.char_count == self.max_char_count {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        let mut len_delta = 0;
        for (start, end, _c) in buf.char_indices() {
            if self.char_count == self.max_char_count {
                break;
            }
            len_delta += end - start;
            self.char_count += 1;
        }
        self.len += len_delta;
        Ok(len_delta)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl TextWrite for CharLimitedLengthAndCharsCountingWriter {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        if self.char_count == self.max_char_count {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        let mut len_delta = 0;
        for (start, end, c) in buf.char_indices() {
            if self.char_count == self.max_char_count {
                break;
            }
            len_delta += end - start;
            // because we never partially succeed, buf is expected to contain
            // valid utf8
            debug_assert!(
                c != char::REPLACEMENT_CHARACTER
                    || !is_utf8_continuation_byte(buf[start])
            );
            self.char_count += 1;
        }
        self.len += len_delta;
        Ok(len_delta)
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct LengthCappedIoWriter<W: std::io::Write> {
    pub base: W,
    pub len_rem: usize,
}

impl<W: std::io::Write> LengthCappedIoWriter<W> {
    pub fn new(base: W, max_len: usize) -> Self {
        Self {
            base,
            len_rem: max_len,
        }
    }
    pub fn len_rem(&self) -> usize {
        self.len_rem
    }
}

impl<W: std::io::Write> std::io::Write for LengthCappedIoWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.len_rem == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        if self.len_rem < buf.len() {
            let written = self.base.write(&buf[0..self.len_rem])?;
            self.len_rem -= written;
            return Ok(written);
        }
        self.base.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.base.flush()
    }
}

pub trait IntoLengthCappedIoWriter: Sized + std::io::Write {
    fn capped(self, max_len: usize) -> LengthCappedIoWriter<Self> {
        LengthCappedIoWriter::new(self, max_len)
    }
}
impl<W: std::io::Write> IntoLengthCappedIoWriter for W {}

pub struct TextInfo {
    pub len: usize,
    pub char_count: usize,
    pub valid_utf8: bool,
}
impl Default for TextInfo {
    fn default() -> Self {
        Self {
            len: 0,
            char_count: 0,
            valid_utf8: true,
        }
    }
}

#[derive(Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct TextInfoWriter(
    MaybeTextWriteFlaggedAdapter<
        TextWriteIoAdapter<LengthAndCharsCountingWriter>,
    >,
);
impl TextInfoWriter {
    pub fn into_text_info(self) -> TextInfo {
        let valid_utf8 = self.0.is_utf8();
        let inner = self.0.into_inner();
        TextInfo {
            valid_utf8,
            len: inner.len,
            char_count: inner.char_count,
        }
    }
    pub fn text_info(&self) -> TextInfo {
        self.clone().into_text_info()
    }
}
