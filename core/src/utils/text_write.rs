use core::panic;
use std::{
    io::ErrorKind,
    ops::{Deref, DerefMut},
};

pub trait TextWrite {
    // SAFETY: assuming that the write succeeds, the result must be valid utf-8
    // If a previous, partial success has split a utf-8 character,
    // the next write must complete it.
    // NOTE: this means that if writes always succeed, buf must always be valid
    // utf-8
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize>;
    // SAFETY: assuming that the write succeeds, the result must be valid utf-8
    // If a previous, partial success has split a utf-8 character,
    // the next write must complete it.
    // NOTE: this means that if writes always succeed, buf must always be valid
    // utf-8
    unsafe fn write_all_text_unchecked(
        &mut self,
        mut buf: &[u8],
    ) -> std::io::Result<()> {
        while !buf.is_empty() {
            match unsafe { self.write_text_unchecked(buf) } {
                Ok(0) => {
                    return Err(ErrorKind::WriteZero.into());
                }
                Ok(n) => buf = &buf[n..],
                Err(e) => {
                    if e.kind() != ErrorKind::Interrupted {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        unsafe { self.write_all_text_unchecked(buf.as_bytes()) }
    }
    fn write_text_fmt(
        &mut self,
        args: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        // Create a shim which translates a Write to a fmt::Write and saves
        // off I/O errors.
        struct Adapter<'a, T: ?Sized + 'a> {
            inner: &'a mut T,
            error: std::io::Result<()>,
        }

        impl<T: TextWrite + ?Sized> std::fmt::Write for Adapter<'_, T> {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                match self.inner.write_all_text(s) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        self.error = Err(e);
                        Err(std::fmt::Error)
                    }
                }
            }
        }

        let mut output = Adapter {
            inner: self,
            error: Ok(()),
        };
        match std::fmt::write(&mut output, args) {
            Ok(()) => Ok(()),
            Err(e) => {
                // check if the error came from the underlying `Write` or not
                if output.error.is_err() {
                    output.error
                } else {
                    Err(std::io::Error::new(ErrorKind::Other, e))
                }
            }
        }
    }
    fn flush_text(&mut self) -> std::io::Result<()>;
}

impl<W: TextWrite> TextWrite for &mut W {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        unsafe { (**self).write_text_unchecked(buf) }
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        (**self).flush_text()
    }

    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        unsafe { (**self).write_all_text_unchecked(buf) }
    }

    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        (**self).write_all_text(buf)
    }

    fn write_text_fmt(
        &mut self,
        args: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        (**self).write_text_fmt(args)
    }
}

#[derive(Default, Clone)]
pub struct TextWriteIoAdapter<W: std::io::Write>(pub W);
impl<W: std::io::Write> TextWrite for TextWriteIoAdapter<W> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        self.0.write_all(buf)
    }
    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        unsafe { self.write_all_text_unchecked(buf.as_bytes()) }
    }
    fn flush_text(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
impl<W: std::io::Write> From<W> for TextWriteIoAdapter<W> {
    fn from(base: W) -> Self {
        Self(base)
    }
}
impl<W: std::io::Write> std::io::Write for TextWriteIoAdapter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }

    fn write_vectored(
        &mut self,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::io::Result<usize> {
        self.0.write_vectored(bufs)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buf)
    }

    fn write_fmt(
        &mut self,
        fmt: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        self.0.write_fmt(fmt)
    }
}
impl<W: std::io::Write> Deref for TextWriteIoAdapter<W> {
    type Target = W;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<W: std::io::Write> DerefMut for TextWriteIoAdapter<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[derive(Default, Clone)]
pub struct TextWriteFormatAdapter<W: std::fmt::Write>(pub W);

impl<W: std::fmt::Write> TextWrite for TextWriteFormatAdapter<W> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        // SAFETY: because we never partially succeed, the state after this
        // call (or any other call in this trait) will always be valid
        // utf-8. Therefore any given `buf` must also be valid utf-8 by
        // itself, due to the precondition of this trait method
        match std::fmt::Write::write_str(&mut self.0, unsafe {
            std::str::from_utf8_unchecked(buf)
        }) {
            Ok(()) => Ok(buf.len()),
            Err(e) => Err(std::io::Error::new(ErrorKind::Other, e)),
        }
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<W: std::fmt::Write> From<W> for TextWriteFormatAdapter<W> {
    fn from(base: W) -> Self {
        Self(base)
    }
}

impl<W: std::fmt::Write> std::fmt::Write for TextWriteFormatAdapter<W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.write_str(s)
    }
    fn write_char(&mut self, c: char) -> std::fmt::Result {
        self.0.write_char(c)
    }
    fn write_fmt(
        &mut self,
        args: std::fmt::Arguments<'_>,
    ) -> std::fmt::Result {
        self.0.write_fmt(args)
    }
}
impl<W: std::fmt::Write> Deref for TextWriteFormatAdapter<W> {
    type Target = W;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<W: std::fmt::Write> DerefMut for TextWriteFormatAdapter<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct TextWriteRefAdapter<'a, W: TextWrite>(pub &'a mut W);

impl<W: TextWrite> TextWrite for TextWriteRefAdapter<'_, W> {
    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        unsafe { self.0.write_all_text_unchecked(buf) }
    }

    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        self.0.write_all_text(buf)
    }

    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        unsafe { self.0.write_text_unchecked(buf) }
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        self.0.flush_text()
    }
}
impl<'a, W: TextWrite> From<&'a mut W> for TextWriteRefAdapter<'a, W> {
    fn from(base: &'a mut W) -> Self {
        Self(base)
    }
}
impl<'a, W: TextWrite> Deref for TextWriteRefAdapter<'a, W> {
    type Target = W;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}
impl<'a, W: TextWrite> DerefMut for TextWriteRefAdapter<'a, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

pub trait MaybeTextWrite: TextWrite + std::io::Write {
    fn as_text_write(&mut self) -> &mut dyn TextWrite;
    fn as_io_write(&mut self) -> &mut dyn std::io::Write;
}
impl<T: TextWrite + std::io::Write + Sized> MaybeTextWrite for T {
    fn as_text_write(&mut self) -> &mut dyn TextWrite {
        self
    }
    fn as_io_write(&mut self) -> &mut dyn std::io::Write {
        self
    }
}

#[derive(Default, Clone)]
pub struct MaybeTextWriteFlaggedAdapter<W> {
    base: W,
    is_utf8: bool,
}
impl<W: MaybeTextWrite> MaybeTextWriteFlaggedAdapter<W> {
    pub fn new(base: W) -> Self {
        Self {
            base,
            is_utf8: true,
        }
    }
    pub fn into_inner(self) -> W {
        self.base
    }
    pub fn is_utf8(&self) -> bool {
        self.is_utf8
    }
    pub unsafe fn set_is_utf8(&mut self, is_utf8: bool) {
        self.is_utf8 = is_utf8;
    }
}
impl<W: MaybeTextWrite> TextWrite for MaybeTextWriteFlaggedAdapter<W> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        unsafe { self.base.write_text_unchecked(buf) }
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        self.base.flush_text()
    }

    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        unsafe { self.base.write_all_text_unchecked(buf) }
    }

    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        self.base.write_all_text(buf)
    }

    fn write_text_fmt(
        &mut self,
        args: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        self.base.write_text_fmt(args)
    }
}
impl<W: MaybeTextWrite> std::io::Write for MaybeTextWriteFlaggedAdapter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.is_utf8 = false;
        self.base.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.base.flush()
    }

    fn write_vectored(
        &mut self,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::io::Result<usize> {
        self.is_utf8 = false;
        self.base.write_vectored(bufs)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.is_utf8 = false;
        self.base.write_all(buf)
    }

    fn write_fmt(
        &mut self,
        fmt: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        self.is_utf8 = false;
        self.base.write_fmt(fmt)
    }

    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}

pub struct MaybeTextWritePanicAdapter<W: TextWrite>(pub W);

impl<W: TextWrite> std::io::Write for MaybeTextWritePanicAdapter<W> {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        panic!("std::io::Write::write called on a MaybeTextWritePanicAdapter")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        panic!("std::io::Write::flush called on a MaybeTextWritePanicAdapter")
    }
}

impl<W: TextWrite> TextWrite for MaybeTextWritePanicAdapter<W> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        unsafe { self.0.write_text_unchecked(buf) }
    }
    fn flush_text(&mut self) -> std::io::Result<()> {
        self.0.flush_text()
    }
    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        unsafe { self.0.write_all_text_unchecked(buf) }
    }
    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        self.0.write_all_text(buf)
    }
    fn write_text_fmt(
        &mut self,
        args: std::fmt::Arguments<'_>,
    ) -> std::io::Result<()> {
        self.0.write_text_fmt(args)
    }
}

impl TextWrite for String {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        unsafe { self.write_all_text_unchecked(buf).unwrap_unchecked() }
        Ok(buf.len())
    }

    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        self.push_str(unsafe { std::str::from_utf8_unchecked(buf) });
        Ok(())
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
