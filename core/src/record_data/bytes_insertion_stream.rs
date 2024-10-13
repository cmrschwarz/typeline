use std::io::Write;

use bstr::ByteSlice;

use crate::utils::{maybe_text::MaybeTextRef, text_write::TextWrite};

use super::{
    field_data::{
        field_value_flags, FieldData, FieldValueFormat, FieldValueRepr,
        INLINE_STR_MAX_LEN,
    },
    push_interface::PushInterface,
};

struct RawBytesInserter<'a> {
    fd: &'a mut FieldData,
    run_len: usize,
    ptr: *mut u8,
    bytes_inserted: usize,
    cap: usize,
    // PERF: we could optimize this to make this type much smaller
    target: Vec<u8>,
}

impl<'a> RawBytesInserter<'a> {
    pub fn new(fd: &'a mut FieldData, run_len: usize) -> Self {
        Self {
            ptr: fd.data.tail_ptr_mut(),
            bytes_inserted: 0,
            cap: fd.data.contiguous_tail_space_available(),
            target: Vec::new(),
            fd,
            run_len,
        }
    }
    fn write_bytes(&mut self, buf: &[u8]) {
        let buf_len = buf.len();
        let len_new = self.bytes_inserted + buf_len;
        if len_new > INLINE_STR_MAX_LEN {
            if self.target.is_empty() {
                self.target.extend_from_slice(unsafe {
                    std::slice::from_raw_parts(
                        self.ptr.sub(self.bytes_inserted),
                        self.bytes_inserted,
                    )
                });
            }
            self.target.extend(buf);
            self.bytes_inserted = len_new;
            return;
        }

        if len_new > self.cap {
            let data_len = self.fd.data.len();
            unsafe {
                self.fd.data.set_len(data_len + self.bytes_inserted);
                self.fd
                    .data
                    .reserve_contiguous(len_new, self.bytes_inserted);
                self.fd.data.set_len(data_len);
            }
            self.ptr = self.fd.data.tail_ptr_mut();
            self.cap = self.fd.data.contiguous_tail_space_available();
        }
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), self.ptr, buf_len);
            self.ptr = self.ptr.add(buf_len);
        }
        self.bytes_inserted = len_new;
    }
    unsafe fn commit_inline(&mut self, text: bool) {
        let fmt = FieldValueFormat {
            repr: if text {
                FieldValueRepr::TextInline
            } else {
                FieldValueRepr::BytesInline
            },
            flags: field_value_flags::SHARED_VALUE,
            size: self.bytes_inserted as u16,
        };
        self.fd.field_count += self.run_len;
        unsafe {
            self.fd
                .data
                .set_len(self.fd.data.len() + self.bytes_inserted);
            self.fd.add_header_for_single_value(
                fmt,
                self.run_len,
                self.fd
                    .headers
                    .back()
                    .map(|h| h.is_compatible(fmt))
                    .unwrap_or(false),
                false,
            )
        }
    }
    unsafe fn commit_heap(&mut self, text: bool) {
        let buffer = std::mem::take(&mut self.target);
        if text {
            self.fd.push_string(
                unsafe { String::from_utf8_unchecked(buffer) },
                self.run_len,
                true,
                false,
            );
        } else {
            self.fd.push_bytes_buffer(buffer, self.run_len, true, false);
        }
    }
    unsafe fn commit(&mut self, text: bool) {
        unsafe {
            if self.bytes_inserted > INLINE_STR_MAX_LEN {
                self.commit_heap(text);
                return;
            }
            self.commit_inline(text);
        }
    }
    unsafe fn commit_maybe_text(&mut self) {
        unsafe { self.commit(self.get_inserted_data().is_utf8()) }
    }
    fn free_memory(&mut self) {
        std::mem::take(&mut self.target);
    }
    fn get_inserted_data(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.sub(self.bytes_inserted),
                self.bytes_inserted,
            )
        }
    }

    fn truncate(&mut self, len: usize) {
        if self.bytes_inserted <= len {
            return;
        }
        self.bytes_inserted = len;
        self.target.truncate(len);
    }
}

pub struct BytesInsertionStream<'a>(RawBytesInserter<'a>);
impl<'a> BytesInsertionStream<'a> {
    pub fn new(fd: &'a mut FieldData, run_len: usize) -> Self {
        Self(RawBytesInserter::new(fd, run_len))
    }
    pub fn commit(self) {
        drop(self)
    }
    pub fn commit_maybe_text(mut self) {
        unsafe { self.0.commit_maybe_text() }
    }
    pub unsafe fn commit_as_text(mut self) {
        unsafe { self.0.commit(true) }
    }
    pub fn abort(mut self) {
        self.0.free_memory();
        std::mem::forget(self);
    }
    pub fn get_inserted_data(&self) -> &[u8] {
        self.0.get_inserted_data()
    }
    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len)
    }
}
impl Drop for BytesInsertionStream<'_> {
    fn drop(&mut self) {
        unsafe { self.0.commit(false) }
    }
}

impl Write for BytesInsertionStream<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write_bytes(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct TextInsertionStream<'a>(RawBytesInserter<'a>);
impl<'a> TextInsertionStream<'a> {
    pub fn new(fd: &'a mut FieldData, run_len: usize) -> Self {
        Self(RawBytesInserter::new(fd, run_len))
    }
    pub fn commit(self) {
        drop(self)
    }
    pub fn abort(mut self) {
        self.0.free_memory();
        std::mem::forget(self);
    }
    pub fn get_inserted_data(&self) -> &str {
        // SAFETY: TextWrite guaranteed that only valid utf-8 is in here
        // (because our `write_text_unchecked` impl never partially succeeds)
        unsafe { std::str::from_utf8_unchecked(self.0.get_inserted_data()) }
    }
}
impl TextWrite for TextInsertionStream<'_> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        self.0.write_bytes(buf);
        Ok(buf.len())
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Drop for TextInsertionStream<'_> {
    fn drop(&mut self) {
        unsafe { self.0.commit(true) }
    }
}

pub struct MaybeTextInsertionStream<'a> {
    base: RawBytesInserter<'a>,
    is_text: bool,
}
impl<'a> MaybeTextInsertionStream<'a> {
    pub fn new(fd: &'a mut FieldData, run_len: usize) -> Self {
        Self {
            base: RawBytesInserter::new(fd, run_len),
            is_text: true,
        }
    }
    pub fn commit(self) {
        drop(self)
    }
    pub fn abort(mut self) {
        self.base.free_memory();
        std::mem::forget(self);
    }
    pub unsafe fn set_is_text(&mut self, is_text: bool) {
        self.is_text = is_text;
    }
    pub fn is_text(&self) -> bool {
        self.is_text
    }
    pub fn get_inserted_data(&self) -> MaybeTextRef {
        let data = self.base.get_inserted_data();
        if self.is_text {
            return unsafe {
                MaybeTextRef::Text(std::str::from_utf8_unchecked(data))
            };
        }
        MaybeTextRef::Bytes(data)
    }
}
impl TextWrite for MaybeTextInsertionStream<'_> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        self.base.write_bytes(buf);
        Ok(buf.len())
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl std::io::Write for MaybeTextInsertionStream<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.is_text = false;
        self.base.write_bytes(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Drop for MaybeTextInsertionStream<'_> {
    fn drop(&mut self) {
        unsafe { self.base.commit(self.is_text) }
    }
}
