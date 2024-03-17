use std::io::Write;

use crate::utils::text_write::TextWrite;

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
            if self.bytes_inserted <= INLINE_STR_MAX_LEN {
                unsafe {
                    self.target.extend_from_slice(std::slice::from_raw_parts(
                        self.ptr.sub(self.bytes_inserted),
                        self.bytes_inserted,
                    ));
                }
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
    fn commit_inline(&mut self, text: bool) {
        let fmt = FieldValueFormat {
            repr: if text {
                FieldValueRepr::TextInline
            } else {
                FieldValueRepr::BytesInline
            },
            flags: field_value_flags::DEFAULT,
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
    fn commit_heap(&mut self, text: bool) {
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
    fn commit(&mut self, text: bool) {
        if self.bytes_inserted > INLINE_STR_MAX_LEN {
            self.commit_heap(text);
            return;
        }
        self.commit_inline(text);
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
}
impl Drop for BytesInsertionStream<'_> {
    fn drop(&mut self) {
        self.0.commit(false)
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
        self.0.commit(true)
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
    pub fn write_bytes(&mut self, buf: &[u8]) {
        self.base.write_bytes(buf);
    }
    pub unsafe fn set_is_text(&mut self, is_text: bool) {
        self.is_text = is_text;
    }
    pub fn is_text(&self) -> bool {
        self.is_text
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
impl Drop for MaybeTextInsertionStream<'_> {
    fn drop(&mut self) {
        self.base.commit(self.is_text)
    }
}
