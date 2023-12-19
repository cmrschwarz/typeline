use std::{
    io::{BufRead, ErrorKind, Read, Write},
    marker::PhantomData,
    ops::Index,
};

use arrayvec::ArrayVec;
use bstr::ByteSlice;

use super::{utf8_codepoint_len_from_first_byte, MAX_UTF8_CHAR_LEN};

pub struct PointerWriter {
    ptr: *mut u8,
    remaining_bytes: usize,
}

pub struct SliceWriter<'a> {
    pw: PointerWriter,
    phantom: PhantomData<&'a [u8]>,
}

impl PointerWriter {
    pub unsafe fn new(ptr: *mut u8, max_len: usize) -> Self {
        Self {
            ptr,
            remaining_bytes: max_len,
        }
    }
    pub fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }
    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl std::io::Write for PointerWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        self.remaining_bytes = self
            .remaining_bytes
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

pub enum ReadCharError {
    Io(std::io::Error),
    InvalidUtf8 {
        len: u8,
        sequence: [u8; MAX_UTF8_CHAR_LEN],
    },
    Eof,
}

pub fn read_char(stream: &mut impl Read) -> Result<char, ReadCharError> {
    let mut c: u8 = 0;

    if let Err(e) = stream.read_exact(std::slice::from_mut(&mut c)) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(ReadCharError::Eof);
        }
        return Err(ReadCharError::Io(e));
    };
    if c.is_ascii() {
        return Ok(c as char);
    }
    let mut buf = [0; MAX_UTF8_CHAR_LEN];
    buf[0] = c;
    let Some(codepoint_len) = utf8_codepoint_len_from_first_byte(c) else {
        return Err(ReadCharError::InvalidUtf8 {
            sequence: buf,
            len: 1,
        });
    };
    if let Err(e) = std::io::copy(
        &mut stream.take((codepoint_len - 1) as u64),
        &mut SliceWriter::new(&mut buf[1..]),
    ) {
        return Err(ReadCharError::Io(e));
    }
    let Some(tok) = buf.chars().next() else {
        //PERF: ?
        return Err(ReadCharError::InvalidUtf8 {
            sequence: buf,
            len: codepoint_len,
        });
    };
    Ok(tok)
}

pub struct UnescapeFailedError<E> {
    pub base: E,
    pub bytes_consumed: usize,
    pub escape_seq_len: usize,
}

pub enum ReadUntilUnescapeError<E> {
    Io(std::io::Error),
    UnescapeFailed(UnescapeFailedError<E>),
}

#[derive(Default, Clone, Copy)]
pub struct EscapeSequenceContinuation {
    pub len_rem: u8,
    pub len_handled: u8,
    pub entry: Option<u8>,
    pub entry_known: bool,
}

pub enum ReplacementError<E> {
    Error(E),
    NeedMoreCharacters,
}

pub struct ReplacementState<'a> {
    buf: &'a mut Vec<u8>,
    pub seq_len: usize,
    pub lookahead: &'a [u8],
}

impl Index<usize> for ReplacementState<'_> {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.seq_len {
            return &self.buf[self.buf.len() - self.seq_len + index];
        }
        &self.lookahead[index - self.seq_len]
    }
}

impl<'a> ReplacementState<'a> {
    pub fn buffer(&self) -> &Vec<u8> {
        self.buf
    }
    pub fn into_buffer(self) -> &'a mut Vec<u8> {
        self.buf
    }
    pub fn buf_offset(&self) -> usize {
        self.buf.len() - self.seq_len
    }
    pub fn get<E>(&self, index: usize) -> Result<u8, ReplacementError<E>> {
        if index < self.seq_len {
            return Ok(self.buf[self.buf_offset() + index]);
        }
        let la_pos = index - self.seq_len;
        if la_pos >= self.lookahead.len() {
            return Err(ReplacementError::NeedMoreCharacters);
        }
        Ok(self.lookahead[la_pos])
    }
    pub fn get_n<E>(
        &self,
        index: usize,
        target: &mut [u8],
    ) -> Result<(), ReplacementError<E>> {
        if index + target.len() <= self.seq_len {
            let start = self.buf_offset() + index;
            target.copy_from_slice(&self.buf[start..start + target.len()]);
            return Ok(());
        }
        let start_in_buf = (self.buf_offset() + index).min(self.buf.len());
        let len_in_buf = self.buf.len() - start_in_buf;
        let la_pos = index - self.seq_len;
        let len_in_la = target.len() - len_in_buf;
        if la_pos + len_in_la >= self.lookahead.len() {
            return Err(ReplacementError::NeedMoreCharacters);
        }

        target[..len_in_buf].copy_from_slice(
            &self.buf[start_in_buf..start_in_buf + len_in_buf],
        );
        target[len_in_buf..]
            .copy_from_slice(&self.lookahead[la_pos..la_pos + len_in_la]);
        Ok(())
    }
    pub fn get_char<E>(
        &self,
        index: usize,
    ) -> Result<
        Result<char, ArrayVec<u8, MAX_UTF8_CHAR_LEN>>,
        ReplacementError<E>,
    > {
        let c = self.get(index)?;
        if c.is_ascii() {
            return Ok(Ok(c as char)); // ascii fast path
        }
        let Some(utf8_len) = utf8_codepoint_len_from_first_byte(c) else {
            return Ok(Err(ArrayVec::from_iter([c])));
        };
        let mut buf = [0u8; MAX_UTF8_CHAR_LEN];
        buf[0] = c;
        self.get_n(index + 1, &mut buf[1..utf8_len as usize])?;
        let Some(c) = buf.chars().next() else {
            return Ok(Err(ArrayVec::from_iter(
                buf.iter().copied().take(utf8_len as usize),
            )));
        };
        Ok(Ok(c))
    }
    pub fn find<E>(
        &self,
        mut offset: usize,
        byte: u8,
    ) -> Result<usize, ReplacementError<E>> {
        if self.seq_len > offset {
            if let Some(i) =
                self.buf[self.buf_offset() + offset..].find_byte(byte)
            {
                return Ok(offset + i);
            }
        }
        offset -= self.seq_len;
        if self.lookahead.len() > offset {
            if let Some(i) = self.lookahead[offset..].find_byte(byte) {
                return Ok(self.seq_len + offset + i);
            }
        }
        Err(ReplacementError::NeedMoreCharacters)
    }
    pub fn find_limited<E>(
        &self,
        mut offset: usize,
        byte: u8,
        max_len: usize,
    ) -> Result<Option<usize>, ReplacementError<E>> {
        if self.seq_len > offset {
            let buf_offset = self.buf_offset();
            if let Some(i) = self.buf
                [buf_offset + offset..self.buf.len().min(buf_offset + max_len)]
                .find_byte(byte)
            {
                return Ok(Some(offset + i));
            }
        }
        if self.seq_len >= max_len {
            return Ok(None);
        }
        offset -= self.seq_len;
        if self.lookahead.len() > offset {
            if let Some(i) =
                self.lookahead[offset..max_len - self.seq_len].find_byte(byte)
            {
                return Ok(Some(self.seq_len + offset + i));
            }
        }
        Err(ReplacementError::NeedMoreCharacters)
    }
    pub fn available_len(&self) -> usize {
        self.seq_len + self.lookahead.len()
    }
    pub fn require_len<E>(
        &self,
        len: usize,
    ) -> Result<(), ReplacementError<E>> {
        if self.available_len() < len {
            return Err(ReplacementError::NeedMoreCharacters);
        }
        Ok(())
    }
    pub fn pull_into_buf<E>(
        self,
        len: usize,
    ) -> Result<&'a mut Vec<u8>, ReplacementError<E>> {
        if self.seq_len + self.lookahead.len() < len {
            return Err(ReplacementError::NeedMoreCharacters);
        }
        if len > self.seq_len {
            self.buf
                .extend_from_slice(&self.lookahead[..len - self.seq_len]);
        }
        Ok(self.buf)
    }
}

pub fn read_until_unescape2<
    S: BufRead + ?Sized,
    E,
    F: FnMut(ReplacementState) -> Result<usize, ReplacementError<E>>,
>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    delim: u8,
    escape_char_1: u8,
    escape_char_2: u8,
    mut replacement_fn: F,
) -> Result<usize, ReadUntilUnescapeError<E>> {
    let mut read = 0;
    let mut curr_esc_len = 0;
    'read_data: loop {
        let mut available = match stream.fill_buf() {
            Ok(data) => data,
            Err(e) => {
                if e.kind() == ErrorKind::Interrupted {
                    continue;
                } else {
                    return Err(ReadUntilUnescapeError::Io(e));
                }
            }
        };
        if available.is_empty() {
            return Err(ReadUntilUnescapeError::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }
        let mut consumed = 0;
        'handle_escapes: loop {
            if curr_esc_len == 0 {
                let Some(i) = memchr::memchr3(
                    delim,
                    escape_char_1,
                    escape_char_2,
                    available,
                ) else {
                    buf.extend_from_slice(available);
                    consumed += available.len();
                    read += consumed;
                    stream.consume(consumed);
                    continue 'read_data;
                };
                if available[i] == delim {
                    buf.extend_from_slice(&available[..i]);
                    consumed += i + 1;
                    stream.consume(consumed);
                    return Ok(read + consumed);
                }
                buf.extend_from_slice(&available[..=i]);
                available = &available[i + 1..];
                consumed += i + 1;
                curr_esc_len = 1;
            }
            let buf_len = buf.len();
            match replacement_fn(ReplacementState {
                buf,
                seq_len: curr_esc_len,
                lookahead: available,
            }) {
                Err(ReplacementError::Error(e)) => {
                    stream.consume(consumed);

                    return Err(ReadUntilUnescapeError::UnescapeFailed(
                        UnescapeFailedError {
                            base: e,
                            bytes_consumed: read + consumed - curr_esc_len,
                            escape_seq_len: curr_esc_len + buf.len() - buf_len,
                        },
                    ));
                }
                Err(ReplacementError::NeedMoreCharacters) => {
                    buf.extend_from_slice(available);
                    curr_esc_len += available.len();
                    consumed += available.len();
                    stream.consume(consumed);
                    continue 'read_data;
                }
                Ok(mut n) => {
                    debug_assert!(n > curr_esc_len);
                    n -= curr_esc_len;
                    consumed += n;
                    available = &available[n..];
                    curr_esc_len = 0;
                    continue 'handle_escapes;
                }
            }
        }
    }
}
