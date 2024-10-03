use std::{io::ErrorKind, mem::ManuallyDrop};

use arrayvec::ArrayVec;
use bstr::ByteSlice;

use super::{
    printable_unicode::is_char_printable,
    text_write::{TextWrite, TextWriteFormatAdapter},
    utf8_codepoint_len_from_first_byte, MAX_UTF8_CHAR_LEN,
};

pub struct EscapedWriter<W: TextWrite> {
    base: W,
    incomplete_char_missing_len: u8,
    buffer_offset: u8,
    quote_to_escape: u8,
    // worst case length is storing the 4 escaped bytes of a
    // broken utf-8 codepoint ('\xFF' * 4) -> 16 bytes
    // PERF: it would probably be better not to have this here
    // and recompute the broken char instead
    buffer: ArrayVec<u8, 16>,
}
pub struct EscapedFmtWriter<F: std::fmt::Write>(
    EscapedWriter<EscapedWriterFmtAdapter<F>>,
);
struct EscapedWriterFmtAdapter<F: std::fmt::Write>(F);

const HEX_DIGITS_UPPER: [u8; 16] = [
    b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'A', b'B',
    b'C', b'D', b'E', b'F',
];

fn push_byte_escape<const C: usize>(byte: u8, output: &mut ArrayVec<u8, C>) {
    match byte {
        b'\t' => output.try_extend_from_slice(b"\\t").unwrap(),
        b'\r' => output.try_extend_from_slice(b"\\r").unwrap(),
        b'\n' => output.try_extend_from_slice(b"\\n").unwrap(),
        b'\\' => output.try_extend_from_slice(b"\\\\").unwrap(),
        b'\'' => output.try_extend_from_slice(b"\\\'").unwrap(),
        b'\"' => output.try_extend_from_slice(b"\\\"").unwrap(),
        _ => {
            if !byte.is_ascii() || !byte.is_ascii_control() || byte == 0 {
                let hi = HEX_DIGITS_UPPER[usize::from(byte >> 4)];
                let lo = HEX_DIGITS_UPPER[usize::from(byte & 0xF)];
                output.extend([b'\\', b'x', hi, lo]);
            } else {
                output.push(byte);
            }
        }
    }
}

fn push_unicode_escape<const C: usize>(c: char, output: &mut ArrayVec<u8, C>) {
    let c = c as u32;

    let len = (c | 1).leading_zeros() as usize / 4;
    output.try_extend_from_slice(b"\\u{").unwrap();
    for i in 0..len {
        output.push(
            HEX_DIGITS_UPPER[((c >> ((len - i - 1) * 4)) & 15) as usize],
        );
    }
    output.push(b'}')
}

fn push_char<const C: usize>(c: char, output: &mut ArrayVec<u8, C>) {
    if c.is_ascii() {
        push_byte_escape(c as u8, output);
    } else if is_char_printable(c) {
        output.extend(std::iter::repeat(0u8).take(c.len_utf8()));
        c.encode_utf8(output);
    } else {
        push_unicode_escape(c, output);
    }
}

impl<W: TextWrite> EscapedWriter<W> {
    pub fn new(base: W, quote_to_escape: u8) -> Self {
        Self {
            base,
            incomplete_char_missing_len: 0,
            buffer_offset: 0,
            quote_to_escape,
            buffer: ArrayVec::new(),
        }
    }
    pub fn into_inner(mut self) -> std::io::Result<W> {
        std::io::Write::flush(&mut self)?;
        Ok(unsafe { std::ptr::read(&ManuallyDrop::new(self).base) })
    }
}

impl<W: std::fmt::Write> EscapedFmtWriter<W> {
    pub fn new(base: W, quote_to_escape: u8) -> Self {
        Self(EscapedWriter::new(
            EscapedWriterFmtAdapter(base),
            quote_to_escape,
        ))
    }
    pub fn into_inner(self) -> Result<W, std::fmt::Error> {
        Ok(self.0.into_inner().map_err(|_| std::fmt::Error)?.0)
    }
}

impl<F: std::fmt::Write> TextWrite for EscapedWriterFmtAdapter<F> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        // SAFETY: EscapedWriter will only split up utf-8 if it is
        // forced to do so by an InterruptedError.
        // As this adapter will never trigger this error
        // we can assume the escaped data to be valid utf-8.
        let buf_str = unsafe { std::str::from_utf8_unchecked(buf) };
        match self.0.write_str(buf_str) {
            Ok(()) => Ok(buf.len()),
            Err(e) => Err(std::io::Error::new(ErrorKind::Other, e)),
        }
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<W: std::fmt::Write> std::fmt::Write for EscapedFmtWriter<W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        std::io::Write::write_all(&mut self.0, s.as_bytes())
            .map_err(|_| std::fmt::Error)
    }
}
impl<W: std::fmt::Write> std::io::Write for EscapedFmtWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl<W: TextWrite> std::io::Write for EscapedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buf_offset = 0;
        if self.incomplete_char_missing_len != 0 {
            if buf.len() < self.incomplete_char_missing_len as usize {
                self.buffer.try_extend_from_slice(buf).unwrap();
                self.incomplete_char_missing_len -= buf.len() as u8;
                return Ok(buf.len());
            }
            self.buffer
                .try_extend_from_slice(
                    &buf[0..self.incomplete_char_missing_len as usize],
                )
                .unwrap();
            buf_offset += self.incomplete_char_missing_len as usize;
            self.incomplete_char_missing_len = 0;

            if let Some(c) = self.buffer.chars().next() {
                push_char(c, &mut self.buffer);
            } else {
                let len = buf.len();
                let mut scratch = [0u8; 4];
                scratch[0..len].copy_from_slice(&self.buffer);
                self.buffer.clear();
                for &b in &scratch[0..len] {
                    push_byte_escape(b, &mut self.buffer);
                }
            }
        }

        'handle_escapes: loop {
            if !self.buffer.is_empty() {
                let written = unsafe {
                    // TODO: this seems wrong
                    self.base.write_text_unchecked(
                        &self.buffer[self.buffer_offset as usize..],
                    )?
                };
                if written != self.buffer.len() + self.buffer_offset as usize {
                    self.buffer_offset += written as u8;
                    if buf_offset == 0 {
                        return Err(ErrorKind::Interrupted.into());
                    }
                    return Ok(buf_offset);
                }
                self.buffer.clear();
                self.buffer_offset = 0;
            }
            for (start, end, c) in buf[buf_offset..].char_indices() {
                if c == std::char::REPLACEMENT_CHARACTER {
                    if &buf[buf_offset..][start..end] == "\u{FFFD}".as_bytes()
                    {
                        // the replacement character is considered printable
                        continue;
                    }
                    if buf.len() - (buf_offset + start) < MAX_UTF8_CHAR_LEN
                        && utf8_codepoint_len_from_first_byte(
                            buf[buf_offset + start],
                        )
                        .map(|len| len as usize > end - start)
                            == Some(true)
                    {
                        self.buffer
                            .try_extend_from_slice(
                                &buf[buf_offset..][start..end],
                            )
                            .unwrap();
                        return Ok(buf_offset + start);
                    }
                    for i in start..end {
                        push_byte_escape(
                            buf[buf_offset + i],
                            &mut self.buffer,
                        );
                    }
                    buf_offset += end;
                    continue 'handle_escapes;
                }
                if c == '\\'
                    || c == self.quote_to_escape as char
                    || !is_char_printable(c)
                {
                    match unsafe {
                        self.base
                            .write_text_unchecked(&buf[buf_offset..][..start])
                    } {
                        Ok(n) => {
                            if n != start {
                                if n == 0 && buf_offset == 0 {
                                    return Err(ErrorKind::Interrupted.into());
                                }
                                return Ok(buf_offset + n);
                            }
                        }
                        Err(e) => {
                            if e.kind() == ErrorKind::Interrupted
                                && buf_offset != 0
                            {
                                return Ok(buf_offset);
                            }
                            return Err(e);
                        }
                    }
                    buf_offset += end;
                    push_char(c, &mut self.buffer);
                    continue 'handle_escapes;
                }
            }
            break;
        }
        match unsafe { self.base.write_text_unchecked(&buf[buf_offset..]) } {
            Ok(n) => {
                if n == 0 && buf_offset == 0 {
                    return Err(ErrorKind::Interrupted.into());
                }
                Ok(buf_offset + n)
            }
            Err(e) => {
                if e.kind() == ErrorKind::Interrupted && buf_offset != 0 {
                    return Ok(buf_offset);
                }
                Err(e)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.incomplete_char_missing_len != 0 {
            let len = self.incomplete_char_missing_len as usize;
            let mut scratch = [0u8; 4];
            scratch[0..len].copy_from_slice(&self.buffer);
            self.buffer.clear();
            for &b in &scratch[0..len] {
                push_byte_escape(b, &mut self.buffer);
            }
            self.incomplete_char_missing_len = 0;
        }
        if !self.buffer.is_empty() {
            unsafe {
                self.base.write_text_unchecked(
                    &self.buffer[self.buffer_offset as usize..],
                )?
            };
            self.buffer.clear();
            self.buffer_offset = 0;
        }
        self.base.flush_text()
    }
}

impl<W: TextWrite> TextWrite for EscapedWriter<W> {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        std::io::Write::write(self, buf)
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(self)
    }

    unsafe fn write_all_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<()> {
        std::io::Write::write_all(self, buf)
    }

    fn write_all_text(&mut self, buf: &str) -> std::io::Result<()> {
        std::io::Write::write_all(self, buf.as_bytes())
    }
}

impl<W: TextWrite> Drop for EscapedWriter<W> {
    fn drop(&mut self) {
        let _ = std::io::Write::flush(self);
    }
}

pub fn escape_to_string(input: &[u8], quote_to_escape: u8) -> String {
    let mut res = String::new();
    let mut w =
        EscapedWriter::new(TextWriteFormatAdapter(&mut res), quote_to_escape);
    std::io::Write::write_all(&mut w, input).unwrap();
    drop(w);
    res
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    fn escape(input: &[u8]) -> String {
        super::escape_to_string(input, b'"')
    }

    #[test]
    fn emtpy_string() {
        assert_eq!(escape(b""), "");
    }

    #[rstest]
    #[case(b"\\", "\\\\")]
    #[case(b"\\n", "\\\\n")]
    #[case(b"\xFF\\", "\\xFF\\\\")]
    fn backslashes(#[case] input: &[u8], #[case] output: &str) {
        assert_eq!(escape(input), output);
    }

    #[rstest]
    #[case("\u{FFFD}", "\u{FFFD}")]
    #[case("foo\u{FFFD}", "foo\u{FFFD}")]
    #[case("\u{FFFD}bar", "\u{FFFD}bar")]
    #[case("baz\u{FFFD}quux", "baz\u{FFFD}quux")]
    fn unicode_replacement_character(
        #[case] input: &str,
        #[case] output: &str,
    ) {
        assert_eq!(escape(input.as_bytes()), output);
    }

    #[rstest]
    #[case("foo\r", "foo\\r")]
    #[case("foo\nbar", "foo\\nbar")]
    #[case("\t", "\\t")]
    #[case("\x00", "\\x00")]
    fn ascii_escapes(#[case] input: &str, #[case] output: &str) {
        assert_eq!(escape(input.as_bytes()), output);
    }

    #[rstest]
    #[case("\u{FEFF}", "\\u{FEFF}")]
    #[case("foo\u{FEFF}", "foo\\u{FEFF}")]
    #[case("\u{FEFF}bar", "\\u{FEFF}bar")]
    fn unicode_escapes(#[case] input: &str, #[case] output: &str) {
        assert_eq!(escape(input.as_bytes()), output);
    }
}
