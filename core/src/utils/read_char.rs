use std::io::Read;

use bstr::ByteSlice;

use super::{
    slice_writer::SliceWriter, utf8_codepoint_len_from_first_byte,
    MAX_UTF8_CHAR_LEN,
};

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
        return Err(ReadCharError::InvalidUtf8 {
            sequence: buf,
            len: codepoint_len,
        });
    };
    Ok(tok)
}
