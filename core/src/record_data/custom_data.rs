use std::{any::Any, borrow::Cow};

use bstr::ByteSlice;

pub unsafe trait CustomData: Any + Send + Sync {
    fn equals(&self, other: &CustomDataBox) -> bool {
        let self_ptr = self as *const Self as *const u8;
        let other_ptr = other.as_ref() as *const dyn CustomData as *const u8;
        self_ptr == other_ptr
    }
    fn clone_dyn(&self) -> CustomDataBox;
    fn type_name(&self) -> Cow<str>;
    fn stringifies_as_valid_utf8(&self) -> bool;
    fn stringified_len(&self) -> Option<usize>;
    fn stringified_char_count(&self) -> Option<usize>;
    fn stringify(&self, w: &mut dyn std::io::Write) -> std::io::Result<()>;
    fn debug_stringifies_as_valid_utf8(&self) -> bool {
        self.stringifies_as_valid_utf8()
    }
    fn debug_stringified_len(&self) -> Option<usize> {
        self.stringified_len().map(|l| l + 2)
    }
    fn debug_stringified_char_count(&self) -> Option<usize> {
        self.stringified_char_count().map(|cc| cc + 2)
    }
    fn debug_stringify(
        &self,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        w.write_all(b"`")?;
        self.stringify(w)?;
        w.write_all(b"`")?;
        Ok(())
    }
}

pub type CustomDataBox = Box<dyn CustomData>;

impl Clone for CustomDataBox {
    fn clone(&self) -> Self {
        self.clone_dyn()
    }
}
impl PartialEq for CustomDataBox {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

pub trait CustomDataSafe: Any + Send + Sync + Clone {
    const STRINGIFIES_VALID_UTF8: bool = true;
    const DEBUG_STRINGIFIES_VALID_UTF8: bool = true;

    fn equals(&self, other: &CustomDataBox) -> bool {
        let self_ptr = self as *const Self as *const u8;
        let other_ptr = other.as_ref() as *const dyn CustomData as *const u8;
        self_ptr == other_ptr
    }
    fn clone_dyn(&self) -> CustomDataBox {
        Box::new(self.clone())
    }
    fn type_name(&self) -> Cow<str>;

    fn stringified_len(&self) -> Option<usize>;
    fn stringified_char_count(&self) -> Option<usize>;
    fn stringify_utf8(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result;
    fn stringify_non_utf8(
        &self,
        _w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn debug_stringified_len(&self) -> Option<usize> {
        self.stringified_len().map(|l| l + 2)
    }
    fn debug_stringified_char_count(&self) -> Option<usize> {
        self.stringified_char_count().map(|cc| cc + 2)
    }
    fn debug_stringify_utf8(
        &self,
        w: &mut dyn std::fmt::Write,
    ) -> std::fmt::Result {
        w.write_char('`')?;
        self.stringify_utf8(w)?;
        w.write_char('`')
    }
    fn debug_stringify_non_utf8(
        &self,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        w.write_all(b"`")?;
        self.stringify_non_utf8(w)?;
        w.write_all(b"`")
    }
}

struct WriteAdapterUtf8<'a> {
    io_error: Option<std::io::Error>,
    base_writer: &'a mut dyn std::io::Write,
    bytes_writter: usize,
    chars_written: usize,
}
impl<'a> WriteAdapterUtf8<'a> {
    fn new(base_writer: &'a mut dyn std::io::Write) -> Self {
        Self {
            io_error: None,
            base_writer,
            bytes_writter: 0,
            chars_written: 0,
        }
    }
}
impl<'a> std::fmt::Write for WriteAdapterUtf8<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.io_error.is_some() {
            return Err(std::fmt::Error);
        }
        self.bytes_writter += s.len();
        self.chars_written += s.chars().count();
        if let Err(e) = self.base_writer.write_all(s.as_bytes()) {
            self.io_error = Some(e);
            return Err(std::fmt::Error);
        }
        Ok(())
    }
}
struct WriteAdapterNonUtf8<'a> {
    base_writer: &'a mut dyn std::io::Write,
    bytes_writter: usize,
    chars_written: usize,
}
impl<'a> WriteAdapterNonUtf8<'a> {
    fn new(base_writer: &'a mut dyn std::io::Write) -> Self {
        Self {
            base_writer,
            bytes_writter: 0,
            chars_written: 0,
        }
    }
}
impl<'a> std::io::Write for WriteAdapterNonUtf8<'a> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.bytes_writter += data.len();
        self.chars_written += data.chars().count();
        self.base_writer.write_all(data).map(|_| data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.base_writer.flush()
    }
}

unsafe impl<T: CustomDataSafe> CustomData for T {
    fn clone_dyn(&self) -> CustomDataBox {
        CustomDataSafe::clone_dyn(self)
    }

    fn type_name(&self) -> Cow<str> {
        CustomDataSafe::type_name(self)
    }

    fn stringifies_as_valid_utf8(&self) -> bool {
        Self::STRINGIFIES_VALID_UTF8
    }

    fn stringified_len(&self) -> Option<usize> {
        CustomDataSafe::stringified_len(self)
    }

    fn stringified_char_count(&self) -> Option<usize> {
        CustomDataSafe::stringified_char_count(self)
    }

    fn debug_stringified_len(&self) -> Option<usize> {
        CustomDataSafe::debug_stringified_len(self)
    }

    fn debug_stringified_char_count(&self) -> Option<usize> {
        CustomDataSafe::debug_stringified_char_count(self)
    }

    fn stringify(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        let (bytes_writter, chars_written) = if Self::STRINGIFIES_VALID_UTF8 {
            let mut adapter = WriteAdapterUtf8::new(w);
            let _ = CustomDataSafe::stringify_utf8(self, &mut adapter);
            if let Some(e) = adapter.io_error {
                return Err(e);
            }
            (adapter.bytes_writter, adapter.chars_written)
        } else {
            let mut adapter = WriteAdapterNonUtf8::new(w);
            CustomDataSafe::stringify_non_utf8(self, &mut adapter)?;
            (adapter.bytes_writter, adapter.chars_written)
        };
        if Some(bytes_writter) != self.stringified_len()
            || chars_written
                != self.stringified_char_count().unwrap_or(chars_written)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "missmatching stringified length for custom data type {}",
                    self.type_name()
                ),
            ));
        }
        Ok(())
    }
    fn debug_stringify(
        &self,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        let (bytes_writter, chars_written) = if Self::STRINGIFIES_VALID_UTF8 {
            let mut adapter = WriteAdapterUtf8::new(w);
            let _ = CustomDataSafe::debug_stringify_utf8(self, &mut adapter);
            if let Some(e) = adapter.io_error {
                return Err(e);
            }
            (adapter.bytes_writter, adapter.chars_written)
        } else {
            let mut adapter = WriteAdapterNonUtf8::new(w);
            CustomDataSafe::debug_stringify_non_utf8(self, &mut adapter)?;
            (adapter.bytes_writter, adapter.chars_written)
        };
        if Some(bytes_writter) != self.debug_stringified_len()
            || chars_written
                != self.debug_stringified_char_count().unwrap_or(chars_written)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "missmatching debug_stringified length for custom data type {}",
                    self.type_name()
                ),
            ));
        }
        Ok(())
    }
}
