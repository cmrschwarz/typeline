use std::{any::Any, borrow::Cow};

pub fn custom_data_reference_eq<T: CustomData + ?Sized>(
    lhs: &T,
    rhs: &dyn CustomData,
) -> bool {
    if std::mem::size_of_val(lhs) == 0 {
        return std::mem::size_of_val(rhs) == 0
            && lhs.type_id() == rhs.type_id();
    }
    let self_ptr = lhs as *const T as *const u8;
    let other_ptr = rhs as *const dyn CustomData as *const u8;
    self_ptr == other_ptr
}

pub unsafe trait CustomData: Any + Send + Sync {
    fn equals(&self, other: &dyn CustomData) -> bool {
        custom_data_reference_eq(self, other)
    }
    fn clone_dyn(&self) -> CustomDataBox;
    fn type_name(&self) -> Cow<str>;

    // SAFETY: if this returns true, subsequent calls to
    // `stringify` / `stringify_expecte_len` **must** only write valid utf8
    fn stringifies_as_valid_utf8(&self) -> bool;

    // SAFETY: **must** return the number of characters that a subsequent
    // `stringify` / `stringify_expect_len` will write.
    // While callers will prevent buffer overruns of the `Write`-rs,
    // they will assume the reported number of bytes to be initialized
    // (written to) after the call.
    fn stringified_len(&self) -> Option<usize>;
    fn stringified_char_count(&self) -> Option<usize>;
    fn stringify(&self, w: &mut dyn std::io::Write) -> std::io::Result<usize>;
    fn stringify_expect_len(
        &self,
        len: usize,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        let reported_len = self.stringify(w)?;
        if reported_len != len {
            panic!(
                "unexpected length for stringify of custom type {}",
                self.type_name()
            );
        }
        Ok(())
    }
    // SAFETY: if this returns true, subsequent calls to
    // `debug_stringify` / `debug_stringify_expecte_len`
    // **must** only write valid utf8
    fn debug_stringifies_as_valid_utf8(&self) -> bool {
        self.stringifies_as_valid_utf8()
    }

    // SAFETY: **must** return the number of characters that a subsequent
    // `debug_stringify` / `debug_stringify_expect_len` will write.
    // While callers will prevent buffer overruns of the `Write`-rs,
    // they will assume the reported number of bytes to be initialized
    // (written to) after the call.
    fn debug_stringified_len(&self) -> Option<usize> {
        self.stringified_len().map(|l| l + 2)
    }
    fn debug_stringified_char_count(&self) -> Option<usize> {
        self.stringified_char_count().map(|cc| cc + 2)
    }
    fn debug_stringify(
        &self,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<usize> {
        w.write_all(b"`")?;
        let len = self.stringify(w)?;
        w.write_all(b"`")?;
        Ok(len + 2)
    }
    fn debug_stringify_expect_len(
        &self,
        len: usize,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        let reported_len = self.stringify(w)?;
        if reported_len != len {
            panic!(
                "unexpected length for debug stringify of custom type {}",
                self.type_name()
            );
        }
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
        self.equals(&**other)
    }
}

pub trait CustomDataSafe: Any + Send + Sync + Clone {
    const STRINGIFIES_VALID_UTF8: bool = true;
    const DEBUG_STRINGIFIES_VALID_UTF8: bool = true;

    fn equals(&self, other: &dyn CustomData) -> bool {
        custom_data_reference_eq(self, other)
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
    bytes_written: usize,
}
impl<'a> WriteAdapterUtf8<'a> {
    fn new(base_writer: &'a mut dyn std::io::Write) -> Self {
        Self {
            io_error: None,
            base_writer,
            bytes_written: 0,
        }
    }
}
impl<'a> std::fmt::Write for WriteAdapterUtf8<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.io_error.is_some() {
            return Err(std::fmt::Error);
        }
        self.bytes_written += s.len();
        if let Err(e) = self.base_writer.write_all(s.as_bytes()) {
            self.io_error = Some(e);
            return Err(std::fmt::Error);
        }
        Ok(())
    }
}
struct WriteAdapterNonUtf8<'a> {
    base_writer: &'a mut dyn std::io::Write,
    bytes_written: usize,
}
impl<'a> WriteAdapterNonUtf8<'a> {
    fn new(base_writer: &'a mut dyn std::io::Write) -> Self {
        Self {
            base_writer,
            bytes_written: 0,
        }
    }
}
impl<'a> std::io::Write for WriteAdapterNonUtf8<'a> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.bytes_written += data.len();
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

    fn debug_stringifies_as_valid_utf8(&self) -> bool {
        Self::DEBUG_STRINGIFIES_VALID_UTF8
    }

    fn debug_stringified_len(&self) -> Option<usize> {
        CustomDataSafe::debug_stringified_len(self)
    }

    fn debug_stringified_char_count(&self) -> Option<usize> {
        CustomDataSafe::debug_stringified_char_count(self)
    }

    fn stringify(&self, w: &mut dyn std::io::Write) -> std::io::Result<usize> {
        let len = if Self::STRINGIFIES_VALID_UTF8 {
            let mut adapter = WriteAdapterUtf8::new(w);
            let Ok(()) = CustomDataSafe::stringify_utf8(self, &mut adapter)
            else {
                return Err(adapter.io_error.unwrap());
            };
            adapter.bytes_written
        } else {
            let mut adapter = WriteAdapterNonUtf8::new(w);
            CustomDataSafe::stringify_non_utf8(self, &mut adapter)?;
            adapter.bytes_written
        };
        Ok(len)
    }
    fn debug_stringify(
        &self,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<usize> {
        let len = if Self::STRINGIFIES_VALID_UTF8 {
            let mut adapter = WriteAdapterUtf8::new(w);
            let Ok(()) =
                CustomDataSafe::debug_stringify_utf8(self, &mut adapter)
            else {
                return Err(adapter.io_error.unwrap());
            };
            adapter.bytes_written
        } else {
            let mut adapter = WriteAdapterNonUtf8::new(w);
            CustomDataSafe::debug_stringify_non_utf8(self, &mut adapter)?;
            adapter.bytes_written
        };
        Ok(len)
    }
}
