use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::os::unix::prelude::{OsStrExt, OsStringExt};

#[repr(transparent)]
pub struct XStr([u8]);

#[derive(Clone, Debug)]
pub struct XString(Vec<u8>);

impl XStr {
    pub unsafe fn from_bytes_unchecked(v: &[u8]) -> &XStr {
        std::mem::transmute(v)
    }
    pub fn from_str(v: &str) -> &XStr {
        unsafe { XStr::from_bytes_unchecked(v.as_bytes()) }
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn to_string_lossy(&self) -> std::borrow::Cow<str> {
        String::from_utf8_lossy(&self.0)
    }
}

impl Deref for XString {
    type Target = XStr;

    fn deref(&self) -> &XStr {
        unsafe { XStr::from_bytes_unchecked(self.0.as_slice()) }
    }
}

impl XString {
    pub fn new(v: &[u8]) -> XString {
        XString(Vec::from(v))
    }
}

impl PartialEq for XStr {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl XStr {
    pub fn equals_str(&self, v: &str) -> bool {
        &self.0 == v.as_bytes()
    }
    pub fn starts_with(&self, v: &XStr) -> bool {
        self.0.starts_with(&v.0)
    }
}

impl<'a> From<&'a str> for &'a XStr {
    fn from(v: &'a str) -> &'a XStr {
        XStr::from_str(v)
    }
}

impl From<OsString> for XString {
    fn from(v: OsString) -> Self {
        XString(v.into_vec())
    }
}

impl From<String> for XString {
    fn from(v: String) -> Self {
        XString(v.into_bytes())
    }
}

impl From<&[u8]> for XString {
    fn from(v: &[u8]) -> Self {
        // TODO: handle encoding
        XString(Vec::from(v))
    }
}
