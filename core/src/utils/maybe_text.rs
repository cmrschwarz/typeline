use std::ops::Deref;

use bstr::{ByteSlice, ByteVec};

use super::text_write::TextWrite;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaybeText {
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaybeTextCow<'a> {
    Text(String),
    Bytes(Vec<u8>),
    TextRef(&'a str),
    BytesRef(&'a [u8]),
}

// slightly more space efficient than MaybeString, at the cost of
// efficient grow / shrink capabilities
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaybeTextBoxed {
    Text(Box<str>),
    Bytes(Box<[u8]>),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaybeTextRef<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

pub enum MaybeTextRefMut<'a> {
    Text(&'a mut String),
    Bytes(&'a mut Vec<u8>),
}

impl Default for MaybeText {
    fn default() -> Self {
        MaybeText::Text(String::default())
    }
}

impl<'a> Default for MaybeTextCow<'a> {
    fn default() -> Self {
        MaybeTextCow::TextRef("")
    }
}

impl MaybeText {
    pub const fn new() -> MaybeText {
        MaybeText::Text(String::new())
    }
    pub fn with_capacity(cap: usize) -> Self {
        MaybeText::Text(String::with_capacity(cap))
    }
    pub fn from_vec_try_str(bytes: Vec<u8>) -> Self {
        match String::from_utf8(bytes) {
            Ok(s) => MaybeText::Text(s),
            Err(e) => MaybeText::Bytes(e.into_bytes()),
        }
    }
    pub fn from_bytes_try_str(bytes: &[u8]) -> Self {
        Self::from_vec_try_str(bytes.to_owned())
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        MaybeText::Bytes(bytes.to_owned())
    }
    pub fn from_text(text: &str) -> Self {
        MaybeText::Text(text.to_owned())
    }
    pub fn as_ref(&self) -> MaybeTextRef {
        match self {
            MaybeText::Text(s) => MaybeTextRef::Text(s),
            MaybeText::Bytes(s) => MaybeTextRef::Bytes(s),
        }
    }
    pub fn as_ref_mut(&mut self) -> MaybeTextRefMut {
        match self {
            MaybeText::Text(s) => MaybeTextRefMut::Text(s),
            MaybeText::Bytes(s) => MaybeTextRefMut::Bytes(s),
        }
    }
    pub fn extend_with_bytes(&mut self, bytes: &[u8]) {
        match self {
            MaybeText::Text(v) => {
                let mut v = std::mem::take(v).into_bytes();
                v.extend_from_slice(bytes);
                *self = MaybeText::Bytes(v);
            }
            MaybeText::Bytes(v) => v.extend_from_slice(bytes),
        }
    }
    pub fn extend_with_text(&mut self, text: &str) {
        match self {
            MaybeText::Text(v) => v.push_str(text),
            MaybeText::Bytes(v) => v.extend_from_slice(text.as_bytes()),
        }
    }
    pub fn extend_with_maybe_text_ref(&mut self, data: MaybeTextRef) {
        match data {
            MaybeTextRef::Text(t) => self.extend_with_text(t),
            MaybeTextRef::Bytes(b) => self.extend_with_bytes(b),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MaybeText::Text(s) => Some(s),
            MaybeText::Bytes(_) => None,
        }
    }
    pub fn into_boxed(self) -> MaybeTextBoxed {
        match self {
            MaybeText::Text(s) => MaybeTextBoxed::Text(s.into_boxed_str()),
            MaybeText::Bytes(b) => MaybeTextBoxed::Bytes(b.into_boxed_slice()),
        }
    }
    pub fn into_text(self) -> Option<String> {
        match self {
            MaybeText::Text(s) => Some(s),
            MaybeText::Bytes(_) => None,
        }
    }
    pub fn into_text_lossy(self) -> String {
        match self {
            MaybeText::Text(s) => s,
            MaybeText::Bytes(v) => v.into_string_lossy(),
        }
    }
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            MaybeText::Text(s) => s.into_bytes(),
            MaybeText::Bytes(b) => b,
        }
    }
    pub fn capacity(&self) -> usize {
        match self {
            MaybeText::Text(s) => s.capacity(),
            MaybeText::Bytes(b) => b.capacity(),
        }
    }
    pub fn clear(&mut self) {
        match self {
            MaybeText::Text(t) => t.clear(),
            MaybeText::Bytes(b) => b.clear(),
        }
    }
    pub unsafe fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        match self {
            MaybeText::Text(s) => unsafe { s.as_mut_vec() },
            MaybeText::Bytes(b) => b,
        }
    }
}

impl<'a> MaybeTextCow<'a> {
    pub fn with_capacity(cap: usize) -> Self {
        MaybeTextCow::Text(String::with_capacity(cap))
    }
    pub fn from_vec_try_str(bytes: Vec<u8>) -> Self {
        match String::from_utf8(bytes) {
            Ok(s) => MaybeTextCow::Text(s),
            Err(e) => MaybeTextCow::Bytes(e.into_bytes()),
        }
    }
    pub fn from_bytes_try_str(bytes: &[u8]) -> Self {
        Self::from_vec_try_str(bytes.to_owned())
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        MaybeTextCow::Bytes(bytes.to_owned())
    }
    pub fn from_text(text: &str) -> Self {
        MaybeTextCow::Text(text.to_owned())
    }
    pub fn as_ref(&self) -> MaybeTextRef {
        match self {
            MaybeTextCow::Text(t) => MaybeTextRef::Text(t),
            MaybeTextCow::Bytes(b) => MaybeTextRef::Bytes(b),
            MaybeTextCow::TextRef(t) => MaybeTextRef::Text(t),
            MaybeTextCow::BytesRef(b) => MaybeTextRef::Bytes(b),
        }
    }
    pub fn make_owned(&mut self) {
        match self {
            MaybeTextCow::Text(_) | MaybeTextCow::Bytes(_) => (),
            MaybeTextCow::TextRef(t) => {
                *self = MaybeTextCow::Text(t.to_owned())
            }
            MaybeTextCow::BytesRef(b) => {
                *self = MaybeTextCow::Bytes(b.to_owned())
            }
        }
    }
    pub fn as_ref_mut(&mut self) -> MaybeTextRefMut {
        self.make_owned();
        match self {
            MaybeTextCow::Text(s) => MaybeTextRefMut::Text(s),
            MaybeTextCow::Bytes(s) => MaybeTextRefMut::Bytes(s),
            MaybeTextCow::TextRef(_) | MaybeTextCow::BytesRef(_) => {
                unreachable!()
            }
        }
    }
    pub fn extend_with_bytes(&mut self, bytes: &[u8]) {
        match self {
            MaybeTextCow::Text(v) => {
                let mut v = std::mem::take(v).into_bytes();
                v.extend_from_slice(bytes);
                *self = MaybeTextCow::Bytes(v);
            }
            MaybeTextCow::Bytes(v) => v.extend_from_slice(bytes),
            MaybeTextCow::TextRef(t) => {
                let mut res = Vec::with_capacity(t.len() + bytes.len());
                res.extend_from_slice(t.as_bytes());
                res.extend_from_slice(bytes.as_bytes());
                *self = MaybeTextCow::Bytes(res);
            }
            MaybeTextCow::BytesRef(b) => {
                let mut res = Vec::with_capacity(b.len() + bytes.len());
                res.extend_from_slice(b);
                res.extend_from_slice(bytes.as_bytes());
                *self = MaybeTextCow::Bytes(res);
            }
        }
    }
    pub fn extend_with_text(&mut self, text: &str) {
        match self {
            MaybeTextCow::Text(v) => v.push_str(text),
            MaybeTextCow::Bytes(v) => v.extend_from_slice(text.as_bytes()),
            MaybeTextCow::TextRef(t) => {
                let mut res = String::with_capacity(t.len() + text.len());
                res.push_str(t);
                res.push_str(text);
                *self = MaybeTextCow::Text(res);
            }
            MaybeTextCow::BytesRef(b) => {
                let mut res = Vec::with_capacity(b.len() + text.len());
                res.extend_from_slice(b);
                res.extend_from_slice(text.as_bytes());
                *self = MaybeTextCow::Bytes(res);
            }
        }
    }
    pub fn extend_with_maybe_text_ref(&mut self, data: MaybeTextRef) {
        match data {
            MaybeTextRef::Text(t) => self.extend_with_text(t),
            MaybeTextRef::Bytes(b) => self.extend_with_bytes(b),
        }
    }
    pub fn extend_with_maybe_text(&mut self, data: &MaybeText) {
        self.extend_with_maybe_text_ref(data.as_ref())
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MaybeTextCow::Text(s) => Some(s),
            MaybeTextCow::TextRef(t) => Some(t),
            MaybeTextCow::Bytes(_) | MaybeTextCow::BytesRef(_) => None,
        }
    }
    pub fn into_owned(self) -> MaybeText {
        match self {
            MaybeTextCow::Text(t) => MaybeText::Text(t),
            MaybeTextCow::Bytes(b) => MaybeText::Bytes(b),
            MaybeTextCow::TextRef(t) => MaybeText::Text(t.to_owned()),
            MaybeTextCow::BytesRef(b) => MaybeText::Bytes(b.to_owned()),
        }
    }
    pub fn into_boxed(self) -> MaybeTextBoxed {
        self.into_owned().into_boxed()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        match self {
            MaybeTextCow::Text(s) => s.len(),
            MaybeTextCow::Bytes(b) => b.len(),
            MaybeTextCow::TextRef(t) => t.len(),
            MaybeTextCow::BytesRef(b) => b.len(),
        }
    }
    pub fn capacity(&self) -> usize {
        match self {
            MaybeTextCow::Text(s) => s.capacity(),
            MaybeTextCow::Bytes(b) => b.capacity(),
            MaybeTextCow::TextRef(t) => t.len(),
            MaybeTextCow::BytesRef(b) => b.len(),
        }
    }
    pub fn clear(&mut self) {
        match self {
            MaybeTextCow::Text(t) => t.clear(),
            MaybeTextCow::Bytes(b) => b.clear(),
            MaybeTextCow::TextRef(t) => *t = "",
            MaybeTextCow::BytesRef(b) => *b = b"",
        }
    }
    pub unsafe fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        match self {
            MaybeTextCow::Text(s) => unsafe { s.as_mut_vec() },
            MaybeTextCow::Bytes(b) => b,
            MaybeTextCow::TextRef(_) => todo!(),
            MaybeTextCow::BytesRef(_) => todo!(),
        }
    }

    pub(crate) fn from_maybe_text(res: MaybeText) -> MaybeTextCow<'static> {
        match res {
            MaybeText::Text(t) => MaybeTextCow::Text(t),
            MaybeText::Bytes(b) => MaybeTextCow::Bytes(b),
        }
    }
}

impl MaybeTextBoxed {
    pub fn from_boxed_bytes_try_str(bytes: Box<[u8]>) -> Self {
        match String::from_utf8(bytes.into_vec()) {
            Ok(s) => MaybeTextBoxed::Text(s.into_boxed_str()),
            Err(e) => MaybeTextBoxed::Bytes(e.into_bytes().into_boxed_slice()),
        }
    }
    pub fn from_bytes_try_str(bytes: &[u8]) -> Self {
        match bytes.to_str() {
            Ok(s) => MaybeTextBoxed::Text(Box::<str>::from(s)),
            Err(_) => MaybeTextBoxed::Bytes(Box::<[u8]>::from(bytes)),
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        MaybeTextBoxed::Bytes(bytes.to_owned().into_boxed_slice())
    }
    pub fn from_text(text: &str) -> Self {
        MaybeTextBoxed::Text(text.to_owned().into_boxed_str())
    }
    pub fn as_ref(&self) -> MaybeTextRef {
        match self {
            MaybeTextBoxed::Text(s) => MaybeTextRef::Text(s),
            MaybeTextBoxed::Bytes(s) => MaybeTextRef::Bytes(s),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MaybeTextBoxed::Text(s) => Some(s),
            MaybeTextBoxed::Bytes(_) => None,
        }
    }
    pub fn into_maybe_string(self) -> MaybeText {
        match self {
            MaybeTextBoxed::Text(s) => MaybeText::Text(s.into_string()),
            MaybeTextBoxed::Bytes(b) => MaybeText::Bytes(b.into_vec()),
        }
    }
}

impl<'a> MaybeTextRef<'a> {
    pub fn as_str(&self) -> Option<&'a str> {
        match self {
            MaybeTextRef::Text(s) => Some(s),
            MaybeTextRef::Bytes(_) => None,
        }
    }
    pub fn as_bytes(&self) -> &'a [u8] {
        match self {
            MaybeTextRef::Text(s) => s.as_bytes(),
            MaybeTextRef::Bytes(s) => s,
        }
    }
    pub fn to_owned(&self) -> MaybeText {
        match self {
            MaybeTextRef::Text(t) => MaybeText::Text((*t).to_string()),
            MaybeTextRef::Bytes(b) => MaybeText::Bytes(b.to_vec()),
        }
    }
    pub fn from_bytes_try_str(bytes: &'a [u8]) -> Self {
        match bytes.to_str() {
            Ok(s) => MaybeTextRef::Text(s),
            Err(_) => MaybeTextRef::Bytes(bytes),
        }
    }
}
impl<'a> MaybeTextRefMut<'a> {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MaybeTextRefMut::Text(s) => Some(s),
            MaybeTextRefMut::Bytes(_) => None,
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            MaybeTextRefMut::Text(s) => s.as_bytes(),
            MaybeTextRefMut::Bytes(s) => s,
        }
    }
}

impl TextWrite for MaybeText {
    unsafe fn write_text_unchecked(
        &mut self,
        buf: &[u8],
    ) -> std::io::Result<usize> {
        match self {
            MaybeText::Text(v) => unsafe {
                v.as_mut_vec().extend_from_slice(buf)
            },
            MaybeText::Bytes(v) => v.extend_from_slice(buf),
        }
        Ok(buf.len())
    }

    fn flush_text(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Write for MaybeText {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_with_bytes(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl From<MaybeText> for MaybeTextBoxed {
    fn from(value: MaybeText) -> Self {
        value.into_boxed()
    }
}

impl From<MaybeTextBoxed> for MaybeText {
    fn from(value: MaybeTextBoxed) -> Self {
        value.into_maybe_string()
    }
}

impl Deref for MaybeText {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl Deref for MaybeTextBoxed {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl<'a> Deref for MaybeTextRef<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl<'a> Deref for MaybeTextRefMut<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}
