use std::ops::{Deref, DerefMut};

use scr_core::record_data::custom_data::CustomDataSafe;
use url::Url;

#[derive(Clone, PartialEq, Eq)]
pub struct UrlValueType(url::Url);

impl Deref for UrlValueType {
    type Target = url::Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UrlValueType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl CustomDataSafe for UrlValueType {
    fn type_name(&self) -> std::borrow::Cow<str> {
        "url".into()
    }

    fn stringified_len(&self) -> Option<usize> {
        Some(self.0.as_str().len())
    }

    fn stringified_char_count(&self) -> Option<usize> {
        Some(self.0.as_str().chars().count())
    }

    fn stringify_utf8(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        w.write_str(self.0.as_str())
    }
}
