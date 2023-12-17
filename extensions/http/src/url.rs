use std::ops::{Deref, DerefMut};

use scr_core::{
    operators::format::RealizedFormatKey,
    record_data::custom_data::CustomDataSafe,
};

#[derive(Clone, PartialEq, Eq, Debug)]
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

    fn stringified_len(&self, _format: &RealizedFormatKey) -> Option<usize> {
        Some(self.0.as_str().len())
    }

    fn stringify_utf8(
        &self,
        w: &mut dyn std::fmt::Write,
        _format: &RealizedFormatKey,
    ) -> std::fmt::Result {
        w.write_str(self.0.as_str())
    }
}
