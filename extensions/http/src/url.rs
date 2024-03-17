use std::ops::{Deref, DerefMut};

use scr_core::{
    operators::format::RealizedFormatKey,
    record_data::custom_data::{CustomData, FieldValueFormattingError},
    utils::text_write::TextWrite,
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

impl CustomData for UrlValueType {
    fn type_name(&self) -> std::borrow::Cow<str> {
        "url".into()
    }

    fn clone_dyn(&self) -> scr_core::record_data::custom_data::CustomDataBox {
        Box::new(self.clone())
    }

    fn format(
        &self,
        w: &mut dyn TextWrite,
        _format: &RealizedFormatKey,
    ) -> Result<(), FieldValueFormattingError> {
        w.write_text_fmt(format_args!("{}", self.0))?;
        Ok(())
    }
}
