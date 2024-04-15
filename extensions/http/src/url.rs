use std::ops::{Deref, DerefMut};

use scr_core::{
    record_data::{
        custom_data::{format_custom_data_padded, CustomData},
        formattable::RealizedFormatKey,
    },
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
        format: &RealizedFormatKey,
    ) -> std::io::Result<()> {
        format_custom_data_padded(self, false, format, w, |w| {
            w.write_text_fmt(format_args!("{}", self.0))?;
            Ok(())
        })
    }
}
