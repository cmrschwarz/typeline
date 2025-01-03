use typeline_core::{
    record_data::{
        custom_data::{format_custom_data_padded, CustomData},
        formattable::RealizedFormatKey,
    },
    utils::text_write::TextWrite,
};
#[allow(unused)] // TODO
#[derive(
    Clone, PartialEq, Eq, Debug, derive_more::Deref, derive_more::DerefMut,
)]
pub struct UrlValueType(url::Url);

impl CustomData for UrlValueType {
    fn type_name(&self) -> std::borrow::Cow<'static, str> {
        "url".into()
    }

    fn clone_dyn(
        &self,
    ) -> typeline_core::record_data::custom_data::CustomDataBox {
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
