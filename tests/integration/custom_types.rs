use std::borrow::Cow;

use typeline::record_data::{
    custom_data::CustomData, formattable::RealizedFormatKey,
};
use typeline_core::{
    operators::string_sink::{create_op_string_sink, StringSinkHandle},
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
};

#[derive(Debug, Clone)]
struct DummyCustomType;

impl CustomData for DummyCustomType {
    fn type_name(&self) -> Cow<'static, str> {
        "dummy".into()
    }

    fn clone_dyn(&self) -> Box<dyn CustomData> {
        Box::new(self.clone())
    }

    fn format(
        &self,
        w: &mut dyn typeline::utils::text_write::TextWrite,
        _format: &RealizedFormatKey,
    ) -> std::io::Result<()> {
        w.write_all_text("dummy")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DummyCustomTypeNoStringify;

impl CustomData for DummyCustomTypeNoStringify {
    fn type_name(&self) -> Cow<'static, str> {
        "dummy_no_stringify".into()
    }
    fn clone_dyn(&self) -> Box<dyn CustomData> {
        Box::new(self.clone())
    }
    fn format(
        &self,
        _w: &mut dyn typeline::utils::text_write::TextWrite,
        _format: &RealizedFormatKey,
    ) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
}

#[test]
fn custom_type_stringify() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .push_custom(Box::new(DummyCustomType), 1)
        .run_collect_stringified()?;
    assert_eq!(res, ["dummy"]);
    Ok(())
}

#[test]
fn custom_type_that_cannot_stringify() -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .push_custom(Box::new(DummyCustomTypeNoStringify), 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("failed to stringify custom type 'dummy_no_stringify': unsupported")
    );
    Ok(())
}
