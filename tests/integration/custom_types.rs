use std::borrow::Cow;

use scr::record_data::{
    custom_data::{CustomData, FieldValueFormattingError},
    formattable::RealizedFormatKey,
};
use scr_core::{
    operators::string_sink::{create_op_string_sink, StringSinkHandle},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[derive(Debug, Clone)]
struct DummyCustomType;

impl CustomData for DummyCustomType {
    fn type_name(&self) -> Cow<str> {
        "dummy".into()
    }

    fn clone_dyn(&self) -> Box<dyn CustomData> {
        Box::new(self.clone())
    }

    fn format(
        &self,
        w: &mut dyn scr::utils::text_write::TextWrite,
        _format: &RealizedFormatKey,
    ) -> Result<(), scr::record_data::custom_data::FieldValueFormattingError>
    {
        w.write_all_text("dummy")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DummyCustomTypeNoStringify;

impl CustomData for DummyCustomTypeNoStringify {
    fn type_name(&self) -> Cow<str> {
        "dummy_no_stringify".into()
    }
    fn clone_dyn(&self) -> Box<dyn CustomData> {
        Box::new(self.clone())
    }
    fn format(
        &self,
        _w: &mut dyn scr::utils::text_write::TextWrite,
        _format: &RealizedFormatKey,
    ) -> Result<(), scr::record_data::custom_data::FieldValueFormattingError>
    {
        Err(FieldValueFormattingError::NotSupported)
    }
}

#[test]
fn custom_type_stringify() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_custom(Box::new(DummyCustomType), 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["dummy"]);
    Ok(())
}

#[test]
fn custom_type_that_cannot_stringify() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_custom(Box::new(DummyCustomTypeNoStringify), 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("failed to stringify custom type 'dummy_no_stringify': not supported")
    );
    Ok(())
}
