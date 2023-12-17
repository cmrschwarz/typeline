use std::borrow::Cow;

use scr_core::{
    operators::{
        format::RealizedFormatKey,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    record_data::custom_data::CustomDataSafe,
    scr_error::ScrError,
};

#[derive(Debug, Clone)]
struct DummyCustomType;

impl CustomDataSafe for DummyCustomType {
    fn type_name(&self) -> Cow<str> {
        "dummy".into()
    }
    fn stringified_len(&self, _fmt: &RealizedFormatKey) -> Option<usize> {
        Some(self.type_name().len())
    }
    fn stringify_utf8(
        &self,
        w: &mut dyn std::fmt::Write,
        _fmt: &RealizedFormatKey,
    ) -> std::fmt::Result {
        w.write_str(&self.type_name())
    }
}

#[derive(Debug, Clone)]
struct DummyCustomTypeNoStringify;

impl CustomDataSafe for DummyCustomTypeNoStringify {
    fn type_name(&self) -> Cow<str> {
        "dummy_no_stringify".into()
    }
    fn stringified_len(&self, _fmt: &RealizedFormatKey) -> Option<usize> {
        None
    }
    fn stringify_utf8(
        &self,
        _w: &mut dyn std::fmt::Write,
        _fmt: &RealizedFormatKey,
    ) -> std::fmt::Result {
        unimplemented!()
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
        Some("cannot stringify custom type dummy_no_stringify")
    );
    Ok(())
}
