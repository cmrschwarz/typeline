use crate::{
    document::DocumentSource,
    operations::string_sink::{OpStringSink, StringSinkHandle},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo".to_owned()))
        .add_op(OpStringSink::new(ss.clone()))
        .run()?;
    assert!(ss.get().as_str() == "foo");
    Ok(())
}
