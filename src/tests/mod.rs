use crate::{
    document::DocumentSource,
    operations::string_sink::{create_op_string_sink, OpStringSink, StringSinkHandle},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo".to_owned()))
        .add_op(create_op_string_sink(ss))
        .run()?;
    //assert!(ss.get().concat() = "foo");
    Ok(())
}
