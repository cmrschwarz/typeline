use crate::{
    document::DocumentSource,
    operations::string_sink::{OpStringSink, StringSinkHandle},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ssh = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo".to_owned()))
        .add_op(OpStringSink::new(ssh.clone()))
        .run()?;
    assert!(ssh.get().as_str() == "foo");
    Ok(())
}
