#![cfg(test)]

use scr::{
    document::DocumentSource,
    operations::{
        regex::{create_op_regex, create_op_regex_lines},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo".to_owned()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn multi_doc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo".to_owned()))
        .add_doc(DocumentSource::String("bar".to_owned()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().as_slice(), ["foo", "bar"]);
    Ok(())
}

#[test]
fn lines_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo\nbar\nbaz\n".to_owned()))
        .add_op(create_op_regex_lines())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().as_slice(), ["foo", "bar", "baz"]);
    Ok(())
}

#[test]
fn regex_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_doc(DocumentSource::String("foo\nbar\nbaz\n".to_owned()))
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex(".*[^r]$", Default::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().as_slice(), ["foo", "baz"]);
    Ok(())
}
