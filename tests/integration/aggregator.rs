use scr_core::{
    operators::{
        literal::create_op_str,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn simple_aggregate() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_aggregate([create_op_str("foo", 1), create_op_str("bar", 1)])
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}
