use scr_core::{
    operators::{
        literal::create_op_str,
        sequence::create_op_seqn,
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

#[test]
fn batched_aggregate() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .add_op_aggregate([
            create_op_seqn(1, 10, 1).unwrap(),
            create_op_seqn(11, 15, 1).unwrap(),
        ])
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        (1..16).map(|i| i.to_string()).collect::<Vec<String>>()
    );
    Ok(())
}
