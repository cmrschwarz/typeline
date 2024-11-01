use scr::{
    operators::{
        compute::create_op_compute, format::create_op_format,
        key::create_op_key, literal::create_op_int, sequence::create_op_seq,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn compute_add() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_int(42, 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_int(27))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_compute("foo+bar").unwrap())
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[69]);
    Ok(())
}

#[test]
fn seq_non_stringified() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(1, 4, 1)?)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_int(10))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_compute("foo+bar")?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[11, 12, 13]);
    Ok(())
}

#[test]
fn seq_batched() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(3)?
        .add_op(create_op_seq(0, 7, 1)?)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_int(10))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_compute("foo+bar")?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[10, 11, 12, 13, 14, 15, 16]);
    Ok(())
}

#[test]
fn cast_to_int() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 7, 1)?)
        .add_op(create_op_format("{}")?)
        .add_op(create_op_compute("int(_)")?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[0, 1, 2, 3, 4, 5, 6]);
    Ok(())
}
