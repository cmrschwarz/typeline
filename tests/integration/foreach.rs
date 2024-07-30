use scr::operators::{chunks::create_op_chunks, sequence::create_op_seq};
use scr_core::{
    operators::{
        foreach::create_op_foreach, join::create_op_join,
        literal::create_op_str_n, sequence::create_op_seqn,
    },
    options::context_builder::ContextBuilder,
    record_data::field_value::FieldValue,
    scr_error::ScrError,
};
use scr_ext_utils::{dup::create_op_dup, sum::create_op_sum};

#[test]
fn empty_foreach() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str_n("foo", 2))
        .add_op(create_op_foreach([]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}

#[test]
fn foreach_sum() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach([create_op_sum()]))
        .run_collect()?;
    assert_eq!(
        res,
        &[FieldValue::Int(1), FieldValue::Int(2), FieldValue::Int(3)]
    );
    Ok(())
}

#[test]
fn foreach_sum_nested() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_seqn(1, 3, 1).unwrap(),
            create_op_sum(),
        ]))
        .run_collect()?;
    assert_eq!(
        res,
        &[FieldValue::Int(6), FieldValue::Int(6), FieldValue::Int(6)]
    );
    Ok(())
}

#[test]
fn foreach_dup_sum() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach([create_op_dup(2), create_op_sum()]))
        .run_collect()?;
    assert_eq!(
        res,
        &[FieldValue::Int(2), FieldValue::Int(4), FieldValue::Int(6)]
    );
    Ok(())
}

#[test]
fn foreach_dup_join() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_dup(2),
            create_op_join(None, None, false),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, &["11", "22", "33"]);
    Ok(())
}

#[test]
fn foreach_seq_seq() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_seq(0, 3, 1).unwrap(),
            create_op_sum(),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, &["3", "3", "3"]);
    Ok(())
}

#[test]
fn chunks() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 10, 1).unwrap())
        .add_op(create_op_chunks(3, [create_op_sum()]))
        .run_collect_stringified()?;
    assert_eq!(res, &["3", "12", "21", "9"]);
    Ok(())
}
