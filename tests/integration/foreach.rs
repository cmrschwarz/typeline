use scr_core::{
    operators::{
        foreach::create_op_foreach, literal::create_op_str_n,
        nop::create_op_nop, sequence::create_op_seqn,
    },
    options::context_builder::ContextBuilder,
    record_data::field_value::FieldValue,
    scr_error::ScrError,
};
use scr_ext_utils::sum::create_op_sum;

#[test]
fn basic_foreach() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_str_n("foo", 2))
        .add_op(create_op_foreach())
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}

#[test]
fn sum_foreach() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach())
        .add_op(create_op_nop())
        // .add_op(create_op_sum())
        .run_collect()?;
    assert_eq!(
        res,
        &[FieldValue::Int(1), FieldValue::Int(2), FieldValue::Int(3)]
    );
    Ok(())
}
