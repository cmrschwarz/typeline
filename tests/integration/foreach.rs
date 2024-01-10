use scr_core::{
    operators::{foreach::create_op_foreach, literal::create_op_str_n},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn basic_foreach() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_str_n("foo", 2))
        //.add_op(create_op_foreach())
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}
