use scr::{
    operators::{
        count::create_op_count, regex::create_op_regex,
        sequence::create_op_seq,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn multibatch_count() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 10000, 1)?)
        .set_batch_size(100)?
        .add_op(create_op_regex("12")?)
        .add_op(create_op_count())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [299]);
    Ok(())
}
