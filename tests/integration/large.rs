use typeline::{
    operators::regex::create_op_regex,
    options::context_builder::ContextBuilder, typeline_error::TypelineError,
};
use typeline_ext_utils::{count::create_op_count, sequence::create_op_seq};

#[test]
fn multibatch_count() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 10000, 1)?)
        .set_batch_size(100)?
        .add_op(create_op_regex("12")?)
        .add_op(create_op_count())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [299]);
    Ok(())
}
