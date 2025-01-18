use rstest::rstest;
use typeline::{
    operators::{
        compute::create_op_compute, format::create_op_format,
        key::create_op_key, literal::create_op_int, sequence::create_op_seq,
    },
    options::context_builder::ContextBuilder,
    record_data::array::Array,
    typeline_error::TypelineError,
};

#[test]
fn compute_add() -> Result<(), TypelineError> {
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
fn seq_non_stringified() -> Result<(), TypelineError> {
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
fn seq_batched() -> Result<(), TypelineError> {
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
fn cast_to_int() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 7, 1)?)
        .add_op(create_op_format("{}")?)
        .add_op(create_op_compute("int(_)")?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[0, 1, 2, 3, 4, 5, 6]);
    Ok(())
}

#[rstest]
#[case("2**10+1", 1025)]
#[case("2**10+1*2", 1026)]
#[case("2**(10+1)*2", 4096)]
fn precedence(
    #[case] expr: &str,
    #[case] expected: i64,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_compute(expr)?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[expected]);
    Ok(())
}

#[rstest]
#[case("float(3)/2", 1.5)]
#[case("float(true)", 1.0)]
#[case("float(false)", 0.0)]
fn float_cast(
    #[case] expr: &str,
    #[case] expected: f64,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_compute(expr)?)
        .run_collect_as::<f64>()?;
    assert_eq!(res, &[expected]);
    Ok(())
}

#[rstest]
#[case("int(4.2)", 4)]
#[case("int(true)", 1)]
fn int_cast(
    #[case] expr: &str,
    #[case] expected: i64,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_compute(expr)?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[expected]);
    Ok(())
}

#[rstest]
#[case("true", true)]
#[case("false", false)]
fn compute_bools(
    #[case] expr: &str,
    #[case] expected: bool,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_compute(expr)?)
        .run_collect_as::<bool>()?;
    assert_eq!(res, &[expected]);
    Ok(())
}

#[rstest]
#[case("[1,2,3]", Array::Int(vec![1, 2, 3]))]
fn compute_array(
    #[case] expr: &str,
    #[case] expected: Array,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_compute(expr)?)
        .run_collect_as::<Array>()?;
    assert_eq!(res, &[expected]);
    Ok(())
}
