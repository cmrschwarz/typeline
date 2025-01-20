use rstest::rstest;
use typeline::{
    operators::{
        compute::create_op_compute, count::create_op_count,
        format::create_op_format, key::create_op_key, literal::create_op_int,
        sequence::create_op_seq,
    },
    options::context_builder::ContextBuilder,
    record_data::{array::Array, field_value::FieldValue},
    typeline_error::TypelineError,
};
use typeline_ext_utils::flatten::create_op_flatten;

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

#[test]
fn compute_array_of_seq() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1)?)
        .add_op(create_op_compute("[42,17,_]")?)
        .run_collect_as::<Array>()?;
    assert_eq!(
        res,
        &[
            Array::Int(vec![42, 17, 0]),
            Array::Int(vec![42, 17, 1]),
            Array::Int(vec![42, 17, 2])
        ]
    );
    Ok(())
}

#[test]
fn compute_array_of_mixed() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1)?)
        .add_op(create_op_compute("[null,_]")?)
        .run_collect_as::<Array>()?;
    assert_eq!(
        res,
        &[
            Array::Mixed(vec![FieldValue::Null, FieldValue::Int(0)]),
            Array::Mixed(vec![FieldValue::Null, FieldValue::Int(1)]),
            Array::Mixed(vec![FieldValue::Null, FieldValue::Int(2)])
        ]
    );
    Ok(())
}

#[test]
fn compute_nested_array_flattened() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1)?)
        .add_op(create_op_compute("[1, [2, _]]")?)
        .add_op(create_op_flatten())
        .run_collect_stringified()?;
    assert_eq!(res, &["1", "[2, 0]", "1", "[2, 1]", "1", "[2, 2]"]);
    Ok(())
}

#[test]
fn multi_batch() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(3)?
        .add_op(create_op_seq(0, 8, 1)?)
        .add_op(create_op_compute("[_]")?)
        .add_op(create_op_flatten())
        .add_op(create_op_count())
        .run_collect_as::<i64>()?;
    assert_eq!(res, &[8]);
    Ok(())
}
