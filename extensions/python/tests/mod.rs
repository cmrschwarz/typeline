use num::BigRational;
use scr_core::{
    cli::call_expr::Span,
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        file_reader::create_op_file_reader_custom,
        foreach::create_op_foreach,
        forkcat::create_op_forkcat,
        join::create_op_join,
        literal::create_op_int,
        operator::OperatorId,
        regex::{create_op_regex_with_opts, RegexOptions},
        sequence::create_op_seqn,
    },
    options::context_builder::ContextBuilder,
    record_data::{
        array::Array,
        field_value::{FieldValue, Object},
    },
    scr_error::ScrError,
    utils::{indexing_type::IndexingType, test_utils::SliceReader},
};
use scr_ext_python::{create_op_to_int, py::create_op_py};
use scr_ext_utils::{
    head::create_op_head, string_utils::create_op_lines, sum::create_op_sum,
    tail::create_op_tail,
};

#[test]
fn python_basic() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("2**10").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["1024"]);
    Ok(())
}

#[test]
fn python_last_statement_expr() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("a=42; a").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["42"]);
    Ok(())
}

#[test]
fn python_multiline() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("a=42\na").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["42"]);
    Ok(())
}

#[test]
fn python_multiline_indentation_error() {
    assert_eq!(
        create_op_py("a=42\n a").err().expect("this shouldn't parse"),
        OperatorCreationError::new(
            "Python failed to parse: IndentationError: unexpected indent (<cmd>, line 2)",
            Span::Generated
        )
    );
}

#[test]
fn python_input_vars() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("foo", create_op_int(7))
        .add_op(create_op_py("foo * 2").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["14"]);
    Ok(())
}

#[test]
fn python_undefined_var() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("foo").unwrap())
        .run_collect_as::<OperatorApplicationError>()?;
    assert_eq!(
        res,
        [OperatorApplicationError::new(
            "Python: NameError: name 'foo' is not defined",
            OperatorId::zero()
        )]
    );
    Ok(())
}

#[test]
fn python_multi_invocation() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("foo", create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_py("foo * 2").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "4", "6"]);
    Ok(())
}

#[test]
fn python_array() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("[1, 2, \"foo\"]").unwrap())
        .run_collect()?;
    assert_eq!(
        res,
        [FieldValue::Array(Array::Mixed(vec![
            FieldValue::Int(1),
            FieldValue::Int(2),
            FieldValue::Text("foo".to_string())
        ]))]
    );
    Ok(())
}

#[test]
fn python_dict() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("{'asdf': 3}").unwrap())
        .run_collect()?;
    assert_eq!(
        res,
        [FieldValue::Object(Box::new(Object::from_iter([(
            "asdf".to_string(),
            FieldValue::Int(3)
        )])))]
    );
    Ok(())
}

#[test]
fn python_rational() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(
            create_op_py("import fractions; fractions.Fraction(1, 3)")
                .unwrap(),
        )
        .run_collect()?;
    assert_eq!(
        res,
        [FieldValue::BigRational(Box::new(BigRational::new_raw(
            1.into(),
            3.into()
        )))]
    );
    Ok(())
}

#[test]
fn aoc2023_day1_part1() -> Result<(), ScrError> {
    let input = r#"
1abc2
pqr3stu8vwx
a1b2c3d4e5f
treb7uchet
    "#;
    let res = ContextBuilder::without_exts()
        .add_ops([
            create_op_file_reader_custom(
                Box::new(SliceReader::new(input.trim().as_bytes())),
                1,
            ),
            create_op_lines(),
            create_op_foreach([
                create_op_regex_with_opts(
                    "\\d",
                    RegexOptions {
                        multimatch: true,
                        ..Default::default()
                    },
                )?,
                create_op_forkcat([[create_op_head(1)], [create_op_tail(1)]]),
                create_op_join(None, None, false),
                create_op_to_int()?,
            ]),
            create_op_sum(),
        ])
        .run_collect_as::<i64>()?;
    assert_eq!(res, [142]);
    Ok(())
}
