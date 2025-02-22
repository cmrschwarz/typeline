#![cfg(not(miri))] // miri does not support FFI, which we need for pyo3

use indexland::indexing_type::IndexingType;
use num::BigRational;
use typeline_core::{
    cli::call_expr::Span,
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        literal::create_op_int,
        operator::OperatorId,
    },
    options::context_builder::ContextBuilder,
    record_data::{array::Array, field_value::FieldValue, object::Object},
    typeline_error::TypelineError,
};
use typeline_ext_python::py::create_op_py;
use typeline_ext_utils::sequence::create_op_seqn;

#[test]
fn python_basic() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("2**10").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["1024"]);
    Ok(())
}

#[test]
fn python_last_statement_expr() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("a=42; a").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["42"]);
    Ok(())
}

#[test]
fn python_multiline() -> Result<(), TypelineError> {
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
fn python_input_vars() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("foo", create_op_int(7))
        .add_op(create_op_py("foo * 2").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["14"]);
    Ok(())
}

#[test]
fn python_undefined_var() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_py("foo").unwrap())
        .run_collect_as::<OperatorApplicationError>()?;
    assert_eq!(
        res,
        [OperatorApplicationError::new(
            "Python: NameError: name 'foo' is not defined",
            OperatorId::ZERO
        )]
    );
    Ok(())
}

#[test]
fn python_multi_invocation() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("foo", create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_py("foo * 2").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "4", "6"]);
    Ok(())
}

#[test]
fn python_array() -> Result<(), TypelineError> {
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
fn python_dict() -> Result<(), TypelineError> {
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
fn python_rational() -> Result<(), TypelineError> {
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
