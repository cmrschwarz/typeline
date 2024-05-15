use scr_core::{
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        literal::create_op_int,
        sequence::create_op_seqn,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};
use scr_ext_python::py::create_op_py;

#[test]
fn python_basic() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_py("2**10").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["1024"]);
    Ok(())
}

#[test]
fn python_last_statement_expr() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_py("a=42; a").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["42"]);
    Ok(())
}

#[test]
fn python_multiline() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
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
            "Python failed to parse: IndentationError: unexpected indent (<cmd>, line 2)"
            , None
        )
    );
}

#[test]
fn python_input_vars() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op_with_label(create_op_int(7), "foo")
        .add_op(create_op_py("foo * 2").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["14"]);
    Ok(())
}

#[test]
fn python_undefined_var() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_py("foo").unwrap())
        .run_collect_as::<OperatorApplicationError>()?;
    assert_eq!(
        res,
        [OperatorApplicationError::new(
            "python error: NameError: name 'foo' is not defined",
            1
        )]
    );
    Ok(())
}

#[test]
fn python_multi_invocation() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op_with_label(create_op_seqn(1, 3, 1).unwrap(), "foo")
        .add_op(create_op_py("foo * 2").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "4", "6"]);
    Ok(())
}
