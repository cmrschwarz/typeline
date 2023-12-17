use rstest::rstest;
use scr_core::{
    operators::{
        explode::create_op_explode,
        flatten::create_op_flatten,
        literal::create_op_v,
        select::create_op_select,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[rstest]
#[case("{}", "null")]
#[case("[]", "[]")]
#[case("[{}]", "[{}]")]
fn explode_output_col(
    #[case] input: &str,
    #[case] output: &str,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_v(input, 1).unwrap())
        .add_op(create_op_explode())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), [output]);
    Ok(())
}

#[test]
fn explode_into_select() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_v("{'foo': 3}", 1).unwrap())
        .add_op(create_op_explode())
        .add_op(create_op_select("foo".into()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["3"]);
    Ok(())
}

#[test]
fn flatten() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_v("[1,2,3]", 1).unwrap())
        .add_op(create_op_flatten())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    Ok(())
}

#[test]
fn object_flatten() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_v("{a: 3, b: '5'}", 1).unwrap())
        .add_op(create_op_flatten())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        [r#"["a", 3]"#, r#"["b", "5"]"#]
    );
    Ok(())
}
