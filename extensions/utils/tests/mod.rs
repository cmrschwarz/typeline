use rstest::rstest;
use scr::{
    operators::sequence::create_op_enum,
    options::session_setup::ScrSetupOptions, record_data::array::Array,
    CliOptionsWithDefaultExtensions,
};
use scr_core::{
    operators::{
        literal::create_op_v,
        select::create_op_select,
        sequence::create_op_seqn,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    record_data::field_value::FieldValue,
    scr_error::ScrError,
};
use scr_ext_utils::{
    explode::create_op_explode, flatten::create_op_flatten,
    head::create_op_head, primes::create_op_primes, tail::create_op_tail_add,
};

#[test]
fn primes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op_with_key("p", create_op_primes())
        .add_op(create_op_enum(0, 3, 1).unwrap())
        .add_op(create_op_select("p".into()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "5"]);
    Ok(())
}

#[test]
fn primes_head() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_primes())
        .add_op(create_op_head(3))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "5"]);
    Ok(())
}

#[test]
fn seq_tail_add() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_tail_add(7))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["8", "9", "10"]);
    Ok(())
}

#[test]
fn primes_head_tail_add() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_primes())
        .add_op(create_op_tail_add(3))
        .add_op(create_op_head(3))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["7", "11", "13"]);
    Ok(())
}

#[test]
fn head_tail_cli() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["scr", "primes", "tail=+3", "head=5"],
    )?
    .run_collect_as::<i64>()?;
    assert_eq!(res, [7, 11, 13, 17, 19]);
    Ok(())
}

#[test]
fn subtractive_head_multibatch() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_head(-5))
        .run_collect_as::<i64>()?;
    assert_eq!(res, [1, 2, 3, 4, 5]);
    Ok(())
}

#[rstest]
#[case("{}", "null")]
#[case("[]", "[]")]
#[case("[{}]", "[{}]")]
fn explode_output_col(
    #[case] input: &str,
    #[case] output: &str,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_v(input).unwrap())
        .add_op(create_op_explode())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), [output]);
    Ok(())
}

#[test]
fn explode_into_select() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_v("{'foo': 3}").unwrap())
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
    ContextBuilder::without_exts()
        .add_op(create_op_v("[1,2,3]").unwrap())
        .add_op(create_op_flatten())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    Ok(())
}

#[test]
fn object_flatten() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_v("{a: 3, b: '5'}").unwrap())
        .add_op(create_op_flatten())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        [r#"["a", 3]"#, r#"["b", "5"]"#]
    );
    Ok(())
}

#[test]
fn flatten_duped_objects() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_v("{a:3}").unwrap())
        .add_op(create_op_flatten())
        .run_collect()?;
    assert_eq!(
        res,
        std::iter::repeat(FieldValue::Array(Array::Mixed(
            [FieldValue::Text("a".to_string()), FieldValue::Int(3)].into()
        )))
        .take(3)
        .collect::<Vec<_>>()
    );
    Ok(())
}
