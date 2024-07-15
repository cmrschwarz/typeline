use rstest::rstest;
use scr_core::{
    operators::{
        call_concurrent::create_op_callcc,
        literal::create_op_int,
        regex::create_op_regex,
        sequence::{create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::{ContextualizedScrError, ScrError},
};

#[test]
fn callcc_needs_threads() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    let err_msg =
        "callcc cannot be used with a max thread count of 1, see `h=j`";
    matches!(
        ContextBuilder::without_exts()
            .set_max_thread_count(1)
            .add_op(create_op_seqn(1, 3, 1).unwrap())
            .add_op(create_op_callcc("foo".to_string()))
            .add_label("foo".to_string())
            .add_op(create_op_string_sink(&ss))
            .run(),
        Err(ContextualizedScrError {
            contextualized_message,
            ..
        }) if contextualized_message == err_msg
    );

    Ok(())
}

#[test]
fn basic_callcc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_max_thread_count(2)
        .set_batch_size(2).unwrap()
        .add_op(create_op_seqn(1, 4, 1).unwrap())
        .add_op(create_op_callcc("foo".to_string()))
        .add_label("foo".to_string())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["1", "2", "3", "4"]);
    Ok(())
}

#[test]
fn callcc_after_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_max_thread_count(2)
        .set_batch_size(7).unwrap()
        .add_op(create_op_seqn(1, 30, 1).unwrap())
        .add_op(create_op_regex("7").unwrap())
        .add_op(create_op_callcc("foo".to_string()))
        .add_label("foo".to_string())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["7", "7", "7"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
fn appending_callcc(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_max_thread_count(2)
        .set_batch_size(batch_size).unwrap()
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_callcc("foo".to_string()))
        .add_label("foo".to_string())
        .add_op_appending(create_op_int(2))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2"]);
    Ok(())
}
