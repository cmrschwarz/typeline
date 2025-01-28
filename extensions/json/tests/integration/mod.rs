use rstest::rstest;
use std::{
    fmt::Write,
    io::Cursor,
    sync::{Arc, LazyLock},
};
use typeline_core::{
    extension::{Extension, ExtensionRegistry},
    operators::{
        format::create_op_format, utils::readable::MutexedReadableTargetOwner,
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
    utils::test_utils::SliceReader,
};
use typeline_ext_json::{
    jsonl::{create_op_jsonl, JsonlOptions},
    JsonExtension,
};
use typeline_ext_utils::{head::create_op_head, sum::create_op_sum};

pub static JSON_EXTENSION_REGISTRY: LazyLock<Arc<ExtensionRegistry>> =
    LazyLock::new(|| {
        ExtensionRegistry::new([Box::<dyn Extension>::from(Box::new(
            JsonExtension::default(),
        ))])
    });

#[test]
fn first_column_becomes_output() -> Result<(), TypelineError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "\"a\"\n\"b\"\n42\n".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(JSON_EXTENSION_REGISTRY.clone())
        .add_op(create_op_jsonl(
            target.create_target(),
            JsonlOptions::default(),
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "b", "42"]);
    Ok(())
}

#[test]
fn jsonl_with_object() -> Result<(), TypelineError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "{\"a\": 42, \"b\": 12}".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(JSON_EXTENSION_REGISTRY.clone())
        .add_op(create_op_jsonl(
            target.create_target(),
            JsonlOptions::default(),
        ))
        .add_op(create_op_format("{b}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["12"]);
    Ok(())
}

// TODO: maybe change semantics here?
#[test]
fn unobserved_error_in_object() -> Result<(), TypelineError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "{\"a\": 42, \"b\": 2}\n{\"a\": 7, \"b\": \"}".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(JSON_EXTENSION_REGISTRY.clone())
        .add_op(create_op_jsonl(
            target.create_target(),
            JsonlOptions::default(),
        ))
        .add_op(create_op_format("{a}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["42", "7"]);
    Ok(())
}

#[test]
fn error_in_object() -> Result<(), TypelineError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "{\"a\": 42, \"b\": 2}\n{\"a\": 7, \"b\": \"}".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(JSON_EXTENSION_REGISTRY.clone())
        .add_op(create_op_jsonl(
            target.create_target(),
            JsonlOptions::default(),
        ))
        .add_op(create_op_format("{b:??}").unwrap())
        .run_collect_stringified()?;
    // TODO: improve message
    assert_eq!(res, ["2", "(error)\"<custom readable>:1 EOF while parsing at line 1 column 15\\n\\n\\t, \\\"b\\\": \\\"}\\n\\t........^\\n\""]);
    Ok(())
}

#[rstest]
#[case(5, 3)]
#[case(1024, 4096)]
fn multibatch(
    #[case] batch_size: usize,
    #[case] count: usize,
) -> Result<(), TypelineError> {
    let mut input = String::new();

    for i in 0..count {
        input.write_fmt(format_args!("{i}\n")).unwrap();
    }

    let target = MutexedReadableTargetOwner::new(Cursor::new(input));

    let res = ContextBuilder::with_exts(JSON_EXTENSION_REGISTRY.clone())
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_jsonl(
            target.create_target(),
            JsonlOptions::default(),
        ))
        .add_op(create_op_sum())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [(count * (count - 1) / 2) as i64]);
    Ok(())
}

#[test]
fn head() -> Result<(), TypelineError> {
    let mut input = String::new();

    for i in 0..10 {
        input.write_fmt(format_args!("{i}\n")).unwrap();
    }

    let target = MutexedReadableTargetOwner::new(Cursor::new(input));

    let res = ContextBuilder::with_exts(JSON_EXTENSION_REGISTRY.clone())
        .set_batch_size(3)
        .unwrap()
        .add_op(create_op_jsonl(
            target.create_target(),
            JsonlOptions::default(),
        ))
        .add_op(create_op_head(5))
        .run_collect_stringified()?;
    assert_eq!(res, ["0", "1", "2", "3", "4"]);
    Ok(())
}
