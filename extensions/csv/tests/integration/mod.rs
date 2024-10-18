use rstest::rstest;
use scr_core::{
    extension::{Extension, ExtensionRegistry},
    operators::{
        format::create_op_format, utils::readable::MutexedReadableTargetOwner,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::test_utils::SliceReader,
};
use scr_ext_csv::{csv::create_op_csv, CsvExtension};
use scr_ext_utils::{head::create_op_head, sum::create_op_sum};
use std::{
    fmt::Write,
    io::Cursor,
    sync::{Arc, LazyLock},
};

pub static CSV_EXTENSION_REGISTRY: LazyLock<Arc<ExtensionRegistry>> =
    LazyLock::new(|| {
        ExtensionRegistry::new([Box::<dyn Extension>::from(Box::new(
            CsvExtension::default(),
        ))])
    });

#[test]
fn first_column_becomes_output() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "a,b,c\n1,2\r\nx,y,z,w".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "1", "x"]);
    Ok(())
}

#[test]
fn end_correctly_truncates_first_column() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "a,b,c\nb\nxyz".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "b", "xyz"]);
    Ok(())
}

#[test]
fn access_second_field() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "a,b,c\r\nb\nx,y,".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_format("{_1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"b\"", "null", "\"y\""]);
    Ok(())
}

#[test]
fn last_row_filled_up_with_nulls() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "1,\r\na,b\nx".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_format("{_1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"\"", "\"b\"", "null"]);
    Ok(())
}

#[test]
fn csv_parses_integers() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "1,2,3\r\na,b,c\nx".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_format("{_1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "\"b\"", "null"]);
    Ok(())
}

#[rstest]
#[case(5, 3)]
#[case(1024, 4096)]
fn multibatch(
    #[case] batch_size: usize,
    #[case] count: usize,
) -> Result<(), ScrError> {
    let mut input = String::new();

    for i in 0..count {
        input.write_fmt(format_args!("{i},\n")).unwrap();
    }

    let target = MutexedReadableTargetOwner::new(Cursor::new(input));

    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_sum())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [(count * (count - 1) / 2) as i64]);
    Ok(())
}

#[test]
fn head() -> Result<(), ScrError> {
    let mut input = String::new();

    for i in 0..10 {
        input.write_fmt(format_args!("{i},\n")).unwrap();
    }

    let target = MutexedReadableTargetOwner::new(Cursor::new(input));

    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .set_batch_size(3)
        .unwrap()
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_head(5))
        .run_collect_stringified()?;
    assert_eq!(res, ["0", "1", "2", "3", "4"]);
    Ok(())
}
