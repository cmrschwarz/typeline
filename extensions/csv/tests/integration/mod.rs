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
use std::sync::{Arc, LazyLock};

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
        .add_op(create_op_csv(target.create_target(), false))
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
        .add_op(create_op_csv(target.create_target(), false))
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
        .add_op(create_op_csv(target.create_target(), false))
        .add_op(create_op_format("{1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["b\"b\"", "null", "b\"y\""]);
    Ok(())
}

#[test]
fn last_row_filled_up_with_nulls() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "1,\r\na,b\nx".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false))
        .add_op(create_op_format("{1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"\"", "b\"b\"", "null"]);
    Ok(())
}

#[test]
fn csv_parses_integers() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "1,2,3\r\na,b,c\nx".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false))
        .add_op(create_op_format("{1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "b\"b\"", "null"]);
    Ok(())
}
