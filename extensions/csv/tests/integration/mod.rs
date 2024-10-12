use scr_core::{
    extension::{Extension, ExtensionRegistry},
    operators::utils::readable::MutexedReadableTargetOwner,
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
