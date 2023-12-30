use scr_core::{
    operators::format::create_op_format,
    options::context_builder::ContextBuilder, scr_error::ScrError,
};
use scr_ext_http::http::create_op_GET;

#[test]
fn multi_get() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .push_str("MQ==", 1)
        .push_str("Mq==", 1)
        .push_str("Mw==", 1)
        .add_op(create_op_format("https://httpbin.org/base64/{}").unwrap())
        .add_op(create_op_GET())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);
    Ok(())
}
