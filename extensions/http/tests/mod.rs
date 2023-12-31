use scr_core::{
    operators::{format::create_op_format, regex::create_op_regex},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};
use scr_ext_http::http::create_op_GET;

#[test]
fn multi_get_http() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .push_str("MQ==", 1)
        .push_str("Mq==", 1)
        .push_str("Mw==", 1)
        .add_op(create_op_format("http://httpbin.org/base64/{}").unwrap())
        .add_op(create_op_GET())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);
    Ok(())
}

#[test]
fn multi_get_http_regex() -> Result<(), ScrError> {
    // regex used to have an issue where it did not
    // keep stream values alive for long enough
    // this is sort of a regression test against this
    let res = ContextBuilder::default()
        // .set_batch_size(2) //TODO
        .push_str("MQ==", 1)
        .push_str("Mq==", 1)
        .push_str("Mw==", 1)
        .add_op(create_op_format("http://httpbin.org/base64/{}").unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_regex(".*").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);
    Ok(())
}

#[test]
fn multi_get_https() -> Result<(), ScrError> {
    //TODO: this test is very flaky. do we have to close the connection
    // ourselves?
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
