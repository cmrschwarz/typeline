use scr_core::{
    operators::{
        format::create_op_format, regex::create_op_regex,
        sequence::create_op_seq,
    },
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
        .set_batch_size(2) //TODO
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

#[test]
fn get_delay() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op_with_label(create_op_seq(0, 6, 2).unwrap(), "a")
        .add_op(create_op_format("https://httpbin.org/delay/0.{}").unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_regex(".").unwrap())
        .add_op(create_op_format("{}{a}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["{0", "{2", "{4"]);
    Ok(())
}
