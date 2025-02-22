use typeline_core::{
    operators::{
        format::create_op_format,
        print::{create_op_print_with_opts, PrintOptions},
        regex::create_op_regex,
        utils::writable::MutexedWriteableTargetOwner,
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
};
use typeline_ext_http::http::create_op_GET;
use typeline_ext_utils::sequence::create_op_seqn;

pub fn setup_mockito_test_server() -> mockito::ServerGuard {
    let mut server = mockito::Server::new();

    server
        .mock("GET", mockito::Matcher::Regex(r"^/echo/.*$".to_string()))
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body_from_request(|req| req.path()[6..].as_bytes().to_owned())
        .create();

    server
        .mock("GET", mockito::Matcher::Regex(r"^/delay/.*$".to_string()))
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body_from_request(|req| {
            let delay = req.path()[7..].parse::<f64>().unwrap();
            std::thread::sleep(std::time::Duration::from_secs_f64(delay));
            format!("ok: {delay}").into_bytes()
        })
        .create();
    server
}

#[test]
fn multi_get_http() -> Result<(), TypelineError> {
    let server = setup_mockito_test_server();
    let fmt = format!("{}/echo/{{}}", server.url());
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);
    Ok(())
}

#[test]
fn multi_get_http_regex() -> Result<(), TypelineError> {
    // regex used to have an issue where it did not
    // keep stream values alive for long enough
    // this is sort of a regression test against this

    let server = setup_mockito_test_server();
    let fmt = format!("{}/echo/{{}}", server.url());

    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_regex(".*").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);
    Ok(())
}

#[test]
fn multi_get_into_print() -> Result<(), TypelineError> {
    let server = setup_mockito_test_server();
    let fmt = format!("{}/echo/{{}}", server.url());
    let target = MutexedWriteableTargetOwner::<Vec<u8>>::default();
    ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_print_with_opts(
            target.create_target(),
            PrintOptions::default(),
        ))
        .run()?;
    assert_eq!(&*target.get(), b"1\n2\n3\n");
    Ok(())
}

#[test]
fn get_delay() -> Result<(), TypelineError> {
    let server = setup_mockito_test_server();
    let fmt = format!("{}/delay/0.0{{}}", server.url());
    let res = ContextBuilder::without_exts()
        .add_op_with_key("a", create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_format("{a}_{}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1_ok: 0.01", "2_ok: 0.02", "3_ok: 0.03"]);
    Ok(())
}

#[test]
fn localhost_not_parsed_as_scheme() -> Result<(), TypelineError> {
    let server = setup_mockito_test_server();
    let fmt = format!(
        "localhost:{}/echo/{{}}",
        server.host_with_port().split(':').nth(1).unwrap()
    );
    let res = ContextBuilder::without_exts()
        .push_str("foo", 1)
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .run_collect_stringified()?;
    assert_eq!(&res, &["foo"]);
    Ok(())
}
