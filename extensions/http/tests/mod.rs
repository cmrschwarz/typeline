use scr_core::{
    operators::{
        format::create_op_format,
        print::create_op_print_with_target,
        regex::create_op_regex,
        sequence::{create_op_seq, create_op_seqn},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::test_utils::DummyWritableTarget,
};
use scr_ext_http::http::create_op_GET;

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
fn multi_get_http() -> Result<(), ScrError> {
    let server = setup_mockito_test_server();
    let fmt = format!("{}/echo/{{}}", server.url());
    let res = ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
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

    let server = setup_mockito_test_server();
    let fmt = format!("{}/echo/{{}}", server.url());

    let res = ContextBuilder::default()
        .set_batch_size(2)
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_regex(".*").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);
    Ok(())
}

#[test]
fn multi_get_into_print() -> Result<(), ScrError> {
    let server = setup_mockito_test_server();
    let fmt = format!("{}/echo/{{}}", server.url());
    let target = DummyWritableTarget::default();
    ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_print_with_target(target.get_target()))
        .run()?;
    assert_eq!(&*target.get(), "1\n2\n3\n");
    Ok(())
}

#[test]
fn get_delay() -> Result<(), ScrError> {
    let server = setup_mockito_test_server();
    let fmt = format!("{}/delay/0.{{}}", server.url());
    let res = ContextBuilder::default()
        .add_op_with_label(create_op_seq(0, 9, 3).unwrap(), "a")
        .add_op(create_op_format(&fmt).unwrap())
        .add_op(create_op_GET())
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_format("{a}_{}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["0_ok: 0", "3_ok: 0.3", "6_ok: 0.6"]);
    Ok(())
}
