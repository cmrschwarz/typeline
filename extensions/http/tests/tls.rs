mod https_mock_server;

use reqwest::{Certificate, ClientBuilder};
use scr_core::{
    operators::{format::create_op_format, sequence::create_op_seqn},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};
use scr_ext_http::{http::create_op_GET_with_opts, tls_client::TlsSettings};

use crate::https_mock_server::{
    abort_https_test_server, spawn_https_echo_server, EXAMPLE_HOST_CERT,
};

#[tokio::test]
async fn tls_server_sanity_check() {
    let server = spawn_https_echo_server(1234);

    let client = ClientBuilder::new()
        .add_root_certificate(
            Certificate::from_pem(EXAMPLE_HOST_CERT).unwrap(),
        )
        .build()
        .unwrap();

    let request = client
        .get("https://localhost:1234/echo/foobar")
        .build()
        .unwrap();
    let response =
        client.execute(request).await.unwrap().text().await.unwrap();

    assert_eq!(response, "foobar");

    abort_https_test_server(server).await;
}

#[tokio::test]
async fn multi_get_https() -> Result<(), ScrError> {
    let server = spawn_https_echo_server(8080);
    server.is_finished();

    let mut tls_settings = TlsSettings::default();
    tls_settings
        .load_additional_root_certs(&mut std::io::Cursor::new(
            EXAMPLE_HOST_CERT,
        ))
        .unwrap();

    let res = ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_format("https://localhost:8080/echo/{}").unwrap())
        .add_op(create_op_GET_with_opts(tls_settings).unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["1", "2", "3"]);

    abort_https_test_server(server).await;

    Ok(())
}
