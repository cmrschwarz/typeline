use super::https_mock_server::{
    abort_https_test_server, spawn_https_echo_server, HttpsTestServerOpts,
    IpSupport, TEST_CA_CERT,
};
use reqwest::{Certificate, ClientBuilder};
use scr_core::{
    operators::{format::create_op_format, sequence::create_op_seqn},
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};
use scr_ext_http::{http::create_op_GET_with_opts, tls_client::TlsSettings};

#[tokio::test]
async fn tls_server_sanity_check() {
    for ip_support in
        [IpSupport::Both, IpSupport::IpV6Only, IpSupport::IpV4Only]
    {
        let server = spawn_https_echo_server(HttpsTestServerOpts {
            port: 1234,
            ip_support,
        });

        let client = ClientBuilder::new()
            .add_root_certificate(Certificate::from_pem(TEST_CA_CERT).unwrap())
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
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_get_https() -> Result<(), ScrError> {
    for ip_support in
        [IpSupport::Both, IpSupport::IpV4Only, IpSupport::IpV6Only]
    {
        let server = spawn_https_echo_server(HttpsTestServerOpts {
            port: 8080,
            ip_support,
        });

        let mut tls_settings = TlsSettings::default();
        tls_settings
            .load_additional_root_certs(&mut std::io::Cursor::new(
                TEST_CA_CERT,
            ))
            .unwrap();

        let res = ContextBuilder::default()
            .add_op(create_op_seqn(1, 3, 1).unwrap())
            .add_op(
                create_op_format("https://localhost:8080/echo/{}").unwrap(),
            )
            .add_op(create_op_GET_with_opts(tls_settings).unwrap())
            .run_collect_stringified();

        assert_eq!(res?, &["1", "2", "3"]);

        abort_https_test_server(server).await;
    }
    Ok(())
}
