use super::https_mock_server::{
    abort_https_test_server, spawn_https_echo_server, HttpsTestServerOpts,
    IpSupport, TEST_CA_CERT,
};
use reqwest::{Certificate, ClientBuilder};
use typeline_core::{
    operators::format::create_op_format,
    options::context_builder::ContextBuilder, typeline_error::TypelineError,
    utils::io::find_free_port,
};
use typeline_ext_http::{
    http::create_op_GET_with_opts, tls_client::TlsSettings,
};
use typeline_ext_utils::sequence::create_op_seqn;

// silence warning generated bv the proc macro. annoying.
// MSRV(1.81): use `#[expect]` instead
#[allow(clippy::needless_return)]
#[tokio::test]
async fn tls_server_sanity_check() {
    for ip_support in
        [IpSupport::Both, IpSupport::IpV6Only, IpSupport::IpV4Only]
    {
        let port = find_free_port().unwrap();
        let server =
            spawn_https_echo_server(HttpsTestServerOpts { port, ip_support })
                .await
                .unwrap();

        let client = ClientBuilder::new()
            .add_root_certificate(Certificate::from_pem(TEST_CA_CERT).unwrap())
            .build()
            .unwrap();

        let request = client
            .get(format!("https://localhost:{port}/echo/foobar"))
            .build()
            .unwrap();
        let response =
            client.execute(request).await.unwrap().text().await.unwrap();

        assert_eq!(response, "foobar");

        abort_https_test_server(server).await;
    }
}

// silence warning generated bv the proc macro. annoying.
// MSRV(1.81): use `#[expect]` instead
#[allow(clippy::needless_return)]
#[tokio::test(flavor = "multi_thread")]
async fn multi_get_https() -> Result<(), TypelineError> {
    for ip_support in
        [IpSupport::Both, IpSupport::IpV4Only, IpSupport::IpV6Only]
    {
        let port = find_free_port().unwrap();
        let server =
            spawn_https_echo_server(HttpsTestServerOpts { port, ip_support })
                .await
                .unwrap();

        let mut tls_settings = TlsSettings::default();
        tls_settings
            .load_additional_root_certs(&mut std::io::Cursor::new(
                TEST_CA_CERT,
            ))
            .unwrap();

        let res = ContextBuilder::without_exts()
            .add_op(create_op_seqn(1, 3, 1).unwrap())
            .add_op(create_op_format(&format!(
                "https://localhost:{port}/echo/{{}}"
            ))?)
            .add_op(create_op_GET_with_opts(tls_settings).unwrap())
            .run_collect_stringified();

        assert_eq!(res?, &["1", "2", "3"]);

        abort_https_test_server(server).await;
    }
    Ok(())
}
