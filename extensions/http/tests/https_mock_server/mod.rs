use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{
    body::{Body, Bytes, Incoming},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

// can be generated using
// `openssl genrsa 2048 > example_host.key`
pub const EXAMPLE_HOST_KEY: &[u8] = include_bytes!("example_host.key");

#[rustfmt::skip]
// can be generated using
// `openssl req -new -x509 -subj "/CN=localhost" -key example_host.key -out example_host.crt`
pub const EXAMPLE_HOST_CERT: &[u8] = include_bytes!("example_host.crt");

pub async fn echo_handler(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    const PREFIX: &str = "/echo/";
    let mut response = Response::new(Full::default());
    let path = req.uri().path();
    if !path.starts_with(PREFIX) {
        *response.status_mut() = StatusCode::NOT_FOUND;
        return Ok(response);
    }
    *response.body_mut() =
        Full::from(path[PREFIX.len()..].as_bytes().to_owned());
    Ok(response)
}

pub async fn run_https_test_server<
    E: std::error::Error + Send + Sync + 'static,
    D: Send + 'static,
    B: Body<Error = E, Data = D> + Send + 'static,
    R: Future<Output = Result<Response<B>, hyper::Error>> + Send + 'static,
    F: Fn(Request<Incoming>) -> R + Send + Copy + 'static,
>(
    port: u16,
    request_handler: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);

    let mut certs_file = std::io::Cursor::new(EXAMPLE_HOST_CERT);
    let certs = rustls_pemfile::certs(&mut certs_file)
        .collect::<std::io::Result<Vec<_>>>()?;

    let mut key_file = std::io::Cursor::new(EXAMPLE_HOST_KEY);
    let key =
        rustls_pemfile::private_key(&mut key_file).map(|key| key.unwrap())?;

    let incoming = TcpListener::bind(&addr).await?;

    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    server_config.alpn_protocols = ["h2" as &str, "http/1.1", "http/1.0"]
        .iter()
        .map(|v| v.as_bytes().to_vec())
        .collect();

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let service = service_fn(request_handler);

    loop {
        let (tcp_stream, _remote_addr) = incoming.accept().await?;
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(tcp_stream).await {
                Ok(tls_stream) => tls_stream,
                Err(err) => {
                    panic!("tls handshake failed: {err:#}");
                }
            };
            // Aborting the server might raise an error, so we ignore it.
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(
                    TokioIo::new(tls_stream),
                    service,
                )
                .await;
        });
    }
}

pub fn spawn_https_echo_server(port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_https_test_server(port, echo_handler).await.unwrap();
    })
}

pub async fn abort_https_test_server(server: tokio::task::JoinHandle<()>) {
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
