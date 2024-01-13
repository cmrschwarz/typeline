use std::{
    future::Future,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

use futures::{stream::FuturesUnordered, StreamExt};

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

// can be generated using ./gen_certs.sh
pub const TEST_HOST_KEY: &[u8] = include_bytes!("host.rsa");
pub const TEST_HOST_CERT: &[u8] = include_bytes!("host.pem");
pub const TEST_CA_CERT: &[u8] = include_bytes!("ca.pem");

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

#[derive(Default)]
pub enum IpSupport {
    #[default]
    Both,
    IpV4Only,
    IpV6Only,
}

pub struct HttpsTestServerOpts {
    pub port: u16,
    pub ip_support: IpSupport,
}

impl Default for HttpsTestServerOpts {
    fn default() -> Self {
        Self {
            port: 8080,
            ip_support: IpSupport::Both,
        }
    }
}

pub async fn run_https_test_server<
    E: std::error::Error + Send + Sync + 'static,
    D: Send + 'static,
    B: Body<Error = E, Data = D> + Send + 'static,
    R: Future<Output = Result<Response<B>, hyper::Error>> + Send + 'static,
    F: Fn(Request<Incoming>) -> R + Send + Copy + 'static,
>(
    request_handler: F,
    opts: HttpsTestServerOpts,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut certs_file = std::io::Cursor::new(TEST_HOST_CERT);
    let certs = rustls_pemfile::certs(&mut certs_file)
        .collect::<std::io::Result<Vec<_>>>()?;

    let mut key_file = std::io::Cursor::new(TEST_HOST_KEY);
    let key =
        rustls_pemfile::private_key(&mut key_file).map(|key| key.unwrap())?;

    let mut listeners = Vec::new();

    if matches!(opts.ip_support, IpSupport::IpV4Only | IpSupport::Both) {
        let addr_v4 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), opts.port);
        listeners.push(TcpListener::bind(&addr_v4).await?);
    }
    if matches!(opts.ip_support, IpSupport::IpV6Only | IpSupport::Both) {
        let addr_v6 = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), opts.port);
        listeners.push(TcpListener::bind(&addr_v6).await?);
    }

    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    server_config.alpn_protocols = ["h2" as &str, "http/1.1", "http/1.0"]
        .iter()
        .map(|v| v.as_bytes().to_vec())
        .collect();

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let service = service_fn(request_handler);

    let mut futures = FuturesUnordered::new();

    let mut prev_listener = None;

    loop {
        for (i, listener) in listeners.iter().enumerate() {
            if Some(i) == prev_listener || prev_listener.is_none() {
                futures.push(async move { (i, listener.accept().await) });
            }
        }
        let (listener_idx, res) = futures.next().await.unwrap();
        let (tcp_stream, _remote_addr) = res?;
        prev_listener = Some(listener_idx);
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(tcp_stream).await {
                Ok(tls_stream) => tls_stream,
                Err(err) => {
                    panic!("tls handshake failed: {err:?}");
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

pub fn spawn_https_echo_server(
    opts: HttpsTestServerOpts,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_https_test_server(echo_handler, opts).await.unwrap();
    })
}

pub async fn abort_https_test_server(server: tokio::task::JoinHandle<()>) {
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
