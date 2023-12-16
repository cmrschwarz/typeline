use std::{fs, io::BufReader, str, sync::Arc};

use pki_types::UnixTime;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerifier},
    crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    DigitallySignedStruct, RootCertStore,
};

#[derive(Debug, Default)]
pub struct TlsSettings {
    // overrides the default TLS version list
    tls_protocol_versions: Vec<String>,

    // overrides the default cipher suite list
    cipher_suite: Vec<String>,

    // use the ALPN extension to offer protocol(s). empty means no ALPN
    alpn_protocol_list: Vec<String>,

    // Limit outgoing messages to M bytes
    flag_max_frag_size: Option<usize>,

    // Read root certificates from the supplied file path
    flag_cafile: Option<String>,

    // disable session ticket support
    no_tickets: bool,

    // disable server name indication support
    no_sni: bool,

    // prevent InvalidCertificateErrors (this is obviously insecure)
    disable_cert_verification: bool,

    client_auth_key: Option<String>,
    client_auth_certs: Option<String>,
}

/// Find a ciphersuite with the given name
fn find_suite(name: &str) -> Option<rustls::SupportedCipherSuite> {
    for suite in rustls::crypto::ring::ALL_CIPHER_SUITES {
        if name
            .trim()
            .eq_ignore_ascii_case(suite.suite().as_str().unwrap())
        {
            return Some(*suite);
        }
    }
    None
}

/// Make a vector of ciphersuites named in `suites`
fn lookup_suites(suites: &[String]) -> Vec<rustls::SupportedCipherSuite> {
    let mut out = Vec::new();

    for csname in suites {
        match find_suite(csname) {
            Some(s) => out.push(s),
            None => panic!("cannot look up ciphersuite '{}'", csname),
        }
    }

    out
}

/// Make a vector of protocol versions named in `versions`
fn lookup_versions(
    versions: &[String],
) -> Vec<&'static rustls::SupportedProtocolVersion> {
    let mut out = Vec::new();

    for vname in versions {
        if let Some(v) = rustls::ALL_VERSIONS.iter().find(|v| {
            v.version
                .as_str()
                .unwrap()
                .eq_ignore_ascii_case(vname.trim())
        }) {
            out.push(*v);
        } else {
            todo!("report error");
        }
    }

    out
}

fn load_certs(filename: &str) -> Vec<CertificateDer<'static>> {
    let certfile =
        fs::File::open(filename).expect("cannot open certificate file");
    let mut reader = BufReader::new(certfile);
    rustls_pemfile::certs(&mut reader)
        .map(|result| result.unwrap())
        .collect()
}

fn load_private_key(filename: &str) -> PrivateKeyDer<'static> {
    let keyfile =
        fs::File::open(filename).expect("cannot open private key file");
    let mut reader = BufReader::new(keyfile);

    loop {
        match rustls_pemfile::read_one(&mut reader)
            .expect("cannot parse private key .pem file")
        {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return key.into(),
            None => break,
            _ => {}
        }
    }

    panic!(
        "no keys found in {:?} (encrypted keys not supported)",
        filename
    );
}

#[derive(Debug)]
pub struct NoCertificateVerification {}
impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error>
    {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider()
                .signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider()
                .signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn make_config(args: &TlsSettings) -> Arc<rustls::ClientConfig> {
    let mut root_store = RootCertStore::empty();

    if args.flag_cafile.is_some() {
        let cafile = args.flag_cafile.as_ref().unwrap();

        let certfile = fs::File::open(cafile).expect("Cannot open CA file");
        let mut reader = BufReader::new(certfile);
        root_store.add_parsable_certificates(
            rustls_pemfile::certs(&mut reader).map(|result| result.unwrap()),
        );
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let suites = if !args.cipher_suite.is_empty() {
        lookup_suites(&args.cipher_suite)
    } else {
        rustls::crypto::ring::DEFAULT_CIPHER_SUITES.to_vec()
    };

    let versions = if !args.tls_protocol_versions.is_empty() {
        lookup_versions(&args.tls_protocol_versions)
    } else {
        rustls::DEFAULT_VERSIONS.to_vec()
    };

    let config = rustls::ClientConfig::builder_with_provider(
        CryptoProvider {
            cipher_suites: suites,
            ..rustls::crypto::ring::default_provider()
        }
        .into(),
    )
    .with_protocol_versions(&versions)
    .expect("inconsistent cipher-suite/versions selected")
    .with_root_certificates(root_store);

    let mut config = match (&args.client_auth_key, &args.client_auth_certs) {
        (Some(key_file), Some(certs_file)) => {
            let certs = load_certs(certs_file);
            let key = load_private_key(key_file);
            config
                .with_client_auth_cert(certs, key)
                .expect("invalid client auth certs/key")
        }
        (None, None) => config.with_no_client_auth(),
        (_, _) => {
            panic!("must provide --auth-certs and --auth-key together");
        }
    };

    config.key_log = Arc::new(rustls::KeyLogFile::new());

    if args.no_tickets {
        config.resumption = config
            .resumption
            .tls12_resumption(rustls::client::Tls12Resumption::SessionIdOnly);
    }

    if args.no_sni {
        config.enable_sni = false;
    }

    config.alpn_protocols = args
        .alpn_protocol_list
        .iter()
        .map(|proto| proto.as_bytes().to_vec())
        .collect();
    config.max_fragment_size = args.flag_max_frag_size;

    if args.disable_cert_verification {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertificateVerification {}));
    }

    Arc::new(config)
}
