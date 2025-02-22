use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
    str,
    sync::Arc,
};

use pki_types::UnixTime;
use rustls::{
    client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    },
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    DigitallySignedStruct, RootCertStore,
};

#[derive(Debug)]
pub struct ClientAuthSettings {
    pub key: PrivateKeyDer<'static>,
    pub cert_chain: Vec<CertificateDer<'static>>,
}

#[derive(Debug, Default)]
pub struct TlsSettings {
    // overrides the default TLS version list
    pub tls_protocol_versions: Vec<String>,

    // overrides the default cipher suite list
    pub cipher_suite: Vec<String>,

    // use the ALPN extension to offer protocol(s). empty means no ALPN
    pub alpn_protocol_list: Vec<String>,

    // Limit outgoing messages to M bytes
    pub flag_max_frag_size: Option<usize>,

    // do not accept the default list of root certificates
    // hardcoded into webpki_roots::TLS_SERVER_ROOTS
    pub no_webpki_root_certs: bool,

    // additional root certificates to accept
    pub additional_root_certs: Vec<CertificateDer<'static>>,

    // disable session ticket support
    pub no_tickets: bool,

    // disable server name indication support
    pub no_sni: bool,

    // prevent InvalidCertificateErrors (this is obviously insecure)
    pub no_cert_verification: bool,

    pub client_auth: Option<ClientAuthSettings>,
}

impl TlsSettings {
    /// load certificates from a stream in the PEM format
    pub fn load_additional_root_certs(
        &mut self,
        reader: &mut impl BufRead,
    ) -> Result<(), std::io::Error> {
        load_ca_certs(reader, &mut self.additional_root_certs)
    }

    /// load client auth key and cert chain from from streams in the PEM format
    pub fn load_client_auth(
        &mut self,
        key_reader: &mut impl BufRead,
        cert_chain_reader: &mut impl BufRead,
    ) -> Result<(), std::io::Error> {
        let mut client_auth = ClientAuthSettings {
            key: load_private_key(key_reader)?,
            cert_chain: Vec::default(),
        };
        load_ca_certs(cert_chain_reader, &mut client_auth.cert_chain)?;
        self.client_auth = Some(client_auth);
        Ok(())
    }
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
fn load_tls_procol_versions<'a>(
    versions: impl IntoIterator<Item = &'a str>,
    target: &mut Vec<&'static rustls::SupportedProtocolVersion>,
) -> Result<(), rustls::Error> {
    for vname in versions {
        if let Some(v) = rustls::ALL_VERSIONS.iter().find(|v| {
            v.version
                .as_str()
                .unwrap()
                .eq_ignore_ascii_case(vname.trim())
        }) {
            target.push(*v);
        } else {
            return Err(rustls::Error::General(format!(
                "invalid TLS procotol version '{vname}'"
            )));
        }
    }

    Ok(())
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
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
        // verify_tls12_signature(
        //    message,
        //    cert,
        //    dss,
        //    &rustls::crypto::ring::default_provider()
        //        .signature_verification_algorithms,
        //)
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
        // verify_tls13_signature(
        //    message,
        //    cert,
        //    dss,
        //    &rustls::crypto::ring::default_provider()
        //        .signature_verification_algorithms,
        //)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn load_private_key(
    reader: &mut impl BufRead,
) -> Result<PrivateKeyDer<'static>, std::io::Error> {
    loop {
        match rustls_pemfile::read_one(reader)? {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return Ok(key.into()),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return Ok(key.into()),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return Ok(key.into()),
            None => break,
            _ => {}
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "no private keys found (encrypted keys not supported)",
    ))
}

/// load certificates from a stream in the PEM format
pub fn load_ca_certs(
    reader: &mut impl BufRead,
    target: &mut Vec<CertificateDer<'static>>,
) -> Result<(), std::io::Error> {
    for cert in rustls_pemfile::certs(reader) {
        target.push(cert?);
    }
    Ok(())
}

pub fn load_ca_certs_from_file(
    path: impl AsRef<Path>,
    target: &mut Vec<CertificateDer<'static>>,
) -> Result<(), std::io::Error> {
    let mut reader = BufReader::new(fs::File::open(path)?);
    load_ca_certs(&mut reader, target)
}

pub fn make_config(
    settings: TlsSettings,
) -> Result<Arc<rustls::ClientConfig>, rustls::Error> {
    let mut root_store = RootCertStore::empty();

    root_store.add_parsable_certificates(settings.additional_root_certs);

    if !settings.no_webpki_root_certs {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let suites = if !settings.cipher_suite.is_empty() {
        lookup_suites(&settings.cipher_suite)
    } else {
        rustls::crypto::ring::ALL_CIPHER_SUITES.to_vec()
    };

    let versions = if !settings.tls_protocol_versions.is_empty() {
        let mut versions = Vec::default();
        load_tls_procol_versions(
            settings.tls_protocol_versions.iter().map(|v| v.as_str()),
            &mut versions,
        )?;
        versions
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
    .with_protocol_versions(&versions)?
    .with_root_certificates(root_store);

    let mut config = if let Some(client_auth) = settings.client_auth {
        config
            .with_client_auth_cert(client_auth.cert_chain, client_auth.key)?
    } else {
        config.with_no_client_auth()
    };

    config.key_log = Arc::new(rustls::KeyLogFile::new());

    if settings.no_tickets {
        config.resumption = config
            .resumption
            .tls12_resumption(rustls::client::Tls12Resumption::SessionIdOnly);
    }

    if settings.no_sni {
        config.enable_sni = false;
    }

    config.alpn_protocols = settings
        .alpn_protocol_list
        .into_iter()
        .map(|proto| proto.into_bytes())
        .collect();
    config.max_fragment_size = settings.flag_max_frag_size;

    if settings.no_cert_verification {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertificateVerification {}));
    }

    Ok(Arc::new(config))
}
