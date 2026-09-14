use crate::pipe::AsyncRw;
use guru_worker_config::TlsHostConfig;
use openssl::ssl::{Ssl, SslAcceptor, SslConnector, SslFiletype, SslMethod};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};

/// Builds a TLS server acceptor from a cert chain + key on disk (parsed once).
pub fn server_acceptor(c: &TlsHostConfig) -> Result<SslAcceptor, crate::BoxError> {
    let mut b = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
    b.set_private_key_file(&c.key, SslFiletype::PEM)?;
    b.set_certificate_chain_file(&c.full_chain)?;
    b.check_private_key()?;
    Ok(b.build())
}

/// Builds a value once per CA path.
fn cached<T: Clone>(
    cache: &Mutex<HashMap<PathBuf, T>>,
    ca: &Path,
    build: impl FnOnce() -> Result<T, crate::BoxError>,
) -> Result<T, crate::BoxError> {
    if let Some(v) = cache.lock().get(ca) {
        return Ok(v.clone());
    }
    let v = build()?;
    Ok(cache.lock().entry(ca.to_path_buf()).or_insert(v).clone())
}

/// TLS connector for dialing relay hops over TLS-over-TCP, verifying peers against
/// `ca` (a PEM bundle) or, without one, the system roots. Built once per CA path.
fn client_connector(ca: Option<&Path>) -> Result<SslConnector, crate::BoxError> {
    static SYSTEM: OnceLock<SslConnector> = OnceLock::new();
    static BY_CA: LazyLock<Mutex<HashMap<PathBuf, SslConnector>>> = LazyLock::new(Mutex::default);
    let Some(ca) = ca else {
        if let Some(c) = SYSTEM.get() {
            return Ok(c.clone());
        }
        let c = SslConnector::builder(SslMethod::tls())?.build(); // default verify + system CA paths
        return Ok(SYSTEM.get_or_init(|| c).clone());
    };
    cached(&BY_CA, ca, || {
        let mut b = SslConnector::builder(SslMethod::tls())?;
        b.set_ca_file(ca)?;
        Ok(b.build())
    })
}

/// Terminates TLS on an accepted stream.
pub async fn accept_tls<S: AsyncRw>(
    acceptor: &SslAcceptor,
    stream: S,
) -> Result<tokio_openssl::SslStream<S>, crate::BoxError> {
    let ssl = Ssl::new(acceptor.context())?;
    let mut s = tokio_openssl::SslStream::new(ssl, stream)?;
    std::pin::Pin::new(&mut s).accept().await?;
    Ok(s)
}

/// Establishes a client TLS session over an existing TCP connection, verifying the
/// server against `ca` (or the system roots) using `sni`.
pub async fn connect_tls(
    sni: &str,
    stream: tokio::net::TcpStream,
    ca: Option<&Path>,
) -> Result<tokio_openssl::SslStream<tokio::net::TcpStream>, crate::BoxError> {
    let ssl = client_connector(ca)?.configure()?.into_ssl(sni)?;
    let mut s = tokio_openssl::SslStream::new(ssl, stream)?;
    std::pin::Pin::new(&mut s).connect().await?;
    Ok(s)
}

/// Builds a quinn server config from a cert chain + key on disk.
pub fn quic_server_config(c: &TlsHostConfig) -> Result<quinn::ServerConfig, crate::BoxError> {
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let chain: Vec<CertificateDer<'static>> = {
        let f = std::fs::File::open(&c.full_chain)?;
        let mut r = std::io::BufReader::new(f);
        rustls_pemfile::certs(&mut r).collect::<Result<Vec<_>, _>>()?
    };
    let key: PrivateKeyDer<'static> = {
        let f = std::fs::File::open(&c.key)?;
        let mut r = std::io::BufReader::new(f);
        rustls_pemfile::private_key(&mut r)?.ok_or("no private key in key file")?
    };
    Ok(quinn::ServerConfig::with_single_cert(chain, key)?)
}

/// Reads every certificate of a PEM bundle into a rustls root store.
fn root_store_from_pem(ca: &Path) -> Result<quinn::rustls::RootCertStore, crate::BoxError> {
    let f = std::fs::File::open(ca)?;
    let mut r = std::io::BufReader::new(f);
    let mut roots = quinn::rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut r) {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err(format!("{}: no certificate in relay CA file", ca.display()).into());
    }
    Ok(roots)
}

fn quic_client_endpoint_with(
    roots: quinn::rustls::RootCertStore,
) -> Result<quinn::Endpoint, crate::BoxError> {
    let client_cfg = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let addr: std::net::SocketAddr = "[::]:0".parse()?;
    let mut ep = quinn::Endpoint::client(addr)?;
    ep.set_default_client_config(client_cfg);
    Ok(ep)
}

/// Shared quinn client endpoint for dialing relay hops over QUIC, verifying peers
/// against `ca` (a PEM bundle) or, without one, the system roots. One endpoint per
/// CA path.
pub fn quic_client_endpoint(ca: Option<&Path>) -> Result<quinn::Endpoint, crate::BoxError> {
    static SYSTEM: OnceLock<quinn::Endpoint> = OnceLock::new();
    static BY_CA: LazyLock<Mutex<HashMap<PathBuf, quinn::Endpoint>>> =
        LazyLock::new(Mutex::default);
    let Some(ca) = ca else {
        if let Some(e) = SYSTEM.get() {
            return Ok(e.clone());
        }
        let mut roots = quinn::rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
        let ep = quic_client_endpoint_with(roots)?;
        return Ok(SYSTEM.get_or_init(|| ep).clone());
    };
    cached(&BY_CA, ca, || {
        quic_client_endpoint_with(root_store_from_pem(ca)?)
    })
}
