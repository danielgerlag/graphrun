use crate::error::{Error, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::{Arc, Once};
use std::time::Duration;

static PROVIDER: Once = Once::new();

pub fn install_provider() {
    PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Clone)]
pub struct TlsMaterial {
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub server_name: String,
}

pub struct CertificateAuthority {
    pub cert: rcgen::Certificate,
    pub key: rcgen::KeyPair,
    pub pem: String,
}

pub fn generate_ca() -> Result<CertificateAuthority> {
    install_provider();
    let key = rcgen::KeyPair::generate().map_err(|err| Error::invalid(err.to_string()))?;
    let mut params = rcgen::CertificateParams::new(vec!["graphrun-ca".to_owned()])
        .map_err(|err| Error::invalid(err.to_string()))?;
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "graphrun-ca");
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
    ];
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);
    let cert = params
        .self_signed(&key)
        .map_err(|err| Error::invalid(err.to_string()))?;
    let pem = cert.pem();
    Ok(CertificateAuthority { cert, key, pem })
}

pub fn issue_node(ca: &CertificateAuthority, node_id: u64) -> Result<TlsMaterial> {
    install_provider();
    let server_name = format!("node-{node_id}.graphrun.local");
    let key = rcgen::KeyPair::generate().map_err(|err| Error::invalid(err.to_string()))?;
    let mut params = rcgen::CertificateParams::new(vec![server_name.clone()])
        .map_err(|err| Error::invalid(err.to_string()))?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, server_name.clone());
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);
    params.serial_number = Some(rcgen::SerialNumber::from(node_id));
    let cert = params
        .signed_by(&key, &ca.cert, &ca.key)
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok(TlsMaterial {
        ca_pem: ca.pem.clone(),
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
        server_name,
    })
}

pub fn server_config(material: &TlsMaterial) -> Result<Arc<ServerConfig>> {
    install_provider();
    let (certs, key) = load_cert_key(material)?;
    let mut roots = RootCertStore::empty();
    for cert in load_certs(&material.ca_pem)? {
        roots
            .add(cert)
            .map_err(|err| Error::invalid(err.to_string()))?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|err| Error::invalid(err.to_string()))?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok(Arc::new(config))
}

pub fn client_config(material: &TlsMaterial) -> Result<Arc<ClientConfig>> {
    install_provider();
    let (certs, key) = load_cert_key(material)?;
    let mut roots = RootCertStore::empty();
    for cert in load_certs(&material.ca_pem)? {
        roots
            .add(cert)
            .map_err(|err| Error::invalid(err.to_string()))?;
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok(Arc::new(config))
}

pub fn load_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut cursor = std::io::Cursor::new(pem.as_bytes());
    rustls_pemfile::certs(&mut cursor)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| Error::invalid(err.to_string()))
}

fn load_cert_key(
    material: &TlsMaterial,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = load_certs(&material.cert_pem)?;
    let mut cursor = std::io::Cursor::new(material.key_pem.as_bytes());
    let key = rustls_pemfile::pkcs8_private_keys(&mut cursor)
        .next()
        .ok_or_else(|| Error::invalid("missing private key"))?
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok((certs, PrivateKeyDer::Pkcs8(key)))
}

pub fn rpc_timeout() -> Duration {
    Duration::from_secs(5)
}
