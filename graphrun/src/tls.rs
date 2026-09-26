use crate::error::{Error, ErrorKind, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::BTreeSet;
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrincipalId(String);

fn validate_identity_segment(value: &str) -> Result<()> {
    if value.is_empty()
        || !value.as_bytes()[0].is_ascii_alphanumeric()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::invalid("invalid certificate identity segment"));
    }
    Ok(())
}

impl ClusterId {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identity_segment(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl PrincipalId {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identity_segment(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Certificate roles from the API authorization contract, distinct from worker execution roles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerRole {
    Member,
    Worker,
    Client,
    /// Privileged publication, policy, membership, intervention, and backup/restore RPCs.
    Admin,
}

impl PeerRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Worker => "worker",
            Self::Client => "client",
            Self::Admin => "admin",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "member" => Ok(Self::Member),
            "worker" => Ok(Self::Worker),
            "client" => Ok(Self::Client),
            "admin" => Ok(Self::Admin),
            _ => Err(Error::invalid("invalid certificate role")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrincipalIdentity {
    cluster_id: ClusterId,
    principal_id: PrincipalId,
    roles: BTreeSet<PeerRole>,
}

impl PrincipalIdentity {
    pub fn new(
        cluster_id: ClusterId,
        principal_id: PrincipalId,
        roles: impl IntoIterator<Item = PeerRole>,
    ) -> Result<Self> {
        let mut unique_roles = BTreeSet::new();
        for role in roles {
            if !unique_roles.insert(role) {
                return Err(Error::invalid("duplicate certificate role"));
            }
        }
        if unique_roles.is_empty() {
            return Err(Error::invalid("certificate identity has no roles"));
        }
        Ok(Self {
            cluster_id,
            principal_id,
            roles: unique_roles,
        })
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    pub fn roles(&self) -> impl Iterator<Item = PeerRole> + '_ {
        self.roles.iter().copied()
    }
}

// Only the verifier constructs this identity; its input must come from an mTLS connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedPeerIdentity(PrincipalIdentity);

impl VerifiedPeerIdentity {
    pub fn cluster_id(&self) -> &ClusterId {
        self.0.cluster_id()
    }

    pub fn principal_id(&self) -> &PrincipalId {
        self.0.principal_id()
    }

    pub fn roles(&self) -> impl Iterator<Item = PeerRole> + '_ {
        self.0.roles()
    }

    pub fn require_role(&self, role: PeerRole) -> Result<()> {
        if self.0.roles.contains(&role) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "certificate does not grant the required role",
            ))
        }
    }

    /// Checks the signed member identity only. The caller must independently check
    /// committed roster membership and the connection endpoint.
    pub fn require_member_identity(
        &self,
        cluster_id: &ClusterId,
        member_id: &PrincipalId,
    ) -> Result<()> {
        self.require_role(PeerRole::Member)?;
        if self.cluster_id() != cluster_id || self.principal_id() != member_id {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "certificate does not match the expected member identity",
            ));
        }
        Ok(())
    }
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

/// Signs one URI SAN per role using the configured cluster CA.
pub fn issue_principal(
    ca: &CertificateAuthority,
    identity: &PrincipalIdentity,
    server_name: &str,
) -> Result<TlsMaterial> {
    install_provider();
    let key = rcgen::KeyPair::generate().map_err(|err| Error::invalid(err.to_string()))?;
    let mut params = rcgen::CertificateParams::new(vec![server_name.to_owned()])
        .map_err(|err| Error::invalid(err.to_string()))?;
    for role in identity.roles() {
        let uri = format!(
            "spiffe://graphrun/{}/{}/{}",
            identity.cluster_id().as_str(),
            role.as_str(),
            identity.principal_id().as_str()
        );
        params
            .subject_alt_names
            .push(rcgen::SanType::URI(uri.try_into().map_err(
                |err: rcgen::Error| Error::invalid(err.to_string()),
            )?));
    }
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, server_name);
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);
    let cert = params
        .signed_by(&key, &ca.cert, &ca.key)
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok(TlsMaterial {
        ca_pem: ca.pem.clone(),
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
        server_name: server_name.to_owned(),
    })
}

/// Verifies the presented client certificate chain against one configured cluster CA
/// and cluster ID before interpreting its signed URI SANs. Roster, endpoint and live session
/// authority are separate checks for the RPC caller. The chain must come from a completed
/// mTLS handshake (proof of private-key possession), never a request field or header.
pub fn verify_peer_identity(
    ca_pem: &str,
    expected_cluster_id: &ClusterId,
    peer_chain: &[CertificateDer<'_>],
) -> Result<VerifiedPeerIdentity> {
    install_provider();
    let (leaf, intermediates) = peer_chain
        .split_first()
        .ok_or_else(|| unauthenticated("missing peer certificate"))?;
    let [ca] = <[_; 1]>::try_from(load_certs(ca_pem)?)
        .map_err(|_| Error::invalid("expected exactly one cluster CA certificate"))?;
    let mut roots = RootCertStore::empty();
    roots
        .add(ca)
        .map_err(|err| Error::invalid(err.to_string()))?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|err| Error::invalid(err.to_string()))?;
    verifier
        .verify_client_cert(leaf, intermediates, UnixTime::now())
        .map_err(|_| unauthenticated("untrusted peer certificate"))?;
    let identity = parse_signed_sans(leaf.as_ref())?;
    if identity.cluster_id() != expected_cluster_id {
        return Err(unauthenticated("peer certificate is for another cluster"));
    }
    Ok(VerifiedPeerIdentity(identity))
}

fn unauthenticated(message: &'static str) -> Error {
    Error::new(ErrorKind::Unauthenticated, message)
}

struct DerReader<'a>(&'a [u8]);

impl<'a> DerReader<'a> {
    fn read(&mut self) -> Result<(u8, &'a [u8])> {
        let (tag, rest) = self
            .0
            .split_first()
            .ok_or_else(|| unauthenticated("malformed certificate SAN"))?;
        let (&length_byte, mut rest) = rest
            .split_first()
            .ok_or_else(|| unauthenticated("malformed certificate SAN"))?;
        let length = if length_byte & 0x80 == 0 {
            usize::from(length_byte)
        } else {
            let count = usize::from(length_byte & 0x7f);
            if count == 0 || count > std::mem::size_of::<usize>() || count > rest.len() {
                return Err(unauthenticated("malformed certificate SAN"));
            }
            let mut length = 0usize;
            for &byte in &rest[..count] {
                length = (length << 8) | usize::from(byte);
            }
            if rest[0] == 0 || length < 128 {
                return Err(unauthenticated("malformed certificate SAN"));
            }
            rest = &rest[count..];
            length
        };
        if length > rest.len() {
            return Err(unauthenticated("malformed certificate SAN"));
        }
        let (value, tail) = rest.split_at(length);
        self.0 = tail;
        Ok((*tag, value))
    }

    fn tagged(&mut self, expected: u8) -> Result<&'a [u8]> {
        let (tag, value) = self.read()?;
        if tag != expected {
            return Err(unauthenticated("malformed certificate SAN"));
        }
        Ok(value)
    }

    fn sequence(&mut self) -> Result<Self> {
        Ok(Self(self.tagged(0x30)?))
    }

    fn finish(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(unauthenticated("malformed certificate SAN"))
        }
    }
}

fn parse_signed_sans(der: &[u8]) -> Result<PrincipalIdentity> {
    let mut outer = DerReader(der);
    let mut certificate = outer.sequence()?;
    outer.finish()?;
    let mut tbs = certificate.sequence()?;
    if tbs.0.first() == Some(&0xa0) {
        let mut version = DerReader(tbs.tagged(0xa0)?);
        version.tagged(0x02)?;
        version.finish()?;
    }
    for tag in [0x02, 0x30, 0x30, 0x30, 0x30, 0x30] {
        tbs.tagged(tag)?;
    }
    let mut san = None;
    let mut saw_extensions = false;
    while !tbs.0.is_empty() {
        match tbs.0[0] {
            0x81 | 0x82 => {
                tbs.read()?;
            }
            0xa3 => {
                if saw_extensions {
                    return Err(unauthenticated("duplicate certificate extensions"));
                }
                saw_extensions = true;
                let mut explicit = DerReader(tbs.tagged(0xa3)?);
                let mut extensions = explicit.sequence()?;
                explicit.finish()?;
                while !extensions.0.is_empty() {
                    let mut extension = extensions.sequence()?;
                    let oid = extension.tagged(0x06)?;
                    if extension.0.first() == Some(&0x01) {
                        extension.tagged(0x01)?;
                    }
                    let value = extension.tagged(0x04)?;
                    extension.finish()?;
                    if oid == [0x55, 0x1d, 0x11] && san.replace(value).is_some() {
                        return Err(unauthenticated("duplicate certificate SAN"));
                    }
                }
            }
            _ => return Err(unauthenticated("malformed certificate SAN")),
        }
    }
    let mut san_value = DerReader(san.ok_or_else(|| unauthenticated("missing URI SAN"))?);
    let mut names = san_value.sequence()?;
    san_value.finish()?;
    let mut cluster_id = None;
    let mut principal_id = None;
    let mut roles = BTreeSet::new();
    while !names.0.is_empty() {
        let (tag, value) = names.read()?;
        if tag != 0x86 {
            continue;
        }
        let uri = std::str::from_utf8(value)
            .map_err(|_| unauthenticated("malformed certificate URI SAN"))?;
        let parts: Vec<_> = uri
            .strip_prefix("spiffe://graphrun/")
            .ok_or_else(|| unauthenticated("malformed certificate URI SAN"))?
            .split('/')
            .collect();
        let [cluster, role, principal] = <[&str; 3]>::try_from(parts)
            .map_err(|_| unauthenticated("malformed certificate URI SAN"))?;
        let cluster = ClusterId::parse(cluster)
            .map_err(|_| unauthenticated("malformed certificate cluster ID"))?;
        let principal = PrincipalId::parse(principal)
            .map_err(|_| unauthenticated("malformed certificate principal ID"))?;
        let role =
            PeerRole::parse(role).map_err(|_| unauthenticated("invalid certificate role"))?;
        if cluster_id.as_ref().is_some_and(|id| id != &cluster)
            || principal_id.as_ref().is_some_and(|id| id != &principal)
        {
            return Err(unauthenticated(
                "conflicting certificate URI SAN identities",
            ));
        }
        if !roles.insert(role) {
            return Err(unauthenticated("duplicate certificate URI SAN role"));
        }
        cluster_id = Some(cluster);
        principal_id = Some(principal);
    }
    let (Some(cluster_id), Some(principal_id)) = (cluster_id, principal_id) else {
        return Err(unauthenticated("missing URI SAN"));
    };
    PrincipalIdentity::new(cluster_id, principal_id, roles)
        .map_err(|_| unauthenticated("malformed certificate URI SAN"))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_uris(ca: &CertificateAuthority, uris: &[&str]) -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::default();
        if !uris.is_empty() {
            params.subject_alt_names.push(rcgen::SanType::DnsName(
                "node-1.graphrun.local".try_into().unwrap(),
            ));
        }
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(
            rcgen::DnType::CommonName,
            "spiffe://graphrun/forged/member/1",
        );
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        for uri in uris {
            params
                .subject_alt_names
                .push(rcgen::SanType::URI((*uri).try_into().unwrap()));
        }
        params.signed_by(&key, &ca.cert, &ca.key).unwrap().pem()
    }

    fn verified(
        ca: &CertificateAuthority,
        cluster_id: &str,
        pem: &str,
    ) -> Result<VerifiedPeerIdentity> {
        verify_peer_identity(
            &ca.pem,
            &ClusterId::parse(cluster_id).unwrap(),
            &load_certs(pem)?,
        )
    }

    #[test]
    fn signed_sans_prove_one_principal_with_multiple_roles() {
        let ca = generate_ca().unwrap();
        let identity = PrincipalIdentity::new(
            ClusterId::parse("cluster-1").unwrap(),
            PrincipalId::parse("7").unwrap(),
            [PeerRole::Worker, PeerRole::Member],
        )
        .unwrap();
        let material = issue_principal(&ca, &identity, "node-7.graphrun.local").unwrap();
        let peer = verified(&ca, "cluster-1", &material.cert_pem).unwrap();
        assert_eq!(peer.cluster_id(), identity.cluster_id());
        assert_eq!(peer.principal_id(), identity.principal_id());
        assert_eq!(
            peer.roles().collect::<Vec<_>>(),
            identity.roles().collect::<Vec<_>>()
        );
        assert!(peer.require_role(PeerRole::Worker).is_ok());
        assert!(peer.require_role(PeerRole::Client).is_err());
        assert_eq!(
            verified(&ca, "another-cluster", &material.cert_pem)
                .unwrap_err()
                .kind,
            ErrorKind::Unauthenticated
        );
        assert!(
            peer.require_member_identity(identity.cluster_id(), identity.principal_id())
                .is_ok()
        );
    }

    #[test]
    fn issued_admin_role_uses_canonical_san_not_operator() {
        let ca = generate_ca().unwrap();
        let identity = PrincipalIdentity::new(
            ClusterId::parse("cluster").unwrap(),
            PrincipalId::parse("admin-1").unwrap(),
            [PeerRole::Admin, PeerRole::Client],
        )
        .unwrap();
        let material = issue_principal(&ca, &identity, "admin.graphrun.local").unwrap();
        let peer = verified(&ca, "cluster", &material.cert_pem).unwrap();
        assert!(peer.require_role(PeerRole::Admin).is_ok());
        assert!(peer.require_role(PeerRole::Client).is_ok());
        assert_eq!(
            verified(
                &ca,
                "cluster",
                &signed_uris(&ca, &["spiffe://graphrun/cluster/operator/admin-1"]),
            )
            .unwrap_err()
            .kind,
            ErrorKind::Unauthenticated
        );
    }

    #[test]
    fn rejects_noncanonical_identity_segments_and_roles() {
        for value in ["", "..", "a/b", "%61", "a?b", "a#b", "é"] {
            assert!(ClusterId::parse(value).is_err(), "{value}");
            assert!(PrincipalId::parse(value).is_err(), "{value}");
        }
        let cluster = ClusterId::parse("c").unwrap();
        let principal = PrincipalId::parse("1").unwrap();
        assert!(
            PrincipalIdentity::new(
                cluster.clone(),
                principal.clone(),
                std::iter::empty::<PeerRole>(),
            )
            .is_err()
        );
        assert!(
            PrincipalIdentity::new(cluster, principal, [PeerRole::Member, PeerRole::Member])
                .is_err()
        );
    }

    #[test]
    fn rejects_inconsistent_or_malformed_signed_sans() {
        let ca = generate_ca().unwrap();
        let member = "spiffe://graphrun/c/member/1";
        for uris in [
            vec![member, "spiffe://graphrun/c/worker/2"],
            vec![member, "spiffe://graphrun/c/member/2"],
            vec![member, "spiffe://graphrun/other/worker/1"],
            vec![member, member],
            vec!["spiffe://graphrun/c/operator/1"],
            vec!["spiffe://graphrun/c/administrator/1"],
            vec!["spiffe://graphrun/c/Member/1"],
            vec!["spiffe://graphrun/c/member/%31"],
            vec!["spiffe://graphrun/c/member/1/extra"],
            vec!["spiffe://other/c/member/1"],
        ] {
            let cert = signed_uris(&ca, &uris);
            let err = verified(&ca, "c", &cert).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Unauthenticated, "{uris:?}: {err}");
        }
    }

    #[test]
    fn rejects_missing_san_bad_ca_and_tampered_san() {
        let ca = generate_ca().unwrap();
        let forged_cn_only = signed_uris(&ca, &[]);
        assert_eq!(
            verified(&ca, "cluster", &forged_cn_only).unwrap_err().kind,
            ErrorKind::Unauthenticated
        );
        let fixture = issue_node(&ca, 1).unwrap();
        assert_eq!(
            verified(&ca, "cluster", &fixture.cert_pem)
                .unwrap_err()
                .kind,
            ErrorKind::Unauthenticated
        );
        let material = issue_principal(
            &ca,
            &PrincipalIdentity::new(
                ClusterId::parse("cluster").unwrap(),
                PrincipalId::parse("1").unwrap(),
                [PeerRole::Member],
            )
            .unwrap(),
            "node-1.graphrun.local",
        )
        .unwrap();
        let other_ca = generate_ca().unwrap();
        assert_eq!(
            verified(&other_ca, "cluster", &material.cert_pem)
                .unwrap_err()
                .kind,
            ErrorKind::Unauthenticated
        );
        let mut cert = load_certs(&material.cert_pem)
            .unwrap()
            .remove(0)
            .as_ref()
            .to_vec();
        let pos = cert
            .windows(b"spiffe://graphrun/cluster/member/1".len())
            .position(|bytes| bytes == b"spiffe://graphrun/cluster/member/1")
            .unwrap();
        cert[pos + "spiffe://graphrun/".len()] = b'X';
        assert_eq!(
            verify_peer_identity(
                &ca.pem,
                &ClusterId::parse("cluster").unwrap(),
                &[CertificateDer::from(cert)],
            )
            .unwrap_err()
            .kind,
            ErrorKind::Unauthenticated
        );
        assert_eq!(
            verify_peer_identity(&ca.pem, &ClusterId::parse("cluster").unwrap(), &[])
                .unwrap_err()
                .kind,
            ErrorKind::Unauthenticated
        );
    }

    #[test]
    fn member_identity_is_not_inferred_from_roster_or_cn() {
        let ca = generate_ca().unwrap();
        let cert = signed_uris(&ca, &["spiffe://graphrun/c/member/1"]);
        let peer = verified(&ca, "c", &cert).unwrap();
        assert!(
            peer.require_member_identity(
                &ClusterId::parse("c").unwrap(),
                &PrincipalId::parse("1").unwrap()
            )
            .is_ok()
        );
        for (cluster, member) in [("other", "1"), ("c", "2")] {
            assert_eq!(
                peer.require_member_identity(
                    &ClusterId::parse(cluster).unwrap(),
                    &PrincipalId::parse(member).unwrap()
                )
                .unwrap_err()
                .kind,
                ErrorKind::PermissionDenied
            );
        }
        let only_worker = signed_uris(&ca, &["spiffe://graphrun/c/worker/1"]);
        assert_eq!(
            verified(&ca, "c", &only_worker)
                .unwrap()
                .require_member_identity(
                    &ClusterId::parse("c").unwrap(),
                    &PrincipalId::parse("1").unwrap()
                )
                .unwrap_err()
                .kind,
            ErrorKind::PermissionDenied
        );
    }
}
