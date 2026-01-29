// SPDX-License-Identifier: BSD-3-Clause

use std::sync::Arc;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

#[derive(Debug)]
pub struct NoCertificateVerification {}

impl ServerCertVerifier for NoCertificateVerification {
  fn verify_server_cert(
    &self,
    _end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    _server_name: &ServerName<'_>,
    _ocsp_response: &[u8],
    _now: UnixTime,
  ) -> Result<ServerCertVerified, Error> {
    Ok(ServerCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    _message: &[u8],
    _cert: &CertificateDer<'_>,
    _dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, Error> {
    Ok(HandshakeSignatureValid::assertion())
  }

  fn verify_tls13_signature(
    &self,
    _message: &[u8],
    _cert: &CertificateDer<'_>,
    _dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, Error> {
    Ok(HandshakeSignatureValid::assertion())
  }

  fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
    vec![
      SignatureScheme::RSA_PKCS1_SHA1,
      SignatureScheme::ECDSA_SHA1_Legacy,
      SignatureScheme::RSA_PKCS1_SHA256,
      SignatureScheme::RSA_PKCS1_SHA384,
      SignatureScheme::RSA_PKCS1_SHA512,
      SignatureScheme::ECDSA_NISTP256_SHA256,
      SignatureScheme::ECDSA_NISTP384_SHA384,
      SignatureScheme::ECDSA_NISTP521_SHA512,
      SignatureScheme::RSA_PSS_SHA256,
      SignatureScheme::RSA_PSS_SHA384,
      SignatureScheme::RSA_PSS_SHA512,
      SignatureScheme::ED25519,
      SignatureScheme::ED448,
    ]
  }
}

pub fn make_tls_client_config() -> Arc<ClientConfig> {
  let mut root_cert_store = RootCertStore::empty();
  root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
  let mut config = ClientConfig::builder().with_root_certificates(root_cert_store).with_no_client_auth();

  config.dangerous().set_certificate_verifier(Arc::new(NoCertificateVerification {}));

  Arc::new(config)
}
