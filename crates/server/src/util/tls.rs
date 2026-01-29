// SPDX-License-Identifier: BSD-3-Clause

use std::sync::Arc;

use anyhow::anyhow;
use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Generates a self-signed certificate and private key.
///
/// This function takes a list of alternative names (`alt_names`) for the certificate,
/// generates a self-signed certificate, and returns both the certificate and the private key.
///
/// # Arguments
///
/// * `alt_names` - A `Vec<String>` containing the alternative names for the certificate.
///
/// # Returns
///
/// * `Ok((Vec<CertificateDer<'static>>, PrivateKeyDer<'static>))` - On success, returns a tuple containing the certificate and private key.
/// * `Err(anyhow::Error)` - If an error occurs during the certificate generation or parsing.
pub fn generate_self_signed_cert(
  alt_names: Vec<String>,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
  let cert = rcgen::generate_simple_self_signed(alt_names)?;

  let cert_pem = cert.cert.pem();
  let key_pem = cert.signing_key.serialize_pem();

  let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| anyhow!("failed to parse generated certificate PEM: {}", e))?;

  let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
    .map_err(|e| anyhow!("failed to parse generated private key PEM: {}", e))?;

  Ok((certs, key))
}

/// Creates a TLS server configuration using the provided certificate and private key.
///
/// This function takes a list of certificates and a private key and creates a `ServerConfig`
/// for TLS, which is then wrapped in an `Arc` for shared ownership.
///
/// # Arguments
///
/// * `certs` - A `Vec<CertificateDer<'static>>` containing the server's certificate chain.
/// * `key` - A `PrivateKeyDer<'static>` containing the server's private key.
///
/// # Returns
///
/// * `Ok(Arc<ServerConfig>)` - On success, returns a `ServerConfig` wrapped in an `Arc`.
/// * `Err(anyhow::Error)` - If an error occurs during the configuration creation.
pub fn create_tls_config(
  certs: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
) -> anyhow::Result<Arc<ServerConfig>> {
  let config = ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key)?;

  Ok(Arc::new(config))
}

/// Loads a list of certificates from a PEM-encoded file.
///
/// This function reads the specified file and parses it as a list of PEM-encoded certificates.
///
/// # Arguments
///
/// * `filename` - A `&str` representing the path to the certificate file.
///
/// # Returns
///
/// * `Ok(Vec<CertificateDer<'static>>)` - On success, returns a vector of `CertificateDer`.
/// * `Err(anyhow::Error)` - If an error occurs while opening or reading the file.
pub fn load_certs(filename: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
  let certs = CertificateDer::pem_file_iter(filename)
    .map_err(|e| anyhow!("failed to open certificate file '{}': {}", filename, e))?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| anyhow!("failed to parse certificate file '{}': {}", filename, e))?;

  Ok(certs)
}

/// Loads a private key from a PEM-encoded file.
///
/// This function reads the specified file and parses it as a PEM-encoded private key.
/// Supports PKCS#1, PKCS#8, and SEC1 key formats.
///
/// # Arguments
///
/// * `filename` - A `&str` representing the path to the private key file.
///
/// # Returns
///
/// * `Ok(PrivateKeyDer<'static>)` - On success, returns a `PrivateKeyDer`.
/// * `Err(anyhow::Error)` - If an error occurs while opening, reading, or parsing the file.
pub fn load_private_key(filename: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
  PrivateKeyDer::from_pem_file(filename).map_err(|e| anyhow!("failed to load private key from '{}': {}", filename, e))
}
