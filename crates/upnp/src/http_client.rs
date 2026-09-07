//! What this crate's HTTP clients trust.

/// A [`reqwest::ClientBuilder`] with the same TLS trust policy as librqbit's
/// `http_client_builder`, which documents it in full: with the `rust-tls`
/// feature (and rustls actually being the backend) the roots are Mozilla's
/// root program as compiled into this binary, never the platform's store,
/// because reqwest builds `rustls_platform_verifier::Verifier` for any
/// client that brings no roots of its own and on Android that verifier has
/// Java download and parse the issuer's CRL on every handshake.
///
/// The policy costs this crate nothing: SSDP hands us `http://` control and
/// description URLs on the LAN, so these clients rarely speak TLS at all.
/// It is here so that no client anywhere in rqbit constructs the platform
/// verifier -- on Android that keeps the JNI initialization off the critical
/// path, and on Linux it stops every `Client::new()` here from re-reading
/// the system trust store from disk.
///
/// This is duplicated from librqbit rather than shared because librqbit
/// depends on this crate, not the other way round, and there is no smaller
/// crate that both build their HTTP clients from.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    #[allow(unused_mut)]
    let mut builder = reqwest::ClientBuilder::new();
    #[cfg(all(feature = "rust-tls", not(feature = "default-tls")))]
    {
        builder = builder.tls_certs_only(mozilla_roots());
    }
    builder
}

/// Mozilla's roots as reqwest certificates. Each is a constant DER blob and
/// `from_der` only stores the bytes, so this cannot fail.
#[cfg(all(feature = "rust-tls", not(feature = "default-tls")))]
fn mozilla_roots() -> impl Iterator<Item = reqwest::Certificate> {
    webpki_root_certs::TLS_SERVER_ROOT_CERTS
        .iter()
        .map(|der| reqwest::Certificate::from_der(der).expect("compiled-in root is DER"))
}
