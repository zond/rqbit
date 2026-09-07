//! The one place that decides what this crate's HTTPS trusts.

/// A [`reqwest::ClientBuilder`] with this crate's TLS trust policy already
/// applied. Every HTTP(S) client librqbit builds starts here -- the session's,
/// which is also the one `tracker_comms` announces with and the one torrent
/// files are fetched with, the blocklist and allowlist loader, and
/// [`crate::http_api_client`] -- and embedders that build their own can start
/// here too.
///
/// With the `rust-tls` feature -- and rustls actually being the backend,
/// i.e. `default-tls` off -- the returned builder trusts Mozilla's root
/// program as compiled into this binary, and nothing else. The roots come
/// from `webpki-root-certs`, which `rustls-platform-verifier` already pulls
/// into the graph, so this compiles nothing new. Without that feature the
/// builder is plain and the TLS backend's own defaults apply, so a desktop
/// build keeps using the system store.
///
/// # Why not the platform store
///
/// reqwest 0.13's rustls path constructs `rustls_platform_verifier::Verifier`
/// for any client that brings no roots of its own (reqwest 0.13.4
/// `src/async_impl/client.rs`, the `!config.tls_certs_only` arm of the
/// verifier `match`). `tls_certs_only` is the one builder state that takes
/// the plain `with_root_certificates` arm instead and never names that
/// verifier: rustls checks the chain itself.
///
/// On Android the platform verifier hands every handshake to Java's
/// `CertPathValidator` with revocation checking set to SOFT_FAIL and without
/// NO_FALLBACK, so for a leaf certificate with no OCSP URL Android downloads
/// the issuer's CRL and parses it in Java. Measured in an app embedding this
/// crate, on a Chromecast with Google TV: one announce to a tracker whose
/// issuer's CRL holds 116,196 entries cost 15 million Java objects and about
/// 400 MB of Java heap -- every announce, no result cache. That was 91% of
/// the whole app's Java allocation and the GC storm behind an ANR. On Linux
/// the same verifier re-reads the system store from disk for every client
/// built.
///
/// # The trade
///
/// A CA that the user or their organisation installed on the device -- a
/// TLS-inspecting corporate proxy, mitmproxy while debugging -- no longer
/// verifies our HTTPS, and a root Mozilla admits after this build ships is
/// not trusted until the crate is rebuilt.
///
/// For a torrent client that is the right way round. The HTTPS it speaks is
/// announces to public trackers, torrent-file and blocklist fetches over the
/// open internet; it has no intranet host to reach, so a private CA on this
/// path is far likelier to be interception than a need. And the traffic is
/// worth protecting from exactly that: an announce URL carries the peer's
/// identity, the torrents it holds, and on a private tracker the user's
/// passkey. A public root program is both sufficient and the conservative
/// choice for it. Someone who does need a private CA can build without
/// `rust-tls`, or hand [`crate::SessionOptions`] a proxy.
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
/// `from_der` only stores the bytes -- rustls parses them when the client is
/// built -- so this cannot fail.
#[cfg(all(feature = "rust-tls", not(feature = "default-tls")))]
fn mozilla_roots() -> impl Iterator<Item = reqwest::Certificate> {
    webpki_root_certs::TLS_SERVER_ROOT_CERTS
        .iter()
        .map(|der| reqwest::Certificate::from_der(der).expect("compiled-in root is DER"))
}

#[cfg(all(test, feature = "rust-tls", not(feature = "default-tls")))]
mod tests {
    /// That the compiled-in set is a real root program and not an empty or stubbed
    /// one -- with `tls_certs_only` an empty set trusts nothing at all, so every
    /// HTTPS request would fail. Mozilla's has held between 130 and 160 roots for
    /// years.
    ///
    /// The other half of the policy -- that the platform verifier is never
    /// constructed -- is `tests/tls_roots.rs`, which needs a test binary of its own.
    #[test]
    fn the_compiled_in_roots_are_a_full_root_program() {
        let count = super::mozilla_roots().count();
        assert!((100..300).contains(&count), "{count} roots");
    }
}
