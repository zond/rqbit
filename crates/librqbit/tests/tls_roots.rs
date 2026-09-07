//! That [`librqbit::http_client_builder`] really does keep the platform
//! verifier out of our HTTPS.
//!
//! reqwest exposes nothing about a built client's certificate verifier, so
//! this proves it by difference: in an environment where the platform
//! verifier cannot be constructed, a plain builder fails and ours does not.
//!
//! Its own test binary on purpose. It has to move `SSL_CERT_FILE` and
//! `SSL_CERT_DIR` out from under the process for a moment, and the unit
//! tests build sessions -- and with them reqwest clients -- in parallel.
#![cfg(all(
    feature = "rust-tls",
    not(feature = "default-tls"),
    target_os = "linux"
))]

/// On Linux `rustls-platform-verifier` loads the system store through
/// `rustls-native-certs`, which takes the store from `SSL_CERT_FILE` and
/// `SSL_CERT_DIR` when either is set. Pointed at an empty file and an empty
/// directory it loads nothing, and `Verifier::new` refuses an empty store
/// ("No CA certificates were loaded from the system"), so a builder that
/// constructs it cannot `build()`. Ours builds anyway, which is only
/// possible if it never asked the platform for roots.
#[test]
fn our_client_builds_where_the_platform_verifier_cannot() {
    let empty_file = tempfile::NamedTempFile::new().expect("temp file");
    let empty_dir = tempfile::tempdir().expect("temp dir");
    let saved = ["SSL_CERT_FILE", "SSL_CERT_DIR"].map(|name| (name, std::env::var_os(name)));

    // Safe here: this test binary is single-threaded at this point (one
    // test, no spawned runtime) and nothing else in it reads the
    // environment.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", empty_file.path());
        std::env::set_var("SSL_CERT_DIR", empty_dir.path());
    }
    let platform = reqwest::Client::builder().build();
    let ours = librqbit::http_client_builder().build();
    unsafe {
        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }

    let error = platform.expect_err("the platform verifier found roots in an empty store");
    assert!(error.is_builder(), "{error:?}");
    ours.expect("our client does not depend on the platform trust store");
}
