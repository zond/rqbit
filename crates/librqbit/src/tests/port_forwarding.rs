//! The session hands out the UPnP port forwarder's state when forwarding is
//! on, for the port it announces.

use std::net::Ipv4Addr;

use crate::{
    ListenerMode, Session, SessionOptions, listen::ListenerOptions,
    tests::test_util::setup_test_logging,
};

async fn session(enable_upnp_port_forwarding: bool) -> std::sync::Arc<Session> {
    Session::new_with_opts(
        std::env::temp_dir().join("does_not_exist"),
        SessionOptions {
            dht: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                // A loopback listener announces nothing unless told what to announce.
                announce_port: Some(40_000),
                enable_upnp_port_forwarding,
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn port_forwarding_state_is_there_when_forwarding_is_on() {
    setup_test_logging();
    let session = session(true).await;
    let rx = session.port_forwarding().expect("forwarding is on");
    let state = rx.borrow().clone();
    assert_eq!(
        state.ports.iter().map(|p| p.port).collect::<Vec<_>>(),
        vec![40_000]
    );
}

#[tokio::test]
async fn no_port_forwarding_state_when_forwarding_is_off() {
    setup_test_logging();
    let session = session(false).await;
    assert!(session.port_forwarding().is_none());
}
