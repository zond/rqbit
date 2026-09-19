//! A session stops when its owner drops it.
//!
//! Its tasks are cancelled by a guard the session itself holds, so anything
//! holding the session strongly while it waits keeps every one of those
//! tasks running after the owner has let go. The download end-to-end tests
//! saw exactly that as a flake: servers dialling each other kept each
//! other's sessions alive through their listeners' handshake checks, and
//! the test found dozens of tasks still running after it ended.

use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::bail;
use tokio::net::TcpStream;

use crate::{
    ConnectionOptions, ListenerMode, PeerConnectionOptions, Session, SessionOptions,
    listen::ListenerOptions,
    tests::test_util::{setup_test_logging, wait_until},
};

#[tokio::test(flavor = "multi_thread")]
async fn a_silent_incoming_connection_does_not_keep_a_dropped_session_alive() {
    setup_test_logging();
    let session = Session::new_with_opts(
        std::env::temp_dir().join("does_not_exist"),
        SessionOptions {
            dht: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            // Far longer than this test waits: a check holding the session would
            // hold it this long, waiting for a handshake that never comes.
            connect: Some(ConnectionOptions {
                peer_opts: Some(PeerConnectionOptions {
                    read_write_timeout: Some(Duration::from_secs(600)),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let addr = session.listen_addr().unwrap();

    // Connects and says nothing, so the listener's check of it waits.
    let silent = TcpStream::connect(addr).await.unwrap();
    // Time for the listener to accept it and start the check.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let weak = Arc::downgrade(&session);
    drop(session);
    wait_until(
        || match weak.upgrade() {
            None => Ok(()),
            Some(_) => bail!("the session is still alive"),
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    drop(silent);
}
