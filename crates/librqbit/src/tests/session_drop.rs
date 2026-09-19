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
use tokio::net::{TcpListener, TcpStream};

use crate::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, CreateTorrentOptions, ListenerMode,
    PeerConnectionOptions, Session, SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
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

/// review #6. A torrent at its peer cap with addresses still to dial is the
/// ordinary state of a swarm, and its peer adder waits for a slot for as
/// long as that lasts. It must not hold the session while it does.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_adder_waiting_for_a_slot_does_not_keep_a_dropped_session_alive() {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, 65536, Some("session_drop_adder"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();

    // Two peers that accept and never say anything: the first dial holds the
    // one slot for as long as its handshake waits, and the adder has the
    // second address in hand, waiting for that slot.
    let silent_a = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let silent_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peers = vec![
        silent_a.local_addr().unwrap(),
        silent_b.local_addr().unwrap(),
    ];

    let dir = tempfile::TempDir::with_prefix("session_drop_adder_client").unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            listen: None,
            disable_local_service_discovery: true,
            // Far longer than this test waits, so the first dial keeps its slot.
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
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes().unwrap().to_vec()),
            Some(AddTorrentOptions {
                initial_peers: Some(peers),
                peer_limit: Some(1),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    let live = handle
        .live_wait_initializing(Duration::from_secs(10))
        .await
        .expect("the torrent goes live");
    wait_until(
        || match live.stats_snapshot().peer_stats {
            s if s.connecting == 1 && s.queued == 1 => Ok(()),
            s => bail!("waiting for one dial and one address waiting on the slot: {s:?}"),
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    drop(live);

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
    drop(handle);
    drop((silent_a, silent_b, files));
}
