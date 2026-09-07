//! The live-peer cap of a running torrent moves at runtime: lowering it hangs up on the
//! surplus and keeps downloading with the rest, raising it brings the parked peers back, and
//! forgetting the disconnected peers takes them out of the table.

use std::{net::Ipv4Addr, num::NonZeroU32, time::Duration};

use anyhow::bail;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
    torrent_state::live::peers::stats::AggregatePeerStats,
};

const SEEDERS: usize = 12;
const LOWERED: usize = 3;
const WAIT: Duration = Duration::from_secs(30);

/// Every peer the table knows, whatever its state.
fn table_size(stats: &AggregatePeerStats) -> usize {
    (stats.queued + stats.connecting + stats.live + stats.dead + stats.not_needed) as usize
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_limit_moves_at_runtime() {
    tokio::time::timeout(
        Duration::from_secs(180),
        peer_limit_moves_at_runtime_inner(),
    )
    .await
    .expect("test timed out");
}

async fn peer_limit_moves_at_runtime_inner() {
    setup_test_logging();

    let tempdir = create_default_random_dir_with_torrents(4, 1_000_000, Some("rqbit_peer_limit"));
    let torrent_file = create_torrent(
        tempdir.path(),
        CreateTorrentOptions {
            piece_length: Some(32768),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    let torrent_bytes = torrent_file.as_bytes().unwrap();

    // Seeders on ephemeral loopback ports, no DHT, no trackers: the client learns of them
    // through `initial_peers` and nothing else, so a peer it forgets is gone for good. Each
    // uploads slowly, so the download outlasts what is asserted about its peers; the limit
    // is lifted at the end.
    let mut seeders = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..SEEDERS {
        let session = Session::new_with_opts(
            std::env::temp_dir().join("does_not_exist"),
            SessionOptions {
                dht: None,
                listen: Some(ListenerOptions {
                    listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                disable_local_service_discovery: true,
                ratelimits: LimitsConfig {
                    upload_bps: NonZeroU32::new(32 * 1024),
                    download_bps: None,
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes.clone()),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(tempdir.path().to_str().unwrap().to_owned()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        assert!(handle.live().unwrap().is_finished(), "a seeder has it all");
        peers.push(session.listen_addr().unwrap());
        seeders.push((session, handle));
    }

    let root = tempfile::TempDir::with_prefix("rqbit_peer_limit_client").unwrap();
    let client = Session::new_with_opts(
        root.path().join("out"),
        SessionOptions {
            dht: None,
            listen: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let handle = client
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent_bytes.clone()),
            Some(AddTorrentOptions {
                initial_peers: Some(peers.clone()),
                peer_limit: Some(SEEDERS),
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
        .expect("the client torrent goes live");
    assert_eq!(live.peer_limit(), SEEDERS, "the add option is the cap");
    let peer_stats = || live.stats_snapshot().peer_stats;

    wait_until(
        || match peer_stats() {
            s if s.live as usize == SEEDERS => Ok(()),
            s => bail!("waiting for every seeder to connect: {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    info!(stats = ?peer_stats(), "all seeders connected");

    // Lowering hangs up on the surplus and parks it; what is left keeps downloading.
    handle.set_peer_limit(LOWERED);
    assert_eq!(live.peer_limit(), LOWERED);
    wait_until(
        || match peer_stats() {
            s if (s.live + s.connecting) as usize == LOWERED
                && s.not_needed as usize == SEEDERS - LOWERED =>
            {
                Ok(())
            }
            s => bail!("waiting for the surplus to hang up: {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    let fetched_before = live.stats_snapshot().fetched_bytes;
    wait_until(
        || match live.stats_snapshot().fetched_bytes {
            fetched if fetched > fetched_before => Ok(()),
            fetched => bail!("download stalled at {fetched} bytes with {LOWERED} peers"),
        },
        WAIT,
    )
    .await
    .unwrap();
    // Setting the cap it already has changes nothing.
    handle.set_peer_limit(LOWERED);
    let stats = peer_stats();
    assert_eq!(
        (stats.live + stats.connecting) as usize,
        LOWERED,
        "{stats:?}"
    );
    assert_eq!(stats.not_needed as usize, SEEDERS - LOWERED, "{stats:?}");
    info!(stats = ?stats, "lowered");

    // Raising re-queues the parked peers: they are all back, none left parked.
    handle.set_peer_limit(SEEDERS);
    wait_until(
        || match peer_stats() {
            s if s.live as usize == SEEDERS && s.not_needed == 0 => Ok(()),
            s => bail!("waiting for the parked peers to come back: {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    info!(stats = ?peer_stats(), "raised");

    // Nothing to forget while everyone is connected.
    assert_eq!(live.forget_disconnected_peers(), 0);
    assert_eq!(table_size(&peer_stats()), SEEDERS);

    // Lowered again and forgotten, the parked peers leave the table, and with no source
    // to name them again a later raise has nobody to bring back.
    handle.set_peer_limit(LOWERED);
    wait_until(
        || match peer_stats() {
            s if (s.live + s.connecting) as usize == LOWERED
                && s.not_needed as usize == SEEDERS - LOWERED =>
            {
                Ok(())
            }
            s => bail!("waiting for the surplus to hang up again: {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    assert_eq!(live.forget_disconnected_peers(), SEEDERS - LOWERED);
    let stats = peer_stats();
    assert_eq!(stats.not_needed, 0, "{stats:?}");
    assert_eq!(table_size(&stats), LOWERED, "{stats:?}");
    handle.set_peer_limit(SEEDERS);
    assert_eq!(live.peer_limit(), SEEDERS);

    // Let it finish on the peers it has left.
    for (seeder, _) in &seeders {
        seeder.ratelimits.set_upload_bps(None);
    }
    handle.wait_until_completed().await.unwrap();
    let stats = peer_stats();
    assert!(
        stats.live as usize <= LOWERED,
        "nobody re-dialled the forgotten peers: {stats:?}"
    );
    drop(seeders);
}
