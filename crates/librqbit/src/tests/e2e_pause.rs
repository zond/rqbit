//! What `Session::pause` and `Session::unpause` do to a live torrent: the pause stops
//! fetching and lets go of every peer, and the unpause resumes from exactly the pieces
//! that were there, without re-checking anything.

use std::{net::Ipv4Addr, num::NonZeroU32, time::Duration};

use anyhow::bail;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
    torrent_state::TorrentStatsState,
};

const WAIT: Duration = Duration::from_secs(30);
/// Long enough that a torrent still talking to its seeder would have moved several
/// chunks at the rate limit below.
const STILLNESS: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread")]
async fn pause_stops_fetching_and_unpause_keeps_the_have_set() {
    tokio::time::timeout(
        Duration::from_secs(180),
        pause_stops_fetching_and_unpause_keeps_the_have_set_inner(),
    )
    .await
    .expect("test timed out");
}

async fn pause_stops_fetching_and_unpause_keeps_the_have_set_inner() {
    setup_test_logging();

    let tempdir = create_default_random_dir_with_torrents(4, 1_000_000, Some("rqbit_pause"));
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

    // One seeder, uploading slowly enough that the download is still going when it is
    // paused, and no DHT or trackers, so the only peer the client will ever know is the
    // one it is handed.
    let seeder = Session::new_with_opts(
        std::env::temp_dir().join("does_not_exist"),
        SessionOptions {
            dht: None,
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            ratelimits: LimitsConfig {
                upload_bps: NonZeroU32::new(64 * 1024),
                download_bps: None,
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let seeder_handle = seeder
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
    seeder_handle.wait_until_initialized().await.unwrap();
    let seeder_addr = seeder.listen_addr().unwrap();

    let root = tempfile::TempDir::with_prefix("rqbit_pause_client").unwrap();
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
                initial_peers: Some(vec![seeder_addr]),
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

    // A whole checked piece, not just bytes on the wire: the point below is that the
    // have-set survives the pause, and only a completed piece is in it.
    wait_until(
        || match (live.stats_snapshot(), handle.stats().progress_bytes) {
            (s, progress) if progress > 0 && s.peer_stats.live == 1 => Ok(()),
            (s, progress) => bail!("waiting for the first piece: {progress} {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    let progress_before = handle.stats().progress_bytes;
    assert!(
        !handle.stats().finished,
        "the rate limit keeps the download unfinished"
    );
    info!(progress_before, "downloading");

    // Pause. The live state goes away with every peer task in it.
    client.pause(&handle).await.unwrap();
    let paused = handle.stats();
    assert!(
        matches!(paused.state, TorrentStatsState::Paused),
        "{:?}",
        paused.state
    );
    assert!(handle.is_paused());
    assert!(handle.live().is_none(), "no live state to fetch with");
    assert!(
        paused.live.is_none(),
        "and no live stats: nothing is connected"
    );

    // And it really has stopped: nothing arrives while it sits there.
    let progress_paused = paused.progress_bytes;
    assert!(
        progress_paused >= progress_before,
        "a piece already in flight may still have landed: {progress_paused} < {progress_before}"
    );
    tokio::time::sleep(STILLNESS).await;
    assert_eq!(
        handle.stats().progress_bytes,
        progress_paused,
        "a paused torrent fetches nothing"
    );
    // Pausing a paused torrent is an error, not a no-op.
    assert!(client.pause(&handle).await.is_err());
    info!(progress_paused, "paused");

    // Unpause. The have-set is exactly what the pause left: carried through
    // TorrentStatePaused's chunk tracker, never re-checked against the storage, so no
    // progress is lost and no piece is re-hashed.
    client.unpause(&handle).await.unwrap();
    let resumed = handle.stats();
    assert!(
        matches!(resumed.state, TorrentStatsState::Live),
        "{:?}",
        resumed.state
    );
    assert!(!handle.is_paused());
    assert_eq!(
        resumed.progress_bytes, progress_paused,
        "unpause keeps every piece that was ours"
    );
    assert_eq!(
        resumed.file_progress, paused.file_progress,
        "and keeps them in the same files"
    );

    // It picks the seeder back up on its own and carries on from there.
    let live = handle.live().expect("live again");
    wait_until(
        || match (live.stats_snapshot(), handle.stats().progress_bytes) {
            (s, progress) if s.peer_stats.live == 1 && progress > progress_paused => Ok(()),
            (s, progress) => bail!("waiting for the download to resume: {progress} {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();

    seeder.ratelimits.set_upload_bps(None);
    handle.wait_until_completed().await.unwrap();
    info!("finished after the pause");
}
