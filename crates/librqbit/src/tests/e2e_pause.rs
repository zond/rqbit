//! The two ways to stop a torrent fetching without dropping it, and what each costs.
//!
//! `Session::pause` stops everything and lets go of every peer; `unpause` resumes from
//! exactly the pieces that were there, without re-checking anything. Deselecting every
//! file instead stops the fetching alone and keeps the swarm, at the price of the torrent
//! reading as finished while it lasts.

use std::{collections::HashSet, net::Ipv4Addr, num::NonZeroU32, time::Duration};

use anyhow::bail;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
    torrent_state::{ManagedTorrentHandle, TorrentStateLive, TorrentStatsState},
};

const WAIT: Duration = Duration::from_secs(30);
/// Long enough that a torrent still talking to its seeder would have moved several
/// chunks at the rate limit below.
const STILLNESS: Duration = Duration::from_secs(3);

/// One rate-limited seeder and one client downloading from it, with no DHT and no
/// trackers, so the only peer the client will ever know is the one it is handed and a
/// disconnection is visible. Kept whole because dropping the sessions ends the transfer.
struct TwoSessions {
    seeder: std::sync::Arc<Session>,
    client: std::sync::Arc<Session>,
    handle: ManagedTorrentHandle,
    live: std::sync::Arc<TorrentStateLive>,
    _seeder_dir: tempfile::TempDir,
    _client_dir: tempfile::TempDir,
}

impl TwoSessions {
    async fn setup(prefix: &str) -> Self {
        setup_test_logging();

        let tempdir = create_default_random_dir_with_torrents(4, 1_000_000, Some(prefix));
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

        // Slow enough that the download is still going when the test interferes with it.
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

        let root = tempfile::TempDir::with_prefix(format!("{prefix}_client")).unwrap();
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

        let this = Self {
            seeder,
            client,
            handle,
            live,
            _seeder_dir: tempdir,
            _client_dir: root,
        };

        // A whole checked piece, not just bytes on the wire: what the tests below say
        // about the have-set is only about completed pieces.
        wait_until(
            || match (this.live.stats_snapshot(), this.handle.stats()) {
                (s, stats) if stats.progress_bytes > 0 && s.peer_stats.live == 1 => Ok(()),
                (s, stats) => bail!(
                    "waiting for the first piece: {} {s:?}",
                    stats.progress_bytes
                ),
            },
            WAIT,
        )
        .await
        .unwrap();
        assert!(
            !this.handle.stats().finished,
            "the rate limit keeps the download unfinished"
        );
        this
    }
}

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
    let t = TwoSessions::setup("rqbit_pause").await;
    let (client, handle) = (&t.client, &t.handle);
    let progress_before = handle.stats().progress_bytes;
    info!(progress_before, "downloading");

    // Pause. The live state goes away with every peer task in it.
    client.pause(handle).await.unwrap();
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
    assert!(client.pause(handle).await.is_err());
    info!(progress_paused, "paused");

    // Unpause. The have-set is exactly what the pause left: carried through
    // TorrentStatePaused's chunk tracker, never re-checked against the storage, so no
    // progress is lost and no piece is re-hashed.
    client.unpause(handle).await.unwrap();
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

    t.seeder.ratelimits.set_upload_bps(None);
    handle.wait_until_completed().await.unwrap();
    info!("finished after the pause");
}

#[tokio::test(flavor = "multi_thread")]
async fn deselecting_every_file_stops_fetching_without_pausing() {
    tokio::time::timeout(
        Duration::from_secs(180),
        deselecting_every_file_stops_fetching_without_pausing_inner(),
    )
    .await
    .expect("test timed out");
}

/// The other way to stop fetching: select no files at all. Nothing is queued, so nothing
/// is requested, and the torrent stays live -- not paused, still listening, still
/// announcing, its state machine untouched -- and putting the selection back starts it
/// again. It is not free of the swarm, though: wanting nothing makes the torrent
/// finished, and a finished torrent hangs up on every peer that has the whole thing, so
/// what comes back is a re-dial rather than a connection that was never dropped.
async fn deselecting_every_file_stops_fetching_without_pausing_inner() {
    let t = TwoSessions::setup("rqbit_deselect").await;
    let (client, handle, live) = (&t.client, &t.handle, &t.live);
    let files = t.handle.metadata.load_full().unwrap().file_infos.len();
    assert!(files > 1, "the fixture has several files");

    // Select nothing.
    client
        .update_only_files(handle, &HashSet::new())
        .await
        .unwrap();
    let stopped = handle.stats();
    assert!(
        matches!(stopped.state, TorrentStatsState::Live),
        "still live: {:?}",
        stopped.state
    );
    assert!(!handle.is_paused());
    assert!(
        stopped.finished,
        "with nothing selected there is nothing left to want"
    );

    // Nothing arrives while it sits there.
    let progress_stopped = stopped.progress_bytes;
    tokio::time::sleep(STILLNESS).await;
    assert_eq!(
        handle.stats().progress_bytes,
        progress_stopped,
        "a torrent that wants no file fetches nothing"
    );

    // What this does not promise is the swarm. Wanting nothing makes the torrent
    // finished, and the first piece to complete after that carries
    // `disconnect_all_peers_that_have_full_torrent` with it, which parks every peer that
    // holds the whole torrent as `NotNeeded`: there is nothing to exchange with one in
    // either direction. So the seeder here is connected or parked depending on whether a
    // piece landed in between, and it is a peer still downloading itself that would
    // certainly be kept.
    let peers = live.stats_snapshot().peer_stats;
    assert_eq!(peers.live + peers.not_needed, 1, "{peers:?}");
    // The torrent itself, though, is untouched: live, not paused, still listening and
    // announcing, nothing re-checked.
    assert!(!handle.is_paused());
    assert!(handle.live().is_some());
    info!(progress_stopped, peers = ?peers, "deselected");

    // Ask for everything again: `update_only_files` re-queues the parked peers itself, so
    // the seeder is dialled straight back and the download carries on.
    client
        .update_only_files(handle, &(0..files).collect())
        .await
        .unwrap();
    assert!(
        !handle.stats().finished,
        "wanting the files back makes it unfinished again"
    );
    wait_until(
        || match (live.stats_snapshot(), handle.stats().progress_bytes) {
            (s, progress) if progress > progress_stopped && s.peer_stats.live == 1 => Ok(()),
            (s, progress) => bail!("waiting for the download to resume: {progress} {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();

    t.seeder.ratelimits.set_upload_bps(None);
    handle.wait_until_completed().await.unwrap();
    info!("finished after the deselection");
}
