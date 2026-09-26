//! A thin swarm: one source, and what happens when it hangs up.
//!
//! The field reading this pins (stream-server `docs/thin-swarm-redial.md`): a torrent
//! whose only reachable peer disconnects sits silent for the third step of the reconnect
//! schedule -- six minutes -- although that peer served it and is back a moment later.
//! Here the seeder is the only peer; it is paused (which closes its connections) and
//! unpaused, three times over, and the client is expected back on it each time within
//! the flat starving retry rather than the exponential schedule's 10 s, 60 s, 360 s.
use std::{net::Ipv4Addr, num::NonZeroU32, sync::Arc, time::Duration};

use anyhow::bail;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
    torrent_state::{ManagedTorrentHandle, live::TorrentStateLive},
};

const WAIT: Duration = Duration::from_secs(30);
/// How long the client may take to be back on the seeder after an unpause. The
/// exponential schedule's second step alone (about a minute) overshoots it.
const REDIAL_BOUND: Duration = Duration::from_secs(10);

struct OneSource {
    seeder: Arc<Session>,
    seeder_handle: ManagedTorrentHandle,
    seeder_addr: std::net::SocketAddr,
    handle: ManagedTorrentHandle,
    live: Arc<TorrentStateLive>,
    _client: Arc<Session>,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

impl OneSource {
    async fn connect(prefix: &str) -> Self {
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

        // Throttled, so the download outlives the test and the torrent keeps wanting
        // pieces -- which is half of what "starving" means.
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
                AddTorrent::TorrentFileBytes(torrent_bytes),
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
        Self {
            seeder,
            seeder_handle,
            seeder_addr,
            handle,
            live,
            _client: client,
            _dirs: (tempdir, root),
        }
    }

    fn live_peers(&self) -> u32 {
        self.live.stats_snapshot().peer_stats.live
    }

    /// The first verified piece: from here the seeder is a *proven* peer.
    async fn wait_for_first_piece(&self) {
        wait_until(
            || match self.handle.stats() {
                s if s.progress_bytes > 0 && self.live_peers() == 1 => Ok(()),
                s => bail!("waiting for the first piece: {} bytes", s.progress_bytes),
            },
            WAIT,
        )
        .await
        .unwrap();
        assert!(
            !self.handle.stats().finished,
            "the rate limit keeps it unfinished"
        );
    }

    /// Pauses the seeder, which closes its connections, and waits for the client to
    /// have marked the peer dead.
    async fn hang_up(&self) {
        self.seeder.pause(&self.seeder_handle).await.unwrap();
        wait_until(
            || match self.live.retry_summary() {
                s if s.dead == 1 && self.live_peers() == 0 => Ok(()),
                s => bail!("waiting for the seeder to be dead in the table: {s:?}"),
            },
            WAIT,
        )
        .await
        .unwrap();
    }

    async fn come_back(&self) {
        self.seeder.unpause(&self.seeder_handle).await.unwrap();
    }

    async fn wait_redialled(&self, bound: Duration) {
        wait_until(
            || match self.live_peers() {
                1 => Ok(()),
                n => bail!("waiting for the client to be back on the seeder: {n} live"),
            },
            bound,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn a_proven_peer_that_hangs_up_is_redialled_on_the_flat_retry() {
    tokio::time::timeout(
        Duration::from_secs(180),
        a_proven_peer_that_hangs_up_is_redialled_on_the_flat_retry_inner(),
    )
    .await
    .expect("test timed out");
}

async fn a_proven_peer_that_hangs_up_is_redialled_on_the_flat_retry_inner() {
    let swarm = OneSource::connect("rqbit_thin_swarm_redial").await;
    let retry = Duration::from_secs(1);
    swarm.handle.set_starving_retry(4, retry);
    swarm.wait_for_first_piece().await;

    // Three deaths: the exponential schedule would wait 10 s, then about a minute, then
    // six minutes. Starving, with a proven peer, every wait is the flat retry.
    for death in 1..=3 {
        swarm.hang_up().await;
        assert!(
            swarm.live.is_starving(),
            "no live peer and pieces still wanted"
        );
        let summary = swarm.live.retry_summary();
        assert_eq!(summary.proven_dead, 1, "death {death}: {summary:?}");
        let next = summary.next_retry.expect("a wait is scheduled");
        assert!(
            next <= retry,
            "death {death}: the next dial is the flat retry, not the schedule: {next:?}"
        );
        swarm.come_back().await;
        swarm.wait_redialled(REDIAL_BOUND).await;
        info!(death, "back on the seeder");
    }
    assert!(
        swarm.handle.stats().progress_bytes > 0 && !swarm.handle.stats().finished,
        "still downloading from the one source"
    );
}

#[tokio::test]
async fn a_sighting_brings_a_dead_proven_peer_forward() {
    tokio::time::timeout(
        Duration::from_secs(120),
        a_sighting_brings_a_dead_proven_peer_forward_inner(),
    )
    .await
    .expect("test timed out");
}

async fn a_sighting_brings_a_dead_proven_peer_forward_inner() {
    let swarm = OneSource::connect("rqbit_thin_swarm_sighting").await;
    // A flat retry long enough that nothing re-dials on its own within the test.
    swarm.handle.set_starving_retry(4, Duration::from_secs(600));
    swarm.wait_for_first_piece().await;

    swarm.hang_up().await;
    let waiting = swarm.live.retry_summary().next_retry.unwrap();
    assert!(
        waiting > Duration::from_secs(60),
        "parked for the long wait: {waiting:?}"
    );
    swarm.come_back().await;

    // The tracker, the DHT or PEX names the address again: an address the table already
    // holds, which used to be ignored outright.
    assert!(
        swarm.live.add_peer_if_not_seen(swarm.seeder_addr).unwrap(),
        "the sighting queued a dial"
    );
    swarm.wait_redialled(REDIAL_BOUND).await;
    assert!(
        swarm.live.retry_summary().next_retry.is_none(),
        "nothing is waiting once it is back"
    );
}
