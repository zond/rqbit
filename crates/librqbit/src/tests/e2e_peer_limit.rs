//! The live-peer cap of a running torrent moves at runtime: lowering it hangs up on the
//! surplus and keeps downloading with the rest, raising it brings the parked peers back, and
//! forgetting the disconnected peers takes them out of the table.
//!
//! Note what a raise does and does not promise. It hands the permits back, so incoming
//! connections are accepted again at once, and it re-queues every parked peer we have an
//! address to dial. A peer that dialled us and never told us where it listens has no such
//! address (see `Peer::reconnect_not_needed_peer`), so it comes back only when it redials.
//! Every client here is outgoing-only, which is the case the raise fully covers.

use std::{net::Ipv4Addr, num::NonZeroU32, sync::Arc, time::Duration};

use anyhow::bail;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
    torrent_state::{
        ManagedTorrentHandle,
        live::{TorrentStateLive, peers::stats::AggregatePeerStats},
    },
};

const SEEDERS: usize = 12;
const LOWERED: usize = 3;
const WAIT: Duration = Duration::from_secs(30);

/// Every peer the table knows, whatever its state.
fn table_size(stats: &AggregatePeerStats) -> usize {
    (stats.queued + stats.connecting + stats.live + stats.dead + stats.not_needed) as usize
}

/// A client downloading from `seeders` slow seeders and knowing of no other peer.
struct Swarm {
    seeders: Vec<(Arc<Session>, ManagedTorrentHandle)>,
    handle: ManagedTorrentHandle,
    live: Arc<TorrentStateLive>,
    // Dropped last: the client session and the directories the torrent lives in.
    _client: Arc<Session>,
    _tempdirs: (tempfile::TempDir, tempfile::TempDir),
}

impl Swarm {
    fn peer_stats(&self) -> AggregatePeerStats {
        self.live.stats_snapshot().peer_stats
    }

    /// Take the rate limit off the seeders so the client can finish.
    fn unthrottle(&self) {
        for (seeder, _) in &self.seeders {
            seeder.ratelimits.set_upload_bps(None);
        }
    }
}

/// Seeders on ephemeral loopback ports, no DHT, no trackers: the client learns of them
/// through `initial_peers` and nothing else, so a peer it forgets is gone for good. Each
/// uploads slowly, so the download outlasts what is asserted about its peers.
async fn swarm(prefix: &str, seeders: usize) -> Swarm {
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

    let mut sessions = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..seeders {
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
        sessions.push((session, handle));
    }

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
                initial_peers: Some(peers.clone()),
                peer_limit: Some(seeders),
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
    assert_eq!(live.peer_limit(), seeders, "the add option is the cap");

    let swarm = Swarm {
        seeders: sessions,
        handle,
        live,
        _client: client,
        _tempdirs: (tempdir, root),
    };
    wait_until(
        || match swarm.peer_stats() {
            s if s.live as usize == seeders => Ok(()),
            s => bail!("waiting for every seeder to connect: {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    info!(stats = ?swarm.peer_stats(), "all seeders connected");
    swarm
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

    let swarm = swarm("rqbit_peer_limit", SEEDERS).await;
    let Swarm {
        ref handle,
        ref live,
        ..
    } = swarm;
    let peer_stats = || swarm.peer_stats();

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
    // Every permit the lowering could not take off the semaphore has been paid back by
    // the peer that held it, and the peers that are left hold all the slots there are.
    assert_eq!(
        live.peer_permit_accounting(),
        (0, 0),
        "free slots and the debt a lowered cap left"
    );

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
    swarm.unthrottle();
    handle.wait_until_completed().await.unwrap();
    let stats = peer_stats();
    assert!(
        stats.live as usize <= LOWERED,
        "nobody re-dialled the forgotten peers: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn raising_the_cap_before_the_surplus_hangs_up_keeps_every_peer() {
    tokio::time::timeout(
        Duration::from_secs(180),
        raising_the_cap_before_the_surplus_hangs_up_keeps_every_peer_inner(),
    )
    .await
    .expect("test timed out");
}

/// A background/foreground toggle at its tightest: the cap goes down and straight back up,
/// with no chance in between for the peers it parked to notice they were asked to leave.
///
/// The raise re-queues their addresses underneath them, so each of those still-running
/// tasks finds its own entry `Queued` when it finally dies -- a state it does not own.
/// Leaving it alone costs nothing; taking it out of the table costs the peer for good,
/// because with `initial_peers` and no tracker or DHT nothing ever names that address
/// again, and the dial waiting on the queue then finds no entry to dial.
async fn raising_the_cap_before_the_surplus_hangs_up_keeps_every_peer_inner() {
    setup_test_logging();

    let swarm = swarm("rqbit_peer_limit_raise", SEEDERS).await;

    // No await between these two lines. Every parked task is still alive.
    swarm.handle.set_peer_limit(LOWERED);
    let parked = swarm.peer_stats();
    swarm.handle.set_peer_limit(SEEDERS);
    assert_eq!(
        parked.not_needed as usize,
        SEEDERS - LOWERED,
        "the surplus was parked before the raise: {parked:?}"
    );
    assert_eq!(table_size(&parked), SEEDERS, "{parked:?}");

    wait_until(
        || match swarm.peer_stats() {
            s if s.live as usize == SEEDERS && s.not_needed == 0 => Ok(()),
            s => bail!("waiting for every peer to come back after the toggle: {s:?}"),
        },
        WAIT,
    )
    .await
    .unwrap();
    let stats = swarm.peer_stats();
    assert_eq!(
        table_size(&stats),
        SEEDERS,
        "nobody was dropped from the table: {stats:?}"
    );
    info!(stats = ?stats, "the whole swarm came back");

    swarm.unthrottle();
    swarm.handle.wait_until_completed().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn forgetting_a_parked_peer_strands_none_of_its_pieces() {
    tokio::time::timeout(
        Duration::from_secs(180),
        forgetting_a_parked_peer_strands_none_of_its_pieces_inner(),
    )
    .await
    .expect("test timed out");
}

/// Lower the cap, then forget the peers it parked -- the pairing the two APIs exist for.
///
/// Forgetting takes their entries out of the table while their tasks are still winding
/// down, so each of those tasks reaches its exit with pieces reserved and no entry to find.
/// It has to hand them back regardless: a piece that is neither queued nor owned by a live
/// peer is downloaded again only if some peer's steal timer happens to reach for it, which
/// at the end of a download may never happen.
async fn forgetting_a_parked_peer_strands_none_of_its_pieces_inner() {
    setup_test_logging();

    let swarm = swarm("rqbit_peer_limit_forget", SEEDERS).await;

    // Wait until the peers have actually reserved pieces, or there is nothing to strand.
    wait_until(
        || match swarm.live.stats_snapshot().fetched_bytes {
            0 => bail!("waiting for the download to start"),
            _ => Ok(()),
        },
        WAIT,
    )
    .await
    .unwrap();

    // No await between these two lines either: the tasks whose entries are removed here
    // are all still running.
    swarm.handle.set_peer_limit(LOWERED);
    let forgotten = swarm.live.forget_disconnected_peers();
    let at_risk = swarm.live.ownerless_inflight_pieces();
    assert_eq!(forgotten, SEEDERS - LOWERED);
    assert!(
        !at_risk.is_empty(),
        "the forgotten peers held no pieces, so this test proves nothing"
    );
    info!(at_risk = at_risk.len(), "forgot the peers the cap parked");

    // Wait for those connections to close, seen from the other end, then let the tasks that
    // owned them finish reporting their death.
    wait_until(
        || {
            let live: u32 = swarm
                .seeders
                .iter()
                .map(|(_, h)| h.live().map(|l| l.stats_snapshot().peer_stats.live))
                .map(|l| l.unwrap_or(0))
                .sum();
            match live as usize {
                LOWERED => Ok(()),
                other => bail!("waiting for the parked connections to close, {other} left"),
            }
        },
        WAIT,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let stranded = swarm.live.ownerless_inflight_pieces();
    assert!(
        stranded.is_empty(),
        "pieces left in flight for peers the table no longer has: {stranded:?}"
    );

    swarm.unthrottle();
    swarm.handle.wait_until_completed().await.unwrap();
}
