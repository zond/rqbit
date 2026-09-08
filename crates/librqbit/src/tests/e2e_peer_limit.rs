//! The live-peer cap of a running torrent moves at runtime: lowering it hangs up on the
//! surplus and keeps downloading with the rest, raising it brings the parked peers back, and
//! forgetting the disconnected peers takes them out of the table.
//!
//! Note what a raise does and does not promise. It hands the permits back, so incoming
//! connections are accepted again at once, and it re-queues every parked peer we have an
//! address to dial. A peer that dialled us and never told us where it listens has no such
//! address (see `Peer::reconnect_not_needed_peer`), so it comes back only when it redials.
//! Every client here is outgoing-only, which is the case the raise fully covers.

use std::{
    net::Ipv4Addr,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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

    /// How many connections the seeders themselves have. Our own peer counters cannot
    /// stand in for this: `queued` and `connecting` both move when the adder claims the
    /// table slot, a transition before the dial task has been polled, so a peer they
    /// account for may have no socket yet -- 3168e821 fixed the sibling test for waiting
    /// on exactly that. A connection the far end has is a socket that exists.
    fn far_end_live(&self) -> usize {
        self.seeders
            .iter()
            .map(|(_, h)| {
                h.live()
                    .map(|l| l.stats_snapshot().peer_stats.live)
                    .unwrap_or(0) as usize
            })
            .sum()
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
async fn swarm(prefix: &str, seeders: usize, bytes_per_file: usize) -> Swarm {
    let tempdir = create_default_random_dir_with_torrents(4, bytes_per_file, Some(prefix));
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

    let swarm = swarm("rqbit_peer_limit", SEEDERS, 1_000_000).await;
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
    // A slot the lowering could not take off the semaphore is paid back by the peer that
    // held it, as it hangs up. Once they all have, the debt is nil and the peers that are
    // left hold every slot there is.
    wait_until(
        || match live.peer_permit_accounting() {
            (0, 0) => Ok(()),
            (free, debt) => bail!("waiting for the slots to settle: {free} free, {debt} owed"),
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

    let swarm = swarm("rqbit_peer_limit_raise", SEEDERS, 1_000_000).await;

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

    let swarm = swarm("rqbit_peer_limit_forget", SEEDERS, 8_000_000).await;

    // Wait until more peers hold a reserved piece than the cap is about to keep. The
    // parked ones then include at least one that has pieces to strand -- without that this
    // test can pass by proving nothing, which is asserted below.
    wait_until(
        || {
            let owners: std::collections::HashSet<_> = swarm
                .live
                .inflight_piece_owners()
                .into_iter()
                .map(|(_, owner)| owner)
                .collect();
            match owners.len() {
                n if n > LOWERED => Ok(()),
                n => bail!("waiting for more than {LOWERED} peers to reserve a piece, {n} have"),
            }
        },
        WAIT,
    )
    .await
    .unwrap();

    // Who holds what, before any of it moves.
    let held = swarm.live.inflight_piece_owners();

    // No await between these two lines either: the tasks whose entries are removed here
    // are all still running.
    swarm.handle.set_peer_limit(LOWERED);
    let forgotten = swarm.live.forget_disconnected_peers();
    assert_eq!(forgotten, SEEDERS - LOWERED);

    // Of the pieces held a moment ago, the ones whose owner has just been forgotten. Those
    // are what a task with no entry left to find has to hand back, and asking after the
    // snapshot rather than of the table now keeps the count from depending on how quickly
    // those tasks get there.
    let table = swarm.live.peer_addresses();
    let at_risk = held.iter().filter(|(_, o)| !table.contains(o)).count();
    assert!(
        at_risk > 0,
        "the forgotten peers held no pieces, so this test proves nothing"
    );
    info!(at_risk, "forgot the peers the cap parked");

    // Wait for those connections to close, seen from the other end, then let the tasks that
    // owned them finish reporting their death.
    wait_until(
        || match swarm.far_end_live() {
            LOWERED => Ok(()),
            other => bail!("waiting for the parked connections to close, {other} left"),
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

/// Listeners that accept a connection and then never speak a word of the protocol, so a
/// peer dialling one wedges waiting for a handshake until it times out.
struct Tarpits {
    addrs: Vec<std::net::SocketAddr>,
    /// Connections accepted, ever.
    accepted: Arc<AtomicUsize>,
    /// Of those, the ones the other end has since hung up on.
    closed: Arc<AtomicUsize>,
}

async fn tarpits(n: usize) -> Tarpits {
    let mut addrs = Vec::new();
    let accepted = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    for _ in 0..n {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        addrs.push(listener.local_addr().unwrap());
        let (accepted, closed) = (accepted.clone(), closed.clone());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::Relaxed);
                let closed = closed.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    let mut buf = [0u8; 1024];
                    while let Ok(read) = sock.read(&mut buf).await {
                        if read == 0 {
                            break;
                        }
                    }
                    closed.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }
    Tarpits {
        addrs,
        accepted,
        closed,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parked_dial_gives_its_slot_back_at_once() {
    tokio::time::timeout(
        Duration::from_secs(120),
        a_parked_dial_gives_its_slot_back_at_once_inner(),
    )
    .await
    .expect("test timed out");
}

/// Lowering the cap has to shed peers that are still connecting, and they are the ones it
/// sheds first: they have proven nothing. But a dial hears nothing we send it until its
/// handshake is done, so a host that accepts the connection and then says nothing holds
/// its slot for a connect timeout and two read timeouts -- here 30 seconds, and in that
/// time a backgrounded app has neither its memory back nor, on the way out of the
/// background, a slot to dial anyone else with.
async fn a_parked_dial_gives_its_slot_back_at_once_inner() {
    setup_test_logging();

    const TARPITS: usize = 4;
    const KEPT: usize = 1;

    let Tarpits {
        addrs,
        closed,
        accepted,
    } = tarpits(TARPITS).await;

    let tempdir = create_default_random_dir_with_torrents(1, 100_000, Some("rqbit_tarpit"));
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
    let root = tempfile::TempDir::with_prefix("rqbit_tarpit_client").unwrap();
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
            AddTorrent::TorrentFileBytes(torrent_file.as_bytes().unwrap()),
            Some(AddTorrentOptions {
                initial_peers: Some(addrs.clone()),
                peer_limit: Some(TARPITS),
                overwrite: true,
                peer_opts: Some(crate::PeerConnectionOptions {
                    // Long enough that nothing here can be explained by a timeout.
                    connect_timeout: Some(Duration::from_secs(30)),
                    read_write_timeout: Some(Duration::from_secs(30)),
                    ..Default::default()
                }),
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

    // Wait for the far end, not for our own count of it. `connecting` is written when the
    // adder claims the table slot, which is before the dial task has run, let alone opened
    // a socket -- and a dial cancelled before it connects leaves nothing at the far end to
    // close, so `closed` below could never reach `TARPITS - KEPT` and the wait for it would
    // run out. The listeners know: a connection they have accepted is a socket that exists.
    wait_until(
        || {
            match (
                live.stats_snapshot().peer_stats,
                accepted.load(Ordering::Relaxed),
            ) {
                (s, a) if s.connecting as usize == TARPITS && a == TARPITS => Ok(()),
                (s, a) => bail!(
                    "waiting for every dial to reach a listener and wedge in the handshake: {s:?}, {a} accepted"
                ),
            }
        },
        WAIT,
    )
    .await
    .unwrap();
    assert_eq!(closed.load(Ordering::Relaxed), 0);

    handle.set_peer_limit(KEPT);
    wait_until(
        || match closed.load(Ordering::Relaxed) {
            n if n == TARPITS - KEPT => Ok(()),
            n => bail!("waiting for the parked dials to drop their sockets, {n} so far"),
        },
        // A fifth of the timeout that would end them on its own.
        Duration::from_secs(6),
    )
    .await
    .unwrap();
    let stats = live.stats_snapshot().peer_stats;
    assert_eq!(stats.connecting as usize, KEPT, "{stats:?}");
    assert_eq!(stats.not_needed as usize, TARPITS - KEPT, "{stats:?}");
    info!(stats = ?stats, "the parked dials let go of their slots");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_raise_dials_the_peers_it_parked_before_the_backlog() {
    tokio::time::timeout(
        Duration::from_secs(180),
        a_raise_dials_the_peers_it_parked_before_the_backlog_inner(),
    )
    .await
    .expect("test timed out");
}

/// While the cap is low the adder drains nothing: it takes one address and then waits for
/// a slot, so everything a tracker, the DHT or PEX names in the meantime piles up behind
/// it. On the way back up, those guesses must not be dialled ahead of the peers the cap
/// parked, which are proven and are the reason the raise happened. The one the adder is
/// holding when the raise comes is a guess as much as the queued ones are, and waits its
/// turn with them.
async fn a_raise_dials_the_peers_it_parked_before_the_backlog_inner() {
    setup_test_logging();

    let swarm = swarm("rqbit_peer_limit_requeue", SEEDERS, 8_000_000).await;

    // A swarm naming addresses at a client with no slot to spare: every peer it has is one
    // it wants, so these pile up undialled and the adder takes the first of them and stands
    // on the semaphore holding it. Named before the cap moves so that it is holding one for
    // certain by the time the raise comes, and so that nothing but the raise itself sits
    // between the backlog arriving and the slots being handed out. Every one of them
    // accepts and then says nothing, so a dial spent on one is a slot wedged for the read
    // timeout.
    let backlog = tarpits(SEEDERS * 4).await;
    for addr in &backlog.addrs {
        assert!(swarm.live.add_peer_if_not_seen(*addr).unwrap());
    }

    swarm.handle.set_peer_limit(LOWERED);
    wait_until(
        || match swarm.peer_stats() {
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
    // And for the seeders to lose those connections, so that every socket the far end
    // reports from here on is one this raise made.
    wait_until(
        || match swarm.far_end_live() {
            LOWERED => Ok(()),
            n => bail!("waiting for the seeders to lose the parked connections, {n} still up"),
        },
        WAIT,
    )
    .await
    .unwrap();

    let dialled_before = backlog.accepted.load(Ordering::Relaxed);
    swarm.handle.set_peer_limit(SEEDERS);
    // The raise frees `SEEDERS - LOWERED` slots, and the count below is done when every
    // one of them has reached a far end: a seeder that has the connection, or a tarpit
    // that accepted one. That is the whole point of asking the far ends rather than our
    // own counters, which is what this waited on before: `queued` drops in
    // `mark_peer_connecting`, one transition before the dial task is polled, so it fell
    // to the backlog's size while a slot handed to a guess was still an unopened socket,
    // and the `dialled` count below read 0 for a dial that had not landed yet. Measured
    // on a `reconnect_all_not_needed_peers` mutated to skip one parked peer -- a real
    // regression of the property asserted here, with the guess winning the last slot
    // rather than the first: the old wait let it through 9 times in 80 runs.
    wait_until(
        || {
            let dialled = backlog.accepted.load(Ordering::Relaxed) - dialled_before;
            // `>=`, not `==`: a slot released and spent again inside the window would put
            // more sockets at the far ends than the raise handed out, and waiting for the
            // exact number would then wait until it ran out.
            match swarm.far_end_live() + dialled {
                n if n >= SEEDERS => Ok(()),
                n => bail!(
                    "waiting for the raise's slots to reach a far end: {n} of {SEEDERS} \
                     sockets, {dialled} of them the backlog's"
                ),
            }
        },
        // Well inside the read timeout a wedged dial would otherwise hold a slot for.
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    // Not one of them: every slot the raise gave out went to a peer we had already talked
    // to, and the address the adder was holding waits its turn with the queued ones. Asked
    // of the other end, which knows for certain, where the peer counters cannot tell a
    // re-queued peer from a fresh guess. Nor can a dial started earlier be landing only
    // now: until this raise every slot was either held by a peer we were keeping or on loan
    // against the debt the lowering booked, so the adder never had one to spend. Nine went
    // this way before the parked peers had a queue of their own, three more while the raise
    // wrote off that debt before filling the queue, and one for as long as the adder dialled
    // whatever it happened to be holding.
    let dialled = backlog.accepted.load(Ordering::Relaxed) - dialled_before;
    assert_eq!(
        dialled, 0,
        "the raise gave {dialled} of its slots to the backlog, ahead of the peers it parked"
    );
    info!(stats = ?swarm.peer_stats(), "the proven peers came back first");
}
