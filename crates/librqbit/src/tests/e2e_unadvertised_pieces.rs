//! What a peer sees of a torrent that holds pieces back.
//!
//! Two paths carry the have-set to a peer, and a held-back piece has to be missing from
//! both: the bitfield sent at handshake, and the Have broadcast a completed piece sets
//! off. These tests drive them over real connections between real sessions, so a leak
//! shows up as the other side learning of a piece it was never told about - and, given
//! long enough, downloading it.

use std::{net::Ipv4Addr, ops::Range, time::Duration};

use anyhow::Context;
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::{io::AsyncReadExt, time::timeout};
use tracing::info;

use crate::{
    AddTorrent, CreateTorrentOptions, ManagedTorrent, ManagedTorrentState, Session, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{TestPeerMetadata, setup_test_logging},
    torrent_state::live::peer::stats::snapshot::{PeerStatsFilter, PeerStatsFilterState},
};

use super::test_util::create_default_random_dir_with_torrents;

const PIECE_LEN: u32 = CHUNK_SIZE;
const TOTAL_PIECES: u32 = 16;
const FILE_SIZE: usize = (PIECE_LEN * TOTAL_PIECES) as usize;
// The first half stands in for a playback window: pieces we have and read, and are about
// to reclaim, so nobody should hear about them.
const HELD_BACK: Range<u32> = 0..TOTAL_PIECES / 2;

type Client = (std::sync::Arc<Session>, std::sync::Arc<ManagedTorrent>);

// A session that has the whole torrent and listens for peers, plus the torrent file, the
// directory it was made from, and the address to dial it on.
async fn seeder(prefix: &str) -> anyhow::Result<(TempDir, Vec<u8>, Client, std::net::SocketAddr)> {
    let files = create_default_random_dir_with_torrents(1, FILE_SIZE, Some(prefix));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LEN),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let torrent_bytes = torrent.as_bytes()?.to_vec();

    let session = Session::new_with_opts(
        files.path().into(),
        crate::SessionOptions {
            dht: None,
            persistence: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            listen: Some(crate::listen::ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .context("error creating seeder session")?;

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;

    let addr = session
        .listen_addr()
        .context("expected listen_addr to be set")?;
    Ok((files, torrent_bytes, (session, handle), addr))
}

// A session with nothing, that knows one peer and has no other way to find any: no DHT,
// no trackers in the torrent. Whatever it ends up with, it got from that peer.
async fn leecher(
    dir: &TempDir,
    torrent_bytes: &[u8],
    peer: std::net::SocketAddr,
) -> anyhow::Result<Client> {
    let session = Session::new_with_opts(
        dir.path().into(),
        crate::SessionOptions {
            dht: None,
            persistence: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.to_owned()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    Ok((session, handle))
}

fn have_pieces(handle: &ManagedTorrent) -> anyhow::Result<Vec<u32>> {
    handle.with_chunk_tracker(|ct| {
        ct.get_have_pieces().as_slice()[..TOTAL_PIECES as usize]
            .iter_ones()
            .filter_map(|id| u32::try_from(id).ok())
            .collect()
    })
}

async fn wait_for_pieces(handle: &ManagedTorrent, want: &[u32]) -> anyhow::Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if have_pieces(handle)? == want {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}

// The bitfield path: a peer sees what we announce and only that, while the pieces we
// held back stay ours to read and to serve. It has no other source, so a piece from the
// held-back half could only come from us having told it.
async fn e2e_unadvertised_pieces() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (_seeder_session, seeder), addr) =
        seeder("test_unadvertised_pieces").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    // Held back before anyone connects, so the bitfield sent at handshake is the first
    // thing that has to be short.
    assert_eq!(
        seeder.set_pieces_advertised(HELD_BACK, false)?,
        HELD_BACK.len()
    );
    // Saying it twice changes nothing: a caller tracking a playhead re-states its window.
    assert_eq!(seeder.set_pieces_advertised(HELD_BACK, false)?, 0);

    // We still have every piece, we are still finished, and the pieces we held back are
    // still ours to hand over if a peer asks for one anyway. Not advertising is not
    // refusing: there is nothing to refuse, we have the piece.
    assert!(seeder.stats().finished);
    assert_eq!(have_pieces(&seeder)?, (0..TOTAL_PIECES).collect::<Vec<_>>());
    let lengths = *seeder
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    seeder.with_chunk_tracker(|ct| {
        for id in HELD_BACK {
            let piece = lengths.validate_piece_index(id).unwrap();
            let chunk = lengths
                .chunk_info_from_received_data(piece, 0, PIECE_LEN)
                .unwrap();
            assert!(
                ct.is_chunk_ready_to_upload(&chunk),
                "held back piece {id} became unservable"
            );
        }
    })?;

    // And still readable, which is the reason a piece is kept `have` in the first place.
    let mut stream = seeder.clone().stream(0).await?;
    let mut read = Vec::new();
    stream.read_to_end(&mut read).await?;
    assert_eq!(read, orig_content);

    info!("holding back pieces {HELD_BACK:?}, starting the peer");

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;

    let advertised = (HELD_BACK.end..TOTAL_PIECES).collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;
    assert!(!leecher.stats().finished);

    // Let it try for a while: a Have that leaked out late would show up here, and this is
    // long enough for the peer to have asked for the piece and got it.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        have_pieces(&leecher)?,
        advertised,
        "the peer got a piece we never announced"
    );
    assert!(!leecher.stats().finished);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), e2e_unadvertised_pieces()).await?
}

// The hook a caller has when the set has to be in force before the torrent says anything:
// pause it, hold the pieces back, unpause. The set lives in the chunk tracker, which is
// what a pause keeps, so the first bitfield after unpausing is already short.
async fn e2e_unadvertised_pieces_applied_while_paused() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, torrent_bytes, (seeder_session, seeder), addr) =
        seeder("test_unadvertised_pieces_paused").await?;

    seeder_session.pause(&seeder).await?;
    assert!(seeder.is_paused());
    // Nobody to tell and nothing to tell them on: a paused torrent has no peers.
    assert_eq!(
        seeder.set_pieces_advertised(HELD_BACK, false)?,
        HELD_BACK.len()
    );
    seeder_session.unpause(&seeder).await?;
    timeout(Duration::from_secs(30), seeder.wait_until_completed()).await??;

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_paused_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;

    let advertised = (HELD_BACK.end..TOTAL_PIECES).collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        have_pieces(&leecher)?,
        advertised,
        "the peer got a piece held back before the torrent was ever live"
    );
    assert!(!leecher.stats().finished);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_applied_while_paused() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_applied_while_paused(),
    )
    .await?
}

// How many times we have dialled this peer, and how many times talking to it went wrong.
// A peer we learn from over the connection we already had moves neither.
fn peer_connection_counters(
    handle: &ManagedTorrent,
    peer: std::net::SocketAddr,
) -> anyhow::Result<(u32, u32)> {
    let live = handle.live().context("expected a live torrent")?;
    // Every state, not just live: a torrent that has just finished parks the seeders it
    // no longer needs, and this is read on both sides of that.
    let stats = live.per_peer_stats_snapshot(PeerStatsFilter {
        state: PeerStatsFilterState::All,
    });
    let peer = stats
        .peers
        .get(&peer.to_string())
        .context("expected the peer to be in the peer table")?;
    Ok((peer.counters.connection_attempts, peer.counters.errors))
}

// The other half: a piece put back has to reach the peers that are already connected.
// Their handshake bitfield came without it, so the only thing that can tell them is a
// Have - and this asserts they got it that way, over the connection they already had,
// rather than by the connection dying and the fresh handshake covering for it.
async fn e2e_unadvertised_pieces_come_back() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (_seeder_session, seeder), addr) =
        seeder("test_unadvertised_pieces_come_back").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    seeder.set_pieces_advertised(HELD_BACK, false)?;

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_come_back_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;
    let advertised = (HELD_BACK.end..TOTAL_PIECES).collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;

    let before = peer_connection_counters(&leecher, addr)?;

    info!("advertising {HELD_BACK:?} again");

    // The window has moved on and these pieces are staying, so announce them.
    assert_eq!(
        seeder.set_pieces_advertised(HELD_BACK, true)?,
        HELD_BACK.len()
    );
    assert_eq!(seeder.set_pieces_advertised(HELD_BACK, true)?, 0);

    timeout(Duration::from_secs(30), leecher.wait_until_completed()).await??;
    assert_eq!(
        std::fs::read(leecher_dir.path().join("0.data")).unwrap(),
        orig_content
    );
    assert_eq!(
        peer_connection_counters(&leecher, addr)?,
        before,
        "the peer only got the pieces after redialling us, so the Have did not reach it"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_come_back() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_come_back(),
    )
    .await?
}

// The default path, which is every torrent that never calls set_pieces_advertised: a peer
// sees the have-set, whole, exactly as it did before any of this existed.
async fn e2e_unadvertised_pieces_default_is_unchanged() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (_seeder_session, seeder), addr) =
        seeder("test_unadvertised_pieces_default").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_default_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;
    timeout(Duration::from_secs(30), leecher.wait_until_completed()).await??;
    assert_eq!(
        std::fs::read(leecher_dir.path().join("0.data")).unwrap(),
        orig_content
    );

    // Nothing was ever held back, so what we announce is the have-bitfield itself and
    // putting pieces "back" is a no-op rather than an allocation.
    assert_eq!(seeder.set_pieces_advertised(0..u32::MAX, true)?, 0);
    seeder.with_chunk_tracker(|ct| {
        assert_eq!(
            ct.advertised_pieces_bytes().as_ref(),
            ct.get_have_pieces().as_bytes()
        );
    })?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_default_is_unchanged() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_default_is_unchanged(),
    )
    .await?
}

// A session that starts with nothing, knows nobody, and listens - so a peer can connect
// to it and watch what it announces while it fills up. Nothing reaches it until the test
// hands it a peer.
async fn middle(
    prefix: &str,
    torrent_bytes: &[u8],
) -> anyhow::Result<(TempDir, Client, std::net::SocketAddr)> {
    let dir = TempDir::with_prefix(prefix)?;
    let session = Session::new_with_opts(
        dir.path().into(),
        crate::SessionOptions {
            dht: None,
            persistence: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            listen: Some(crate::listen::ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.to_owned()),
            Some(crate::AddTorrentOptions {
                paused: false,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    let addr = session
        .listen_addr()
        .context("expected listen_addr to be set")?;
    Ok((dir, (session, handle), addr))
}

// How many peers this torrent is connected to right now, and how many of them it thinks
// have the whole torrent. The second number is what a peer's Haves add up to: it moves
// the moment one arrives, without waiting for anything to be asked for or sent.
fn live_peers(handle: &ManagedTorrent) -> anyhow::Result<(u32, u32)> {
    let stats = handle
        .live()
        .context("expected a live torrent")?
        .stats_snapshot();
    Ok((stats.peer_stats.live, stats.peer_stats.live_seeders))
}

// Wait until this torrent has `n` peers connected, so what happens next happens on
// connections that are already open.
async fn wait_for_live_peers(handle: &ManagedTorrent, n: u32) -> anyhow::Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if handle.live().is_some() && live_peers(handle)?.0 >= n {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}

// The Have path, which the bitfield path cannot reach: a peer that is already connected
// when a held-back piece completes. It got no bitfield - we had nothing to send one about
// - so a Have is the only thing that could tell it, and there must not be one.
//
// Three sessions, because the piece has to complete on the session being watched: a
// seeder, a middle that downloads from it with everything held back, and a watcher that
// knows only the middle and so can have learnt nothing anywhere else.
async fn e2e_unadvertised_pieces_completing_while_held_back() -> anyhow::Result<()> {
    setup_test_logging();
    let (seeder_files, torrent_bytes, (_seeder_session, _seeder), seeder_addr) =
        seeder("test_unadvertised_pieces_completing").await?;
    let orig_content = std::fs::read(seeder_files.path().join("0.data")).unwrap();

    // Held back before the middle has a single piece, and before it has anywhere to get
    // one: every piece it ever completes, it completes held back.
    let (_middle_dir, (_middle_session, middle), middle_addr) =
        middle("test_unadvertised_pieces_completing_middle", &torrent_bytes).await?;
    assert_eq!(
        middle.set_pieces_advertised(0..TOTAL_PIECES, false)?,
        TOTAL_PIECES as usize
    );
    assert_eq!(have_pieces(&middle)?, Vec::<u32>::new());

    // The watcher connects while there is still nothing to announce, so the handshake
    // bitfield tells it nothing and cannot be what tells it anything later.
    let watcher_dir = TempDir::with_prefix("test_unadvertised_pieces_completing_watcher")?;
    let (_watcher_session, watcher) = leecher(&watcher_dir, &torrent_bytes, middle_addr).await?;
    // Both sides, and the middle's side is the one that matters: it must have the watcher
    // to broadcast to before it has a piece to broadcast about.
    wait_for_live_peers(&watcher, 1).await?;
    wait_for_live_peers(&middle, 1).await?;
    assert_eq!(
        have_pieces(&middle)?,
        Vec::<u32>::new(),
        "the middle got a piece before the watcher was connected"
    );

    info!("watcher is connected, giving the middle a seeder");

    // Only now does the middle get a source. Every piece completes with the watcher on
    // the other end of a connection that is already open.
    middle
        .live()
        .context("expected a live torrent")?
        .add_peer_if_not_seen(seeder_addr)?;
    timeout(Duration::from_secs(30), middle.wait_until_completed()).await??;
    assert_eq!(have_pieces(&middle)?, (0..TOTAL_PIECES).collect::<Vec<_>>());

    // Long enough for a Have queued behind the completion to have gone out, been asked
    // about and answered. Watched throughout rather than sampled at the end: a Have moves
    // the watcher's picture of us the instant it lands, and it would move back if the
    // connection were replaced by a fresh handshake.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        assert_eq!(
            live_peers(&watcher)?.1,
            0,
            "the watcher was told about a piece that completed while held back"
        );
        assert_eq!(
            have_pieces(&watcher)?,
            Vec::<u32>::new(),
            "the watcher got a piece that completed while held back"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Silence, not a dead peer: it spent all of that connected to the session that had
    // every piece and said nothing.
    assert_eq!(live_peers(&watcher)?.0, 1);

    info!("nothing leaked, now advertising the lot");

    // The control, and the reason the silence above means something: the same connection
    // carries every one of those pieces the moment they are put back.
    assert_eq!(
        middle.set_pieces_advertised(0..TOTAL_PIECES, true)?,
        TOTAL_PIECES as usize
    );
    timeout(Duration::from_secs(30), watcher.wait_until_completed()).await??;
    assert_eq!(
        std::fs::read(watcher_dir.path().join("0.data")).unwrap(),
        orig_content
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_completing_while_held_back() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_completing_while_held_back(),
    )
    .await?
}

// Whether the Have path can skip the state lock, which is the whole cost of this feature
// for a torrent that never uses it.
fn gate(handle: &ManagedTorrent) -> bool {
    handle
        .shared
        .unadvertised_pieces
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn anything_held_back(handle: &ManagedTorrent) -> anyhow::Result<bool> {
    handle.with_chunk_tracker(|ct| ct.has_unadvertised_pieces())
}

// The gate is allowed to be true with nothing held back - it costs a lock and the lock
// gives the right answer. It is not allowed to be false while something is held back:
// false is the fast path that never looks at the set, so a Have would go out for a piece
// the caller is holding back.
//
// That holds only if both writes to the gate happen inside the write guard on
// ManagedTorrent::locked that the call takes and keeps around the change to the set. That
// guard is not the lock the set itself changes under - it is held around it - but it is
// what keeps two callers off each other. This is the half of it a single caller can show:
// the gate must not move before the guard is taken, because a caller already holding it
// can be about to compute "nothing held back" and store that over the top.
async fn e2e_unadvertised_pieces_gate_waits_for_the_lock() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, _torrent_bytes, (_session, handle), _addr) =
        seeder("test_unadvertised_pieces_gate").await?;
    assert!(!gate(&handle));

    // All sync: holding the state lock across an await would stall the whole runtime.
    tokio::task::block_in_place(|| -> anyhow::Result<()> {
        let g = handle.locked.write();
        let holder = std::thread::spawn({
            let handle = handle.clone();
            move || handle.set_pieces_advertised(HELD_BACK, false)
        });
        // Long enough for it to have got as far as it is going to get, which is the lock.
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !gate(&handle),
            "the gate went up before the lock the call holds around the change to the set"
        );
        drop(g);
        holder
            .join()
            .map_err(|_| anyhow::anyhow!("the holding-back thread panicked"))??;
        Ok(())
    })?;

    assert!(gate(&handle));
    assert!(anything_held_back(&handle)?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_gate_waits_for_the_lock() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_gate_waits_for_the_lock(),
    )
    .await?
}

// And the other half, which needs two callers: one holding pieces back while another puts
// pieces back. Whichever of them changes the set last must be the one whose answer the
// gate ends up with. Store the gate outside that guard and it is not: the advertiser can
// read "nothing held back" off a set the other thread has not touched yet, and write that
// after the other thread has held pieces back.
//
// A leak like that is not a moment, it is a state: the gate stays wrong until the next
// call to this API, which is why looking after the threads are done finds it.
async fn e2e_unadvertised_pieces_gate_survives_two_callers() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, _torrent_bytes, (_session, handle), _addr) =
        seeder("test_unadvertised_pieces_race").await?;

    // Two threads that live for the whole test and are let off a barrier together, rather
    // than a pair spawned per round: spawning them is slow enough that the first would be
    // done before the second started, and there would be no race to lose. The window a
    // wrong ordering leaves open is a few instructions wide, so the rounds are many and
    // cheap - the whole thing is under a second, and it caught the bad ordering on every
    // one of eight runs.
    const ROUNDS: usize = 60000;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let failed = std::sync::Arc::new(parking_lot::Mutex::new(None));
    let caller = |advertised: bool| {
        let handle = handle.clone();
        let barrier = barrier.clone();
        let failed = failed.clone();
        std::thread::spawn(move || {
            for _ in 0..ROUNDS {
                barrier.wait();
                if let Err(e) = handle.set_pieces_advertised(HELD_BACK, advertised) {
                    *failed.lock() = Some(e);
                }
                // Never skipped, whatever happened: the other two are waiting on it.
                barrier.wait();
            }
        })
    };

    let threads =
        tokio::task::block_in_place(|| -> anyhow::Result<[std::thread::JoinHandle<()>; 2]> {
            let threads = [caller(false), caller(true)];
            for round in 0..ROUNDS {
                // Between rounds, with both threads parked on the barrier.
                handle.set_pieces_advertised(0..TOTAL_PIECES, true)?;
                assert!(!gate(&handle), "round {round}: the reset left the gate up");

                barrier.wait();
                barrier.wait();

                if let Some(e) = failed.lock().take() {
                    return Err(e);
                }
                assert!(
                    !anything_held_back(&handle)? || gate(&handle),
                    "round {round}: pieces are held back and the gate says nothing is, \
                     so the Have path will announce them without ever looking"
                );
            }
            Ok(threads)
        })?;
    for t in threads {
        t.join()
            .map_err(|_| anyhow::anyhow!("a calling thread panicked"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_gate_survives_two_callers() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_gate_survives_two_callers(),
    )
    .await?
}

// A hold-back that holds nothing back - an empty range, or one past the last piece - must
// leave the gate down. It goes up on the way in, before the set is touched, because at
// that point we do not yet know; what brings it down again is that the gate is written
// from the set on the way out whichever direction the call was going.
async fn e2e_unadvertised_pieces_gate_comes_back_down() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, _torrent_bytes, (_session, handle), _addr) =
        seeder("test_unadvertised_pieces_gate_down").await?;

    assert_eq!(handle.set_pieces_advertised(0..0, false)?, 0);
    assert!(!anything_held_back(&handle)?);
    assert!(
        !gate(&handle),
        "a hold-back that held nothing back left the Have path taking the lock forever"
    );

    assert_eq!(
        handle.set_pieces_advertised(TOTAL_PIECES..TOTAL_PIECES + 8, false)?,
        0
    );
    assert!(!gate(&handle));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_gate_comes_back_down() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_gate_comes_back_down(),
    )
    .await?
}

// A call the torrent refuses must leave the gate exactly as it found it. The gate lives on
// ManagedTorrentShared, which outlives every state the torrent passes through, so nothing
// later comes along to correct one left up: every Have takes the state lock for the rest of
// the torrent's life. And the raise happens on the way in, before we know whether the call
// can go through at all.
//
// The refused state is swapped in by hand. The one a caller actually meets is
// `initializing` - the window between add_torrent(paused: true) returning and the check of
// what is on disk finishing, which is what the test below is about - and that window cannot
// be held open from outside. The arm that refuses the call is the same one either way.
async fn e2e_unadvertised_pieces_gate_survives_a_refused_call() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, _torrent_bytes, (_session, handle), _addr) =
        seeder("test_unadvertised_pieces_gate_refused").await?;

    // In and out with no await in between, so nothing else gets a look at it.
    let refuse = |handle: &ManagedTorrent| {
        let stashed = std::mem::replace(
            &mut handle.locked.write().state,
            ManagedTorrentState::Error(anyhow::anyhow!("a state this call does not serve")),
        );
        let refused = handle.set_pieces_advertised(HELD_BACK, false);
        let gate_after = gate(handle);
        handle.locked.write().state = stashed;
        (refused, gate_after)
    };

    // Refused with nothing held back: the gate has to come back down.
    let (refused, gate_after) = refuse(&handle);
    assert!(refused.is_err());
    assert!(!anything_held_back(&handle)?);
    assert!(
        !gate_after,
        "a refused hold-back left the Have path taking the lock for the life of the torrent"
    );

    // Refused with pieces held back: the gate has to stay up. So it is put back to what it
    // said, not cleared - clearing it here would be the one thing the gate may never do.
    assert_eq!(
        handle.set_pieces_advertised(HELD_BACK, false)?,
        HELD_BACK.len()
    );
    assert!(gate(&handle));
    let (refused, gate_after) = refuse(&handle);
    assert!(refused.is_err());
    assert!(anything_held_back(&handle)?);
    assert!(
        gate_after,
        "a refused call put the gate down with pieces still held back, so the Have path \
         will announce them without ever looking"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_gate_survives_a_refused_call() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_gate_survives_a_refused_call(),
    )
    .await?
}

// The sequence set_pieces_advertised documents for a torrent that has to be added again
// with the set already in force: add it paused, wait for the check of what is on disk to
// finish, hold back, then unpause. The wait is the step that is easy to leave out and
// cannot be skipped - add_torrent returns while the torrent is still `initializing`, and
// holding back is refused there.
async fn e2e_unadvertised_pieces_readded_paused() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (session, seeder), addr) =
        seeder("test_unadvertised_pieces_readd").await?;

    // Out of the session, data left where it is. The set is per-session, so the torrent
    // comes back announcing everything it finds - which is what the sequence is for.
    session.delete(seeder.id().into(), false).await?;
    drop(seeder);

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: true,
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    // The step. Without it the torrent is still checking the files and the hold-back below
    // is refused; with it the torrent is paused, which is a state this call serves.
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    assert!(handle.is_paused());
    assert!(handle.live().is_none());
    assert_eq!(
        handle.set_pieces_advertised(HELD_BACK, false)?,
        HELD_BACK.len()
    );

    // Only now does it get to talk to anyone, and the first bitfield it sends is already
    // short.
    session.unpause(&handle).await?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_readd_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;
    let advertised = (HELD_BACK.end..TOTAL_PIECES).collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        have_pieces(&leecher)?,
        advertised,
        "the peer got a piece held back on the torrent before it was ever unpaused"
    );
    assert!(!leecher.stats().finished);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_readded_paused() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_readded_paused(),
    )
    .await?
}
