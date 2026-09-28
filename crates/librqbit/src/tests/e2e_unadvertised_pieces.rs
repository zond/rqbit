//! What a peer sees of a torrent that does not advertise every piece.
//!
//! Two paths carry the have-set to a peer, and a piece we do not advertise has to be
//! missing from both: the bitfield sent at handshake, and the Have a completed piece sets
//! off. A third path is the upload: a request for such a piece is not served. These tests
//! drive them over real connections between real sessions - and, where the exact bytes
//! on the wire are the claim, from a hand-driven peer - so a leak shows up as the other
//! side learning of a piece it was never told about, or being sent one.
//!
//! Under [`crate::SessionOptions::explicit_piece_advertising`] nothing is announced
//! until [`ManagedTorrent::set_pieces_advertised`] advertises it, and an announcement is
//! never taken back while the torrent is in the swarm. Without it every piece is
//! advertised, as upstream, and pieces may be held back.

use std::{net::Ipv4Addr, ops::Range, time::Duration};

use anyhow::Context;
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::{io::AsyncReadExt, time::timeout};
use tracing::info;

use crate::{
    AddTorrent, ManagedTorrent, Session,
    storage::{StorageFactoryExt, examples::inmemory::InMemoryPieceStorageFactory},
    tests::test_util::{TestPeerMetadata, setup_test_logging},
    torrent_state::{
        advertised::WithdrawRefused,
        live::peer::stats::snapshot::{PeerStatsFilter, PeerStatsFilterState},
    },
};

use super::test_util;

const PIECE_LEN: u32 = CHUNK_SIZE;
const TOTAL_PIECES: u32 = 16;
// The first half stands in for a playback window: pieces we have and read, and are about
// to reclaim, so nobody should hear about them.
const HELD_BACK: Range<u32> = 0..TOTAL_PIECES / 2;
// What an explicitly advertising torrent announces in most tests below: the other half.
const ADVERTISED: Range<u32> = TOTAL_PIECES / 2..TOTAL_PIECES;

type Client = (std::sync::Arc<Session>, std::sync::Arc<ManagedTorrent>);

// A session that has the whole torrent and listens for peers, plus the torrent file, the
// directory it was made from, and the address to dial it on. Upstream's default: it
// advertises every piece.
async fn seeder(prefix: &str) -> anyhow::Result<(TempDir, Vec<u8>, Client, std::net::SocketAddr)> {
    seeder_of(prefix, TOTAL_PIECES, false).await
}

// The same under explicit advertising: it has every piece and announces none of them.
async fn explicit_seeder(
    prefix: &str,
) -> anyhow::Result<(TempDir, Vec<u8>, Client, std::net::SocketAddr)> {
    seeder_of(prefix, TOTAL_PIECES, true).await
}

// The same, for a torrent of `pieces` pieces.
async fn seeder_of(
    prefix: &str,
    pieces: u32,
    explicit: bool,
) -> anyhow::Result<(TempDir, Vec<u8>, Client, std::net::SocketAddr)> {
    let seeder = test_util::seeder_advertising(
        prefix,
        (PIECE_LEN * pieces) as usize,
        PIECE_LEN,
        Default::default(),
        explicit,
    )
    .await?;
    Ok((
        seeder.files,
        seeder.torrent_bytes,
        (seeder.session, seeder.handle),
        seeder.addr,
    ))
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

// The pieces this torrent announces right now: what a peer connecting now is told.
fn announced(handle: &ManagedTorrent) -> anyhow::Result<Vec<u32>> {
    let bytes = handle.announced_bitfield()?;
    Ok(
        crate::type_aliases::BF::from_boxed_slice(bytes.into_boxed_slice())
            .iter_ones()
            .filter_map(|id| u32::try_from(id).ok())
            .collect(),
    )
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

// The bitfield path, under explicit advertising: a peer sees what we advertised and only
// that, while the pieces we did not advertise stay ours to read. It has no other source,
// so a piece from the unadvertised half could only come from us having told it.
async fn e2e_unadvertised_pieces() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (_seeder_session, seeder), addr) =
        explicit_seeder("test_unadvertised_pieces").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    // Every piece is ours and none of them is announced: nothing was advertised, and
    // under this option that is from the moment the torrent existed.
    assert!(seeder.stats().finished);
    assert_eq!(have_pieces(&seeder)?, (0..TOTAL_PIECES).collect::<Vec<_>>());
    assert_eq!(announced(&seeder)?, Vec::<u32>::new());

    // Advertised before anyone connects, so the bitfield sent at handshake is the first
    // thing that has to be short.
    assert_eq!(
        seeder.set_pieces_advertised(ADVERTISED, true)?,
        ADVERTISED.len()
    );
    // Saying it twice changes nothing: a caller re-states its set.
    assert_eq!(seeder.set_pieces_advertised(ADVERTISED, true)?, 0);
    assert_eq!(announced(&seeder)?, ADVERTISED.collect::<Vec<_>>());

    // Still readable, which is the reason a piece is kept `have` in the first place.
    let mut stream = seeder.clone().stream(0).await?;
    let mut read = Vec::new();
    stream.read_to_end(&mut read).await?;
    assert_eq!(read, orig_content);

    info!("advertising {ADVERTISED:?}, starting the peer");

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;

    let advertised = ADVERTISED.collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;
    assert!(!leecher.stats().finished);

    // The window a "nothing else arrives" claim is measured over: a Have that leaked out
    // late would show up here, and this is long enough for the peer to have asked for the
    // piece and got it.
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

// The other half: a piece advertised later has to reach the peers that are already
// connected. Their handshake bitfield came without it, so the only thing that can tell
// them is a Have - and this asserts they got it that way, over the connection they already
// had, rather than by the connection dying and the fresh handshake covering for it.
async fn e2e_unadvertised_pieces_come_back() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (_seeder_session, seeder), addr) =
        explicit_seeder("test_unadvertised_pieces_come_back").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    seeder.set_pieces_advertised(ADVERTISED, true)?;

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_come_back_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;
    let advertised = (HELD_BACK.end..TOTAL_PIECES).collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;

    let before = peer_connection_counters(&leecher, addr)?;

    info!("advertising {HELD_BACK:?} too");

    // These pieces are to be shared as well, so announce them.
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

// More pieces advertised at once than the Have broadcast holds, to a peer that is already
// connected. A caller that shares a finished download advertises a whole file in one
// call, and that is hundreds of pieces. The broadcast keeps the last 128 and a writer that
// falls behind it skips what it missed, so the peer would hear of the last 128 and never
// of the rest: it has no other way to learn of them short of hanging up and getting a
// fresh bitfield, which nothing makes it do.
async fn e2e_unadvertised_pieces_come_back_in_bulk() -> anyhow::Result<()> {
    const PIECES: u32 = 512;
    setup_test_logging();
    let (files, torrent_bytes, (_seeder_session, seeder), addr) =
        seeder_of("test_unadvertised_pieces_bulk", PIECES, true).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    // Nothing advertised, so the handshake bitfield tells the peer nothing and every
    // piece it gets it has to have heard of by Have.
    assert_eq!(announced(&seeder)?, Vec::<u32>::new());

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_bulk_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;
    // Both sides, and the seeder's is the one that matters: the pieces go back with the
    // peer already on the other end of an open connection.
    wait_for_live_peers(&leecher, 1).await?;
    wait_for_live_peers(&seeder, 1).await?;
    let before = peer_connection_counters(&leecher, addr)?;

    info!("advertising all {PIECES} pieces again");
    assert_eq!(
        seeder.set_pieces_advertised(0..PIECES, true)?,
        PIECES as usize
    );

    timeout(Duration::from_secs(30), leecher.wait_until_completed()).await??;
    assert_eq!(
        std::fs::read(leecher_dir.path().join("0.data")).unwrap(),
        orig_content
    );
    assert_eq!(
        peer_connection_counters(&leecher, addr)?,
        before,
        "the peer only got the pieces after redialling us, so the Haves did not reach it"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unadvertised_pieces_come_back_in_bulk() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_unadvertised_pieces_come_back_in_bulk(),
    )
    .await?
}

// The default path, which is every torrent that never calls set_pieces_advertised: a peer
// sees the have-set, whole.
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
    // advertising pieces is a no-op rather than an allocation.
    assert_eq!(seeder.set_pieces_advertised(0..u32::MAX, true)?, 0);
    assert_eq!(seeder.advertised_pieces(), None);
    assert_eq!(
        seeder.announced_bitfield()?,
        seeder.with_chunk_tracker(|ct| ct.get_have_pieces().as_bytes().to_vec())?
    );

    // And a live hold-back is upstream's to make: not refused, as it is under explicit
    // advertising, but a narrowing of what a peer connecting later is sent.
    assert_eq!(
        seeder.set_pieces_advertised(HELD_BACK, false)?,
        HELD_BACK.len(),
        "the default refused a live hold-back"
    );
    assert_eq!(announced(&seeder)?, ADVERTISED.collect::<Vec<_>>());
    assert_eq!(
        seeder.set_pieces_advertised(HELD_BACK, true)?,
        HELD_BACK.len()
    );
    assert_eq!(announced(&seeder)?, (0..TOTAL_PIECES).collect::<Vec<_>>());
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
// hands it a peer. Under explicit advertising, so it announces only what it is told to.
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
            explicit_piece_advertising: true,
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
// when a piece completes. It got no bitfield - we had nothing to send one about - so a
// Have is the only thing that can tell it anything. Under explicit advertising a piece
// advertised before it arrives is announced the moment it completes, and a piece that
// was not advertised is not announced at all.
//
// Three sessions, because the pieces have to complete on the session being watched: a
// seeder, a middle that downloads from it, and a watcher that knows only the middle and
// so can have learnt nothing anywhere else.
async fn e2e_unadvertised_pieces_completing_while_held_back() -> anyhow::Result<()> {
    setup_test_logging();
    let (_seeder_files, torrent_bytes, (_seeder_session, _seeder), seeder_addr) =
        seeder("test_unadvertised_pieces_completing").await?;

    // Half advertised before the middle has a single piece, and before it has anywhere
    // to get one: every piece it ever completes, it completes under this set.
    let (_middle_dir, (_middle_session, middle), middle_addr) =
        middle("test_unadvertised_pieces_completing_middle", &torrent_bytes).await?;
    assert_eq!(
        middle.set_pieces_advertised(ADVERTISED, true)?,
        ADVERTISED.len()
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

    // The advertised half was announced as it completed: the watcher heard of it by Have,
    // since it had no bitfield, and fetched it.
    let advertised = ADVERTISED.collect::<Vec<_>>();
    wait_for_pieces(&watcher, &advertised).await?;

    // The window a "nothing else arrives" claim is measured over: long enough for a Have
    // queued behind a completion to have gone out, been asked about and answered. Watched
    // throughout rather than sampled at the end: a Have moves the watcher's picture of us
    // the instant it lands, and it would move back if the connection were replaced by a
    // fresh handshake.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        assert_eq!(
            live_peers(&watcher)?.1,
            0,
            "the watcher was told about a piece that completed unadvertised"
        );
        assert_eq!(
            have_pieces(&watcher)?,
            advertised,
            "the watcher got a piece that completed unadvertised"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Silence, not a dead peer: it spent all of that connected to the session that had
    // every piece and said nothing more.
    assert_eq!(live_peers(&watcher)?.0, 1);

    info!("nothing leaked, now advertising the rest");

    // The control, and the reason the silence above means something: that connection was
    // never mute, it just had nothing more to say. Advertise the rest and its Haves travel
    // down it at once. Same connection, so the counters must not move: a redial would
    // carry the pieces too, in a fresh handshake bitfield, and would say nothing about
    // the Have path.
    let before = peer_connection_counters(&watcher, middle_addr)?;
    assert_eq!(
        middle.set_pieces_advertised(0..TOTAL_PIECES, true)?,
        HELD_BACK.len()
    );
    // Either the watcher now sees the middle as a seeder, or it has already
    // fetched the lot and the two have parted as finished peers do: a
    // request loop woken by the Haves it was waiting on downloads eight
    // small pieces over loopback faster than this polls. Both mean the Haves
    // arrived; the counters below say on which connection.
    timeout(Duration::from_secs(30), async {
        loop {
            if live_peers(&watcher)?.1 == 1 || have_pieces(&watcher)?.len() == TOTAL_PIECES as usize
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert_eq!(
        peer_connection_counters(&watcher, middle_addr)?,
        before,
        "the watcher heard about the pieces on a new connection, so the Have did not reach it"
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

// A torrent added again, with its data on disk, under explicit advertising: from the
// moment it exists - through its initial check and into live - it announces nothing it
// was not told to, and the set can be given to it while it is still checking. No pause,
// no waiting for the check: there is no moment at which it announced its pieces by
// default.
async fn e2e_unadvertised_pieces_readded_paused() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, (session, seeder), addr) =
        explicit_seeder("test_unadvertised_pieces_readd").await?;

    // Out of the session, data left where it is. The set is per-session, so the torrent
    // comes back with a fresh one.
    session.delete(seeder.id().into(), false).await?;
    drop(seeder);

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
    // Whatever state the check has reached, the call is served.
    assert_eq!(
        handle.set_pieces_advertised(ADVERTISED, true)?,
        ADVERTISED.len()
    );
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert!(handle.live().is_some());
    assert_eq!(announced(&handle)?, ADVERTISED.collect::<Vec<_>>());

    let leecher_dir = TempDir::with_prefix("test_unadvertised_pieces_readd_leecher")?;
    let (_leecher_session, leecher) = leecher(&leecher_dir, &torrent_bytes, addr).await?;
    let advertised = ADVERTISED.collect::<Vec<_>>();
    wait_for_pieces(&leecher, &advertised).await?;
    // The window a "nothing else arrives" claim is measured over.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        have_pieces(&leecher)?,
        advertised,
        "the peer got a piece the re-added torrent was never told to advertise"
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

// A peer driven by hand, so the bytes on the wire are the claim and not what a session
// made of them: the handshake, then messages as `(id, payload)`. It announces no
// extension (reserved bits all zero), so the other side sends it nothing but BEP-3.
struct RawPeer {
    stream: tokio::net::TcpStream,
}

const MSG_UNCHOKE: u8 = 1;
const MSG_INTERESTED: u8 = 2;
const MSG_HAVE: u8 = 4;
const MSG_BITFIELD: u8 = 5;
const MSG_REQUEST: u8 = 6;
const MSG_PIECE: u8 = 7;

impl RawPeer {
    async fn connect(
        addr: std::net::SocketAddr,
        info_hash: librqbit_core::hash_id::Id20,
    ) -> anyhow::Result<Self> {
        use tokio::io::AsyncWriteExt;
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        let mut handshake = Vec::with_capacity(68);
        handshake.push(19);
        handshake.extend_from_slice(b"BitTorrent protocol");
        handshake.extend_from_slice(&[0; 8]);
        handshake.extend_from_slice(&info_hash.0);
        handshake.extend_from_slice(&TestPeerMetadata::good().as_peer_id().0);
        stream.write_all(&handshake).await?;
        let mut theirs = [0u8; 68];
        stream.read_exact(&mut theirs).await?;
        anyhow::ensure!(&theirs[28..48] == info_hash.0.as_slice(), "wrong torrent");
        Ok(Self { stream })
    }

    async fn send(&mut self, id: u8, payload: &[u8]) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let len = u32::try_from(payload.len() + 1)?;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&[id]).await?;
        self.stream.write_all(payload).await?;
        Ok(())
    }

    async fn request(&mut self, piece: u32) -> anyhow::Result<()> {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&piece.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&PIECE_LEN.to_be_bytes());
        self.send(MSG_REQUEST, &payload).await
    }

    // The next message, keep-alives skipped. Bounded, so a peer that has gone quiet
    // fails the test with what it was waiting for rather than hanging it.
    async fn next(&mut self) -> anyhow::Result<(u8, Vec<u8>)> {
        timeout(Duration::from_secs(30), async {
            loop {
                let mut len = [0u8; 4];
                self.stream.read_exact(&mut len).await?;
                let len = u32::from_be_bytes(len) as usize;
                if len == 0 {
                    continue;
                }
                let mut msg = vec![0u8; len];
                self.stream.read_exact(&mut msg).await?;
                let id = msg.remove(0);
                return Ok::<_, anyhow::Error>((id, msg));
            }
        })
        .await
        .context("the peer sent nothing for 30 s")?
    }

    // What the other side announced at the handshake: its bitfield, which it sends
    // before its unchoke.
    async fn bitfield(&mut self) -> anyhow::Result<Vec<u32>> {
        let (id, payload) = self.next().await?;
        anyhow::ensure!(
            id == MSG_BITFIELD,
            "expected a bitfield first, got message {id}"
        );
        Ok(bits(&payload))
    }

    // Wait for the message `id`, collecting the Haves that arrive on the way.
    async fn until(&mut self, id: u8, haves: &mut Vec<u32>) -> anyhow::Result<Vec<u8>> {
        loop {
            let (got, payload) = self.next().await?;
            if got == id {
                return Ok(payload);
            }
            if got == MSG_HAVE {
                haves.push(u32::from_be_bytes(payload[..4].try_into()?));
            }
        }
    }
}

fn bits(bytes: &[u8]) -> Vec<u32> {
    crate::type_aliases::BF::from_boxed_slice(bytes.to_vec().into_boxed_slice())
        .iter_ones()
        .filter_map(|id| u32::try_from(id).ok())
        .collect()
}

// The piece a Piece message carries.
fn piece_index(payload: &[u8]) -> anyhow::Result<u32> {
    Ok(u32::from_be_bytes(payload[..4].try_into()?))
}

// Under explicit advertising, on the wire: the bitfield a peer is sent at its handshake
// names nothing until something is advertised, then exactly what was; advertising a
// piece we have sends a connected peer its Have; a request for a piece we have and did
// not advertise is dropped, not served, and the connection stays; and an announcement is
// never taken back.
async fn e2e_explicit_advertising_on_the_wire() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, _torrent_bytes, (_session, seeder), addr) =
        explicit_seeder("test_explicit_advertising_wire").await?;
    let info_hash = seeder.info_hash();

    // Every piece is ours; the bitfield says none of them is.
    let mut first = RawPeer::connect(addr, info_hash).await?;
    assert_eq!(first.bitfield().await?, Vec::<u32>::new());
    let mut haves = Vec::new();
    first.until(MSG_UNCHOKE, &mut haves).await?;
    first.send(MSG_INTERESTED, &[]).await?;

    // Advertised with the peer connected: it hears of each piece by Have.
    assert_eq!(
        seeder.set_pieces_advertised(ADVERTISED, true)?,
        ADVERTISED.len()
    );
    // A request for a piece we have and did not advertise, then one for a piece we did.
    // The upload is served in order, so had the first been served its Piece would be
    // the first to arrive.
    first.request(HELD_BACK.start).await?;
    first.request(ADVERTISED.start).await?;
    let piece = first.until(MSG_PIECE, &mut haves).await?;
    assert_eq!(
        piece_index(&piece)?,
        ADVERTISED.start,
        "we served a piece we never advertised"
    );
    // Not hung up on: the next request on the same connection is served too.
    first.request(ADVERTISED.start + 1).await?;
    let piece = first.until(MSG_PIECE, &mut haves).await?;
    assert_eq!(piece_index(&piece)?, ADVERTISED.start + 1);
    haves.sort_unstable();
    assert_eq!(haves, ADVERTISED.collect::<Vec<_>>());

    // An announcement stays: a live torrent refuses to withdraw one, and changes nothing.
    for range in [
        ADVERTISED,
        0..TOTAL_PIECES,
        ADVERTISED.end - 1..ADVERTISED.end,
    ] {
        let refused = seeder
            .set_pieces_advertised(range.clone(), false)
            .expect_err("a live torrent withdrew an announcement");
        let refused = refused
            .downcast_ref::<WithdrawRefused>()
            .context("expected the typed refusal")?;
        assert_eq!(
            refused.pieces,
            range.filter(|p| ADVERTISED.contains(p)).collect::<Vec<_>>()
        );
    }
    assert_eq!(announced(&seeder)?, ADVERTISED.collect::<Vec<_>>());
    assert_eq!(seeder.advertised_pieces(), Some(ADVERTISED.collect()));

    // And a peer connecting now is told exactly the advertised half.
    let mut second = RawPeer::connect(addr, info_hash).await?;
    assert_eq!(second.bitfield().await?, ADVERTISED.collect::<Vec<_>>());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_explicit_advertising_on_the_wire() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_explicit_advertising_on_the_wire(),
    )
    .await?
}

// A restart out of an error throws the chunk tracker away and checks the disk again. The
// advertised set is not in the tracker, so the torrent comes back announcing what it
// announced before - no less, since those peers were told, and no more, since nobody
// asked for more.
async fn e2e_explicit_advertising_survives_a_restart_from_error() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, _torrent_bytes, (session, seeder), addr) =
        explicit_seeder("test_explicit_advertising_restart").await?;
    let info_hash = seeder.info_hash();
    seeder.set_pieces_advertised(ADVERTISED, true)?;

    seeder.stop_with_error(anyhow::anyhow!("simulated fatal error"));
    assert!(seeder.live().is_none());
    // Out of the swarm, so it has nobody to answer to. Nothing asked here: the set is
    // what it was.
    session.unpause(&seeder).await?;
    timeout(Duration::from_secs(30), seeder.wait_until_completed()).await??;
    assert!(seeder.live().is_some(), "the restart did not go live");

    let mut peer = RawPeer::connect(addr, info_hash).await?;
    assert_eq!(
        peer.bitfield().await?,
        ADVERTISED.collect::<Vec<_>>(),
        "the restart changed what the torrent announces"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_explicit_advertising_survives_a_restart_from_error() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_explicit_advertising_survives_a_restart_from_error(),
    )
    .await?
}

// Under explicit advertising a live torrent keeps what it announced: dropping it would
// leave a peer that was told asking for a piece we no longer have. Out of the swarm -
// paused - it drops like any other piece, and its set can be emptied, which is how a
// caller starts it again from nothing.
async fn e2e_explicit_advertising_keeps_what_it_announced() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, torrent_bytes, (_seeder_session, _seeder), seeder_addr) =
        seeder("test_explicit_advertising_keeps").await?;

    let dir = TempDir::with_prefix("test_explicit_advertising_keeps_client")?;
    let session = Session::new_with_opts(
        dir.path().into(),
        crate::SessionOptions {
            dht: None,
            persistence: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            explicit_piece_advertising: true,
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![seeder_addr]),
                piece_reclaim: true,
                storage_factory: Some(InMemoryPieceStorageFactory::default().boxed()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    handle.set_pieces_advertised(ADVERTISED, true)?;

    // Live: everything but the announced half goes.
    let claim = handle.drop_pieces(0..TOTAL_PIECES)?;
    assert_eq!(claim.pieces(), HELD_BACK.collect::<Vec<_>>().as_slice());
    drop(claim);
    assert_eq!(have_pieces(&handle)?, ADVERTISED.collect::<Vec<_>>());
    assert_eq!(announced(&handle)?, ADVERTISED.collect::<Vec<_>>());

    // Paused: out of the swarm, so the set may be emptied and the pieces dropped.
    session.pause(&handle).await?;
    assert_eq!(
        handle.set_pieces_advertised(0..TOTAL_PIECES, false)?,
        ADVERTISED.len()
    );
    assert_eq!(handle.advertised_pieces(), Some(Vec::new()));
    let claim = handle.drop_pieces(ADVERTISED)?;
    assert_eq!(claim.pieces(), ADVERTISED.collect::<Vec<_>>().as_slice());
    drop(claim);
    assert_eq!(have_pieces(&handle)?, Vec::<u32>::new());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_explicit_advertising_keeps_what_it_announced() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_explicit_advertising_keeps_what_it_announced(),
    )
    .await?
}
