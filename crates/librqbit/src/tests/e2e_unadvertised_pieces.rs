//! What a peer sees of a torrent that holds pieces back.
//!
//! Two paths carry the have-set to a peer, and a held-back piece has to be missing from
//! both: the bitfield sent at handshake, and the Have broadcast a completed piece sets
//! off. These tests drive them over a real connection between two sessions, so a leak
//! shows up as the other side downloading a piece it was never told about.

use std::{net::Ipv4Addr, ops::Range, time::Duration};

use anyhow::Context;
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::{io::AsyncReadExt, time::timeout};
use tracing::info;

use crate::{
    AddTorrent, CreateTorrentOptions, ManagedTorrent, Session, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{TestPeerMetadata, setup_test_logging},
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
