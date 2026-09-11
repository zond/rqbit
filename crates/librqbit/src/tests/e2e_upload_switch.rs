//! The session's upload switch, over real connections between real sessions: off, a peer
//! that can see every piece we have gets none of them and we still download; on again, the
//! same connection is unchoked and the peer finishes.

use std::{net::Ipv4Addr, num::NonZeroU32, sync::Arc, time::Duration};

use anyhow::{Context, bail};
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::{
    AddTorrent, CreateTorrentOptions, ManagedTorrent, Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging, wait_until,
    },
    torrent_state::live::peer::stats::snapshot::{PeerStatsFilter, PeerStatsFilterState},
};

const PIECE_LEN: u32 = CHUNK_SIZE;
const TOTAL_PIECES: u32 = 16;
const FILE_SIZE: usize = (PIECE_LEN * TOTAL_PIECES) as usize;
const WAIT: Duration = Duration::from_secs(30);

type Client = (Arc<Session>, Arc<ManagedTorrent>);

// A session with the whole torrent, listening, and the address to dial it on.
async fn seeder(
    prefix: &str,
    limits: LimitsConfig,
) -> anyhow::Result<(TempDir, Vec<u8>, Client, std::net::SocketAddr)> {
    let files = create_default_random_dir_with_torrents(1, FILE_SIZE, Some(prefix));
    let torrent_bytes = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LEN),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?
    .to_vec();
    let session = Session::new_with_opts(
        files.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ratelimits: limits,
            ..Default::default()
        },
    )
    .await
    .context("error creating seeder session")?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(WAIT, handle.wait_until_completed()).await??;
    let addr = session
        .listen_addr()
        .context("the seeder is not listening")?;
    Ok((files, torrent_bytes, (session, handle), addr))
}

// A session with nothing, that knows one peer and has no other way to find any.
async fn leecher(
    dir: &TempDir,
    torrent_bytes: &[u8],
    peer: std::net::SocketAddr,
) -> anyhow::Result<Client> {
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
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
                initial_peers: Some(vec![peer]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(WAIT, handle.wait_until_initialized()).await??;
    Ok((session, handle))
}

fn have_count(handle: &ManagedTorrent) -> anyhow::Result<usize> {
    handle.with_chunk_tracker(|ct| {
        ct.get_have_pieces().as_slice()[..TOTAL_PIECES as usize].count_ones()
    })
}

fn uploaded(handle: &ManagedTorrent) -> u64 {
    handle.stats().uploaded_bytes
}

// How many times the leecher dialled the seeder, and how many times talking to it went
// wrong. `(1, 0)` is one connection that did everything: a peer left unchoked while its
// requests go unanswered does finish, but only after its read timeout hangs up and the
// fresh connection's handshake covers for it - which is what this tells apart.
fn connection_counters(
    handle: &ManagedTorrent,
    peer: std::net::SocketAddr,
) -> anyhow::Result<(u32, u32)> {
    let stats = handle
        .live()
        .context("expected a live torrent")?
        .per_peer_stats_snapshot(PeerStatsFilter {
            state: PeerStatsFilterState::All,
        });
    let peer = stats
        .peers
        .get(&peer.to_string())
        .context("expected the peer to be in the peer table")?;
    Ok((peer.counters.connection_attempts, peer.counters.errors))
}

async fn wait_for_a_live_peer(handle: &ManagedTorrent) -> anyhow::Result<()> {
    wait_until(
        || match handle.live() {
            Some(live) if live.stats_snapshot().peer_stats.live >= 1 => Ok(()),
            _ => bail!("no live peer yet"),
        },
        WAIT,
    )
    .await
}

async fn wait_for_completion(handle: &ManagedTorrent) -> anyhow::Result<()> {
    wait_until(
        || match have_count(handle)? {
            n if n == TOTAL_PIECES as usize => Ok(()),
            n => bail!("{n} of {TOTAL_PIECES} pieces"),
        },
        WAIT,
    )
    .await
}

// A peer that connects while the switch is off is never unchoked: it is told about every
// piece and gets none. Turning it on unchokes the connection already open - nobody
// reconnects - and the peer finishes.
async fn e2e_upload_switch_off_at_connect() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, torrent_bytes, (seeder_session, seeder), addr) =
        seeder("test_upload_switch_connect", LimitsConfig::default()).await?;
    seeder_session.set_upload_enabled(false);
    assert!(!seeder_session.upload_enabled());

    let dir = TempDir::with_prefix("test_upload_switch_connect_leecher")?;
    let (_leecher_session, leecher) = leecher(&dir, &torrent_bytes, addr).await?;
    wait_for_a_live_peer(&leecher).await?;

    // Long enough for a peer that was unchoked to have taken the lot.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        have_count(&leecher)?,
        0,
        "a peer got a piece with upload off"
    );
    assert_eq!(uploaded(&seeder), 0);
    assert_eq!(
        leecher
            .live()
            .context("leecher live")?
            .stats_snapshot()
            .peer_stats
            .live,
        1,
        "still connected: the switch chokes, it does not hang up"
    );

    seeder_session.set_upload_enabled(true);
    wait_for_completion(&leecher).await?;
    assert_eq!(
        connection_counters(&leecher, addr)?,
        (1, 0),
        "finished over the connection it had"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_upload_switch_off_at_connect() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), e2e_upload_switch_off_at_connect()).await?
}

// A connection that is uploading stops when the switch goes off - including what the upload
// scheduler had already queued for it - and starts again when it comes back on. The seeder's
// upload is rate limited to about a piece a second so the switch lands mid-transfer.
async fn e2e_upload_switch_off_mid_transfer() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, torrent_bytes, (seeder_session, seeder), addr) = seeder(
        "test_upload_switch_mid",
        LimitsConfig {
            upload_bps: NonZeroU32::new(PIECE_LEN),
            download_bps: None,
        },
    )
    .await?;

    let dir = TempDir::with_prefix("test_upload_switch_mid_leecher")?;
    let (_leecher_session, leecher) = leecher(&dir, &torrent_bytes, addr).await?;
    wait_until(
        || match have_count(&leecher)? {
            n if n >= 2 => Ok(()),
            n => bail!("{n} pieces so far"),
        },
        WAIT,
    )
    .await?;

    seeder_session.set_upload_enabled(false);
    // What was already on the wire lands; nothing after it.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (pieces, bytes) = (have_count(&leecher)?, uploaded(&seeder));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        have_count(&leecher)?,
        pieces,
        "pieces kept arriving with upload off"
    );
    assert_eq!(
        uploaded(&seeder),
        bytes,
        "bytes kept leaving with upload off"
    );
    assert!(
        pieces < TOTAL_PIECES as usize,
        "the switch landed after the transfer"
    );

    seeder_session.set_upload_enabled(true);
    wait_for_completion(&leecher).await?;
    assert_eq!(
        connection_counters(&leecher, addr)?,
        (1, 0),
        "finished over the connection it had"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_upload_switch_off_mid_transfer() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_upload_switch_off_mid_transfer(),
    )
    .await?
}

// Off is about uploading only: a session with it off still downloads the whole torrent.
async fn e2e_upload_switch_off_still_downloads() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, torrent_bytes, _seeder, addr) =
        seeder("test_upload_switch_download", LimitsConfig::default()).await?;
    let dir = TempDir::with_prefix("test_upload_switch_download_leecher")?;
    let (leecher_session, leecher) = leecher(&dir, &torrent_bytes, addr).await?;
    leecher_session.set_upload_enabled(false);
    wait_for_completion(&leecher).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_upload_switch_off_still_downloads() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_upload_switch_off_still_downloads(),
    )
    .await?
}

// An embedder re-asserts the switch as often as it recomputes it; saying what is already in
// force must not wake every peer connection's writer to find nothing changed.
#[tokio::test]
async fn test_upload_switch_wakes_nobody_when_nothing_changes() -> anyhow::Result<()> {
    let dir = TempDir::with_prefix("test_upload_switch_wakes")?;
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            ..Default::default()
        },
    )
    .await?;
    let mut rx = session.upload_enabled.subscribe();
    session.set_upload_enabled(true);
    assert!(!rx.has_changed()?, "on was already on");
    session.set_upload_enabled(false);
    assert!(rx.has_changed()?);
    rx.borrow_and_update();
    session.set_upload_enabled(false);
    assert!(!rx.has_changed()?, "off was already off");
    Ok(())
}
