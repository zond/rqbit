use std::{
    io::SeekFrom,
    net::Ipv4Addr,
    pin::Pin,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::{io::AsyncSeek, time::timeout};
use tracing::info;

use crate::{
    AddTorrent, CreateTorrentOptions, Session, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{TestPeerMetadata, setup_test_logging},
};

use super::test_util::create_default_random_dir_with_torrents;

const PIECE_LEN: u32 = CHUNK_SIZE;
const TOTAL_PIECES: u32 = 16;
const FILE_SIZE: usize = (PIECE_LEN * TOTAL_PIECES) as usize;
const DROP: std::ops::Range<u32> = 0..TOTAL_PIECES / 2;
const SEEK_TO: u32 = TOTAL_PIECES / 2;

type Client = (
    std::sync::Arc<Session>,
    std::sync::Arc<crate::ManagedTorrent>,
);

async fn add_client(
    dir: &TempDir,
    torrent: &[u8],
    peer: std::net::SocketAddr,
    piece_reclaim: bool,
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
            AddTorrent::from_bytes(torrent.to_owned()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                piece_reclaim,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    Ok((session, handle))
}

async fn e2e_piece_reclaim() -> anyhow::Result<()> {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, FILE_SIZE, Some("test_piece_reclaim"));
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
    let torrent_bytes = torrent.as_bytes()?;

    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let server_session = Session::new_with_opts(
        files.path().into(),
        crate::SessionOptions {
            dht: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            persistence: None,
            listen: Some(crate::listen::ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .context("error creating server session")?;

    timeout(
        Duration::from_secs(30),
        server_session
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
            .context("expected a handle")?
            .wait_until_completed(),
    )
    .await?
    .context("error adding torrent to server")?;

    let peer = server_session
        .listen_addr()
        .context("expected listen_addr to be set")?;

    // Without opting in, the API is refused and the torrent is upstream's torrent.
    let plain_dir = TempDir::with_prefix("test_piece_reclaim_plain")?;
    let (_plain_session, plain) = add_client(&plain_dir, &torrent_bytes, peer, false).await?;
    assert!(plain.drop_pieces(DROP).is_err());
    assert!(plain.reselect_pieces(DROP).is_err());
    assert!(plain.stats().finished);

    let client_dir = TempDir::with_prefix("test_piece_reclaim_client")?;
    let (client_session, handle) = add_client(&client_dir, &torrent_bytes, peer, true).await?;
    let downloaded = client_dir.path().join("0.data");
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);

    // A live reader's lookahead protects its pieces: dropping them would only make them
    // be re-requested at once, and stall the reader in the meantime.
    {
        let _stream = handle.clone().stream(0).await?;
        assert!(handle.drop_pieces(DROP)?.is_empty());
    }

    // The guard is evaluated under the write lock, not before taking it: a reader that
    // seeks while drop_pieces() waits for a contended lock must not lose its lookahead.
    {
        let mut stream = handle.clone().stream(0).await?;
        // At EOF the reader's lookahead covers nothing, so everything may go.
        Pin::new(&mut stream).start_seek(SeekFrom::Start(FILE_SIZE as u64))?;
        let live = handle.live().context("expected a live torrent")?;

        // All sync: holding the state lock across an await would stall the whole runtime.
        let dropped = tokio::task::block_in_place(|| -> anyhow::Result<Vec<u32>> {
            let g = live.lock_write("test_drop_pieces_guard");
            let dropper = std::thread::spawn({
                let handle = handle.clone();
                move || handle.drop_pieces(0..TOTAL_PIECES)
            });
            // Let it block on the lock, then seek back. The reader now wants SEEK_TO
            // onwards, and that is what the guard has to see.
            std::thread::sleep(Duration::from_millis(200));
            Pin::new(&mut stream).start_seek(SeekFrom::Start((SEEK_TO * PIECE_LEN) as u64))?;
            drop(g);
            dropper.join().map_err(|_| anyhow!("dropper panicked"))?
        })?;

        assert!(
            !dropped.contains(&SEEK_TO),
            "piece {SEEK_TO} was dropped from under a reader that had seeked to it: {dropped:?}"
        );
        assert_eq!(handle.reselect_pieces(0..TOTAL_PIECES)?, dropped.len());
    }
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);

    info!("downloaded, now dropping pieces {DROP:?}");

    assert_eq!(handle.drop_pieces(DROP)?, DROP.collect::<Vec<_>>());

    // We no longer have them, so we no longer advertise them, and a peer asking for one
    // is refused - is_chunk_ready_to_upload() is the predicate the request path bails on.
    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    handle.with_chunk_tracker(|ct| {
        let have = ct.get_have_pieces().as_slice();
        for id in 0..TOTAL_PIECES {
            assert_eq!(have[id as usize], !DROP.contains(&id), "piece {id}");
            let chunk = lengths
                .chunk_info_from_received_data(
                    lengths.validate_piece_index(id).unwrap(),
                    0,
                    PIECE_LEN,
                )
                .unwrap();
            assert_eq!(
                ct.is_chunk_ready_to_upload(&chunk),
                !DROP.contains(&id),
                "piece {id}"
            );
        }
    })?;

    // The torrent is still finished: a piece we threw away on purpose is not a piece we
    // are missing. Dropping is bookkeeping - the bytes are still on disk until the caller
    // releases the storage, which with one file per piece is a file deletion.
    assert!(handle.stats().finished);
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);

    // Dropping is sticky across a pause, which requeues everything we don't have and is
    // the likeliest place for a dropped piece to come straight back.
    client_session.pause(&handle).await?;
    assert!(handle.stats().finished);
    client_session.unpause(&handle).await?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    assert!(handle.stats().finished);
    handle.with_chunk_tracker(|ct| {
        let have = ct.get_have_pieces().as_slice();
        for id in DROP {
            assert!(!have[id as usize], "piece {id} came back after a pause");
        }
    })?;

    info!("dropped, now re-selecting");

    assert_eq!(handle.reselect_pieces(DROP)?, DROP.len());
    assert!(!handle.stats().finished);

    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;

    // Re-downloaded, hash-checked by librqbit on the way in, and byte-identical again.
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);
    handle.with_chunk_tracker(|ct| {
        assert!(ct.get_have_pieces().as_slice()[..TOTAL_PIECES as usize].all());
    })?;

    // "Everything from here on" is a natural way to ask for a tail, and the range is the
    // caller's, not ours. It has to be clamped to the torrent: walking it to the end of
    // u32 takes ~26 seconds in a debug build, all of it blocking the executor.
    let started = Instant::now();
    let dropped = handle.drop_pieces(TOTAL_PIECES - 2..u32::MAX)?;
    let elapsed = started.elapsed();
    assert_eq!(dropped, vec![TOTAL_PIECES - 2, TOTAL_PIECES - 1]);
    assert!(
        elapsed < Duration::from_secs(1),
        "drop_pieces to u32::MAX took {elapsed:?}"
    );

    let started = Instant::now();
    assert_eq!(handle.reselect_pieces(0..u32::MAX)?, 2);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "reselect_pieces to u32::MAX took {elapsed:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), e2e_piece_reclaim()).await?
}
