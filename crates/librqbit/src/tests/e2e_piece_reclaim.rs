use std::{
    io::SeekFrom,
    net::Ipv4Addr,
    pin::Pin,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncSeek},
    time::timeout,
};
use tracing::info;

use crate::{
    AddTorrent, CreateTorrentOptions, DroppedPieces, Session, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{StorageFactoryExt, examples::inmemory::InMemoryPieceStorageFactory},
    tests::test_util::{TestPeerMetadata, setup_test_logging},
    torrent_state::live::peer::stats::snapshot::{PeerStatsFilter, PeerStatsFilterState},
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
    add_client_with_storage(dir, torrent, peer, piece_reclaim, None).await
}

async fn add_client_with_storage(
    dir: &TempDir,
    torrent: &[u8],
    peer: std::net::SocketAddr,
    piece_reclaim: bool,
    storage_factory: Option<crate::storage::BoxStorageFactory>,
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
                storage_factory,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    Ok((session, handle))
}

// A session that has the whole torrent and will serve it, plus the torrent file and the
// address to connect to.
async fn seeding_server(
    prefix: &str,
    file_size: usize,
) -> anyhow::Result<(
    TempDir,
    Vec<u8>,
    std::sync::Arc<Session>,
    std::net::SocketAddr,
)> {
    let files = create_default_random_dir_with_torrents(1, file_size, Some(prefix));
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
    Ok((files, torrent_bytes.to_vec(), server_session, peer))
}

// Read the file back through the torrent, which is what a consumer of these pieces does.
async fn read_back(handle: std::sync::Arc<crate::ManagedTorrent>) -> anyhow::Result<Vec<u8>> {
    let mut stream = handle.stream(0).await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}

async fn e2e_piece_reclaim() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    // Without opting in, the API is refused and the torrent is upstream's torrent.
    let plain_dir = TempDir::with_prefix("test_piece_reclaim_plain")?;
    let (_plain_session, plain) = add_client(&plain_dir, &torrent_bytes, peer, false).await?;
    assert!(plain.drop_pieces(DROP).is_err());
    assert!(plain.reselect_pieces(DROP).is_err());
    assert!(plain.stats().finished);

    // Opting in takes a storage that can release a piece. The default filesystem storage
    // can't - a piece of a whole file can't be deleted on its own - so with it dropping
    // would free nothing, and the torrent is refused rather than let the caller find that
    // out from a disk that doesn't empty.
    let refused_dir = TempDir::with_prefix("test_piece_reclaim_refused")?;
    let err = add_client(&refused_dir, &torrent_bytes, peer, true)
        .await
        .err()
        .context("expected piece_reclaim on the filesystem storage to be refused")?;
    let err = format!("{err:#}");
    assert!(err.contains("FilesystemStorageFactory"), "{err}");
    assert!(err.contains("ensure_can_release_pieces"), "{err}");

    let client_dir = TempDir::with_prefix("test_piece_reclaim_client")?;
    let (client_session, handle) = add_client_with_storage(
        &client_dir,
        &torrent_bytes,
        peer,
        true,
        Some(ReleasingStorageFactory::default().boxed()),
    )
    .await?;
    let downloaded = client_dir.path().join("0.data");
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);

    // A live reader's lookahead protects its pieces: dropping them would only make them
    // be re-requested at once, and stall the reader in the meantime.
    {
        let _stream = handle.clone().stream(0).await?;
        assert!(handle.drop_pieces(DROP)?.pieces().is_empty());
    }

    // The guard is evaluated under the write lock, not before taking it: a reader that
    // seeks while drop_pieces() waits for a contended lock must not lose its lookahead.
    {
        let mut stream = handle.clone().stream(0).await?;
        // At EOF the reader's lookahead covers nothing, so everything may go.
        Pin::new(&mut stream).start_seek(SeekFrom::Start(FILE_SIZE as u64))?;
        let live = handle.live().context("expected a live torrent")?;

        // All sync: holding the state lock across an await would stall the whole runtime.
        let dropped = tokio::task::block_in_place(|| -> anyhow::Result<DroppedPieces> {
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
            !dropped.pieces().contains(&SEEK_TO),
            "piece {SEEK_TO} was dropped from under a reader that had seeked to it: {dropped:?}"
        );
        let count = dropped.pieces().len();
        // Pretend we released the storage: until the claim goes, nothing re-downloads
        // them, so the reselect below would queue pieces that no peer is allowed to take.
        drop(dropped);
        assert_eq!(handle.reselect_pieces(0..TOTAL_PIECES)?, count);
    }
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);

    info!("downloaded, now dropping pieces {DROP:?}");

    let dropped = handle.drop_pieces(DROP)?;
    assert_eq!(dropped.pieces(), DROP.collect::<Vec<_>>());

    // We no longer have them, so we no longer advertise them, and a peer asking for one
    // is refused - is_chunk_ready_to_upload() is the predicate the request path bails on.
    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let live = handle.live().context("expected a live torrent")?;
    for id in 0..TOTAL_PIECES {
        // A Have queued before the drop must not go out after it: the peer would ask for
        // the piece, we couldn't serve it, and it would hang up on us.
        assert_eq!(
            live.should_advertise_have(lengths.validate_piece_index(id).unwrap()),
            !DROP.contains(&id),
            "piece {id}"
        );
    }
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
    // releases the storage, which this storage leaves to the caller and never gets round
    // to.
    assert!(handle.stats().finished);
    assert_eq!(std::fs::read(&downloaded).unwrap(), orig_content);

    // The caller has released the storage of those pieces, so the claim on them goes.
    drop(dropped);

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
    assert_eq!(dropped.pieces(), [TOTAL_PIECES - 2, TOTAL_PIECES - 1]);
    drop(dropped);
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

// The whole loop, with a storage that actually releases what it is told to: download,
// drop, delete the pieces, seek back into the range, get them again.
async fn e2e_piece_reclaim_storage_loop() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_storage", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let storage = InMemoryPieceStorageFactory::default();
    let dir = TempDir::with_prefix("test_piece_reclaim_storage_client")?;
    let (_session, handle) = add_client_with_storage(
        &dir,
        &torrent_bytes,
        peer,
        true,
        Some(storage.clone().boxed()),
    )
    .await?;

    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let piece = |id: u32| lengths.validate_piece_index(id).unwrap();
    let info_hash = handle.info_hash();

    // One entry per piece, which is what makes releasing one meaningful.
    assert_eq!(storage.piece_count(info_hash), TOTAL_PIECES as usize);
    assert_eq!(read_back(handle.clone()).await?, orig_content);

    // The loop: ask what may go, delete exactly that, then let the claim go.
    let dropped = handle.drop_pieces(DROP)?;
    assert_eq!(dropped.pieces(), DROP.collect::<Vec<_>>());
    for id in dropped.pieces() {
        assert!(
            storage.release_piece(info_hash, piece(*id)),
            "piece {id} wasn't there"
        );
    }
    drop(dropped);

    assert_eq!(
        storage.piece_count(info_hash),
        TOTAL_PIECES as usize - DROP.len()
    );
    for id in 0..TOTAL_PIECES {
        assert_eq!(
            storage.has_piece(info_hash, piece(id)),
            !DROP.contains(&id),
            "{id}"
        );
    }

    // The memory is gone and the torrent is still finished: a piece we threw away on
    // purpose is not a piece we are missing.
    assert!(handle.stats().finished);

    // Asking for the range back re-downloads it from the peer, hash-checked on the way
    // in, and the file reads back byte-identical.
    assert_eq!(handle.reselect_pieces(DROP)?, DROP.len());
    assert!(!handle.stats().finished);
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(storage.piece_count(info_hash), TOTAL_PIECES as usize);
    assert_eq!(read_back(handle.clone()).await?, orig_content);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_storage_loop() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), e2e_piece_reclaim_storage_loop()).await?
}

// A claim on dropped pieces has to survive a pause: pausing takes the piece tracker
// apart and unpausing builds a new one, and if the claim went with it, an unpaused
// torrent would download pieces whose storage the caller is still deleting - and the
// deletion would then remove pieces we have and are advertising. A pause is an ordinary
// user action, and also what a fatal error does.
async fn e2e_piece_reclaim_claim_survives_pause() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_pause", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let storage = InMemoryPieceStorageFactory::default();
    let dir = TempDir::with_prefix("test_piece_reclaim_pause_client")?;
    let (session, handle) = add_client_with_storage(
        &dir,
        &torrent_bytes,
        peer,
        true,
        Some(storage.clone().boxed()),
    )
    .await?;

    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let piece = |id: u32| lengths.validate_piece_index(id).unwrap();
    let info_hash = handle.info_hash();
    let have =
        |id: u32| handle.with_chunk_tracker(|ct| ct.get_have_pieces().as_slice()[id as usize]);

    // Take the pieces and delete them, but hold on to the claim: we are "still deleting".
    let dropped = handle.drop_pieces(DROP)?;
    assert_eq!(dropped.pieces(), DROP.collect::<Vec<_>>());
    for id in dropped.pieces() {
        assert!(
            storage.release_piece(info_hash, piece(*id)),
            "piece {id} wasn't there"
        );
    }

    session.pause(&handle).await?;
    session.unpause(&handle).await?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;

    // Reselecting is the loudest way to ask for them back, and the one a reader's seek
    // does by itself. It may queue them, but nothing may take them off the queue yet.
    assert_eq!(handle.reselect_pieces(DROP)?, DROP.len());
    assert!(!handle.stats().finished);
    tokio::time::sleep(Duration::from_secs(2)).await;
    for id in DROP {
        assert!(
            !have(id)?,
            "piece {id} was downloaded again while its storage was still being released"
        );
        assert!(
            !storage.has_piece(info_hash, piece(id)),
            "piece {id} came back in storage"
        );
    }

    // The deletion is done. Now they may come back.
    drop(dropped);
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(storage.piece_count(info_hash), TOTAL_PIECES as usize);
    assert_eq!(read_back(handle.clone()).await?, orig_content);

    // Same thing, but the caller finishes deleting while the torrent is paused: the
    // paused torrent is what holds the claim then, and it has to take the report.
    let dropped = handle.drop_pieces(DROP)?;
    for id in dropped.pieces() {
        assert!(
            storage.release_piece(info_hash, piece(*id)),
            "piece {id} wasn't there"
        );
    }
    session.pause(&handle).await?;
    drop(dropped);
    session.unpause(&handle).await?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    assert_eq!(handle.reselect_pieces(DROP)?, DROP.len());
    // Nothing is holding them back anymore, so this finishes - if the report had gone
    // nowhere, these pieces would stay unpickable forever and this would time out.
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(read_back(handle.clone()).await?, orig_content);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_claim_survives_pause() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_claim_survives_pause(),
    )
    .await?
}

// A storage whose pieces the caller releases, on top of the filesystem one.
//
// It is a middleware in the sense of storage::middleware: it forwards everything to a
// FilesystemStorage, and forwards ensure_persistable() too, so it makes the same promise
// to session persistence that the storage underneath does. What it promises on its own is
// ensure_can_release_pieces(), which the filesystem storage can't, and which is what lets
// the bookkeeping tests above run piece_reclaim over real files.
//
// What it adds is a released-set, which is what has_piece() answers from. The bytes of a
// released piece are still on disk here - deleting them is the caller's job and it hasn't
// got round to it - and that is the point: the storage is the authority on what we have,
// not the bytes, and not the resume data.
#[derive(Clone, Default)]
struct ReleasingStorageFactory {
    underlying_factory: crate::storage::filesystem::FilesystemStorageFactory,
    released: std::sync::Arc<parking_lot::RwLock<std::collections::HashSet<u32>>>,
}

impl ReleasingStorageFactory {
    fn release_piece(&self, piece_id: u32) {
        self.released.write().insert(piece_id);
    }
}

impl crate::storage::StorageFactory for ReleasingStorageFactory {
    type Storage = ReleasingStorage;

    fn create(
        &self,
        shared: &crate::ManagedTorrentShared,
        metadata: &crate::torrent_state::TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        Ok(ReleasingStorage {
            underlying: Box::new(self.underlying_factory.create(shared, metadata)?),
            released: self.released.clone(),
        })
    }

    fn ensure_persistable(&self) -> anyhow::Result<()> {
        self.underlying_factory.ensure_persistable()
    }

    // It can't actually free a piece's bytes - that is the point of it as a test double -
    // but it does the storage's half of the reclaim contract: it takes a release one piece
    // at a time and answers has_piece() from it.
    fn ensure_can_release_pieces(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        self.clone().boxed()
    }
}

struct ReleasingStorage {
    underlying: Box<dyn crate::storage::TorrentStorage>,
    released: std::sync::Arc<parking_lot::RwLock<std::collections::HashSet<u32>>>,
}

impl crate::storage::TorrentStorage for ReleasingStorage {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.underlying.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.underlying.pwrite_all(file_id, offset, buf)
    }

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [std::io::IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        self.underlying.pwrite_all_vectored(file_id, offset, bufs)
    }

    fn remove_file(&self, file_id: usize, filename: &std::path::Path) -> anyhow::Result<()> {
        self.underlying.remove_file(file_id, filename)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.underlying.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn crate::storage::TorrentStorage>> {
        Ok(Box::new(ReleasingStorage {
            underlying: self.underlying.take()?,
            released: self.released.clone(),
        }))
    }

    fn init(
        &mut self,
        shared: &crate::ManagedTorrentShared,
        metadata: &crate::torrent_state::TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.underlying.init(shared, metadata)
    }

    fn remove_directory_if_empty(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.underlying.remove_directory_if_empty(path)
    }

    fn on_piece_completed(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<()> {
        self.underlying.on_piece_completed(piece_index)
    }

    fn has_piece(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<bool> {
        Ok(!self.released.read().contains(&piece_index.get()))
    }
}

// What survives a restart is the have-set, and the storage decides that. The want-set
// doesn't, and can't be read off the storage: a piece the caller released and a piece it
// never downloaded are the same hole. So a restored torrent wants every hole - and used to
// come back without piece_reclaim at all, and with drop_pieces() refusing pieces we don't
// have, so nothing could be done about it: every relaunch refilled the disk that reclaim
// was keeping small. Now the flag is persisted, and the caller re-applies its want-set on
// the restored torrent while it is still paused, so no peer gets a chance to fill a hole.
//
// Paused is not the record's choice to make. The torrent here is live when the process
// dies - a kill, a crash, the ENOSPC that ended the torrent - and its record says so; a
// restore that believed it would come back live and wanting the holes, with a seeder that
// has them, and the caller's drop would land after the first pieces did. A reclaim torrent
// comes back paused whatever the record says.
async fn e2e_piece_reclaim_want_set_is_reapplied_after_a_restart() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, server_session, peer) =
        seeding_server("test_piece_reclaim_restart", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let dir = TempDir::with_prefix("test_piece_reclaim_restart_client")?;
    let output_folder = dir.path().join("out");
    let persistence_folder = dir.path().join("session");
    let storage = InMemoryPieceStorageFactory::default();
    // The client listens, so the seeder can come to it after the restart: a restored
    // torrent has no peers of its own, and a hole nobody can reach proves nothing.
    let session_opts = || crate::SessionOptions {
        dht: None,
        persistence: Some(crate::SessionPersistenceConfig::Json {
            folder: Some(persistence_folder.clone()),
        }),
        disable_local_service_discovery: true,
        peer_id: Some(TestPeerMetadata::good().as_peer_id()),
        default_storage_factory: Some(storage.clone().boxed()),
        listen: Some(crate::listen::ListenerOptions {
            listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let session = Session::new_with_opts(output_folder.clone(), session_opts()).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                piece_reclaim: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;

    // Half the pieces go, and the process dies with the torrent live: the record says live.
    release(&storage, &handle, DROP)?;
    let held = storage_has(&storage, &handle)?;
    assert!(!handle.is_paused());
    drop(handle);
    drop(session);

    let session = Session::new_with_opts(output_folder.clone(), session_opts()).await?;
    let handle = session
        .get(crate::api::TorrentIdOrHash::Id(0))
        .context("expected the torrent to be restored from persistence")?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    assert!(
        handle.with_state(|s| matches!(s, crate::ManagedTorrentState::Paused(_))),
        "a reclaim torrent that was live at shutdown was not restored paused"
    );

    // The have-set is the storage's, holes included, and every hole is wanted.
    assert_eq!(torrent_has(&handle)?, held);
    assert!(!handle.stats().finished);

    // A seeder with all of it comes knocking before the caller has spoken. Paused, the
    // torrent doesn't answer, and the holes stay holes.
    let listen_addr = session
        .listen_addr()
        .context("expected the client to listen")?;
    server_session
        .get(crate::api::TorrentIdOrHash::Id(0))
        .context("expected the seeder's torrent")?
        .live()
        .context("expected the seeder to be live")?
        .add_peer_if_not_seen(listen_addr)?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(handle.with_state(|s| matches!(s, crate::ManagedTorrentState::Paused(_))));
    assert_eq!(
        storage.piece_count(handle.info_hash()),
        TOTAL_PIECES as usize - DROP.len(),
        "a hole was filled before the caller re-applied its want-set"
    );
    assert_eq!(torrent_has(&handle)?, held);

    // The caller re-applies its want-set: the same range, none of which we have now.
    let dropped = handle
        .drop_pieces(DROP)
        .context("drop_pieces on the restored, paused torrent")?;
    assert_eq!(
        dropped.pieces(),
        DROP.collect::<Vec<_>>(),
        "pieces we don't have were not dropped"
    );
    drop(dropped);
    assert!(handle.stats().finished);
    assert_eq!(torrent_has(&handle)?, held);

    // Live, with a seeder that has everything, nothing is downloaded: the holes are not
    // wanted.
    session.unpause(&handle).await?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    handle
        .live()
        .context("expected a live torrent")?
        .add_peer_if_not_seen(peer)?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        storage.piece_count(handle.info_hash()),
        TOTAL_PIECES as usize - DROP.len(),
        "a dropped piece was downloaded again after the restart"
    );
    assert!(handle.stats().finished);

    // Wanted again on request, and only then.
    assert_eq!(handle.reselect_pieces(DROP)?, DROP.len());
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(
        storage.piece_count(handle.info_hash()),
        TOTAL_PIECES as usize
    );
    assert_eq!(read_back(handle.clone()).await?, orig_content);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_want_set_is_reapplied_after_a_restart() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_want_set_is_reapplied_after_a_restart(),
    )
    .await?
}

// When the torrent finishes under an open stream, the peers that have all of it are sent
// away (there is nothing left to want from them). Dropping pieces keeps the torrent
// finished, so nothing brings them back when that same stream then seeks into the dropped
// range: the read parked forever. A stream opened after the drop was fine - creating one
// reconnects peers when its file is unfinished - and so is a reselect; it is the long-lived
// reader that seeks, exactly what a player does, that hung. A parked read now asks for the
// peers back itself.
async fn e2e_piece_reclaim_seek_back_after_finishing() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_seek_back", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let storage = InMemoryPieceStorageFactory::default();
    let dir = TempDir::with_prefix("test_piece_reclaim_seek_back_client")?;
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
                piece_reclaim: true,
                storage_factory: Some(storage.clone().boxed()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    // The stream is open while the torrent is still downloading, and stays open across it
    // finishing: that is what parks the seeder.
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    let mut stream = handle.clone().stream(0).await?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    assert_eq!(buf, orig_content);

    // The reader is at EOF, so its lookahead protects nothing and the range goes.
    release(&storage, &handle, DROP)?;
    assert!(handle.stats().finished);

    // The player seeks back to the start and reads. Without the fix this parks forever.
    Pin::new(&mut stream).start_seek(SeekFrom::Start(0))?;
    let mut piece = vec![0u8; PIECE_LEN as usize];
    timeout(Duration::from_secs(30), stream.read_exact(&mut piece))
        .await
        .context("the read parked: nothing brought the peers back for the dropped range")??;
    assert_eq!(piece, orig_content[..PIECE_LEN as usize]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_seek_back_after_finishing() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_seek_back_after_finishing(),
    )
    .await?
}

// Outside a window of the first half of the file: what a caller keeping a bounded window
// has dropped before any of it arrived.
const OUTSIDE_THE_WINDOW: std::ops::Range<u32> = TOTAL_PIECES / 2..TOTAL_PIECES;

// A reclaiming client whose window, the first half of the file, is in and whose rest was
// dropped before a single piece of it could arrive. It wants nothing, and it is still
// connected to the seeder it downloaded the window from.
async fn windowed_client(
    prefix: &str,
    torrent_bytes: &[u8],
    peer: std::net::SocketAddr,
) -> anyhow::Result<(TempDir, Client, InMemoryPieceStorageFactory)> {
    let storage = InMemoryPieceStorageFactory::default();
    let dir = TempDir::with_prefix(prefix)?;
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
                paused: true,
                initial_peers: Some(vec![peer]),
                piece_reclaim: true,
                storage_factory: Some(storage.clone().boxed()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    let claim = handle.drop_pieces(OUTSIDE_THE_WINDOW)?;
    assert_eq!(claim.pieces(), OUTSIDE_THE_WINDOW.collect::<Vec<_>>());
    drop(claim);
    session.unpause(&handle).await?;

    // The window is in: nothing left that the torrent wants.
    timeout(Duration::from_secs(30), async {
        while !handle.stats().finished {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    Ok((dir, (session, handle), storage))
}

// A caller keeping a bounded window drops what is outside it before it arrives, and
// wants it again as the window moves. The moment the window is in, the torrent wants
// nothing - and has half of its file. It used to call that finished and hang up on the
// seeder it was downloading from, as a finished torrent does, so the reselect that moved
// the window had to dial it all over again: a connect, a handshake and a bitfield per
// window, with no stream open to keep the seeder around.
async fn e2e_piece_reclaim_a_full_window_keeps_its_seeder() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_window", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();
    let (_dir, (_session, handle), _storage) =
        windowed_client("test_piece_reclaim_window_client", &torrent_bytes, peer).await?;

    let live = handle.live().context("expected a live torrent")?;
    assert!(
        !live.is_finished(),
        "half the file is missing, so the torrent is not finished"
    );
    // Two things hang up on a seeder once the torrent is finished: the piece that finishes
    // it, at once, and the seeder's own request loop, the next time it wakes to look for
    // work - which, with nothing to ask for, is on a five-second timer. So watched for
    // longer than that.
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        assert_eq!(
            live.stats_snapshot().peer_stats.live,
            1,
            "the torrent hung up on its seeder once the window was in"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The window moves on.
    assert_eq!(
        handle.reselect_pieces(OUTSIDE_THE_WINDOW)?,
        OUTSIDE_THE_WINDOW.len()
    );
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(read_back(handle.clone()).await?, orig_content);
    let stats = live.per_peer_stats_snapshot(PeerStatsFilter {
        state: PeerStatsFilterState::All,
    });
    let seeder = stats
        .peers
        .get(&peer.to_string())
        .context("expected the seeder in the peer table")?;
    assert_eq!(
        seeder.counters.connection_attempts, 1,
        "the next window had to dial the seeder again"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_a_full_window_keeps_its_seeder() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_a_full_window_keeps_its_seeder(),
    )
    .await?
}

// A stream that seeks out of the window, into a piece that was dropped. The stream pulls
// it in through its priority window, which is not the queue, and the seeder's request loop
// found nothing to ask for when the window filled: it sleeps until a piece is queued or
// its five-second timer fires. Nothing queues a dropped piece, so the read waited out the
// timer.
async fn e2e_piece_reclaim_a_seek_out_of_the_window_wakes_the_seeder() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_window_seek", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();
    let (_dir, (_session, handle), _storage) = windowed_client(
        "test_piece_reclaim_window_seek_client",
        &torrent_bytes,
        peer,
    )
    .await?;

    // A lookahead of one piece, so the stream wants the piece it reads and nothing else.
    let mut stream = handle
        .clone()
        .stream_with_options(
            0,
            crate::FileStreamOptions {
                lookahead_bytes: PIECE_LEN as u64,
            },
        )
        .await?;
    let target = OUTSIDE_THE_WINDOW.start + 2;
    let offset = (target * PIECE_LEN) as usize;
    Pin::new(&mut stream).start_seek(SeekFrom::Start(offset as u64))?;
    let started = Instant::now();
    let mut piece = vec![0u8; PIECE_LEN as usize];
    timeout(Duration::from_secs(30), stream.read_exact(&mut piece)).await??;
    assert_eq!(piece, orig_content[offset..offset + PIECE_LEN as usize]);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the read took {:?}: the seeder was asleep and nothing woke it",
        started.elapsed()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_a_seek_out_of_the_window_wakes_the_seeder() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_a_seek_out_of_the_window_wakes_the_seeder(),
    )
    .await?
}

// A stream parked on a piece whose storage a claim is still releasing. The seeder's
// request loop passes over a piece under a claim, finds nothing else, and goes to sleep.
// The release is what makes the piece available, and it woke the peers only for pieces it
// put back in the queue - which a dropped piece is not, so the read waited out the timer.
async fn e2e_piece_reclaim_a_release_under_a_parked_read_wakes_the_seeder() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_window_release", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();
    let (_dir, (_session, handle), storage) = windowed_client(
        "test_piece_reclaim_window_release_client",
        &torrent_bytes,
        peer,
    )
    .await?;

    // Dropped before the stream exists, since a stream's lookahead protects what it covers.
    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let claim = handle.drop_pieces(0..1)?;
    assert_eq!(claim.pieces(), &[0]);
    assert!(storage.release_piece(handle.info_hash(), lengths.validate_piece_index(0).unwrap()));

    let mut stream = handle
        .clone()
        .stream_with_options(
            0,
            crate::FileStreamOptions {
                lookahead_bytes: PIECE_LEN as u64,
            },
        )
        .await?;
    let reader = tokio::spawn(async move {
        let mut piece = vec![0u8; PIECE_LEN as usize];
        stream.read_exact(&mut piece).await?;
        Ok::<_, anyhow::Error>((piece, Instant::now()))
    });
    // Long enough for the read to park and for the seeder to look, pass over the piece
    // under the claim, and go back to sleep.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!reader.is_finished(), "the read did not wait for the claim");

    let released = Instant::now();
    drop(claim);
    let (piece, read) = timeout(Duration::from_secs(30), reader).await???;
    assert_eq!(piece, orig_content[..PIECE_LEN as usize]);
    assert!(
        read - released < Duration::from_secs(2),
        "the read took {:?} after the release: the seeder was asleep and nothing woke it",
        read - released
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_a_release_under_a_parked_read_wakes_the_seeder()
-> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_a_release_under_a_parked_read_wakes_the_seeder(),
    )
    .await?
}

// A peer's writer reads the chunk it uploads long after the upload scheduler checked we
// have the piece. Dropped in between and downloading again, the piece has the first chunk
// of the new download staged, and a storage that stages serves that copy first: the peer
// would be sent part of a piece, and fail its hash.
async fn e2e_piece_reclaim_a_read_queued_before_a_drop_is_refused() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_stale_read", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();
    let (_dir, (_session, handle), _storage) =
        windowed_client("test_piece_reclaim_stale_read_client", &torrent_bytes, peer).await?;
    let live = handle.live().context("expected a live torrent")?;
    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let piece = lengths.validate_piece_index(0).unwrap();
    let chunk = lengths
        .chunk_info_from_received_data(piece, 0, CHUNK_SIZE)
        .unwrap();

    let mut buf = vec![0u8; CHUNK_SIZE as usize];
    live.read_chunk_for_upload(&chunk, &mut buf)?;
    assert_eq!(buf, orig_content[..CHUNK_SIZE as usize]);

    // Dropped with its storage still there, and the first chunk of a new download staged.
    let claim = handle.drop_pieces(0..1)?;
    assert_eq!(claim.pieces(), &[0]);
    live.files
        .pwrite_all(0, 0, &vec![0u8; CHUNK_SIZE as usize])?;

    let res = live.read_chunk_for_upload(&chunk, &mut buf);
    assert!(
        res.is_err(),
        "a read for a piece we no longer have went through, and read {:?}",
        &buf[..8]
    );
    drop(claim);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_a_read_queued_before_a_drop_is_refused() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_a_read_queued_before_a_drop_is_refused(),
    )
    .await?
}

// A storage that takes every chunk and refuses to commit any piece: what a full disk or
// a directory that won't take a rename looks like to a storage that stages pieces.
#[derive(Clone, Default)]
struct RefusingCommitStorageFactory {
    underlying: InMemoryPieceStorageFactory,
    attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl crate::storage::StorageFactory for RefusingCommitStorageFactory {
    type Storage = RefusingCommitStorage;

    fn create(
        &self,
        shared: &crate::ManagedTorrentShared,
        metadata: &crate::torrent_state::TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        Ok(RefusingCommitStorage {
            underlying: Box::new(self.underlying.create(shared, metadata)?),
            attempts: self.attempts.clone(),
        })
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        self.clone().boxed()
    }
}

struct RefusingCommitStorage {
    underlying: Box<dyn crate::storage::TorrentStorage>,
    attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl crate::storage::TorrentStorage for RefusingCommitStorage {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.underlying.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.underlying.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &std::path::Path) -> anyhow::Result<()> {
        self.underlying.remove_file(file_id, filename)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.underlying.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn crate::storage::TorrentStorage>> {
        Ok(Box::new(RefusingCommitStorage {
            underlying: self.underlying.take()?,
            attempts: self.attempts.clone(),
        }))
    }

    fn init(
        &mut self,
        shared: &crate::ManagedTorrentShared,
        metadata: &crate::torrent_state::TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.underlying.init(shared, metadata)
    }

    fn remove_directory_if_empty(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.underlying.remove_directory_if_empty(path)
    }

    fn on_piece_completed(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<()> {
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        anyhow::bail!("refusing to commit piece {piece_index}: no space left on device")
    }

    fn has_piece(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<bool> {
        self.underlying.has_piece(piece_index)
    }
}

// on_piece_completed() is the storage's word that the piece is there to stay, and a
// storage that stages pieces gives it by moving the piece into place. It used to be asked
// after the have-bit was set, and a refusal was logged at debug and ignored: the torrent
// finished, advertised every piece and served them, over bytes has_piece() said it didn't
// have. A refused commit is a disk failure like a failed write, and ends the torrent the
// same way, before the piece is anyone's.
async fn e2e_refused_commit_is_fatal() -> anyhow::Result<()> {
    setup_test_logging();
    let (_files, torrent_bytes, _server_session, peer) =
        seeding_server("test_refused_commit", FILE_SIZE).await?;

    let storage = RefusingCommitStorageFactory::default();
    let dir = TempDir::with_prefix("test_refused_commit_client")?;
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
                storage_factory: Some(storage.clone().boxed()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    // The first piece to pass its hash check is refused, and that is the end of it.
    timeout(Duration::from_secs(30), async {
        loop {
            if handle.with_state(|s| matches!(s, crate::ManagedTorrentState::Error(_))) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("the torrent didn't stop: a refused commit was swallowed")?;

    let stats = handle.stats();
    assert!(!stats.finished);
    assert_eq!(stats.progress_bytes, 0);
    let error = stats.error.context("expected the torrent's error")?;
    assert!(error.contains("no space left on device"), "{error}");

    // Nothing was committed, so nothing is ours: has_piece() and the have-set agree.
    assert!(storage.attempts.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    assert_eq!(storage.underlying.piece_count(handle.info_hash()), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_refused_commit_is_fatal() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), e2e_refused_commit_is_fatal()).await?
}

// The have-bitfield is flushed lazily (16 MiB of piece completions), so a caller that
// releases pieces and then dies leaves resume data claiming pieces it no longer has.
// Startup intersects that resume data with TorrentStorage::has_piece(), and this is that
// path end to end: download with session persistence on, release half the pieces without
// flushing anything, then start a second session over the same persistence and storage.
//
// Nothing else would catch it: the fastresume hash check validates one piece per file
// plus at most 64 sampled ones, and here the released pieces' bytes are still on disk, so
// they would pass it.
async fn e2e_piece_reclaim_resume_data_is_intersected_with_storage() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_resume", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let dir = TempDir::with_prefix("test_piece_reclaim_resume_client")?;
    let output_folder = dir.path().join("out");
    let persistence_folder = dir.path().join("session");
    let storage = ReleasingStorageFactory::default();

    let session_opts = || crate::SessionOptions {
        dht: None,
        persistence: Some(crate::SessionPersistenceConfig::Json {
            folder: Some(persistence_folder.clone()),
        }),
        fastresume: true,
        disable_local_service_discovery: true,
        peer_id: Some(TestPeerMetadata::good().as_peer_id()),
        default_storage_factory: Some(storage.clone().boxed()),
        ..Default::default()
    };

    let session = Session::new_with_opts(output_folder.clone(), session_opts()).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                piece_reclaim: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(
        std::fs::read(output_folder.join("0.data")).unwrap(),
        orig_content
    );

    // The have-bitfield as it stands with everything downloaded. This is what is on disk
    // at the moment of the crash below: dropping pieces defers the flush to the same
    // 16 MiB threshold as completing them, and this torrent is 256 KiB.
    let bitv = std::fs::read_dir(&persistence_folder)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|e| e == "bitv"))
        .context("expected a .bitv file in the persistence folder")?;
    let resume_before_drop = std::fs::read(&bitv)?;

    // The caller takes half the pieces and releases their storage.
    let dropped = handle.drop_pieces(DROP)?;
    assert_eq!(dropped.pieces(), DROP.collect::<Vec<_>>());
    for id in dropped.pieces() {
        storage.release_piece(*id);
    }
    drop(dropped);

    // Shut the session down and put back the bitfield a crash would have left. Dropping
    // the session is an orderly shutdown - DiskBackedBitV flushes on drop - and that is
    // exactly what a crash doesn't get to do.
    drop(handle);
    drop(session);
    let mut flushed = false;
    for _ in 0..100 {
        if std::fs::read(&bitv)? != resume_before_drop {
            flushed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        flushed,
        "the shutdown never wrote the bitfield, so putting back the pre-drop one proves nothing"
    );
    std::fs::write(&bitv, &resume_before_drop)?;

    let session = Session::new_with_opts(output_folder.clone(), session_opts()).await?;
    let handle = session
        .get(crate::api::TorrentIdOrHash::Id(0))
        .context("expected the torrent to be restored from persistence")?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;

    handle.with_chunk_tracker(|ct| {
        let have = ct.get_have_pieces().as_slice();
        for id in 0..TOTAL_PIECES {
            assert_eq!(
                have[id as usize],
                !DROP.contains(&id),
                "piece {id}: the have-set came from the resume data, not from the storage"
            );
        }
    })?;
    assert!(!handle.stats().finished);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_resume_data_is_intersected_with_storage() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_resume_data_is_intersected_with_storage(),
    )
    .await?
}

// Which pieces of the torrent the storage holds, complete.
fn storage_has(
    storage: &InMemoryPieceStorageFactory,
    handle: &crate::ManagedTorrent,
) -> anyhow::Result<Vec<bool>> {
    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    Ok((0..TOTAL_PIECES)
        .map(|id| {
            storage.has_piece(
                handle.info_hash(),
                lengths.validate_piece_index(id).unwrap(),
            )
        })
        .collect())
}

// The have-set the torrent came up with.
fn torrent_has(handle: &crate::ManagedTorrent) -> anyhow::Result<Vec<bool>> {
    handle.with_chunk_tracker(|ct| {
        let have = ct.get_have_pieces().as_slice();
        (0..TOTAL_PIECES).map(|id| have[id as usize]).collect()
    })
}

// Release the pieces of a range and let the claim go: the storage has holes where they
// were, and the torrent knows it.
fn release(
    storage: &InMemoryPieceStorageFactory,
    handle: &std::sync::Arc<crate::ManagedTorrent>,
    pieces: std::ops::Range<u32>,
) -> anyhow::Result<()> {
    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let dropped = handle.drop_pieces(pieces.clone())?;
    assert_eq!(dropped.pieces(), pieces.collect::<Vec<_>>());
    for id in dropped.pieces() {
        assert!(
            storage.release_piece(
                handle.info_hash(),
                lengths.validate_piece_index(*id).unwrap()
            ),
            "piece {id} wasn't there"
        );
    }
    Ok(())
}

// The intersection above only runs when there is resume data to intersect, and fastresume
// is off by default - rqbit, the desktop app and a Session::new_with_opts that leaves it
// alone all do a full check at startup. So this is the path every shipped default takes,
// and it has to reach the same have-set: the full check asks the storage about each piece
// before reading it. It didn't, and a read of a released piece failed the way a missing
// file does, which wrote off every later piece of the file: one hole, and the rest of the
// film is downloaded again on every relaunch.
async fn e2e_piece_reclaim_full_check_asks_the_storage() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_full_check", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let dir = TempDir::with_prefix("test_piece_reclaim_full_check_client")?;
    let output_folder = dir.path().join("out");
    let persistence_folder = dir.path().join("session");
    let storage = InMemoryPieceStorageFactory::default();

    // Persistence on, fastresume left at its default: what a restart gets is the record
    // and the storage, with no bitfield to intersect.
    let session_opts = || crate::SessionOptions {
        dht: None,
        persistence: Some(crate::SessionPersistenceConfig::Json {
            folder: Some(persistence_folder.clone()),
        }),
        disable_local_service_discovery: true,
        peer_id: Some(TestPeerMetadata::good().as_peer_id()),
        default_storage_factory: Some(storage.clone().boxed()),
        ..Default::default()
    };

    let session = Session::new_with_opts(output_folder.clone(), session_opts()).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                piece_reclaim: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(read_back(handle.clone()).await?, orig_content);

    // One hole, early in the file: the only piece the storage doesn't have.
    release(&storage, &handle, 1..2)?;
    let held = storage_has(&storage, &handle)?;
    assert_eq!(held.iter().filter(|h| !**h).count(), 1);

    drop(handle);
    drop(session);

    let session = Session::new_with_opts(output_folder.clone(), session_opts()).await?;
    let handle = session
        .get(crate::api::TorrentIdOrHash::Id(0))
        .context("expected the torrent to be restored from persistence")?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;

    assert_eq!(
        torrent_has(&handle)?,
        held,
        "the full check didn't come up with what the storage holds"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_full_check_asks_the_storage() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_full_check_asks_the_storage(),
    )
    .await?
}

// Restarting a torrent that hit a fatal error throws the bitfield away and does a full
// check - and a fatal error is what ENOSPC on a full disk is, which is the situation
// piece reclaim exists for. The have-set that check comes up with has to be what the
// storage holds, holes included, as the DroppedPieces doc promises.
async fn e2e_piece_reclaim_errored_torrent_asks_the_storage() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_piece_reclaim_errored", FILE_SIZE).await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let storage = InMemoryPieceStorageFactory::default();
    let dir = TempDir::with_prefix("test_piece_reclaim_errored_client")?;
    let (session, handle) = add_client_with_storage(
        &dir,
        &torrent_bytes,
        peer,
        true,
        Some(storage.clone().boxed()),
    )
    .await?;

    // A hole in the middle of the file, then the error.
    release(&storage, &handle, 4..8)?;
    let held = storage_has(&storage, &handle)?;
    handle.stop_with_error(anyhow!("simulated fatal error"));
    assert!(handle.with_state(|s| matches!(s, crate::ManagedTorrentState::Error(_))));

    // Restart it into a paused state, so that what the check came up with is what we
    // look at, and not what a peer had time to fill in since.
    handle.start(None, true)?;
    timeout(Duration::from_secs(30), handle.wait_until_initialized()).await??;
    assert_eq!(
        torrent_has(&handle)?,
        held,
        "the check after the error didn't come up with what the storage holds"
    );

    // And from there it downloads exactly the hole.
    session.unpause(&handle).await?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    assert_eq!(
        storage.piece_count(handle.info_hash()),
        TOTAL_PIECES as usize
    );
    assert_eq!(read_back(handle.clone()).await?, orig_content);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_piece_reclaim_errored_torrent_asks_the_storage() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        e2e_piece_reclaim_errored_torrent_asks_the_storage(),
    )
    .await?
}
