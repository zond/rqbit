// What session persistence needs from a storage, end to end.
//
// The persisted record is the torrent, an output folder, a file selection and a paused
// flag. What isn't in it is the storage: a restart replays the record through
// add_torrent, which builds the session's default storage from the output folder and the
// file selection, and beside the record there is a have-bitfield the previous run wrote.
// So what makes a torrent persistable is a promise its storage makes - see
// StorageFactory::ensure_persistable - and not which storage it happens to be.

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Context;
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tokio::{io::AsyncReadExt, time::timeout};

use crate::{
    AddTorrent, CreateTorrentOptions, Session, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{
        StorageFactoryExt,
        examples::inmemory::{InMemoryExampleStorageFactory, InMemoryPieceStorageFactory},
    },
    tests::test_util::{TestPeerMetadata, setup_test_logging},
    type_aliases::BF,
};

use super::test_util::create_default_random_dir_with_torrents;

const PIECE_LEN: u32 = CHUNK_SIZE;
const TOTAL_PIECES: u32 = 8;
const FILE_SIZE: usize = (PIECE_LEN * TOTAL_PIECES) as usize;
const DROP: std::ops::Range<u32> = 0..TOTAL_PIECES / 2;

// A session that has the whole torrent and will serve it, plus the torrent file and the
// address to connect to.
async fn seeding_server(
    prefix: &str,
) -> anyhow::Result<(
    TempDir,
    Vec<u8>,
    std::sync::Arc<Session>,
    std::net::SocketAddr,
)> {
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

// A client session with persistence and fastresume on, so that a second one over the same
// folders is a restart.
fn session_opts(
    persistence_folder: &std::path::Path,
    storage_factory: Option<crate::storage::BoxStorageFactory>,
) -> crate::SessionOptions {
    crate::SessionOptions {
        dht: None,
        persistence: Some(crate::SessionPersistenceConfig::Json {
            folder: Some(persistence_folder.to_owned()),
        }),
        fastresume: true,
        disable_local_service_discovery: true,
        peer_id: Some(TestPeerMetadata::good().as_peer_id()),
        default_storage_factory: storage_factory,
        ..Default::default()
    }
}

// The have-bitfield the previous run left behind, which is the claim a restart starts
// from. Written when the session is dropped, so this waits for it.
async fn resume_data(persistence_folder: &std::path::Path) -> anyhow::Result<BF> {
    let filename = std::fs::read_dir(persistence_folder)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|e| e == "bitv"))
        .context("expected a .bitv file in the persistence folder")?;
    let mut last = BF::default();
    for _ in 0..100 {
        last = BF::from_boxed_slice(std::fs::read(&filename)?.into_boxed_slice());
        if last.count_ones() == TOTAL_PIECES as usize {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(last)
}

// Read the file back through the torrent, which is what a consumer of it does.
async fn read_back(handle: std::sync::Arc<crate::ManagedTorrent>) -> anyhow::Result<Vec<u8>> {
    let mut stream = handle.stream(0).await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}

// The default path, asserted rather than assumed: the filesystem storage promises what
// persistence needs, so a torrent using it is written to the database and comes back
// whole.
async fn the_filesystem_storage_is_persisted_and_restored() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_persistence_fs").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let dir = TempDir::with_prefix("test_persistence_fs_client")?;
    let output_folder = dir.path().join("out");
    let persistence_folder = dir.path().join("session");

    let session = Session::new_with_opts(
        output_folder.clone(),
        // No default_storage_factory: this is what everyone gets.
        session_opts(&persistence_folder, None),
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
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

    // The record is on disk, and it says where the data is - which for this storage is
    // all a restart needs.
    let db: serde_json::Value =
        serde_json::from_slice(&std::fs::read(persistence_folder.join("session.json"))?)?;
    assert_eq!(
        db["torrents"]["0"]["output_folder"]
            .as_str()
            .map(std::path::Path::new),
        Some(output_folder.as_path())
    );

    drop(handle);
    drop(session);
    assert_eq!(
        resume_data(&persistence_folder).await?.count_ones(),
        TOTAL_PIECES as usize
    );

    let session = Session::new_with_opts(
        output_folder.clone(),
        session_opts(&persistence_folder, None),
    )
    .await?;
    let handle = session
        .get(crate::api::TorrentIdOrHash::Id(0))
        .context("expected the torrent to be restored from persistence")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;
    handle.with_chunk_tracker(|ct| {
        assert!(ct.get_have_pieces().as_slice()[..TOTAL_PIECES as usize].all());
    })?;
    assert_eq!(read_back(handle).await?, orig_content);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_the_filesystem_storage_is_persisted_and_restored() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        the_filesystem_storage_is_persisted_and_restored(),
    )
    .await?
}

// A storage that isn't the filesystem one at all - it keeps whole pieces in memory and
// never writes a file - is persisted and restored all the same, because it can promise
// both halves of what the record needs: the session keeps the factory, so the pieces are
// still reachable, and has_piece() answers for the ones that aren't.
//
// The have-set on the way back up is the storage's, not the record's: the pieces released
// here are released behind the bitfield's back, exactly as they would be if the process
// had died between the release and the next flush.
async fn a_non_filesystem_storage_is_persisted_and_restored() -> anyhow::Result<()> {
    setup_test_logging();
    let (files, torrent_bytes, _server_session, peer) =
        seeding_server("test_persistence_inmemory").await?;
    let orig_content = std::fs::read(files.path().join("0.data")).unwrap();

    let dir = TempDir::with_prefix("test_persistence_inmemory_client")?;
    let output_folder = dir.path().join("out");
    let persistence_folder = dir.path().join("session");
    let storage = InMemoryPieceStorageFactory::default();

    let session = Session::new_with_opts(
        output_folder.clone(),
        session_opts(&persistence_folder, Some(storage.clone().boxed())),
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(crate::AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;

    // The data is in the store, and nowhere on disk: the output folder in the record
    // names nothing.
    assert_eq!(storage.piece_count(), TOTAL_PIECES as usize);
    assert_eq!(read_back(handle.clone()).await?, orig_content);
    assert!(!output_folder.join("0.data").exists());

    let lengths = *handle
        .metadata
        .load_full()
        .context("no metadata")?
        .lengths();
    let piece = |id: u32| lengths.validate_piece_index(id).unwrap();

    // Half the pieces go while the bitfield isn't looking.
    for id in DROP {
        assert!(storage.release_piece(piece(id)), "piece {id} wasn't there");
    }

    drop(handle);
    drop(session);

    // The record claims every piece - so a restart that believed it would advertise and
    // serve the ones that are gone.
    assert_eq!(
        resume_data(&persistence_folder).await?.count_ones(),
        TOTAL_PIECES as usize
    );

    let session = Session::new_with_opts(
        output_folder.clone(),
        session_opts(&persistence_folder, Some(storage.clone().boxed())),
    )
    .await?;
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
                "piece {id}: the have-set came from the record, not from the storage"
            );
        }
    })?;
    assert!(!handle.stats().finished);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_non_filesystem_storage_is_persisted_and_restored() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        a_non_filesystem_storage_is_persisted_and_restored(),
    )
    .await?
}

// A storage that can't promise it is refused when the torrent is added, naming what it
// didn't promise. The in-memory example storage is one: its map goes with the process,
// and it doesn't implement has_piece(), so a restart would take the record's word for a
// have-set whose bytes are gone.
async fn a_storage_that_cant_promise_persistence_is_refused_at_add_time() -> anyhow::Result<()> {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, FILE_SIZE, Some("test_persistence_bad"));
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

    let dir = TempDir::with_prefix("test_persistence_bad_client")?;
    let persistence_folder = dir.path().join("session");
    let session = Session::new_with_opts(
        dir.path().join("out"),
        session_opts(
            &persistence_folder,
            Some(InMemoryExampleStorageFactory::default().boxed()),
        ),
    )
    .await?;

    let err = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(crate::AddTorrentOptions {
                paused: true,
                ..Default::default()
            }),
        )
        .await
        .err()
        .context("expected adding a torrent with this storage to fail")?;
    let err = format!("{err:#}");
    assert!(err.contains("InMemoryExampleStorageFactory"), "{err}");
    assert!(err.contains("ensure_persistable"), "{err}");

    // Loudly, and at add time: nothing is left in the session, and nothing was written
    // for a restart to find.
    assert!(session.get(crate::api::TorrentIdOrHash::Id(0)).is_none());
    assert!(!persistence_folder.join("session.json").exists());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_storage_that_cant_promise_persistence_is_refused_at_add_time() -> anyhow::Result<()>
{
    timeout(
        Duration::from_secs(120),
        a_storage_that_cant_promise_persistence_is_refused_at_add_time(),
    )
    .await?
}
