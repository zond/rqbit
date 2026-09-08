//! Pausing and unpausing while the initial check is running.
//!
//! `ManagedTorrent::is_paused` is the *intent* -- what the torrent is meant to be doing,
//! whatever state it happens to be in right now -- and it is what session persistence
//! writes down. The initial check runs in a spawned task, so the intent can move while
//! it runs. What these tests pin down is that the state the check lands the torrent in
//! is the intent as it stands when the check finishes, not as it stood when the check
//! was started.

use std::{
    any::TypeId,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use tempfile::TempDir;
use tokio::time::timeout;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ManagedTorrentShared, Session,
    SessionOptions, SessionPersistenceConfig, TorrentMetadata, TorrentStatsState,
    api::TorrentIdOrHash,
    create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{
        BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
        filesystem::FilesystemStorageFactory,
    },
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
    type_aliases::BF,
};

const WAIT: Duration = Duration::from_secs(30);
const PIECE_LENGTH: u32 = 32768;
const FILES: usize = 2;
const FILE_SIZE: usize = 512 * 1024;

/// A hold on the initial check: every read it makes stops here until the test opens the
/// gate. That is what lets a test pause or unpause with the check provably still
/// running, instead of racing a sleep against it.
#[derive(Default)]
struct Gate {
    reads: AtomicUsize,
    open: AtomicBool,
}

impl Gate {
    fn hold(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
        while !self.open.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
    }

    /// Returns once a check is inside `hold`: it has started, and it cannot get past
    /// this read until the test says so.
    async fn wait_until_check_started(&self) -> anyhow::Result<()> {
        wait_until(
            || match self.reads.load(Ordering::SeqCst) {
                0 => bail!("the initial check hasn't read anything yet"),
                _ => Ok(()),
            },
            WAIT,
        )
        .await
    }
}

/// Filesystem storage with [`Gate`] in front of its reads.
#[derive(Clone)]
struct GatedStorageFactory {
    gate: Arc<Gate>,
    inner: FilesystemStorageFactory,
}

impl GatedStorageFactory {
    fn new(gate: &Arc<Gate>) -> Self {
        Self {
            gate: gate.clone(),
            inner: FilesystemStorageFactory::default(),
        }
    }
}

impl StorageFactory for GatedStorageFactory {
    type Storage = GatedStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<GatedStorage> {
        Ok(GatedStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            gate: self.gate.clone(),
        })
    }

    // Session persistence refuses any storage that isn't the filesystem one, so this
    // has to keep answering for what it wraps.
    fn is_type_id(&self, type_id: TypeId) -> bool {
        self.inner.is_type_id(type_id)
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

struct GatedStorage {
    inner: Box<dyn TorrentStorage>,
    gate: Arc<Gate>,
}

impl TorrentStorage for GatedStorage {
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.gate.hold();
        self.inner.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }

    // The gate is for the check only: what the torrent runs on afterwards is the
    // ungated storage underneath.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        self.inner.take()
    }
}

/// A directory of random files with the torrent for them, complete on disk: the initial
/// check has every piece to read and hash, which is what these tests need it busy with.
async fn complete_torrent_on_disk(prefix: &str) -> anyhow::Result<(TempDir, Vec<u8>)> {
    let dir = create_default_random_dir_with_torrents(FILES, FILE_SIZE, Some(prefix));
    let torrent = create_torrent(
        dir.path(),
        CreateTorrentOptions {
            piece_length: Some(PIECE_LENGTH),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    Ok((dir, torrent.as_bytes()?.to_vec()))
}

fn add_opts(dir: &TempDir, paused: bool, gate: &Arc<Gate>) -> AddTorrentOptions {
    AddTorrentOptions {
        paused,
        overwrite: true,
        output_folder: Some(dir.path().to_str().unwrap().to_owned()),
        storage_factory: Some(GatedStorageFactory::new(gate).boxed()),
        ..Default::default()
    }
}

fn session_opts(
    persistence_folder: Option<&Path>,
    storage_factory: Option<BoxStorageFactory>,
) -> SessionOptions {
    SessionOptions {
        dht: None,
        listen: None,
        disable_local_service_discovery: true,
        persistence: persistence_folder.map(|f| SessionPersistenceConfig::Json {
            folder: Some(f.to_owned()),
        }),
        fastresume: persistence_folder.is_some(),
        default_storage_factory: storage_factory,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unpause_during_the_initial_check_starts_the_torrent() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        unpause_during_the_initial_check_starts_the_torrent_inner(),
    )
    .await?
}

/// Added paused, so the check runs with "stay paused" true at the time it starts. An
/// unpause lands while it is running -- `Session::unpause` returns Ok -- and by the time
/// the check finishes the intent is "run", so the torrent has to be running.
async fn unpause_during_the_initial_check_starts_the_torrent_inner() -> anyhow::Result<()> {
    setup_test_logging();
    let (dir, torrent_bytes) = complete_torrent_on_disk("rqbit_unpause_during_check").await?;
    let gate = Arc::new(Gate::default());

    let session = Session::new_with_opts(dir.path().into(), session_opts(None, None)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes),
            Some(add_opts(&dir, true, &gate)),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    gate.wait_until_check_started().await?;
    session.unpause(&handle).await?;
    info!("unpaused while the initial check was running");
    gate.open();

    wait_until(
        || match handle.stats().state {
            TorrentStatsState::Live => Ok(()),
            other => bail!("waiting for the torrent to go live, it is {other:?}"),
        },
        WAIT,
    )
    .await
    .context("the unpause was swallowed by the initial check")?;
    assert!(handle.live().is_some(), "a running torrent has live state");
    assert!(
        handle.stats().finished,
        "and the check it ran found the data that was there"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_during_a_fastresume_check_leaves_the_torrent_paused() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        pause_during_a_fastresume_check_leaves_the_torrent_paused_inner(),
    )
    .await?
}

/// The other direction, and the one that costs bandwidth: a fastresume check only
/// samples the pieces the previous run claimed, and unlike the full check it never looks
/// at the pause request at all. So a pause that lands during one is not seen by the
/// check -- `Session::pause` returns Ok -- and the torrent must still end up stopped
/// rather than going live behind the caller's back.
async fn pause_during_a_fastresume_check_leaves_the_torrent_paused_inner() -> anyhow::Result<()> {
    setup_test_logging();
    let (dir, torrent_bytes) = complete_torrent_on_disk("rqbit_pause_during_fastresume").await?;
    let persistence = dir.path().join("session");

    // First run: an ordinary check finds every piece, and the have-bitfield it writes is
    // what the next run resumes from.
    let session =
        Session::new_with_opts(dir.path().into(), session_opts(Some(&persistence), None)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    timeout(WAIT, handle.wait_until_completed()).await??;
    let total_pieces = handle
        .metadata
        .load_full()
        .context("expected metadata")?
        .lengths()
        .total_pieces() as usize;
    drop(handle);
    drop(session);
    // Written as the session goes away, so wait for it, and insist it is the complete
    // claim: an incomplete one would send the restart down the full-check path instead,
    // which is a different bug's test.
    wait_until_resume_data_is_complete(&persistence, total_pieces).await?;

    // Second run: a restart. The restored torrent revalidates a sample of the pieces it
    // claims to have, and the gate holds it inside the first of those reads.
    let gate = Arc::new(Gate::default());
    let session = Session::new_with_opts(
        dir.path().into(),
        session_opts(
            Some(&persistence),
            Some(GatedStorageFactory::new(&gate).boxed()),
        ),
    )
    .await?;
    let handle = session
        .get(TorrentIdOrHash::Id(0))
        .context("expected the torrent to be restored from persistence")?;

    gate.wait_until_check_started().await?;
    session.pause(&handle).await?;
    info!("paused while the fastresume check was running");
    gate.open();

    wait_until(
        || match handle.stats().state {
            TorrentStatsState::Paused => Ok(()),
            other => bail!("waiting for the torrent to settle paused, it is {other:?}"),
        },
        WAIT,
    )
    .await
    .context("the fastresume check started a torrent that had been paused")?;
    assert!(
        handle.live().is_none(),
        "a stopped torrent has no live state"
    );
    Ok(())
}

/// The `.bitv` file the run before left behind, polled until it claims every piece.
async fn wait_until_resume_data_is_complete(
    persistence_folder: &Path,
    total_pieces: usize,
) -> anyhow::Result<()> {
    wait_until(
        || {
            let filename = std::fs::read_dir(persistence_folder)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .find(|p| p.extension().is_some_and(|e| e == "bitv"))
                .context("expected a .bitv file in the persistence folder")?;
            let bf = BF::from_boxed_slice(std::fs::read(&filename)?.into_boxed_slice());
            match bf.count_ones() {
                c if c == total_pieces => Ok(()),
                c => bail!("resume data claims {c} pieces of {total_pieces}"),
            }
        },
        WAIT,
    )
    .await
}
