//! Pausing and unpausing while the initial check is running.
//!
//! `ManagedTorrent::is_paused` is the *intent* -- what the torrent is meant to be doing,
//! whatever state it happens to be in right now -- and it is what session persistence
//! writes down. The initial check runs in a spawned task, so the intent can move while
//! it runs. What these tests pin down is that the state the check lands the torrent in
//! is the intent as it stands when the check finishes, not as it stood when the check
//! was started - and that a check a pause stopped for good is reported rather than
//! waited on.

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

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// Returns once a check is inside `hold`: it has started, and it cannot get past
    /// this read until the test says so.
    async fn wait_until_check_started(&self) -> anyhow::Result<()> {
        wait_until(
            || match self.reads() {
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

    // Session persistence asks the storage what it promises about a restart, and a
    // wrapper has to keep answering for what it wraps.
    fn is_type_id(&self, type_id: TypeId) -> bool {
        self.inner.is_type_id(type_id)
    }

    fn ensure_persistable(&self) -> anyhow::Result<()> {
        self.inner.ensure_persistable()
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

/// A session holding the whole torrent and listening, with the address to
/// dial it on: a real peer source, which is what tells "live" apart from
/// "live and able to fetch".
async fn seeding_session(
    prefix: &str,
) -> anyhow::Result<(TempDir, Vec<u8>, Arc<Session>, std::net::SocketAddr)> {
    let files = create_default_random_dir_with_torrents(FILES, FILE_SIZE, Some(prefix));
    let torrent_bytes = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LENGTH),
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
            listen: Some(crate::listen::ListenerOptions {
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a seeder handle")?
        .wait_until_completed()
        .await?;
    let peer = session
        .listen_addr()
        .context("the seeder is not listening")?;
    Ok((files, torrent_bytes, session, peer))
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

#[tokio::test(flavor = "multi_thread")]
async fn a_paused_initial_check_fails_the_wait_instead_of_hanging() -> anyhow::Result<()> {
    timeout(
        Duration::from_secs(120),
        a_paused_initial_check_fails_the_wait_instead_of_hanging_inner(),
    )
    .await?
}

/// A pause aborts the initial check between pieces, and there is nowhere for the torrent
/// to go from there: it stays Initializing with no check running, and only an unpause
/// starts a new one. `wait_until_initialized` polls that state, so it has to say so
/// rather than wait for a check that will never run.
async fn a_paused_initial_check_fails_the_wait_instead_of_hanging_inner() -> anyhow::Result<()> {
    setup_test_logging();
    let (dir, torrent_bytes) = complete_torrent_on_disk("rqbit_pause_during_check").await?;
    let gate = Arc::new(Gate::default());

    let session = Session::new_with_opts(dir.path().into(), session_opts(None, None)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes),
            Some(add_opts(&dir, false, &gate)),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    gate.wait_until_check_started().await?;
    session.pause(&handle).await?;
    info!("paused while the initial check was running");
    gate.open();

    let err = timeout(Duration::from_secs(10), handle.wait_until_initialized())
        .await
        .context("wait_until_initialized hung on a check that had stopped")?
        .expect_err("the check was paused, so it never initialized");
    info!("wait_until_initialized said: {err:#}");
    assert!(
        format!("{err:#}").contains("paused"),
        "the error should say why nothing is coming: {err:#}"
    );
    assert!(
        matches!(handle.stats().state, TorrentStatsState::Initializing { .. }),
        "the torrent is still where the check left it: {:?}",
        handle.stats().state
    );

    // And it is not a dead end: unpausing runs a new check, which this time finishes.
    let reads_while_stopped = gate.reads();
    session.unpause(&handle).await?;
    timeout(WAIT, handle.wait_until_initialized()).await??;
    assert!(
        gate.reads() > reads_while_stopped,
        "the unpause read the files again rather than resuming the abandoned check"
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

/// **A torrent unpaused mid-check must go live with a peer stream**, not
/// merely into `Live`.
///
/// `start` takes the peer stream as an argument and hands it to the initial
/// check's continuation. A torrent added *paused* is given `None` --
/// `Session::add_torrent` only builds one when it is not pausing -- and the
/// unpause that arrives while the check is running builds a real one and
/// then drops it on `start`'s early return, because a check is already
/// going. The continuation still holds the `None` it captured at add time,
/// so the torrent reaches `Live` with no peer adder and no announce, and
/// stays there: `start` on a live torrent bails, so unpausing again cannot
/// repair it.
///
/// `unpause_during_the_initial_check_starts_the_torrent` above does this
/// exact sequence and passes, because `live().is_some()` is true of a
/// torrent that can never fetch a byte. So this one gives it a seeder and
/// an empty directory and asks it to actually download -- which is the bar
/// the comment in `torrent_state/mod.rs` set for a test of this, and the
/// reason the attempted fix recorded there was reverted rather than
/// finished.
#[tokio::test(flavor = "multi_thread")]
async fn unpause_during_the_initial_check_keeps_the_peer_stream() -> anyhow::Result<()> {
    timeout(
        WAIT,
        unpause_during_the_initial_check_keeps_the_peer_stream_inner(),
    )
    .await?
}

async fn unpause_during_the_initial_check_keeps_the_peer_stream_inner() -> anyhow::Result<()> {
    setup_test_logging();
    let (seeder_dir, torrent_bytes, _seeder, peer) = seeding_session("rqbit_unpause_peers").await?;
    let gate = Arc::new(Gate::default());

    // An empty directory, so finishing means bytes really arrived from the
    // seeder rather than the check finding them already there.
    let dir = TempDir::with_prefix("rqbit_unpause_peers_leecher")?;
    let session = Session::new_with_opts(dir.path().into(), session_opts(None, None)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![peer]),
                ..add_opts(&dir, true, &gate)
            }),
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

    wait_until(
        || {
            if handle.stats().finished {
                Ok(())
            } else {
                bail!(
                    "the torrent is live but has fetched nothing: {:?}",
                    handle.stats()
                )
            }
        },
        WAIT,
    )
    .await
    .context("live with no peer adder: the unpause's peer stream was dropped")?;
    drop(seeder_dir);
    Ok(())
}
