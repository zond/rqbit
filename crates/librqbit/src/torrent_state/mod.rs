pub mod initializing;
pub mod live;
pub mod paused;
pub mod stats;
mod streaming;
pub mod utils;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use arc_swap::ArcSwapOption;
use buffers::ByteBufOwned;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use librqbit_core::hash_id::Id20;
use librqbit_core::lengths::Lengths;

use librqbit_core::spawn_utils::spawn_with_cancel;
use librqbit_core::torrent_metainfo::ValidatedTorrentMetaV1Info;
pub use live::*;
use parking_lot::RwLock;

use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::warn;

use crate::Session;
use crate::chunk_tracker::{ChunkTracker, PieceChunkProgress};
use crate::file_info::FileInfo;
use crate::limits::LimitsConfig;
use crate::session::TorrentId;
use crate::spawn_utils::BlockingSpawner;
use crate::storage::BoxStorageFactory;
use crate::stream_connect::StreamConnector;
use crate::torrent_state::stats::LiveStats;
use crate::type_aliases::FileInfos;
use crate::type_aliases::PeerStream;

use initializing::TorrentStateInitializing;

use self::paused::TorrentStatePaused;
pub use self::stats::{TorrentStats, TorrentStatsState};
pub use self::streaming::{DEFAULT_STREAM_LOOKAHEAD_BYTES, FileStream, FileStreamOptions};

// State machine transitions.
//
// - error -> initializing
// - initializing -> paused
// - paused -> live
// - live -> paused
//
// - initializing -> error
// - live -> error
pub enum ManagedTorrentState {
    Initializing(Arc<TorrentStateInitializing>),
    Paused(TorrentStatePaused),
    Live(Arc<TorrentStateLive>),
    Error(anyhow::Error),

    // This is used when swapping between states, outside world should never see it.
    None,
}

impl ManagedTorrentState {
    pub fn name(&self) -> &'static str {
        match self {
            ManagedTorrentState::Initializing(_) => "initializing",
            ManagedTorrentState::Paused(_) => "paused",
            ManagedTorrentState::Live(_) => "live",
            ManagedTorrentState::Error(_) => "error",
            ManagedTorrentState::None => "<invalid: none>",
        }
    }

    fn assert_paused(self) -> TorrentStatePaused {
        match self {
            Self::Paused(paused) => paused,
            _ => panic!("Expected paused state"),
        }
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::None)
    }
}

/// How many peers a torrent keeps connected (or connecting) at once when nothing says
/// otherwise: neither [`crate::AddTorrentOptions::peer_limit`] nor
/// [`crate::SessionOptions::peer_limit`].
pub const DEFAULT_PEER_LIMIT: usize = 128;

pub(crate) struct ManagedTorrentLocked {
    // The torrent might not be in "paused" state technically,
    // but the intention might be for it to stay paused.
    //
    // This should change only on "unpause".
    pub(crate) paused: bool,
    pub(crate) state: ManagedTorrentState,
    pub(crate) only_files: Option<Vec<usize>>,
}

#[derive(Default)]
pub(crate) struct ManagedTorrentOptions {
    pub force_tracker_interval: Option<Duration>,
    pub peer_connect_timeout: Option<Duration>,
    pub peer_read_write_timeout: Option<Duration>,
    pub allow_overwrite: bool,
    pub output_folder: PathBuf,
    pub ratelimits: LimitsConfig,
    pub initial_peers: Vec<SocketAddr>,
    pub piece_reclaim: bool,
    #[cfg(feature = "disable-upload")]
    pub _disable_upload: bool,
}

impl ManagedTorrentOptions {
    #[cfg(feature = "disable-upload")]
    pub fn disable_upload(&self) -> bool {
        self._disable_upload
    }

    #[cfg(not(feature = "disable-upload"))]
    pub const fn disable_upload(&self) -> bool {
        false
    }
}

// Torrent bencodee "info" + some precomputed fields based on it for frequent access.
pub struct TorrentMetadata {
    pub info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
    pub torrent_bytes: Bytes,
    pub info_bytes: Bytes,
    pub file_infos: FileInfos,
}

impl TorrentMetadata {
    pub(crate) fn new(
        info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
        torrent_bytes: Bytes,
        info_bytes: Bytes,
    ) -> anyhow::Result<Self> {
        let file_infos = info
            .iter_file_details_ext()
            .map(|fd| {
                Ok::<_, anyhow::Error>(FileInfo {
                    relative_filename: fd.details.filename.to_pathbuf(),
                    offset_in_torrent: fd.offset,
                    piece_range: fd.pieces,
                    len: fd.details.len,
                    attrs: fd.details.attrs(),
                })
            })
            .collect::<anyhow::Result<Vec<FileInfo>>>()?;

        Ok(Self {
            info,
            torrent_bytes,
            info_bytes,
            file_infos,
        })
    }

    pub fn lengths(&self) -> &Lengths {
        self.info.lengths()
    }
}

/// Common information about torrent shared among all possible states.
///
// The reason it's not inlined into ManagedTorrent is to break the Arc cycle:
// ManagedTorrent contains the current torrent state, which in turn needs access to a bunch
// of stuff, but it shouldn't access the state.
pub struct ManagedTorrentShared {
    pub id: TorrentId,
    pub info_hash: Id20,
    pub(crate) spawner: BlockingSpawner,
    pub trackers: HashSet<url::Url>,
    pub peer_id: Id20,
    pub span: tracing::Span,
    pub(crate) options: ManagedTorrentOptions,
    /// The live-peer cap in force: [`crate::AddTorrentOptions::peer_limit`], else
    /// [`crate::SessionOptions::peer_limit`], else [`DEFAULT_PEER_LIMIT`] -- until
    /// [`ManagedTorrent::set_peer_limit`] changes it. Read when the torrent goes live.
    pub(crate) peer_limit: AtomicUsize,
    /// Whether [`ManagedTorrent::set_pieces_advertised`] has anything held back, so the
    /// Have path can answer "announce it" without taking the state lock when it doesn't.
    ///
    /// Written only inside [`ManagedTorrent::set_pieces_advertised`], which holds
    /// [`ManagedTorrent::locked`] for write from before the gate goes up to after it is
    /// written back down. That is not the lock the set changes under - the set lives in
    /// the chunk tracker, reached through [`TorrentStateLive`]'s own lock on the live
    /// path and through no lock at all on the paused one - it is a lock held *around*
    /// that, and it is what serialises the pair, because that call is the only thing in
    /// the crate that changes the set. So two callers run one after the other, and each
    /// leaves the gate agreeing with the set it left behind.
    ///
    /// It is a fast-path gate and not the truth: it can be true with nothing held back -
    /// while a hold-back call is in flight, and after a re-check throws the set away -
    /// which costs a lock and answers correctly. It is never false while something is
    /// held back.
    pub(crate) unadvertised_pieces: AtomicBool,
    pub(crate) connector: Arc<StreamConnector>,
    pub(crate) storage_factory: BoxStorageFactory,
    pub(crate) session: Weak<Session>,

    // "dn" from magnet link
    pub(crate) magnet_name: Option<String>,

    pub(crate) client_name_and_version: String,
}

impl ManagedTorrentShared {
    pub(crate) fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }

    /// The live-peer cap in force for this torrent (see [`ManagedTorrent::set_peer_limit`]).
    pub fn peer_limit(&self) -> usize {
        self.peer_limit.load(Ordering::Relaxed)
    }
}

pub struct ManagedTorrent {
    // Static torrent configuration that doesn't change.
    pub shared: Arc<ManagedTorrentShared>,
    // Torrent metadata. Maybe be None when the magnet is resolving (not implemented yet)
    pub metadata: ArcSwapOption<TorrentMetadata>,
    pub(crate) state_change_notify: Notify,
    pub(crate) locked: RwLock<ManagedTorrentLocked>,
}

/// The pieces [`ManagedTorrent::drop_pieces`] dropped, and a claim on them.
///
/// While this is alive nothing will download those pieces again, so their storage can be
/// released without racing a piece coming back - and so can whatever a half-finished
/// download left behind a piece we didn't have, which is in here too. Without the claim there is a window: the
/// have-bit is cleared, but a live stream's lookahead can reach the same piece, download
/// it and set the bit again before the caller's deletion lands - and the deletion then
/// removes a piece we have and are advertising.
///
/// So: read [`Self::pieces`], release their storage, then drop this. Dropping it is what
/// tells the torrent the pieces are gone for real and may be downloaded again; a piece
/// that was reselected in the meantime is queued for download right then.
///
/// The claim survives pausing and unpausing the torrent: it is held by the chunk tracker,
/// which is what a pause keeps. It does not survive the torrent being re-checked
/// (`error` -> `initializing`, i.e. restarting a torrent that hit a fatal error) or
/// removed from the session: the have-set is then rebuilt from
/// [`crate::storage::TorrentStorage::has_piece`], so a piece the caller has not deleted
/// yet comes back as one we have. Dropping the claim in that state logs a warning and
/// does nothing else. Finish releasing before restarting an errored torrent.
#[must_use = "the pieces stay claimed until this is dropped: release their storage first"]
pub struct DroppedPieces {
    torrent: Weak<ManagedTorrent>,
    pieces: Vec<u32>,
}

impl DroppedPieces {
    /// The pieces that were actually dropped, in the order they were passed in.
    pub fn pieces(&self) -> &[u32] {
        &self.pieces
    }
}

impl std::fmt::Debug for DroppedPieces {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DroppedPieces").field(&self.pieces).finish()
    }
}

impl Drop for DroppedPieces {
    fn drop(&mut self) {
        if let Some(torrent) = self.torrent.upgrade() {
            torrent.finish_release(&self.pieces);
        }
        // If the torrent is gone from the session there is nothing left to claim.
    }
}

impl ManagedTorrent {
    pub fn id(&self) -> TorrentId {
        self.shared.id
    }

    pub fn name(&self) -> Option<String> {
        if let Some(m) = &*self.metadata.load() {
            return m
                .info
                .name()
                .map(|n| n.into_owned())
                .or_else(|| self.shared.magnet_name.clone());
        }
        self.shared.magnet_name.clone()
    }

    pub fn shared(&self) -> &ManagedTorrentShared {
        &self.shared
    }

    /// The resolved on-disk folder this torrent's files are written under.
    pub fn output_folder(&self) -> &Path {
        &self.shared.options.output_folder
    }

    pub fn with_metadata<R>(
        &self,
        mut f: impl FnMut(&Arc<TorrentMetadata>) -> R,
    ) -> anyhow::Result<R> {
        let r = self.metadata.load();
        let r = r.as_ref().context("torrent is not resolved")?;
        Ok(f(r))
    }

    pub fn info_hash(&self) -> Id20 {
        self.shared.info_hash
    }

    pub fn only_files(&self) -> Option<Vec<usize>> {
        self.locked.read().only_files.clone()
    }

    pub fn with_state<R>(&self, f: impl FnOnce(&ManagedTorrentState) -> R) -> R {
        f(&self.locked.read().state)
    }

    pub(crate) fn with_state_mut<R>(&self, f: impl FnOnce(&mut ManagedTorrentState) -> R) -> R {
        f(&mut self.locked.write().state)
    }

    pub(crate) fn with_chunk_tracker<R>(
        &self,
        f: impl FnOnce(&ChunkTracker) -> R,
    ) -> anyhow::Result<R> {
        let g = self.locked.read();
        match &g.state {
            ManagedTorrentState::Paused(p) => Ok(f(&p.chunk_tracker)),
            ManagedTorrentState::Live(l) => Ok(f(l
                .lock_read("chunk_tracker")
                .get_chunks()
                .context("error getting chunks")?)),
            _ => bail!("no chunk tracker, torrent neither paused nor live"),
        }
    }

    /// How much of the given piece has been downloaded, in 16 KiB chunks.
    ///
    /// Whole pieces are visible through the have-bitfield already (see
    /// [`crate::Api::api_dump_haves`]), but a piece can be many megabytes, so that is too
    /// coarse to show progress to a user waiting on one specific piece.
    ///
    /// NOTE: this is "downloaded", not "verified". A chunk counts as soon as it has been
    /// written to storage; the piece's hash is only checked once all of its chunks are in.
    /// If that check fails the piece is discarded and the count drops back to zero, so
    /// this value CAN GO BACKWARDS. [`PieceChunkProgress::verified`] tells a piece that is
    /// merely fully downloaded from one that is known good.
    ///
    /// Errors if the torrent is neither live nor paused, or if the piece index is out of
    /// range. Cheap enough to poll: it takes the state lock only to count bits in an
    /// existing bitfield, and allocates nothing.
    pub fn piece_chunk_progress(&self, piece_index: u32) -> anyhow::Result<PieceChunkProgress> {
        self.with_chunk_tracker(|chunks| chunks.piece_chunk_progress(piece_index))?
            .with_context(|| format!("piece index {piece_index} is out of range"))
    }

    /// Drop the pieces in `pieces`: forget that we have the ones we do, stop advertising
    /// them to peers, and stop wanting them either way. Returns the pieces that were
    /// actually dropped, so the caller can release the storage behind them, together with
    /// a claim on them - see [`DroppedPieces`].
    ///
    /// This is bookkeeping only: it does not touch storage. Releasing a dropped piece is
    /// the caller's job, and it takes a storage that can let one piece go - one entry or
    /// file per piece, like `storage::examples::inmemory::InMemoryPieceStorage`,
    /// where releasing a piece is deleting its file and works on any filesystem. The
    /// default [`crate::storage::filesystem::FilesystemStorage`] writes the torrent's own
    /// files and can't: with it dropping frees nothing, and the dropped pieces are only
    /// downloaded again over bytes still on disk. That is why `add_torrent` refuses
    /// `piece_reclaim` with a storage whose factory doesn't promise
    /// [`crate::storage::StorageFactory::ensure_can_release_pieces`].
    ///
    /// The have-bitfield is flushed lazily, so what keeps it honest across a crash is
    /// [`crate::storage::TorrentStorage::has_piece`]: startup asks the storage what it
    /// still holds, and believes it over the resume data. A storage whose pieces are
    /// released this way must implement it.
    ///
    /// This is what makes it possible to keep streaming a torrent that doesn't fit on the
    /// disk while still seeding everything that does. Deciding *which* pieces to drop is
    /// the caller's job.
    ///
    /// Requires `piece_reclaim` in [`crate::AddTorrentOptions`], and a live or paused
    /// torrent.
    ///
    /// Pieces we don't have are dropped too: a dropped piece is one we don't want, and
    /// whether we ever had it doesn't come into that. This is what re-applies a want-set
    /// after a restart. The want-set is per-session - see
    /// [`crate::AddTorrentOptions::piece_reclaim`] - and the have-set a restart comes up
    /// with is what the storage holds, holes and all, every one of them wanted. That is
    /// why a restored reclaim torrent is always paused: the caller drops what it doesn't
    /// want and unpauses, and no peer gets a chance to fill a hole in between. There is
    /// nothing of ours to release behind such a piece, but a half-finished download may
    /// have left something, and the piece is in the returned list so that can go too.
    ///
    /// Skipped: pieces already dropped; pieces that a live stream is about to read -
    /// dropping those would only make them be re-requested at once; and pieces a peer is
    /// working on, in-flight or fully downloaded and being hash-checked, which complete
    /// and can be dropped then.
    ///
    /// A dropped piece stays dropped until [`Self::reselect_pieces`] is called for it,
    /// the file it belongs to is re-selected through `update_only_files`, or a live
    /// stream's lookahead reaches it: a reader that seeks backwards into a reclaimed
    /// range gets it back on its own.
    pub fn drop_pieces(self: &Arc<Self>, pieces: Range<u32>) -> anyhow::Result<DroppedPieces> {
        let mut g = self.locked.write();
        let pieces = match &mut g.state {
            // Under this lock a live torrent still has its piece tracker, same as in
            // finish_release() below.
            ManagedTorrentState::Live(live) => live.drop_pieces(pieces)?,
            ManagedTorrentState::Paused(paused) => paused.drop_pieces(pieces)?,
            state => bail!("torrent is neither live nor paused: {}", state.name()),
        };
        drop(g);
        Ok(DroppedPieces {
            torrent: Arc::downgrade(self),
            pieces,
        })
    }

    /// The caller is done releasing the storage of the pieces it was handed: they may be
    /// downloaded again. See [`DroppedPieces`], which is what calls this.
    ///
    /// Both a live and a paused torrent hold the claim - it lives in the chunk tracker,
    /// which a pause keeps - so this works in either state. Anything else and there is no
    /// chunk tracker to tell: say so instead of dropping it on the floor.
    pub(crate) fn finish_release(&self, pieces: &[u32]) {
        let mut g = self.locked.write();
        match &mut g.state {
            // Under this lock a live torrent still has its piece tracker: pause() and
            // stop_with_error() both take it before they can take the tracker away.
            ManagedTorrentState::Live(live) => live.finish_release(pieces),
            ManagedTorrentState::Paused(paused) => paused.finish_release(pieces),
            state => warn!(
                id = self.shared.id,
                info_hash = ?self.shared.info_hash,
                state = state.name(),
                pieces = pieces.len(),
                "released pieces have nowhere to be reported to: the torrent will decide \
                 what it has by asking the storage, so make sure it is done being deleted"
            ),
        }
    }

    /// Hold `pieces` back from what we announce to peers, or put them back.
    ///
    /// A held-back piece is one we may have, read and serve, but do not tell anyone
    /// about: it is cleared from the bitfield we send on handshake, and completing it
    /// sends no Have. Nothing else changes - we still download it, a stream still reads
    /// it, and a peer that asks for it anyway is served, because we do have it. There is
    /// no refusal path here and no need for one.
    ///
    /// A client is free to announce less than it holds; BEP-3 says what a Have and a
    /// bitfield mean, not that every piece must produce one. This is that freedom, made
    /// explicit and per piece.
    ///
    /// # What it is for
    ///
    /// An application that streams video and bounds its cache reclaims pieces behind the
    /// playhead within seconds of reading them (see [`Self::drop_pieces`]). Such a piece
    /// must be `have` while the reader is on it, or the stream cannot read it - but
    /// announcing it invites a request for a piece we are about to throw away, so the
    /// peer spends a round trip to be disappointed. BEP-6's Reject Request only makes
    /// that exchange formally legal - the peer still wasted the round trip, and clients
    /// hold a rejection against the peer that sent it. Not advertising in the first place
    /// costs the peer nothing.
    ///
    /// So: hold the reclaim window back, advertise a piece once it leaves the window and
    /// is there to stay. It is equally the answer for anything else we hold but do not
    /// want traffic for.
    ///
    /// # Using it
    ///
    /// The set is a range at a time and idempotent, so a moving window is two calls -
    /// advertise what the playhead has left, hold back what it has reached - each one a
    /// bit-range fill. Returns how many pieces actually changed.
    ///
    /// Hold a piece back BEFORE it completes if the goal is that no Have ever goes out
    /// for it. There is no un-Have in BitTorrent: a peer we have already told cannot be
    /// untold, and holding the piece back afterwards only stops us repeating it to peers
    /// that connect later.
    ///
    /// Putting pieces back sends a Have for each one we have and had held back, since the
    /// peers already connected got a bitfield without them. Those go through the same
    /// broadcast as a completed piece, which a peer far enough behind can miss - so
    /// prefer to advertise as the window moves rather than a whole torrent at once.
    ///
    /// Holding back is orthogonal to having: a piece can be held back before it is
    /// downloaded, and stays held back if it is dropped and downloaded again. It is a
    /// policy set the caller owns, and nothing but this call changes it.
    ///
    /// The set is per-session and is not persisted, like the want-set of
    /// [`Self::drop_pieces`]. It lives in the chunk tracker: a pause keeps that, so a
    /// pause keeps the set, and it is still in force when the torrent goes live again. A
    /// re-check builds a new tracker, so the set is gone with the old one and everything
    /// we have is announced again.
    ///
    /// Whether that is recoverable depends on how the re-check came about. A torrent
    /// added again is, but not in one breath: adding it with
    /// [`crate::AddTorrentOptions::paused`] returns while it is still `initializing` -
    /// the check of what is on disk runs in the background - and this call refuses that
    /// state. So: add it paused, await [`Self::wait_until_initialized`], which returns
    /// once the check is done and the torrent is `paused`, hold back what must be held
    /// back, then unpause. Nothing has been announced at any point in that, because a
    /// torrent that has not been live has had no peers to announce to.
    ///
    /// A torrent restarted after an error (`error` -> `initializing`) is not - the check
    /// runs in the background and the torrent goes initializing -> paused -> live in one
    /// locked step when it finishes, so there is no state a caller can catch it in and
    /// re-apply the set at. Watching for it to come back and re-applying then is after
    /// the fact: it is live, and announcing, first. If those pieces must not be
    /// announced, remove the torrent and add it again paused instead of restarting it.
    ///
    /// Works on a live or paused torrent, needs no options to have been set, and with
    /// nothing held back costs nothing: what we announce is then the have-set itself.
    pub fn set_pieces_advertised(
        &self,
        pieces: Range<u32>,
        advertised: bool,
    ) -> anyhow::Result<usize> {
        let gate = &self.shared.unadvertised_pieces;
        let mut g = self.locked.write();
        let was = gate.load(Ordering::Relaxed);
        if !advertised {
            // Before the set itself changes, never after, so the window between the two
            // may only cost the Have path a lock and not let out a piece the caller has
            // just asked us to hold back. Inside the lock, because holding that lock
            // across both writes is what orders this against another caller doing the
            // same thing - see the field's doc.
            gate.store(true, Ordering::Relaxed);
        }
        let result = match &mut g.state {
            ManagedTorrentState::Live(live) => live.set_pieces_advertised(pieces, advertised),
            ManagedTorrentState::Paused(paused) => {
                Ok(paused.set_pieces_advertised(pieces, advertised))
            }
            state => Err(anyhow::anyhow!(
                "torrent is neither live nor paused: {}",
                state.name()
            )),
        };
        match result {
            // Recomputed rather than cleared, since this call may have put back only part
            // of what is held back - and unconditionally, so a hold-back that held nothing
            // back does not leave the gate stuck on. Still under the lock: two callers
            // write the gate in the order they wrote the set, so whoever goes last leaves
            // the gate saying what the set says. Store it after the lock and the loser of
            // that race stores what the set looked like before the winner changed it.
            Ok((changed, still_held_back)) => {
                gate.store(still_held_back, Ordering::Relaxed);
                drop(g);
                Ok(changed)
            }
            // Nothing reached the set: the state arm refused the call, or the live arm
            // failed to reach the chunk tracker, both before a bit was touched. So the
            // gate goes back to what it said when we came in - it was right about the set
            // then and the set has not moved. Without this the raise above sticks: this
            // lives on ManagedTorrentShared, which outlives every state the torrent goes
            // through, so there is nothing later to bring it down and every Have takes the
            // lock for the life of the torrent.
            Err(e) => {
                gate.store(was, Ordering::Relaxed);
                drop(g);
                Err(e)
            }
        }
    }

    /// Make pieces dropped by [`Self::drop_pieces`] wanted again, e.g. after seeking
    /// backwards into a range that was reclaimed. Pieces that weren't dropped are left
    /// alone, and so are pieces belonging to a file the user has deselected. Returns how
    /// many pieces stopped being dropped. Works on a live or paused torrent.
    pub fn reselect_pieces(&self, pieces: Range<u32>) -> anyhow::Result<usize> {
        let mut g = self.locked.write();
        match &mut g.state {
            ManagedTorrentState::Live(live) => live.reselect_pieces(pieces),
            ManagedTorrentState::Paused(paused) => paused.reselect_pieces(pieces),
            state => bail!("torrent is neither live nor paused: {}", state.name()),
        }
    }

    /// Change how many peers this torrent keeps connected at once, now and whenever it
    /// (re)starts.
    ///
    /// On a live torrent it takes effect immediately. Lowering it hangs up on the surplus,
    /// least useful first -- a peer still connecting before one that is talking to us, and
    /// among those the one that has moved fewest bytes lately, in either direction. Parked
    /// peers stay in the table and nothing re-dials them until the cap goes back up.
    /// Raising it hands the slots back, so incoming connections are accepted again at once,
    /// and re-queues every parked peer we have an address to dial -- one we dialled
    /// ourselves, or one that told us where it listens. A peer that dialled us and did not
    /// say has no such address, and comes back only when it dials us again. Read the cap
    /// back with [`ManagedTorrentShared::peer_limit`].
    ///
    /// On a torrent in any other state it is the cap the next live state opens with.
    ///
    /// # Memory
    ///
    /// A peer costs about 48 KB of read and write buffers while it is connected, and
    /// hanging up on it frees them -- to the allocator. Whether the process gives the
    /// pages back to the OS is the allocator's decision and not this call's: measured on
    /// x86-64 Linux, dropping from 40 peers to 4 moved resident memory by nothing at all
    /// under either glibc or mimalloc, and glibc gave 4 MB back only when asked directly
    /// with `malloc_trim(0)`. What lowering the cap does buy, on every allocator, is
    /// sockets, file descriptors, tasks, CPU and bandwidth. An embedder that needs the
    /// resident memory back has to ask its allocator for it.
    pub fn set_peer_limit(&self, limit: usize) {
        self.shared.peer_limit.store(limit, Ordering::Relaxed);
        if let Some(live) = self.live() {
            live.set_peer_limit(limit);
        }
    }

    /// Get the live state if the torrent is live.
    pub fn live(&self) -> Option<Arc<TorrentStateLive>> {
        let g = self.locked.read();
        match &g.state {
            ManagedTorrentState::Live(live) => Some(live.clone()),
            _ => None,
        }
    }

    // Get live torrent but wait a bit until it's initialized if it is
    pub(crate) async fn live_wait_initializing(
        &self,
        duration: Duration,
    ) -> Option<Arc<TorrentStateLive>> {
        timeout(duration, self.wait_until_initialized())
            .await
            .ok()?
            .ok()?;
        self.live()
    }

    pub(crate) fn stop_with_error(&self, error: anyhow::Error) {
        let mut g = self.locked.write();

        match g.state.take() {
            ManagedTorrentState::Live(live) => {
                if let Err(err) = live.pause() {
                    warn!(
                        id = self.shared.id,
                        info_hash = ?self.shared.info_hash,
                        "error pausing live torrent during fatal error handling: {err:#}",
                    );
                }
            }
            ManagedTorrentState::Error(e) => {
                warn!(
                    id = self.shared.id,
                    info_hash = ?self.shared.info_hash,
                    "bug: torrent already was in error state when trying to stop it. Previous error was: {e:#}",
                );
            }
            ManagedTorrentState::None => {
                warn!(
                    id = self.shared.id,
                    info_hash = ?self.shared.info_hash,
                    "bug: torrent encountered in None state during fatal error handling"
                )
            }
            _ => {}
        };

        self.state_change_notify.notify_waiters();

        g.state = ManagedTorrentState::Error(error)
    }

    /// peer_rx: the peer stream. If start_paused=false, must be set.
    /// start_paused: if set, the torrent will initialize (check file integrity), but will not start
    pub(crate) fn start(
        self: &Arc<Self>,
        peer_rx: Option<PeerStream>,
        start_paused: bool,
    ) -> anyhow::Result<()> {
        fn _start<'a>(
            t: &'a Arc<ManagedTorrent>,
            peer_rx: Option<PeerStream>,
            start_paused: bool,
            session: Arc<Session>,
            g: Option<parking_lot::RwLockWriteGuard<'a, ManagedTorrentLocked>>,
            token: CancellationToken,
        ) -> anyhow::Result<()> {
            let mut g = g.unwrap_or_else(|| t.locked.write());

            match &g.state {
                ManagedTorrentState::Live(_) => {
                    bail!("torrent is already live");
                }
                ManagedTorrentState::Initializing(init) => {
                    let init = init.clone();
                    init.clear_pause_request();
                    // KNOWN BUG, not yet fixed. This early return makes both
                    // `Session::pause` and `Session::unpause` return `Ok(())`
                    // having done nothing, and the torrent then settles on the
                    // *add-time* `start_paused` captured by the in-flight
                    // check's continuation rather than on the intent just
                    // recorded. So `is_paused()` can disagree with the state in
                    // BOTH directions:
                    //
                    //  * unpause during a running check -> parked in `Paused`
                    //    with `is_paused() == false`;
                    //  * pause during a *fastresume* check -> `Live` with
                    //    `is_paused() == true`, because `validate_fastresume`
                    //    never reads `pause_requested` the way
                    //    `FileOps::initial_check` does. Measured downstream as a
                    //    torrent that kept downloading after being told to stop.
                    //
                    // Both are reachable through the HTTP API by hitting
                    // pause/start during the initial check.
                    //
                    // AN ATTEMPTED FIX WAS REVERTED, and the trap is worth
                    // recording: making the continuation read the live intent is
                    // necessary but NOT sufficient. It also has to carry the
                    // *current* peer stream. `start()` builds a peer_rx and
                    // drops it on this early return, while the continuation
                    // holds the one captured at add time -- which is `None` for
                    // any torrent added paused. Honouring the intent without
                    // fixing that takes the torrent Live with no peers and no
                    // announce, permanently, because `start()` on a `Live`
                    // torrent bails. That is strictly worse than the bug: this
                    // one is recoverable by unpausing again, that one is not.
                    //
                    // A test for it needs a real peer source, so that "started"
                    // means "can actually fetch" and not merely `live().is_some()`.
                    if !init.try_start_check() {
                        return Ok(());
                    }

                    let t = t.clone();
                    let span = t.shared().span.clone();
                    let token = token.clone();

                    spawn_with_cancel(
                        debug_span!(parent: span.clone(), "initialize_and_start"),
                        "initialize_and_start",
                        token.clone(),
                        async move {
                            let concurrent_init_semaphore =
                                session.concurrent_initialize_semaphore.clone();
                            let _permit = concurrent_init_semaphore
                                .acquire()
                                .await
                                .context("bug: concurrent init semaphore was closed")?;

                            let check_result = init.check().await;
                            init.finish_check();

                            match check_result {
                                Ok(paused) => {
                                    let mut g = t.locked.write();
                                    if let ManagedTorrentState::Initializing(_) = &g.state {
                                    } else {
                                        debug!(
                                            "no need to start torrent anymore, as it switched state from initializing"
                                        );
                                        return Ok(());
                                    }

                                    g.state = ManagedTorrentState::Paused(paused);
                                    t.state_change_notify.notify_waiters();
                                    _start(&t, peer_rx, start_paused, session, Some(g), token)
                                }
                                Err(err) => {
                                    if init.is_pause_requested() {
                                        debug!("initial check paused");
                                        t.state_change_notify.notify_waiters();
                                        return Ok(());
                                    }

                                    let result = anyhow::anyhow!("{:?}", err);
                                    t.locked.write().state = ManagedTorrentState::Error(err);
                                    t.state_change_notify.notify_waiters();
                                    Err(result)
                                }
                            }
                        },
                    );
                    Ok(())
                }
                ManagedTorrentState::Paused(_) => {
                    if start_paused {
                        return Ok(());
                    }
                    let paused = g.state.take().assert_paused();
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let live = TorrentStateLive::new(paused, tx, token.clone())?;
                    g.state = ManagedTorrentState::Live(live.clone());
                    t.state_change_notify.notify_waiters();

                    spawn_fatal_errors_receiver(t, rx, token);
                    if let Some(peer_rx) = peer_rx {
                        spawn_peer_adder(&live, peer_rx);
                    }
                    Ok(())
                }
                ManagedTorrentState::Error(_) => {
                    let metadata = t.metadata.load_full().expect("TODO");
                    let initializing = Arc::new(TorrentStateInitializing::new(
                        t.shared.clone(),
                        metadata.clone(),
                        g.only_files.clone(),
                        t.shared
                            .storage_factory
                            .create_and_init(t.shared(), &metadata)?,
                        true,
                    ));
                    g.state = ManagedTorrentState::Initializing(initializing.clone());
                    t.state_change_notify.notify_waiters();

                    // Recurse.
                    _start(t, peer_rx, start_paused, session, Some(g), token)
                }
                ManagedTorrentState::None => bail!("bug: torrent is in empty state"),
            }
        }

        let session = self
            .shared
            .session
            .upgrade()
            .context("session is dead, cannot start torrent")?;
        let mut g = self.locked.write();
        g.paused = start_paused;
        let cancellation_token = session.cancellation_token().child_token();

        _start(
            self,
            peer_rx,
            start_paused,
            session,
            Some(g),
            cancellation_token,
        )
    }

    pub fn is_paused(&self) -> bool {
        self.locked.read().paused
    }

    /// Pause the torrent if it's live.
    pub(crate) fn pause(&self) -> anyhow::Result<()> {
        let mut g = self.locked.write();
        match &g.state {
            ManagedTorrentState::Live(live) => {
                let paused = live.pause()?;
                g.state = ManagedTorrentState::Paused(paused);
                g.paused = true;
                self.state_change_notify.notify_waiters();
                Ok(())
            }
            ManagedTorrentState::Initializing(init) => {
                let init = init.clone();
                g.paused = true;
                init.request_pause();
                self.state_change_notify.notify_waiters();
                Ok(())
            }
            ManagedTorrentState::Paused(_) => {
                bail!("torrent is already paused");
            }
            ManagedTorrentState::Error(_) => {
                bail!("can't pause torrent in error state")
            }
            ManagedTorrentState::None => bail!("bug: torrent is in empty state"),
        }
    }

    /// Get stats.
    pub fn stats(&self) -> TorrentStats {
        use stats::TorrentStatsState as S;
        let mut resp = TorrentStats {
            total_bytes: self
                .metadata
                .load()
                .as_ref()
                .map(|r| r.info.lengths().total_length())
                .unwrap_or_default(),
            file_progress: Vec::new(),
            state: S::Error,
            error: None,
            progress_bytes: 0,
            uploaded_bytes: 0,
            finished: false,
            live: None,
        };

        {
            let g = self.locked.read();
            match &g.state {
                ManagedTorrentState::Initializing(i) => {
                    resp.state = S::Initializing { paused: g.paused };
                    resp.progress_bytes = i.checked_bytes.load(Ordering::Relaxed);
                }
                ManagedTorrentState::Paused(p) => {
                    resp.state = S::Paused;
                    let hns = p.hns();
                    resp.total_bytes = hns.total();
                    resp.progress_bytes = hns.progress();
                    resp.finished = hns.finished();
                    resp.file_progress = p.chunk_tracker.per_file_have_bytes().to_owned();
                }
                ManagedTorrentState::Live(l) => {
                    resp.state = S::Live;
                    let live_stats = LiveStats::from(l.as_ref());
                    let hns = l.get_hns().unwrap_or_default();
                    resp.total_bytes = hns.total();
                    resp.progress_bytes = hns.progress();
                    resp.finished = hns.finished();
                    resp.uploaded_bytes = l.get_uploaded_bytes();
                    resp.file_progress = l
                        .lock_read("file_progress")
                        .get_chunks()
                        .ok()
                        .map(|c| c.per_file_have_bytes().to_owned())
                        .unwrap_or_default();
                    resp.live = Some(live_stats);
                }
                ManagedTorrentState::Error(e) => {
                    resp.state = S::Error;
                    resp.error = Some(format!("{e:?}"))
                }
                ManagedTorrentState::None => {
                    resp.state = S::Error;
                    resp.error = Some("bug: torrent in broken \"None\" state".to_string());
                }
            }
        }

        resp
    }

    #[inline(never)]
    /// # Known hang
    ///
    /// This never returns for a torrent whose initial check was *paused*:
    /// `pause()` on an `Initializing` torrent sets `pause_requested`, the check
    /// bails, and the `Err` arm leaves the state `Initializing` with
    /// `check_running == false`. Nothing will move that state, so the loop below
    /// polls forever and callers have to bound it themselves.
    ///
    /// An attempted fix -- bail when `is_pause_requested() && !is_check_running()`
    /// -- was reverted, because that pair is ALSO true in a healthy state: in
    /// `Session::add_torrent` the handle is published and awaited on before
    /// `start()` runs, so a `pause()` landing in that window sets exactly those
    /// two conditions on a torrent whose check then completes normally. A correct
    /// fix has to distinguish "the check stalled" from "the check has not started
    /// yet", which the current state does not express.
    pub fn wait_until_initialized(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        async move {
            // TODO: rewrite, this polling is horrible
            loop {
                let done = self.with_state(|s| match s {
                    ManagedTorrentState::Initializing(_) => Ok(false),
                    ManagedTorrentState::Error(e) => bail!("{:?}", e),
                    ManagedTorrentState::None => bail!("bug: torrent state is None"),
                    _ => Ok(true),
                })?;
                if done {
                    return Ok(());
                }
                let _ = timeout(
                    Duration::from_millis(100),
                    self.state_change_notify.notified(),
                )
                .await;
            }
        }
        .boxed()
    }

    #[inline(never)]
    pub fn wait_until_completed(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        async move {
            // TODO: rewrite, this polling is horrible
            let live = loop {
                let live = self.with_state(|s| match s {
                    ManagedTorrentState::Initializing(_) | ManagedTorrentState::Paused(_) => {
                        Ok(None)
                    }
                    ManagedTorrentState::Live(l) => Ok(Some(l.clone())),
                    ManagedTorrentState::Error(e) => bail!("{:?}", e),
                    ManagedTorrentState::None => bail!("bug: torrent state is None"),
                })?;
                if let Some(live) = live {
                    break live;
                }
                let _ = timeout(Duration::from_secs(1), self.state_change_notify.notified()).await;
            };

            live.wait_until_completed().await;
            Ok(())
        }
        .boxed()
    }

    // Returns true if needed to unpause torrent.
    // This is just implementation detail - it's easier to pause/unpause than to tinker with internals.
    pub(crate) fn update_only_files(&self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        let metadata = self.metadata.load();
        let metadata = metadata.as_ref().context("torrent is not resolved")?;
        let file_count = metadata.file_infos.len();
        for f in only_files.iter().copied() {
            if f >= file_count {
                anyhow::bail!("only_files contains invalid value {f}")
            }
        }

        // if live, need to update chunk tracker
        // - if already finished: need to pause, then unpause (to reopen files etc)
        // if paused, need to update chunk tracker

        let mut g = self.locked.write();
        match &mut g.state {
            ManagedTorrentState::Initializing(_) => bail!("can't update initializing torrent"),
            ManagedTorrentState::Error(_) => {}
            ManagedTorrentState::None => {}
            ManagedTorrentState::Paused(p) => {
                p.update_only_files(only_files)?;
            }
            ManagedTorrentState::Live(l) => {
                l.update_only_files(only_files)?;
            }
        };

        g.only_files = Some(only_files.iter().copied().collect());
        Ok(())
    }
}

pub type ManagedTorrentHandle = Arc<ManagedTorrent>;

fn spawn_fatal_errors_receiver(
    state: &Arc<ManagedTorrent>,
    rx: tokio::sync::oneshot::Receiver<anyhow::Error>,
    token: CancellationToken,
) {
    let span = state.shared.span.clone();
    let id = state.shared.id;
    let info_hash = state.shared.info_hash;
    let state = Arc::downgrade(state);
    spawn_with_cancel::<&'static str>(
        debug_span!(parent: span, "fatal_errors_receiver"),
        "fatal_errors_receiver",
        token,
        async move {
            let e = match rx.await {
                Ok(e) => e,
                Err(_) => return Ok(()),
            };
            if let Some(state) = state.upgrade() {
                state.stop_with_error(e);
            } else {
                warn!(
                    ?id,
                    ?info_hash,
                    "tried to stop the torrent with error, but couldn't upgrade the arc"
                );
            }
            Ok(())
        },
    );
}

fn spawn_peer_adder(live: &Arc<TorrentStateLive>, mut peer_rx: PeerStream) {
    live.spawn(
        debug_span!(parent: live.torrent().span.clone(), "external_peer_adder"),
        format!("[{}]external_peer_adder", live.shared.id),
        {
            let live = live.clone();
            async move {
                let live = {
                    let weak = Arc::downgrade(&live);
                    drop(live);
                    weak
                };

                loop {
                    match timeout(Duration::from_secs(5), peer_rx.next()).await {
                        Ok(Some(peer)) => {
                            trace!(?peer, "received peer");
                            let live = match live.upgrade() {
                                Some(live) => live,
                                None => return Ok(()),
                            };
                            live.add_peer_if_not_seen(peer)?;
                        }
                        Ok(None) => {
                            debug!("peer_rx closed, closing peer adder");
                            return Ok(());
                        }
                        // If timeout, check if the torrent is live.
                        Err(_) if live.strong_count() == 0 => {
                            debug!("timed out waiting for peers, torrent isn't live, closing peer adder");
                            return Ok(());
                        }
                        Err(_) => continue,
                    }
                }
            }
        },
    );
}
