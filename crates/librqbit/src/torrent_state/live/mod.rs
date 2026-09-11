// The main logic of rqbit is here - connecting to peers, reading and writing messages
// to them, tracking peer state etc.
//
// ## Architecture
// There are many tasks cooperating to download the torrent. Tasks communicate both with message passing
// and shared memory.
//
// ### Shared locked state
// Shared state is access by almost all actors through RwLocks.
//
// There's one source of truth (TorrentStateLocked) for which chunks we have, need, and what peers are we waiting them from.
//
// Peer states that are important to the outsiders (tasks other than manage_peer) are in a sharded hash-map (DashMap)
//
// ### Tasks (actors)
// Peer adder task:
// - spawns new peers as they become known. It pulls them from a queue. The queue is filled in by DHT and torrent trackers.
//   Also gets updated when peers are reconnecting after errors.
//
// Each peer has one main task "manage_peer". It's composed of 2 futures running as one task through tokio::select:
// - "manage_peer" - this talks to the peer over network and calls callbacks on PeerHandler. The callbacks are not async,
//   and are supposed to finish quickly (apart from writing to disk, which is accounted for as "spawn_blocking").
// - "peer_chunk_requester" - this continuously sends requests for chunks to the peer.
//   it may steal chunks/pieces from other peers.
//
// ## Peer lifecycle
// State transitions:
// - queued (initial state) -> connected
// - connected -> live
// - ANY STATE -> dead (on error)
// - ANY STATE -> not_needed (when we don't need to talk to the peer anymore)
//
// When the peer dies, it's rescheduled with exponential backoff.
//
// > NOTE: deadlock notice:
// > peers and stateLocked are behind 2 different locks.
// > if you lock them in different order, this may deadlock.
// >
// > so don't lock them both at the same time at all, or at the worst lock them in the
// > same order (peers one first, then the global one).

pub mod peer;
pub mod peers;
pub mod stats;

use std::{
    borrow::Cow,
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    ops::{Deref, DerefMut, Range},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use buffers::{ByteBuf, ByteBufOwned};
use clone_to_owned::CloneToOwned;
use librqbit_core::{
    constants::CHUNK_SIZE,
    hash_id::Id20,
    lengths::{ChunkInfo, Lengths, ValidPieceIndex},
    spawn_utils::spawn_with_cancel,
    speed_estimator::SpeedEstimator,
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use peer_binary_protocol::{
    Handshake, Message, Piece, Request,
    extended::{
        self, ExtendedMessage,
        handshake::ExtendedHandshake,
        ut_metadata::{UtMetadata, UtMetadataData},
        ut_pex::UtPex,
    },
};
use tokio::sync::{
    Notify, OwnedSemaphorePermit, Semaphore,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, debug_span, error, info, trace, warn};

use crate::{
    Error,
    chunk_tracker::{ChunkMarkingResult, ChunkTracker, HaveNeededSelected},
    file_ops::FileOps,
    limits::Limits,
    peer_connection::{
        PeerConnection, PeerConnectionHandler, PeerConnectionOptions, WriterRequest,
    },
    piece_tracker::{AcquireRequest, AcquireResult, PieceTracker},
    session::CheckedIncomingConnection,
    session_stats::SessionStats,
    stream_connect::ConnectionKind,
    torrent_state::{peer::Peer, utils::atomic_inc},
    type_aliases::{BF, FilePriorities, FileStorage, PeerHandle},
};

use self::{
    peer::{
        PeerRx, PeerState, PeerTx, RemoveInflightRequestResult,
        stats::{
            atomic::PeerCountersAtomic as AtomicPeerCounters,
            snapshot::{PeerStatsFilter, PeerStatsSnapshot},
        },
    },
    peers::PeerStates,
    stats::{atomic::AtomicStats, snapshot::StatsSnapshot},
};

use super::{
    ManagedTorrentShared, TorrentMetadata,
    paused::TorrentStatePaused,
    streaming::TorrentStreams,
    utils::{TimedExistence, timeit},
};

fn make_piece_bitfield(lengths: &Lengths) -> BF {
    BF::from_boxed_slice(vec![0; lengths.piece_bitfield_bytes()].into_boxed_slice())
}

// The piece range is the caller's, and "everything from here on" is a natural way to
// ask for it. Walking it to the end of u32 to find out that none of it is a piece of
// this torrent stalls the executor for seconds, so bound it first.
pub(crate) fn clamp_piece_range(pieces: Range<u32>, lengths: &Lengths) -> Range<u32> {
    let end = pieces.end.min(lengths.total_pieces());
    pieces.start.min(end)..end
}

pub(crate) struct TorrentStateLocked {
    // Coordinates piece state: what chunks we have, need, and what pieces are in-flight.
    // If this is None, the torrent was paused, and this live state is useless, and needs to be dropped.
    pub(crate) pieces: Option<PieceTracker>,

    // The sorted file list in which order to download them.
    file_priorities: FilePriorities,

    // If this is None, then it was already used
    fatal_errors_tx: Option<tokio::sync::oneshot::Sender<anyhow::Error>>,

    unflushed_bitv_bytes: u64,
}

impl TorrentStateLocked {
    pub(crate) fn get_chunks(&self) -> crate::Result<&ChunkTracker> {
        self.pieces
            .as_ref()
            .map(|p| p.chunks())
            .ok_or(Error::ChunkTrackerEmpty)
    }

    pub(crate) fn get_pieces(&self) -> crate::Result<&PieceTracker> {
        self.pieces.as_ref().ok_or(Error::ChunkTrackerEmpty)
    }

    pub(crate) fn get_pieces_mut(&mut self) -> crate::Result<&mut PieceTracker> {
        self.pieces.as_mut().ok_or(Error::ChunkTrackerEmpty)
    }

    fn try_flush_bitv(&mut self, shared: &ManagedTorrentShared, flush_async: bool) {
        if self.unflushed_bitv_bytes == 0 {
            return;
        }
        trace!("trying to flush bitfield");
        if let Some(Err(e)) = self
            .pieces
            .as_mut()
            .map(|pt| pt.flush_have_pieces(flush_async))
        {
            warn!(id=?shared.id, info_hash = ?shared.info_hash, "error flushing bitfield: {e:#}");
        } else {
            trace!("flushed bitfield");
            self.unflushed_bitv_bytes = 0;
        }
    }
}

const FLUSH_BITV_EVERY_BYTES: u64 = 16 * 1024 * 1024;

pub enum AddIncomingPeerResult {
    Added,
    AlreadyActive,
    ConcurrencyLimitReached,
}

#[cfg(debug_assertions)]
thread_local! {
    /// How many guards on a live torrent's state lock this thread holds. The peer table
    /// checks it on every access, see [`peers::PeerTable`].
    ///
    /// It is one count across all live torrents, not one per torrent, so it also trips on
    /// holding torrent A's state lock while touching torrent B's peer table, which cannot
    /// deadlock on its own. No such path exists: every site takes a torrent's own lock and
    /// touches its own table, and the check stays that simple for it. A path that needs
    /// the cross-torrent case would have to key the count by torrent, not just remove the
    /// assertion.
    static STATE_LOCKS_HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Whether this thread holds a live torrent's state lock. Always false in release builds,
/// which don't keep track.
fn state_lock_held_by_this_thread() -> bool {
    #[cfg(debug_assertions)]
    {
        STATE_LOCKS_HELD.with(|c| c.get() > 0)
    }
    #[cfg(not(debug_assertions))]
    {
        false
    }
}

/// A guard on a live torrent's state lock (`TorrentStateLive::_locked`).
///
/// In debug builds it counts, per thread, how many such guards the thread holds, so that
/// the peer table can catch the lock order being inverted (see [`peers::PeerTable`]). The
/// guard is not `Send`, so the count cannot leak to another thread.
pub(crate) struct StateGuard<G>(TimedExistence<G>);

impl<G> StateGuard<G> {
    fn new(guard: G, reason: &'static str) -> Self {
        #[cfg(debug_assertions)]
        STATE_LOCKS_HELD.with(|c| c.set(c.get() + 1));
        Self(TimedExistence::new(guard, reason))
    }
}

#[cfg(debug_assertions)]
impl<G> Drop for StateGuard<G> {
    fn drop(&mut self) {
        STATE_LOCKS_HELD.with(|c| c.set(c.get() - 1));
    }
}

impl<G> Deref for StateGuard<G> {
    type Target = G;

    #[inline(always)]
    fn deref(&self) -> &G {
        &self.0
    }
}

impl<G> DerefMut for StateGuard<G> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut G {
        &mut self.0
    }
}

pub struct TorrentStateLive {
    peers: PeerStates,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    metadata: Arc<TorrentMetadata>,
    _locked: RwLock<TorrentStateLocked>,

    pub(crate) files: FileStorage,

    per_piece_locks: Vec<RwLock<()>>,

    stats: AtomicStats,
    lengths: Lengths,

    // Limits how many active (occupying network resources) peers there are at a moment in time.
    peer_semaphore: Arc<Semaphore>,
    // The cap `peer_semaphore` enforces (see `set_peer_limit`). Invariant: the semaphore's
    // permits (available plus held) minus `peer_permits_to_forget` equal this.
    peer_limit: AtomicUsize,
    // Permits a lowered cap could not take from the semaphore because peers held them: each
    // is forgotten instead of released when its peer dies (`release_peer_permit`).
    peer_permits_to_forget: AtomicUsize,

    // The queue for peer manager to connect to them.
    peer_queue_tx: UnboundedSender<SocketAddr>,
    // The same queue, for peers we have already talked to and want back: the ones a raised
    // cap un-parks, and the ones a reselection makes interesting again. The adder drains
    // this one first. See `reconnect_all_not_needed_peers`.
    peer_requeue_tx: UnboundedSender<SocketAddr>,

    finished_notify: Notify,
    new_pieces_notify: Notify,

    down_speed_estimator: SpeedEstimator,
    up_speed_estimator: SpeedEstimator,
    cancellation_token: CancellationToken,

    session_stats: Arc<SessionStats>,

    pub(crate) streams: Arc<TorrentStreams>,
    have_broadcast_tx: tokio::sync::broadcast::Sender<ValidPieceIndex>,

    ratelimit_upload_tx: tokio::sync::mpsc::UnboundedSender<(
        tokio::sync::mpsc::UnboundedSender<WriterRequest>,
        ChunkInfo,
    )>,
    ratelimits: Limits,
    /// The session's upload switch ([`Session::set_upload_enabled`]), or `None` for a
    /// torrent whose session was gone when it went live, which uploads.
    upload_enabled: Option<tokio::sync::watch::Receiver<bool>>,
}

impl TorrentStateLive {
    pub(crate) fn new(
        paused: TorrentStatePaused,
        fatal_errors_tx: tokio::sync::oneshot::Sender<anyhow::Error>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        let (peer_queue_tx, peer_queue_rx) = unbounded_channel();
        let (peer_requeue_tx, peer_requeue_rx) = unbounded_channel();
        let session = paused
            .shared
            .session
            .upgrade()
            .context("session is dead, cannot start torrent")?;
        let session_stats = session.stats.clone();
        let down_speed_estimator = SpeedEstimator::default();
        let up_speed_estimator = SpeedEstimator::default();

        let have_bytes = paused.chunk_tracker.get_hns().have_bytes;
        let lengths = *paused.chunk_tracker.get_lengths();

        // TODO: make it configurable
        let file_priorities = {
            let mut pri = (0..paused.metadata.file_infos.len()).collect::<Vec<usize>>();
            // sort by filename, cause many torrents have random sort order.
            pri.sort_unstable_by_key(|id| {
                paused
                    .metadata
                    .file_infos
                    .get(*id)
                    .map(|fi| fi.relative_filename.as_path())
            });
            pri
        };

        let (have_broadcast_tx, _) = tokio::sync::broadcast::channel(128);
        let peer_limit = paused.shared.peer_limit();

        let (ratelimit_upload_tx, ratelimit_upload_rx) = tokio::sync::mpsc::unbounded_channel::<(
            tokio::sync::mpsc::UnboundedSender<WriterRequest>,
            ChunkInfo,
        )>();
        let ratelimits = Limits::new(paused.shared.options.ratelimits);
        let upload_enabled = paused
            .shared
            .session
            .upgrade()
            .map(|session| session.upload_enabled.subscribe());

        let state = Arc::new(TorrentStateLive {
            shared: paused.shared.clone(),
            metadata: paused.metadata.clone(),
            peers: PeerStates {
                session_stats: session_stats.peers.clone(),
                stats: Default::default(),
                states: Default::default(),
                live_outgoing_peers: Default::default(),
            },
            _locked: RwLock::new(TorrentStateLocked {
                pieces: Some(PieceTracker::new(paused.chunk_tracker)),
                file_priorities,
                fatal_errors_tx: Some(fatal_errors_tx),
                unflushed_bitv_bytes: 0,
            }),
            files: paused.files,
            stats: AtomicStats {
                have_bytes: AtomicU64::new(have_bytes),
                ..Default::default()
            },
            lengths,
            peer_semaphore: Arc::new(Semaphore::new(peer_limit)),
            peer_limit: AtomicUsize::new(peer_limit),
            peer_permits_to_forget: AtomicUsize::new(0),
            new_pieces_notify: Notify::new(),
            peer_queue_tx,
            peer_requeue_tx,
            finished_notify: Notify::new(),
            down_speed_estimator,
            up_speed_estimator,
            cancellation_token,
            have_broadcast_tx,
            session_stats,
            streams: paused.streams,
            per_piece_locks: (0..lengths.total_pieces())
                .map(|_| RwLock::new(()))
                .collect(),
            ratelimit_upload_tx,
            ratelimits,
            upload_enabled,
        });

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "speed_estimator_updater"),
            format!("[{}]speed_estimator_updater", state.shared.id),
            {
                let state = Arc::downgrade(&state);
                async move {
                    loop {
                        let state = match state.upgrade() {
                            Some(state) => state,
                            None => return Ok(()),
                        };
                        let now = Instant::now();
                        let stats = state.stats_snapshot();
                        let fetched = stats.fetched_bytes;
                        let remaining = state
                            .lock_read("get_remaining_bytes")
                            .get_chunks()?
                            .get_remaining_bytes();
                        state
                            .down_speed_estimator
                            .add_snapshot(fetched, Some(remaining), now);
                        state
                            .up_speed_estimator
                            .add_snapshot(stats.uploaded_bytes, None, now);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            },
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "peer_adder"),
            format!("[{}]peer_adder", state.shared.id),
            state
                .clone()
                .task_peer_adder(peer_queue_rx, peer_requeue_rx),
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "upload_scheduler"),
            format!("[{}]upload_scheduler", state.shared.id),
            state.clone().task_upload_scheduler(ratelimit_upload_rx),
        );
        Ok(state)
    }

    #[track_caller]
    pub(crate) fn spawn(
        &self,
        span: tracing::Span,
        name: impl Into<Cow<'static, str>>,
        fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
    ) {
        spawn_with_cancel(span, name, self.cancellation_token.clone(), fut);
    }

    pub fn down_speed_estimator(&self) -> &SpeedEstimator {
        &self.down_speed_estimator
    }

    pub fn up_speed_estimator(&self) -> &SpeedEstimator {
        &self.up_speed_estimator
    }

    pub(crate) fn add_incoming_peer(
        self: &Arc<Self>,
        checked_peer: CheckedIncomingConnection,
    ) -> anyhow::Result<AddIncomingPeerResult> {
        use dashmap::mapref::entry::Entry;
        let (tx, rx) = unbounded_channel();
        let Some(permit) = PeerPermit::try_acquire(self) else {
            debug!("limit of live peers reached, dropping incoming peer");
            self.peers.with_peer(checked_peer.addr, |p| {
                atomic_inc(&p.stats.counters.incoming_connections);
            });
            return Ok(AddIncomingPeerResult::ConcurrencyLimitReached);
        };

        let counters = match self.peers.states.entry(checked_peer.addr) {
            Entry::Occupied(mut occ) => {
                let peer = occ.get_mut();
                if let Err(e) = peer.incoming_connection(
                    checked_peer.handshake.peer_id,
                    tx.clone(),
                    &self.peers,
                    checked_peer.kind,
                ) {
                    match e {
                        peer::IncomingConnectionResult::AlreadyActive => {
                            debug!(
                                addr = %checked_peer.addr,
                                kind = %checked_peer.kind,
                                "peer already active, ignoring incoming connection"
                            );
                            return Ok(AddIncomingPeerResult::AlreadyActive);
                        }
                    }
                }
                peer.stats.counters.clone()
            }
            Entry::Vacant(vac) => {
                atomic_inc(&self.peers.stats.seen);
                let peer = Peer::new_live_for_incoming_connection(
                    *vac.key(),
                    checked_peer.handshake.peer_id,
                    tx.clone(),
                    &self.peers,
                    checked_peer.kind,
                );
                let counters = peer.stats.counters.clone();
                vac.insert(peer);
                counters
            }
        };
        atomic_inc(&counters.incoming_connections);

        self.spawn(
            debug_span!(
                parent: self.shared.span.clone(),
                "manage_incoming_peer",
                addr = %checked_peer.addr
            ),
            format!(
                "[{}][addr={}]manage_incoming_peer",
                self.shared.id, checked_peer.addr
            ),
            aframe!(
                self.clone()
                    .task_manage_incoming_peer(checked_peer, counters, tx, rx, permit)
            ),
        );
        Ok(AddIncomingPeerResult::Added)
    }

    async fn task_upload_scheduler(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<(
            tokio::sync::mpsc::UnboundedSender<WriterRequest>,
            ChunkInfo,
        )>,
    ) -> crate::Result<()> {
        while let Some((tx, ci)) = rx.recv().await {
            tokio::select! {
                _ = tx.closed() => {
                    continue;
                }
                res = self.ratelimits.prepare_for_upload(NonZeroU32::new(ci.size).unwrap()) => {
                    res?;
                }
            };
            if let Some(session) = self.shared.session.upgrade() {
                tokio::select! {
                    _ = tx.closed() => {
                        continue;
                    }
                    res = session.ratelimits.prepare_for_upload(NonZeroU32::new(ci.size).unwrap()) => {
                        res?;
                    }
                }
            }
            // The have-check in on_download_request happened before rate limiting, which
            // can take a long time. If the piece was dropped in between, the storage
            // behind it may already be gone, and reading it would fail deep inside the
            // peer's writer - or worse, succeed and serve whatever is there now. Vanilla
            // BitTorrent has no "reject request", so the only way to tell the peer is to
            // hang up: it reconnects and gets a bitfield without the piece in it.
            // Guarded on the opt-in flag so the default path takes no extra lock.
            if self.shared.options.piece_reclaim
                && !self
                    .lock_read("recheck_chunk_ready_to_upload")
                    .get_chunks()
                    .is_ok_and(|ct| ct.is_chunk_ready_to_upload(&ci))
            {
                let _ = tx.send(WriterRequest::Disconnect(Err(anyhow::anyhow!(
                    "piece {} was dropped while the request for it was queued",
                    ci.piece_index
                ))));
                continue;
            }
            let _ = tx.send(WriterRequest::ReadChunkRequest(ci));
        }
        Ok(())
    }

    async fn task_manage_incoming_peer(
        self: Arc<Self>,
        checked_peer: CheckedIncomingConnection,
        counters: Arc<AtomicPeerCounters>,
        tx: PeerTx,
        rx: PeerRx,
        permit: PeerPermit,
    ) -> crate::Result<()> {
        let handler = PeerHandler {
            addr: checked_peer.addr,
            incoming: true,
            on_bitfield_notify: Default::default(),
            flow_control: Mutex::new(PeerFlowControl::default()),
            state: self.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: self.cancellation_token.child_token(),
            client_name_and_version: self.shared.client_name_and_version().to_owned(),
        };
        let _token_guard = handler.cancel_token.clone().drop_guard();
        let options = PeerConnectionOptions {
            connect_timeout: self.shared.options.peer_connect_timeout,
            read_write_timeout: self.shared.options.peer_read_write_timeout,
            ..Default::default()
        };
        let peer_connection = PeerConnection::new(
            checked_peer.addr,
            self.shared.info_hash,
            self.shared.peer_id,
            &handler,
            Some(options),
            self.shared.spawner.clone(),
            self.shared.connector.clone(),
        )
        .with_upload_switch(self.upload_enabled.clone());
        let requester = handler.task_peer_chunk_requester();

        let res = tokio::select! {
            r = requester => {r}
            r = peer_connection.manage_peer_incoming(
                rx,
                checked_peer,
                self.have_broadcast_tx.subscribe()
            ) => {r}
        };

        match res {
            // We disconnected the peer ourselves as we don't need it
            Ok(()) => {
                handler.on_peer_died(None)?;
            }
            Err(e) => {
                debug!("error managing peer: {:#}", e);
                handler.on_peer_died(Some(e))?;
            }
        };
        drop(permit);
        Ok(())
    }

    async fn task_manage_outgoing_peer(
        self: Arc<Self>,
        addr: SocketAddr,
        permit: PeerPermit,
        rx: PeerRx,
        tx: PeerTx,
        counters: Arc<AtomicPeerCounters>,
        dial_cancel: CancellationToken,
    ) -> crate::Result<()> {
        let state = self;
        let handler = PeerHandler {
            addr,
            incoming: false,
            on_bitfield_notify: Default::default(),
            flow_control: Mutex::new(PeerFlowControl::default()),
            state: state.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: dial_cancel,
            client_name_and_version: state.shared.client_name_and_version().to_owned(),
        };
        let _token_guard = handler.cancel_token.clone().drop_guard();

        let options = PeerConnectionOptions {
            connect_timeout: state.shared.options.peer_connect_timeout,
            read_write_timeout: state.shared.options.peer_read_write_timeout,
            ..Default::default()
        };
        let peer_connection = PeerConnection::new(
            addr,
            state.shared.info_hash,
            state.shared.peer_id,
            &handler,
            Some(options),
            state.shared.spawner.clone(),
            state.shared.connector.clone(),
        )
        .with_upload_switch(state.upload_enabled.clone());
        let requester = aframe!(
            handler
                .task_peer_chunk_requester()
                .instrument(debug_span!("chunk_requester"))
        );
        let conn_manager = aframe!(
            peer_connection
                .manage_peer_outgoing(rx, state.have_broadcast_tx.subscribe())
                .instrument(debug_span!("peer_connection"))
        );

        handler
            .counters
            .outgoing_connection_attempts
            .fetch_add(1, Ordering::Relaxed);
        let res = tokio::select! {
            r = requester => {r}
            r = conn_manager => {r}
            // A lowered cap wants this slot back and cannot ask through the writer
            // channel, because a dial that has not finished handshaking is not reading it.
            _ = handler.cancel_token.cancelled() => Err(Error::Disconnect)
        };

        match res {
            // We disconnected the peer ourselves as we don't need it
            Ok(()) => {
                handler.on_peer_died(None)?;
            }
            Err(e) => {
                debug!("error managing peer: {:#}", e);
                handler.on_peer_died(Some(e))?;
            }
        }
        drop(permit);
        Ok(())
    }

    /// Whether the peer adder should spend a slot dialling `addr`: a torrent that has
    /// everything and will not upload wants nobody, and the session's own rules -- the
    /// blocklist, the allowlist, ipv4-only -- rule out the rest. Marks a peer it turns down
    /// on the first count as not needed, the way the adder always has.
    fn worth_dialling(&self, session: &crate::Session, addr: SocketAddr) -> bool {
        if self.shared.options.disable_upload() && self.is_finished_and_no_active_streams() {
            debug!(?addr, "ignoring peer as we are finished");
            self.peers.mark_peer_not_needed(addr);
            return false;
        }

        if session.ipv4_only && addr.is_ipv6() {
            debug!(?addr, "skipping ipv6 peer (ipv4_only=true)");
            return false;
        }

        if addr.port() == 0 {
            debug!(?addr, "skipping peer with port 0");
            return false;
        }

        if session.blocklist.has(addr.ip()) {
            session
                .stats
                .counters
                .blocked_outgoing
                .fetch_add(1, Ordering::Relaxed);
            debug!(?addr, "blocked outgoing connection (by the blacklist)");
            return false;
        }

        if session
            .allowlist
            .as_ref()
            .is_some_and(|l| !l.has(addr.ip()))
        {
            session
                .stats
                .counters
                .blocked_outgoing
                .fetch_add(1, Ordering::Relaxed);
            debug!(?addr, "blocked outgoing connection (by the allowlist)");
            return false;
        }

        true
    }

    async fn task_peer_adder(
        self: Arc<Self>,
        mut peer_queue_rx: UnboundedReceiver<SocketAddr>,
        mut peer_requeue_rx: UnboundedReceiver<SocketAddr>,
    ) -> crate::Result<()> {
        let state = self;
        // A guess taken off the queue that no slot was free to dial. It waits here rather
        // than going back on the queue, which would put it behind everything named since.
        let mut in_hand: Option<SocketAddr> = None;
        loop {
            // Peers we have already talked to go first. A raised cap asks for every peer
            // the lowered one parked, and those are the proven ones -- at the back of a
            // queue holding whatever the trackers and the DHT named while the cap was low,
            // they would be dialled minutes after the app came back to the foreground.
            let (addr, is_proven) = match in_hand.take() {
                Some(addr) => (addr, false),
                None => {
                    let (addr, is_proven) = tokio::select! {
                        biased;
                        addr = peer_requeue_rx.recv() => (addr, true),
                        addr = peer_queue_rx.recv() => (addr, false),
                    };
                    (addr.ok_or(Error::TorrentIsNotLive)?, is_proven)
                }
            };

            let session = state
                .shared
                .session
                .upgrade()
                .ok_or(Error::SessionDestroyed)?;

            if !state.worth_dialling(&session, addr) {
                continue;
            }

            let permit = PeerPermit::acquire(&state).await?;
            // The wait for that slot is as long as the cap is low, which on a backgrounded
            // app is however long it stays in the background -- and what ends it is the
            // raise, which asks for every peer it parked back before it hands a slot out.
            // A guess picked up before that wait has no claim on the slot over them: let one
            // of them have it and keep the guess for the next. A proven peer keeps the slot
            // it waited for, so the order they were asked back in is the order they go out.
            let addr = if is_proven {
                addr
            } else {
                match peer_requeue_rx.try_recv() {
                    Ok(proven) => {
                        in_hand = Some(addr);
                        if !state.worth_dialling(&session, proven) {
                            continue;
                        }
                        proven
                    }
                    Err(_) => addr,
                }
            };
            // Claim the table slot under the same permit, before the spawn. A cap lowered
            // in between would find this peer neither `Live` nor `Connecting`, rank it as
            // absent and leave it unparked, and the swarm would settle one peer above the
            // cap for as long as it lives.
            let dial_cancel = state.cancellation_token.child_token();
            let (rx, tx) = match state.peers.mark_peer_connecting(addr, dial_cancel.clone()) {
                Ok(v) => v,
                Err(e) => {
                    debug!(?addr, "not dialling: {e:#}");
                    continue;
                }
            };
            let Some(counters) = state.peers.with_peer(addr, |p| p.stats.counters.clone()) else {
                debug!(?addr, "not dialling: no longer in the peer table");
                continue;
            };
            state.spawn(
                debug_span!(parent: state.shared.span.clone(), "manage_peer", peer = ?addr),
                format!("[{}][addr={addr}]manage_peer", state.shared.id),
                aframe!(state.clone().task_manage_outgoing_peer(
                    addr,
                    permit,
                    rx,
                    tx,
                    counters,
                    dial_cancel
                )),
            );
        }
    }

    pub fn torrent(&self) -> &ManagedTorrentShared {
        &self.shared
    }

    pub fn info(&self) -> &ValidatedTorrentMetaV1Info<ByteBufOwned> {
        &self.metadata.info
    }
    pub fn info_hash(&self) -> Id20 {
        self.shared.info_hash
    }
    pub fn peer_id(&self) -> Id20 {
        self.shared.peer_id
    }
    pub(crate) fn file_ops(&self) -> FileOps<'_> {
        FileOps::new(&self.metadata.info, &*self.files, &self.metadata.file_infos)
    }

    pub(crate) fn lock_read(
        &self,
        reason: &'static str,
    ) -> StateGuard<RwLockReadGuard<'_, TorrentStateLocked>> {
        StateGuard::new(timeit(reason, || self._locked.read()), reason)
    }
    pub(crate) fn lock_write(
        &self,
        reason: &'static str,
    ) -> StateGuard<RwLockWriteGuard<'_, TorrentStateLocked>> {
        StateGuard::new(timeit(reason, || self._locked.write()), reason)
    }

    fn set_peer_live(&self, handle: PeerHandle, h: Handshake, connection_kind: ConnectionKind) {
        self.peers.with_peer_mut(handle, "set_peer_live", |p| {
            p.connecting_to_live(h.peer_id, &self.peers, connection_kind);
        });
    }

    pub fn get_uploaded_bytes(&self) -> u64 {
        self.stats.uploaded_bytes.load(Ordering::Relaxed)
    }
    pub fn get_downloaded_bytes(&self) -> u64 {
        self.stats
            .downloaded_and_checked_bytes
            .load(Ordering::Acquire)
    }

    pub fn get_approx_have_bytes(&self) -> u64 {
        self.stats.have_bytes.load(Ordering::Relaxed)
    }

    pub fn get_hns(&self) -> Option<HaveNeededSelected> {
        self.lock_read("get_hns")
            .get_chunks()
            .ok()
            .map(|c| *c.get_hns())
    }

    fn transmit_haves(&self, index: ValidPieceIndex) {
        let _ = self.have_broadcast_tx.send(index);
    }

    pub(crate) fn add_peer_if_not_seen(&self, addr: SocketAddr) -> crate::Result<bool> {
        match self.peers.add_if_not_seen(addr) {
            Some(handle) => handle,
            None => return Ok(false),
        };

        self.peer_queue_tx
            .send(addr)
            .ok()
            .ok_or(Error::TorrentIsNotLive)?;
        Ok(true)
    }

    pub fn stats_snapshot(&self) -> StatsSnapshot {
        use Ordering::*;
        let downloaded_bytes = self.stats.downloaded_and_checked_bytes.load(Relaxed);
        StatsSnapshot {
            downloaded_and_checked_bytes: downloaded_bytes,
            downloaded_and_checked_pieces: self.stats.downloaded_and_checked_pieces.load(Relaxed),
            fetched_bytes: self.stats.fetched_bytes.load(Relaxed),
            uploaded_bytes: self.stats.uploaded_bytes.load(Relaxed),
            total_piece_download_ms: self.stats.total_piece_download_ms.load(Relaxed),
            peer_stats: self.peers.stats(),
        }
    }

    pub fn per_peer_stats_snapshot(&self, filter: PeerStatsFilter) -> PeerStatsSnapshot {
        PeerStatsSnapshot {
            peers: self
                .peers
                .states
                .iter()
                .filter(|e| filter.state.matches(e.value().get_state()))
                .map(|e| (e.key().to_string(), e.value().into()))
                .collect(),
        }
    }

    pub async fn wait_until_completed(&self) {
        if self.is_finished() {
            return;
        }
        self.finished_notify.notified().await;
    }

    pub fn pause(&self) -> anyhow::Result<TorrentStatePaused> {
        self.cancellation_token.cancel();

        let mut g = self.lock_write("pause");

        // It should be impossible to make a fatal error after pausing.
        g.fatal_errors_tx.take();

        let piece_tracker = g
            .pieces
            .take()
            .context("bug: pausing already paused torrent")?;
        // into_chunks() will requeue any in-flight pieces. It also carries over the claim
        // on pieces the caller is releasing (see crate::DroppedPieces) - the whole point
        // of that living in the ChunkTracker is that requeuing must not hand a peer a
        // piece whose storage is being deleted.
        let chunk_tracker = piece_tracker.into_chunks();

        Ok(TorrentStatePaused {
            shared: self.shared.clone(),
            metadata: self.metadata.clone(),
            files: self.files.take()?,
            chunk_tracker,
            streams: self.streams.clone(),
        })
    }

    fn on_fatal_error(&self, e: anyhow::Error) -> anyhow::Result<()> {
        let mut g = self.lock_write("fatal_error");
        let tx = g
            .fatal_errors_tx
            .take()
            .context("fatal_errors_tx already taken")?;
        let res = anyhow::anyhow!("fatal error: {:?}", e);
        if tx.send(e).is_err() {
            warn!(id=self.shared.id, info_hash=?self.shared.info_hash, "there's nowhere to send fatal error, receiver is dead");
        }
        Err(res)
    }

    /// Drop the pieces in the given range: forget that we have the ones we do, stop
    /// advertising them, and stop wanting them either way. Bookkeeping only - releasing
    /// the storage is the caller's job. See [`crate::ManagedTorrent::drop_pieces`].
    ///
    /// Pieces a live stream is about to read are skipped.
    pub(crate) fn drop_pieces(&self, pieces: Range<u32>) -> anyhow::Result<Vec<u32>> {
        let mut g = self.lock_write("drop_pieces");
        let locked = &mut **g;
        // The guard is evaluated here, under the write lock, and not before taking it.
        // Waiting for a contended write lock is exactly when a reader is likely to seek,
        // and a lookahead computed before the wait would be stale by the time we drop.
        // What is left racing is a seek concurrent with this very iteration, and that one
        // is harmless: the picker's priority path ignores "dropped", so the reader pulls
        // the piece back in by itself.
        let wanted = self.streams.wanted_ranges(&self.lengths);
        let candidates = clamp_piece_range(pieces, &self.lengths)
            .filter(|id| !wanted.iter().any(|r| r.contains(id)))
            .filter_map(|id| self.lengths.validate_piece_index(id));
        let (dropped, freed) = {
            let pieces = locked.get_pieces_mut()?;
            let have_before = pieces.chunks().get_hns().have_bytes;
            let dropped = pieces.drop_pieces(&self.metadata.file_infos, candidates)?;
            // Only the pieces we had move the have-bitfield and the have counter; a
            // dropped piece we didn't have changes neither.
            (dropped, have_before - pieces.chunks().get_hns().have_bytes)
        };
        self.stats.have_bytes.fetch_sub(freed, Ordering::Relaxed);
        locked.unflushed_bitv_bytes += freed;
        // Same deal as on piece completion: let the bitfield drift until it's worth a
        // write. A crash that beats the flush leaves resume data claiming a piece whose
        // storage the caller has since released, which is why startup intersects the
        // resume data with TorrentStorage::has_piece() - the storage decides the have-set,
        // so the claim cannot outlive the bytes.
        if locked.unflushed_bitv_bytes >= FLUSH_BITV_EVERY_BYTES {
            locked.try_flush_bitv(&self.shared, true);
        }
        drop(g);

        Ok(dropped.into_iter().map(|id| id.get()).collect())
    }

    /// Whether to tell peers we have this piece.
    ///
    /// Two reasons to stay quiet. A Have is queued when the piece completes and can go out
    /// much later - after rate limiting, behind whatever else that peer's writer has to
    /// send. If we dropped the piece in between, advertising it earns us a request we
    /// cannot serve and, with no reject-request in vanilla BitTorrent, a disconnect. And
    /// the caller may have held the piece back on purpose - see
    /// [`crate::ManagedTorrent::set_pieces_advertised`].
    ///
    /// Only a torrent that opted into reclaim can lose a piece it had, and only a torrent
    /// that has held something back has anything to hide, so only those pay for the lock.
    /// The `unadvertised_pieces` gate is a fast path and not the truth: it goes up before
    /// the set it summarises and comes down only under the lock that set changes under,
    /// so it can be true with nothing held back - which costs a lock and answers
    /// correctly - but never false while something is.
    pub(crate) fn should_advertise_have(&self, id: ValidPieceIndex) -> bool {
        if !self.shared.options.piece_reclaim
            && !self.shared.unadvertised_pieces.load(Ordering::Relaxed)
        {
            return true;
        }
        self.lock_read("should_advertise_have")
            .get_chunks()
            .is_ok_and(|ct| ct.is_piece_advertised(id))
    }

    /// Hold pieces back from what we announce, or put them back: see
    /// [`crate::ManagedTorrent::set_pieces_advertised`]. Returns how many pieces changed,
    /// and whether anything at all is still held back.
    ///
    /// Pieces that just became advertised and that we have get a Have, because the peers
    /// already connected got a handshake bitfield without them and there is no other way
    /// to tell them. Peers that connect afterwards see them in that bitfield instead.
    pub(crate) fn set_pieces_advertised(
        &self,
        pieces: Range<u32>,
        advertised: bool,
    ) -> anyhow::Result<(usize, bool)> {
        let ids = || {
            clamp_piece_range(pieces.clone(), &self.lengths)
                .filter_map(|id| self.lengths.validate_piece_index(id))
        };
        let mut g = self.lock_write("set_pieces_advertised");
        let pt = g.get_pieces_mut()?;
        // Collected before the change, while "held back" is still readable. Only the
        // pieces that were hidden AND that we have need a Have; re-announcing the rest
        // would be telling peers something they were already told.
        let announce: Vec<ValidPieceIndex> = if advertised {
            let ct = pt.chunks();
            ids()
                .filter(|id| ct.is_piece_have(*id) && ct.is_piece_held_back(*id))
                .collect()
        } else {
            Vec::new()
        };
        let changed = pt.set_pieces_advertised(ids(), advertised);
        let still_held_back = pt.chunks().has_unadvertised_pieces();
        drop(g);

        self.announce_to_connected_peers(&announce);
        Ok((changed, still_held_back))
    }

    /// Queue a Have for each of these pieces on every peer that has a writer, over the
    /// peer's own channel rather than the broadcast.
    ///
    /// The broadcast keeps the last 128 pieces, and a writer that falls behind skips what
    /// it missed. Completions arrive one by one, at download speed; a hold-back lifted
    /// all at once is a single synchronous burst of hundreds, so every connected peer
    /// would hear of the last 128 and never of the rest - its handshake bitfield came
    /// without them, and nothing else tells it. The peer's channel is unbounded and
    /// loses nothing.
    ///
    /// Run after the set has changed, so a peer this misses because it is not in the
    /// table yet serializes its handshake bitfield after the change and has them there.
    /// A peer that has both is told twice, which costs 9 bytes. The writer still asks
    /// `should_transmit_have` before it sends, as it does for the broadcast, so a piece
    /// dropped while its Have waits in the queue is not announced.
    fn announce_to_connected_peers(&self, pieces: &[ValidPieceIndex]) {
        if pieces.is_empty() {
            return;
        }
        for pe in self.peers.states.iter() {
            let tx = match pe.value().get_state() {
                PeerState::Live(live) => &live.tx,
                PeerState::Connecting(tx) => tx,
                _ => continue,
            };
            for id in pieces {
                if tx.send(WriterRequest::Have(*id)).is_err() {
                    break;
                }
            }
        }
    }

    /// The caller is done releasing the storage of these pieces: they may be downloaded
    /// again. See [`crate::DroppedPieces`].
    ///
    /// Called with the torrent's state lock held, which is what makes the piece tracker
    /// certain to be here: pausing takes that lock before it takes the tracker.
    pub(crate) fn finish_release(&self, pieces: &[u32]) {
        let queued = match self.lock_write("finish_release").get_pieces_mut() {
            Ok(pt) => pt.finish_release(
                pieces
                    .iter()
                    .filter_map(|id| self.lengths.validate_piece_index(*id)),
            ),
            Err(e) => {
                warn!(
                    id = self.shared.id,
                    info_hash = ?self.shared.info_hash,
                    pieces = pieces.len(),
                    "bug: a live torrent has no piece tracker to release pieces into: {e:#}"
                );
                return;
            }
        };
        if queued > 0 {
            self.reconnect_all_not_needed_peers();
            self.new_pieces_notify.notify_waiters();
        }
    }

    /// Make previously dropped pieces wanted again. Returns how many pieces stopped being
    /// dropped.
    pub(crate) fn reselect_pieces(&self, pieces: Range<u32>) -> anyhow::Result<usize> {
        let pieces = clamp_piece_range(pieces, &self.lengths)
            .filter_map(|id| self.lengths.validate_piece_index(id));
        let res = self
            .lock_write("reselect_pieces")
            .get_pieces_mut()?
            .reselect_pieces(pieces)?;
        // Only a piece that went back into the queue is one a peer can do something
        // about. A piece whose file the user has deselected is wanted again but not
        // queued, and waking every peer for it wakes them up to find nothing to do.
        if res.queued > 0 {
            self.reconnect_all_not_needed_peers();
            self.new_pieces_notify.notify_waiters();
        }
        Ok(res.reselected)
    }

    pub(crate) fn update_only_files(&self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        let hns = self
            .lock_write("update_only_files")
            .get_pieces_mut()?
            .update_only_files(&self.metadata.file_infos, only_files)?;
        // With the state lock released. A dying peer holds its shard of the peer table
        // while it asks whether the torrent is finished (on_peer_died), so touching the
        // table under the state lock is the reverse order, and the two deadlock.
        if !hns.finished() {
            self.reconnect_all_not_needed_peers();
        }
        Ok(())
    }

    // If we have all selected pieces but not necessarily all pieces.
    pub(crate) fn is_finished(&self) -> bool {
        self.get_hns().map(|h| h.finished()).unwrap_or_default()
    }

    fn has_active_streams_unfinished_files(&self, state: &TorrentStateLocked) -> bool {
        let chunks = match state.get_chunks() {
            Ok(c) => c,
            Err(_) => return false,
        };
        self.streams
            .streamed_file_ids()
            .any(|file_id| !chunks.is_file_finished(&self.metadata.file_infos[file_id]))
    }

    // We might have the torrent "finished" i.e. no selected files. But if someone is streaming files despite
    // them being selected, we aren't fully "finished".
    fn is_finished_and_no_active_streams(&self) -> bool {
        self.is_finished()
            && !self.has_active_streams_unfinished_files(
                &self.lock_read("is_finished_and_dont_need_peers"),
            )
    }

    fn on_piece_completed(&self, id: ValidPieceIndex) -> anyhow::Result<()> {
        let mut g = self.lock_write("on_piece_completed");
        let locked = &mut **g;

        self.streams
            .wake_streams_on_piece_completed(id, self.metadata.lengths());

        locked.unflushed_bitv_bytes += self.metadata.lengths().piece_length(id) as u64;
        if locked.unflushed_bitv_bytes >= FLUSH_BITV_EVERY_BYTES {
            locked.try_flush_bitv(&self.shared, true)
        }

        let chunks = locked.get_chunks()?;
        if chunks.is_finished() {
            if chunks.get_selected_pieces()[id.get_usize()] {
                locked.try_flush_bitv(&self.shared, false);
                info!(id=self.shared.id, info_hash=?self.shared.info_hash, "torrent finished downloading");
            }
            self.finished_notify.notify_waiters();

            if !self.has_active_streams_unfinished_files(locked) {
                // prevent deadlocks.
                drop(g);
                // There is not point being connected to peers that have all the torrent, when
                // we don't need anything from them, and they don't need anything from us.
                self.disconnect_all_peers_that_have_full_torrent();
            }
        }
        Ok(())
    }

    /// Every piece being downloaded right now, as `(piece, the address that reserved it)`.
    /// Tests only.
    #[cfg(test)]
    pub(crate) fn inflight_piece_owners(&self) -> Vec<(u32, SocketAddr)> {
        let g = self.lock_read("inflight_piece_owners");
        let Ok(pieces) = g.get_pieces() else {
            return Vec::new();
        };
        (0..self.lengths.total_pieces())
            .filter_map(|index| self.lengths.validate_piece_index(index))
            .filter_map(|piece| {
                pieces
                    .get_inflight(piece)
                    .map(|inf| (piece.get(), inf.peer))
            })
            .collect()
    }

    /// Every address the peer table has right now, whatever state it is in. Tests only.
    #[cfg(test)]
    pub(crate) fn peer_addresses(&self) -> HashSet<SocketAddr> {
        self.peers.states.iter().map(|pe| *pe.key()).collect()
    }

    /// Of those, the ones reserved for an address the peer table no longer has. Such a
    /// piece is in no queue and has no owner that can still deliver it, so nothing
    /// downloads it until a steal happens by -- see `on_peer_died`, which hands a dying
    /// task's pieces back before it looks the table up at all. Always empty except in the
    /// instant between a peer being forgotten and its task noticing.
    #[cfg(test)]
    pub(crate) fn ownerless_inflight_pieces(&self) -> Vec<(u32, SocketAddr)> {
        // Read the table first: the state lock may not be held while it is touched.
        let known = self.peer_addresses();
        self.inflight_piece_owners()
            .into_iter()
            .filter(|(_, owner)| !known.contains(owner))
            .collect()
    }

    /// The peer-slot bookkeeping as `(free slots, slots a lowered cap is still owed)`.
    /// Once every peer a lowering asked to leave has left, the debt is nil and the free
    /// slots plus the connections in hand come to the cap. Tests only.
    #[cfg(test)]
    pub(crate) fn peer_permit_accounting(&self) -> (usize, usize) {
        (
            self.peer_semaphore.available_permits(),
            self.peer_permits_to_forget.load(Ordering::Acquire),
        )
    }

    /// How many peers this torrent keeps connected (or connecting) at once.
    pub fn peer_limit(&self) -> usize {
        self.peer_limit.load(Ordering::Acquire)
    }

    /// Change the live-peer cap of a running torrent.
    ///
    /// Lowering it takes the spare permits away at once and, if more peers are connected
    /// than the new cap has room for, disconnects the surplus -- least useful first, by the
    /// order [`surplus_rank`] lays out -- and forgets their permits as they come back
    /// instead of releasing them. A peer asked to go ends like one we drop after finishing:
    /// its in-flight pieces return to the queue and it stays in the table as `NotNeeded`, so
    /// nothing re-dials it until the cap is raised; incoming connections beyond the cap are
    /// refused as before. Live peers exceed the cap by the number still hanging up, and by
    /// no more than that: a dial in flight holds its slot from the moment it takes it, so
    /// the ranking sees every peer that holds one.
    ///
    /// Raising it hands the peer adder that many more permits and asks for the parked peers
    /// back, ahead of every address a tracker or the DHT has named in the meantime -- see
    /// [`Self::reconnect_all_not_needed_peers`]. That includes the address the peer adder
    /// is already holding when the raise arrives, which it hands the slot to a parked peer
    /// instead. They still come back over a moment rather than at once, since each has to
    /// be dialled and handshaked again.
    ///
    /// Only peers we have an address to dial come back that way -- one we dialled ourselves,
    /// or one that named its listening port in the extended handshake. A peer that dialled
    /// us from an ephemeral port has no dialable address at all, so the raise cannot ask for
    /// it; it returns when it dials us again, which the restored permits let it do at once.
    /// rqbit does not send its own listening port in that handshake, so between two rqbit
    /// nodes this is the case on the seeding side of every connection.
    ///
    /// Idempotent, and no I/O: the state lock is taken only to read the queue of pieces
    /// still needed, and never while the peer table is touched. Lowering walks the peer
    /// table once, asking of each live peer whether it holds anything we still want -- a
    /// byte-wise AND of two bitfields, so one operation per eight pieces. Raising walks it
    /// once and pushes each parked peer onto the queue.
    pub fn set_peer_limit(&self, limit: usize) {
        let prev = self.peer_limit.swap(limit, Ordering::AcqRel);
        if limit > prev {
            let add = limit - prev;
            // Ask for the parked peers back before a slot to dial them with can exist.
            // Not merely before `add_permits`: writing the debt off is itself a way of
            // handing slots out. A peer the lowering parked returns its permit as it dies,
            // and while the debt stands that permit is forgotten to pay it; the instant the
            // debt is gone the next one goes to the semaphore instead, and the adder --
            // which has been standing on the semaphore with a guessed address in hand --
            // takes it. Every one of those returns that lands between the write-off and
            // this walk is a slot spent on a guess with the proven queue still empty.
            // Measured, by moving this walk below the write-off with a 20 ms sleep in
            // the gap: 10 failures in 10 of
            // `a_raise_dials_the_peers_it_parked_before_the_backlog`, against 25 clean
            // runs unmutated. That test must NOT wait for the seeders to close the
            // parked sockets before raising: a far-end close is strictly later than the
            // parked peer's own permit drop, so waiting for it empties the debt and the
            // semaphore, and this ordering stops mattering.
            self.reconnect_all_not_needed_peers();
            // A lower cap may still be waiting to collect permits from dying peers; those
            // debts are simply written off before any new permit is issued.
            let written_off = sub_saturating(&self.peer_permits_to_forget, add);
            if add > written_off {
                self.peer_semaphore.add_permits(add - written_off);
            }
        } else if limit < prev {
            let remove = prev - limit;
            // Book the debt first, then take what is not out on loan right away. A peer
            // dying in between forgets its permit against the debt, in which case the
            // semaphore was drained by more than it owed: hand the difference back.
            self.peer_permits_to_forget
                .fetch_add(remove, Ordering::AcqRel);
            let forgotten = self.peer_semaphore.forget_permits(remove);
            let over = forgotten - sub_saturating(&self.peer_permits_to_forget, forgotten);
            if over > 0 {
                self.peer_semaphore.add_permits(over);
            }
            self.disconnect_surplus_peers(limit);
        }
    }

    /// Hang up on the peers a cap of `limit` has no room for, least useful first (see
    /// [`surplus_rank`]), the way a peer we no longer need after finishing is dropped:
    /// parked as `NotNeeded` first, then asked to disconnect. Its task ends on the request,
    /// and `on_peer_died` finds it parked, hands back the pieces it had in flight and
    /// returns its permit.
    ///
    /// A peer still connecting is parked the same way, but asking is not enough for it: it
    /// reads its writer channel only once the handshake is done, which may be a connect
    /// timeout and two read timeouts away, and against a host that accepts and then says
    /// nothing it is the full wait. Since these are the peers a lowering sheds first --
    /// they have proven nothing -- waiting would mean the cap holding its own slots hostage
    /// for half a minute. So the dial is cancelled outright.
    fn disconnect_surplus_peers(&self, limit: usize) {
        // Read the queue of pieces still needed first, and let go of the state lock before
        // the table is touched (see `PeerTable` for the lock order).
        let needed: Option<BF> = self
            .lock_read("disconnect_surplus_peers")
            .get_chunks()
            .ok()
            .map(|chunks| chunks.get_queue_pieces().clone());
        let has_needed_piece = |bitfield: &BF| {
            needed
                .as_ref()
                .is_some_and(|n| has_any_needed_piece(n, bitfield))
        };

        let mut ranked: Vec<(SurplusRank, SocketAddr)> = Vec::new();
        for pe in self.peers.states.iter() {
            let peer = pe.value();
            if let Some(rank) = surplus_rank(peer, &has_needed_piece) {
                ranked.push((rank, peer.addr));
            }
        }
        let surplus = ranked.len().saturating_sub(limit);
        if surplus == 0 {
            return;
        }
        ranked.sort_unstable();
        let mut parked = 0usize;
        for (_, addr) in ranked.into_iter().take(surplus) {
            self.peers
                .with_peer_mut(addr, "disconnect_surplus_peers", |peer| {
                    // Gone or changed since the ranking: nothing to hang up on.
                    if !matches!(
                        peer.get_state(),
                        PeerState::Live(_) | PeerState::Connecting(_)
                    ) {
                        return;
                    }
                    let dial_cancel = peer.dial_cancel.take();
                    let tx = match peer.set_not_needed(&self.peers) {
                        PeerState::Live(live) => live.tx,
                        PeerState::Connecting(tx) => {
                            // It is not reading the channel yet, and will not be for up to
                            // a connect timeout plus two read timeouts. Ask anyway, in case
                            // the handshake lands first, but end the task so the slot and
                            // the socket come back now rather than in half a minute.
                            if let Some(cancel) = dial_cancel {
                                cancel.cancel();
                            }
                            tx
                        }
                        _ => return,
                    };
                    let _ = tx.send(WriterRequest::Disconnect(Ok(())));
                    parked += 1;
                });
        }
        debug!(
            limit,
            parked, "peer limit lowered, disconnecting the surplus"
        );
    }
}

/// Does `bitfield` hold any of the pieces `needed` still wants?
///
/// Byte-wise, because this runs once per live peer and a season pack has a hundred
/// thousand pieces: `domain()` hands back the underlying bytes with the partial head and
/// tail already masked, so the answer is one AND per eight pieces instead of a random
/// lookup per piece we still want. `zip` stops at the shorter of the two, which is what a
/// peer whose bitfield has not arrived yet needs: length zero, holds nothing.
fn has_any_needed_piece(needed: &BF, bitfield: &BF) -> bool {
    needed
        .domain()
        .zip(bitfield.domain())
        .any(|(needed, has)| needed & has != 0)
}

/// (talking to us, useful either way, bytes moved recently, bytes moved ever). See
/// [`surplus_rank`].
type SurplusRank = (bool, bool, u64, u64);

/// Where a peer stands when a lowered cap has to let some go: sorted ascending, the front
/// is the least worth keeping. `None` for a peer we are neither talking to nor dialling,
/// which the cap neither counts nor touches.
///
/// The order is, in this order:
///
/// 1. a peer still connecting, which has proven nothing;
/// 2. a peer with nothing to exchange in *either* direction -- not interested in anything
///    we have, and holding no piece we still want;
/// 3. fewest bytes moved either way in the recent window
///    ([`AtomicPeerCounters::bytes_moved_recently`]);
/// 4. fewest bytes moved either way over the whole connection, which breaks the tie
///    between peers that have been quiet for a window or two.
///
/// Recent bytes ahead of lifetime bytes because the lifetime totals on their own merely
/// favour whoever connected first: a peer that gave us 50 MB an hour ago and has since
/// gone silent would outrank one that arrived a minute ago and is feeding us now.
///
/// # Both directions count the same
///
/// A peer we only upload to and a peer that only feeds us are worth exactly the same
/// here, and nothing in this function asks whether the torrent is downloading or seeding.
/// That is deliberate. A torrent crosses between the two constantly -- a stream finishes,
/// the app goes to the background, the viewer seeks back into what we already have -- and
/// a rank that changed with the crossing would re-cut the swarm at every one of them, and
/// would have a regime to misdetect on top. A symmetric rank has neither problem, and the
/// cap exists to bound memory, which a peer costs the same either way.
///
/// Where a peer *is* plays no part either. Latency is a proxy for throughput and a poor
/// one -- a peer down the street on a saturated uplink is worth less than a distant one
/// with room -- and the bytes counted above are the very thing it would be standing in
/// for, measured rather than guessed.
fn surplus_rank(peer: &Peer, has_needed_piece: &impl Fn(&BF) -> bool) -> Option<SurplusRank> {
    let counters = &peer.stats.counters;
    match peer.get_state() {
        PeerState::Connecting(_) => Some((false, false, 0, 0)),
        PeerState::Live(live) => Some((
            true,
            live.peer_interested || has_needed_piece(&live.bitfield),
            counters.bytes_moved_recently(),
            counters.fetched_bytes.load(Ordering::Relaxed)
                + counters.uploaded_bytes.load(Ordering::Relaxed),
        )),
        _ => None,
    }
}

impl TorrentStateLive {
    /// Drop from the peer table every peer we are neither talking to nor about to: the ones
    /// that died and are waiting out a backoff (`Dead`) and the ones we hung up on because
    /// there was nothing to exchange (`NotNeeded`). Returns how many.
    ///
    /// The table otherwise keeps every address a tracker, the DHT or PEX ever named for as
    /// long as the torrent is live -- thousands after a download from a busy swarm, each
    /// with its counters and backoff state. A forgotten peer comes back, with a fresh
    /// backoff, the next time a source names it or it dials us; a dead peer's pending
    /// reconnect finds no entry and does nothing. Call it before lowering the peer limit
    /// rather than after, or the peers the lower cap parks are forgotten too and a later
    /// raise has none of them to re-queue -- on a torrent with no tracker and no DHT, that
    /// loses them for good.
    ///
    /// It does not collect addresses still waiting to be dialled, which nothing else
    /// collects either: the address may be in the peer adder's queue, and dropping the
    /// entry from under it would leave the adder with a slot and nothing to spend it on.
    /// So a torrent left at a low cap in a busy swarm does still accumulate `Queued`
    /// entries -- a few hundred bytes each -- for as long as it stays live.
    pub fn forget_disconnected_peers(&self) -> usize {
        let is_disconnected =
            |peer: &Peer| matches!(peer.get_state(), PeerState::Dead | PeerState::NotNeeded);
        let candidates: Vec<SocketAddr> = self
            .peers
            .states
            .iter()
            .filter(|pe| is_disconnected(pe.value()))
            .map(|pe| pe.value().addr)
            .collect();
        candidates
            .into_iter()
            .filter(|addr| self.peers.drop_peer_if(*addr, is_disconnected).is_some())
            .count()
    }

    fn disconnect_all_peers_that_have_full_torrent(&self) {
        for mut pe in self.peers.states.iter_mut() {
            if let PeerState::Live(l) = pe.value().get_state()
                && l.has_full_torrent(self.lengths.total_pieces() as usize)
            {
                let prev = pe.value_mut().set_not_needed(&self.peers);
                let _ = prev
                    .take_live_no_counters()
                    .unwrap()
                    .tx
                    .send(WriterRequest::Disconnect(Ok(())));
            }
        }
    }

    /// Put every `NotNeeded` outgoing peer back in the queue to be dialled: the ones a
    /// lowered cap parked, and equally the ones that left cleanly and the seeders parked
    /// when the torrent finished.
    ///
    /// They go on a queue of their own, which the adder drains before the one
    /// `add_peer_if_not_seen` feeds with every address a tracker, the DHT and PEX name.
    /// They are peers we have already talked to, and the backlog on the other queue is a
    /// list of guesses -- at a low cap nothing drains it, so it is exactly when these peers
    /// matter most, on the way back from a lowered cap, that it would be longest.
    pub(crate) fn reconnect_all_not_needed_peers(&self) {
        self.peers
            .states
            .iter_mut()
            .filter_map(|mut p| p.value_mut().reconnect_not_needed_peer(&self.peers))
            .map(|socket_addr| self.peer_requeue_tx.send(socket_addr))
            .take_while(|r| r.is_ok())
            .last();
    }

    async fn task_send_pex_to_peer(
        self: Arc<Self>,
        this_peer_addr: SocketAddr,
        tx: PeerTx,
    ) -> anyhow::Result<()> {
        // As per BEP 11 we should not send more than 50 peers at once
        // (here it also applies to fist message, should be OK as we anyhow really have more)
        const MAX_SENT_PEERS: usize = 50;
        // As per BEP 11 recommended interval is min 60 seconds
        const PEX_MESSAGE_INTERVAL: Duration = Duration::from_secs(60);

        let mut connected = Vec::with_capacity(MAX_SENT_PEERS);
        let mut dropped = Vec::with_capacity(MAX_SENT_PEERS);
        let mut peer_view_of_live_peers = HashSet::new();

        // Wait 10 seconds before sending the first message to assure that peer will stay with us
        tokio::time::sleep(Duration::from_secs(10)).await;

        let mut interval = tokio::time::interval(PEX_MESSAGE_INTERVAL);

        loop {
            interval.tick().await;

            // This task should die with the cancellation token, but check defensively just in case.
            if tx.is_closed() {
                return Ok(());
            }

            {
                let live_peers = self.peers.live_outgoing_peers.read();
                connected.clear();
                dropped.clear();

                connected.extend(
                    live_peers
                        .difference(&peer_view_of_live_peers)
                        .take(MAX_SENT_PEERS)
                        .copied(),
                );
                dropped.extend(
                    peer_view_of_live_peers
                        .difference(&live_peers)
                        .take(MAX_SENT_PEERS)
                        .copied(),
                );
            }

            trace!(connected_len = connected.len(), dropped_len = dropped.len());

            let peer_ip_non_local = match this_peer_addr.ip() {
                IpAddr::V4(a) => !a.is_loopback() && !a.is_private(),
                IpAddr::V6(a) => !a.is_loopback() && !a.is_unique_local(),
            };

            let other_ip_is_local = |addr: &IpAddr| match addr {
                IpAddr::V4(a) => a.is_loopback() || a.is_private(),
                IpAddr::V6(a) => {
                    a.is_loopback() || a.is_unicast_link_local() || a.is_unique_local()
                }
            };

            let filter = |addr: &SocketAddr| !(peer_ip_non_local && other_ip_is_local(&addr.ip()));

            // BEP 11 - Dont send closed if they are now in live
            // it's assured by mutual exclusion of two  above sets  if in sent_peers_live, it cannot be in addrs_live_to_sent,
            // and addrs_closed_to_sent are only filtered addresses from sent_peers_live

            if !connected.is_empty() || !dropped.is_empty() {
                let pex_msg = extended::ut_pex::UtPex::from_addrs(
                    connected.iter().copied(),
                    dropped.iter().copied(),
                );
                if tx.send(WriterRequest::UtPex(pex_msg)).is_err() {
                    return Ok(()); // Peer disconnected
                }

                for addr in &dropped {
                    peer_view_of_live_peers.remove(addr);
                }
                peer_view_of_live_peers.extend(connected.iter().filter(|a| filter(a)).copied());
            }
        }
    }
}

const DEFAULT_PEER_REQUEST_WINDOW: usize = 128;

struct PeerFlowControl {
    i_am_choked: bool,
    request_window: usize,
}

impl Default for PeerFlowControl {
    fn default() -> Self {
        Self {
            i_am_choked: true,
            request_window: DEFAULT_PEER_REQUEST_WINDOW,
        }
    }
}

// All peer state that would never be used by other actors should pe put here.
// This state tracks a live peer.
struct PeerHandler {
    state: Arc<TorrentStateLive>,
    counters: Arc<AtomicPeerCounters>,
    // Semantically, we don't need a lock here, as this is only requested from
    // one future (requester + manage_peer).
    //
    // However as PeerConnectionHandler takes &self everywhere, we need shared mutability.
    // RefCell would do, but tokio is unhappy when we use it.
    flow_control: Mutex<PeerFlowControl>,

    // This is used to unpause chunk requester once the bitfield
    // is received.
    on_bitfield_notify: Notify,

    addr: SocketAddr,
    incoming: bool,
    tx: PeerTx,

    first_message_received: AtomicBool,

    cancel_token: CancellationToken,

    client_name_and_version: String,
}

impl PeerConnectionHandler for &'_ PeerHandler {
    fn on_connected(&self, connection_time: Duration) {
        self.counters
            .outgoing_connections
            .fetch_add(1, Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        self.counters
            .total_time_connecting_ms
            .fetch_add(connection_time.as_millis() as u64, Ordering::Relaxed);
    }

    async fn on_received_message(&self, message: Message<'_>) -> anyhow::Result<()> {
        // The first message must be "bitfield", but if it's not sent,
        // assume the bitfield is all zeroes and was sent.
        if !matches!(&message, Message::Bitfield(..))
            && !self.first_message_received.swap(true, Ordering::Relaxed)
        {
            self.on_bitfield_notify.notify_waiters();
        }

        match message {
            Message::Request(request) => {
                self.on_download_request(request)
                    .context("on_download_request")?;
            }
            Message::Bitfield(b) => self
                .on_bitfield(b.clone_to_owned(None))
                .context("on_bitfield")?,
            Message::Choke => self.on_i_am_choked(),
            Message::Unchoke => self.on_i_am_unchoked(),
            Message::Interested => self.on_peer_interested(),
            Message::Piece(piece) => self
                .on_received_piece(piece)
                .await
                .context("on_received_piece")?,
            Message::KeepAlive => {
                trace!("keepalive received");
            }
            Message::Have(h) => self.on_have(h),
            Message::NotInterested => self.on_peer_not_interested(),
            Message::Cancel(_) => {
                trace!("received \"cancel\", but we don't process it yet")
            }
            Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Request(
                metadata_piece_id,
            ))) => {
                if self.state.metadata.info.info().private {
                    warn!(
                        id = self.state.shared.id,
                        info_hash = ?self.state.shared.info_hash,
                        "received noncompliant ut_metadata message from {}, ignoring",
                        self.addr
                    );
                } else {
                    self.send_metadata_piece(metadata_piece_id)
                        .with_context(|| {
                            format!("error sending metadata piece {metadata_piece_id}")
                        })?;
                }
            }
            Message::Extended(ExtendedMessage::UtPex(pex)) => {
                if self.state.metadata.info.info().private {
                    warn!(
                        id = self.state.shared.id,
                        info_hash = ?self.state.shared.info_hash,
                        "received noncompliant PEX message from {}, ignoring",
                        self.addr
                    );
                } else {
                    self.on_pex_message(pex);
                }
            }
            message => {
                warn!(
                    id = self.state.shared.id,
                    info_hash = ?self.state.shared.info_hash,
                    "received unsupported message {:?}, ignoring", message
                );
            }
        };
        Ok(())
    }

    fn serialize_bitfield_message_to_buf(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
        let g = self.state.lock_read("serialize_bitfield_message_to_buf");
        // Not the have-bitfield: the pieces the caller has held back are cleared from it.
        // A borrow of the have-bytes unless something actually is held back.
        let advertised = g.get_chunks()?.advertised_pieces_bytes();
        let msg = Message::Bitfield(ByteBuf(&advertised));
        let len = msg.serialize(buf, &Default::default)?;
        trace!("sending: {:?}, length={}", &msg, len);
        Ok(len)
    }

    fn on_handshake(&self, handshake: Handshake, ckind: ConnectionKind) -> anyhow::Result<()> {
        self.state.set_peer_live(self.addr, handshake, ckind);
        Ok(())
    }

    fn on_uploaded_bytes(&self, bytes: u32) {
        self.counters
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.counters.on_bytes_moved(bytes as u64);
        self.state
            .stats
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.state
            .session_stats
            .counters
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn read_chunk(&self, chunk: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()> {
        self.state.file_ops().read_chunk(self.addr, chunk, buf)
    }

    fn on_extended_handshake(&self, hs: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
        if let Some(client_name) = hs.v.as_ref().and_then(format_peer_client_name) {
            self.state
                .peers
                .with_live_mut(self.addr, "update peer client name", |live| {
                    live.client_name = Some(client_name);
                });
        }

        if let Some(reqq) = hs.reqq.and_then(|reqq| usize::try_from(reqq).ok())
            && reqq > 0
        {
            let request_window = reqq.min(DEFAULT_PEER_REQUEST_WINDOW);
            let mut flow = self.lock_flow_control("update request window");
            if flow.request_window != request_window {
                debug!(
                    reqq,
                    request_window, "updated peer request window from extended handshake"
                );
                flow.request_window = request_window;
                self.notify_request_slots_changed();
            }
        }

        if !self.state.metadata.info.info().private && hs.m.ut_pex.is_some() {
            spawn_with_cancel(
                debug_span!(
                    parent: self.state.shared.span.clone(),
                    "sending_pex_to_peer",
                    peer = ?self.addr,
                ),
                format!(
                    "[{}][addr={}]sending_pex_to_peer",
                    self.state.shared.id, self.addr
                ),
                self.cancel_token.clone(),
                self.state
                    .clone()
                    .task_send_pex_to_peer(self.addr, self.tx.clone()),
            );
        }
        // Lets update outgoing Socket address for incoming connection
        if self.incoming
            && let Some(port) = hs.port()
        {
            let peer_ip = hs.ip_addr().unwrap_or(self.addr.ip());
            let outgoing_addr = SocketAddr::new(peer_ip, port);
            self.state
                .peers
                .with_peer_mut(self.addr, "update outgoing addr", |peer| {
                    peer.outgoing_address = Some(outgoing_addr)
                });
        }

        Ok(())
    }

    fn should_send_bitfield(&self) -> bool {
        if self.state.torrent().options.disable_upload() {
            return false;
        }

        self.state.get_approx_have_bytes() > 0
    }

    fn should_transmit_have(&self, id: ValidPieceIndex) -> bool {
        if self.state.shared.options.disable_upload() {
            return false;
        }
        if !self.state.should_advertise_have(id) {
            return false;
        }
        let have = self
            .state
            .peers
            .with_live(self.addr, |l| {
                // An empty bitfield is a peer that has told us nothing yet, not a peer
                // that has everything: a client with no pieces sends no bitfield at all,
                // which is why on_have() allocates one on the first Have it gets. Reading
                // that as "it already has the piece" silences every Have we would ever
                // send it, and it is the peer that needs them most. A Have it turns out
                // not to need costs 9 bytes.
                l.bitfield.get(id.get_usize()).is_some_and(|p| *p)
            })
            // Not live: nobody to tell.
            .unwrap_or(true);
        !have
    }

    fn update_my_extended_handshake(
        &self,
        handshake: &mut ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()> {
        let info_bytes = &self.state.metadata.info_bytes;
        if !info_bytes.is_empty()
            && let Ok(len) = info_bytes.len().try_into()
        {
            handshake.metadata_size = Some(len);
        }

        Ok(())
    }

    fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }
}

impl PeerHandler {
    fn on_peer_died(self, error: Option<crate::Error>) -> crate::Result<()> {
        let peers = &self.state.peers;
        let handle = self.addr;

        // The task that is dying may still own pieces it reserved while it was live, and a
        // reserved piece is owned by an address, not by a table entry: the entry may since
        // have been parked by a lowered peer limit, re-queued by a raised one, given to a
        // fresh dial, or dropped outright by `forget_disconnected_peers`. Hand the pieces
        // back before the table is even looked at -- until they are back in the queue
        // nobody else may download them, and the torrent stalls until a steal comes by.
        // Not fatal if the chunk tracker is gone: the torrent is being paused.
        let released = self
            .state
            .lock_write("release_dead_peer_pieces")
            .get_pieces_mut()
            .map(|pieces| pieces.release_pieces_owned_by(self.addr))
            .unwrap_or(0);
        if released > 0 {
            trace!(
                released,
                "peer dead, released its in-flight pieces to queue"
            );
            self.state.new_pieces_notify.notify_waiters();
        }

        let mut pe = match peers.states.get_mut(&handle) {
            Some(peer) => TimedExistence::new(peer, "on_peer_died"),
            None => {
                // Expected, not a bug: `forget_disconnected_peers` drops the entry of a
                // peer we already hung up on while its task is still winding down.
                debug!(addr = ?handle, "peer is no longer in the table, nothing to update");
                return Ok(());
            }
        };

        let prev = pe.value_mut().take_state(peers);

        // Only the task that owns the entry may end it. Two tasks name the same address in
        // turn -- a peer is parked by a lowered cap, re-queued by a raised one and dialled
        // again, or it dials us while we are hanging up on it -- and writing `NotNeeded` or
        // `Dead` over the newcomer would kill a connection that is coming up, while the
        // `Dead` backoff would hold the address for a minute or more.
        let ours = match &prev {
            PeerState::Connecting(tx) => tx.same_channel(&self.tx),
            PeerState::Live(live) => live.tx.same_channel(&self.tx),
            // A txless state names no task, so the channel cannot decide it. `NotNeeded` is
            // the parking this task was asked to honour, and honouring it is what it is
            // doing now. `Queued` and `Dead` mean the entry has already been handed on --
            // a raised cap re-queued the address, or a backoff holds it -- and whoever
            // takes it next reports its own death.
            PeerState::NotNeeded => true,
            PeerState::Queued | PeerState::Dead => false,
        };
        if !ours {
            trace!(
                state = %prev,
                "peer entry belongs to a newer connection, leaving it alone"
            );
            pe.value_mut().set_state(prev, peers);
            return Ok(());
        }

        match prev {
            PeerState::Live(live) => {
                for req in live.inflight_requests() {
                    trace!(
                        "peer dead, marking chunk request cancelled, index={}, chunk={}",
                        req.piece_index.get(),
                        req.chunk_index
                    );
                }
            }
            PeerState::NotNeeded => {
                // Restore it as take_state() replaced it above. A raise re-queues it.
                pe.value_mut().set_state(PeerState::NotNeeded, peers);
                return Ok(());
            }
            // Queued and Dead were sent back above; Connecting has nothing to hand back.
            PeerState::Connecting(_) | PeerState::Queued | PeerState::Dead => {}
        };

        let _error = match error {
            Some(e) => e,
            None => {
                trace!("peer died without errors, not re-queueing");
                pe.value_mut().set_state(PeerState::NotNeeded, peers);
                return Ok(());
            }
        };

        self.counters.errors.fetch_add(1, Ordering::Relaxed);

        if self.state.is_finished_and_no_active_streams() {
            debug!("torrent finished, not re-queueing");
            pe.value_mut().set_state(PeerState::NotNeeded, peers);
            return Ok(());
        }

        pe.value_mut().set_state(PeerState::Dead, peers);

        if self.incoming {
            // do not retry incoming peers
            debug!(
                peer = handle.to_string(),
                "incoming peer died, not re-queueing"
            );
            return Ok(());
        }

        let backoff = pe.value_mut().stats.backoff.next();

        // Prevent deadlocks.
        drop(pe);

        if let Some(dur) = backoff {
            if cfg!(feature = "_disable_reconnect_test") {
                return Ok(());
            }
            self.state.clone().spawn(
                debug_span!(
                    parent: self.state.shared.span.clone(),
                    "wait_for_peer",
                    peer = ?handle,
                    duration = format!("{dur:?}")
                ),
                format!("[{}][addr={}]wait_for_peer", self.state.shared.id, handle),
                async move {
                    trace!("waiting to reconnect again");
                    tokio::time::sleep(dur).await;
                    trace!("finished waiting");
                    let should_requeue = self
                        .state
                        .peers
                        .with_peer_mut(handle, "dead_to_queued", |peer| {
                            match peer.get_state() {
                                PeerState::Dead => {
                                    peer.set_state(PeerState::Queued, &self.state.peers);
                                    true
                                }
                                // Peer reconnected (e.g. via incoming connection) while we were
                                // waiting. No need to queue - it's already connected or queued.
                                PeerState::Live(_)
                                | PeerState::Connecting(_)
                                | PeerState::Queued => {
                                    trace!(
                                        state = peer.get_state().name(),
                                        "peer is no longer dead, skipping requeue"
                                    );
                                    false
                                }
                                // Don't need this peer anymore.
                                PeerState::NotNeeded => false,
                            }
                        })
                        .unwrap_or(false);
                    if should_requeue {
                        self.state
                            .peer_requeue_tx
                            .send(handle)
                            .ok()
                            .ok_or(Error::TorrentIsNotLive)?;
                    }
                    Ok::<_, Error>(())
                },
            );
        } else {
            debug!("dropping peer, backoff exhausted");
            self.state.peers.drop_peer(handle);
        };
        Ok(())
    }

    /// Acquire a piece for this peer: try steal (10x) → reserve → steal (3x).
    ///
    /// Returns the piece index to download, or None if no pieces are available.
    fn acquire_next_piece(&self) -> crate::Result<Option<ValidPieceIndex>> {
        if self.is_choked() {
            debug!("we are choked, can't acquire piece");
            return Ok(None);
        }

        // Steal info to process after releasing the peer lock
        let mut steal_info: Option<(SocketAddr, ValidPieceIndex)> = None;

        let result = self
            .state
            .peers
            .with_live_mut(self.addr, "acquire_next_piece", |live| {
                let mut g = self.state.lock_write("acquire_next_piece");

                let bf = &live.bitfield;
                // Extract references to disjoint fields
                let TorrentStateLocked {
                    pieces,
                    file_priorities,
                    ..
                } = &mut **g;
                let pieces = pieces.as_mut().ok_or(Error::ChunkTrackerEmpty)?;
                let result = pieces.acquire_piece(AcquireRequest {
                    peer: self.addr,
                    peer_avg_time: self.counters.average_piece_download_time(),
                    priority_pieces: self.state.streams.iter_next_pieces(&self.state.lengths),
                    file_priorities,
                    file_infos: &self.state.metadata.file_infos,
                    peer_has_piece: |p| bf.get(p.get() as usize).map(|v| *v) == Some(true),
                    can_steal: |p| {
                        self.state.per_piece_locks[p.get_usize()]
                            .try_write()
                            .is_some()
                    },
                });

                match result {
                    AcquireResult::Reserved(piece) => {
                        trace!("reserved piece {}", piece);
                        Ok(Some(piece))
                    }
                    AcquireResult::Stolen { piece, from_peer } => {
                        debug!("stole piece {} from {}", piece, from_peer);
                        // Store steal info to process after releasing peer lock to avoid deadlock
                        steal_info = Some((from_peer, piece));
                        Ok(Some(piece))
                    }
                    AcquireResult::NoneAvailable => Ok(None),
                }
            })
            .transpose()
            .map(|r| r.flatten());

        // Process steal notification outside the peer lock to avoid deadlock
        if let Some((from_peer, piece)) = steal_info {
            self.state.peers.on_steal(from_peer, self.addr, piece);
        }

        result
    }

    fn on_download_request(&self, request: Request) -> anyhow::Result<()> {
        if self.state.torrent().options.disable_upload() {
            anyhow::bail!("upload disabled, but peer requested a piece")
        }

        let piece_index = match self.state.lengths.validate_piece_index(request.index) {
            Some(p) => p,
            None => {
                anyhow::bail!(
                    "received {:?}, but it is not a valid chunk request (piece index is invalid). Ignoring.",
                    request
                );
            }
        };

        let chunk_info = match self.state.lengths.chunk_info_from_received_data(
            piece_index,
            request.begin,
            request.length,
        ) {
            Some(d) => d,
            None => {
                anyhow::bail!(
                    "received {:?}, but it is not a valid chunk request (chunk data is invalid). Ignoring.",
                    request
                );
            }
        };

        if !self
            .state
            .lock_read("is_chunk_ready_to_upload")
            .get_chunks()?
            .is_chunk_ready_to_upload(&chunk_info)
        {
            anyhow::bail!(
                "got request for a chunk that is not ready to upload. chunk {:?}",
                chunk_info
            );
        }

        self.state
            .ratelimit_upload_tx
            .send((self.tx.clone(), chunk_info))?;
        Ok(())
    }

    fn on_have(&self, have: u32) {
        self.state
            .peers
            .with_live_mut(self.addr, "on_have", |live| {
                // If bitfield wasn't allocated yet, let's do it. Some clients start empty so they never
                // send bitfields.
                if live.bitfield.is_empty() {
                    live.bitfield = make_piece_bitfield(&self.state.lengths);
                }
                match live.bitfield.get_mut(have as usize) {
                    Some(mut v) => *v = true,
                    None => {
                        warn!(
                            id = self.state.shared.id,
                            info_hash = ?self.state.shared.info_hash,
                            addr = ?self.addr,
                            "received have {} out of range",
                            have
                        );
                        return;
                    }
                };
                trace!("updated bitfield with have={}", have);
                self.state
                    .peers
                    .update_seeder_flag(live, self.state.lengths.total_pieces() as usize);
                if live.seeder {
                    debug!("peer has full torrent");
                }
            });
        self.on_bitfield_notify.notify_waiters();
    }

    fn on_bitfield(&self, bitfield: ByteBufOwned) -> anyhow::Result<()> {
        if bitfield.as_ref().len() != self.state.lengths.piece_bitfield_bytes() {
            anyhow::bail!(
                "dropping peer as its bitfield has unexpected size. Got {}, expected {}",
                bitfield.as_ref().len(),
                self.state.lengths.piece_bitfield_bytes(),
            );
        }
        let bf = BF::from_boxed_slice(bitfield.0.to_vec().into_boxed_slice());
        if let Some(true) = bf
            .get(..self.state.lengths.total_pieces() as usize)
            .map(|s| s.all())
        {
            debug!("peer has full torrent");
        }
        self.state
            .peers
            .update_bitfield(self.addr, bf, self.state.lengths.total_pieces() as usize);
        self.on_bitfield_notify.notify_waiters();
        Ok(())
    }

    async fn wait_for_any_notify(&self, notify: &Notify, check: impl Fn() -> bool) {
        loop {
            // To remove possibility of races, we first grab a token, then check
            // if we need it, and only if so, await.
            let notified = notify.notified();
            if check() {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_bitfield(&self) {
        self.wait_for_any_notify(&self.on_bitfield_notify, || {
            self.state
                .peers
                .with_live(self.addr, |live| !live.bitfield.is_empty())
                .unwrap_or_default()
        })
        .await;
    }

    async fn wait_for_request_slot(&self) {
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return;
            };
            let notified = notify.notified();
            if self.can_send_request() {
                return;
            }
            notified.await;
        }
    }

    // The job of this is to request chunks and also to keep peer alive.
    // The moment this ends, the peer is disconnected.
    async fn task_peer_chunk_requester(&self) -> crate::Result<()> {
        let handle = self.addr;
        self.wait_for_bitfield().await;

        let mut update_interest = {
            let mut current = false;
            move |h: &PeerHandler, new_value: bool| -> crate::Result<()> {
                if new_value != current {
                    h.tx.send(if new_value {
                        WriterRequest::Message(Message::Interested)
                    } else {
                        WriterRequest::Message(Message::NotInterested)
                    })
                    .ok()
                    .ok_or(Error::PeerTaskDead)?;
                    current = new_value;
                }
                Ok(())
            }
        };

        loop {
            // If we have full torrent, we don't need to request more pieces.
            // However we might still need to seed them to the peer.
            if self.state.is_finished_and_no_active_streams() {
                update_interest(self, false)?;
                if self
                    .state
                    .peers
                    .is_peer_not_interested_and_has_full_torrent(
                        self.addr,
                        self.state.lengths.total_pieces() as usize,
                    )
                {
                    debug!("nothing left to do, neither of us is interested, disconnecting peer");
                    self.tx
                        .send(WriterRequest::Disconnect(Ok(())))
                        .ok()
                        .ok_or(Error::PeerTaskDead)?;
                    // wait until the receiver gets the message so that it doesn't finish with an error.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return Ok(());
                } else {
                    // TODO: wait for a notification of interest, e.g. update of selected files or new streams or change
                    // in peer interest.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }

            update_interest(self, true)?;
            aframe!(self.wait_for_request_slot()).await;

            // Acquire a piece using the strategy: try steal (10x) → reserve → steal (3x).
            let new_piece_notify = self.state.new_pieces_notify.notified();
            let next = match self.acquire_next_piece()? {
                Some(next) => next,
                None => {
                    debug!("no pieces to request");
                    match aframe!(tokio::time::timeout(
                        // Half of default rw timeout not to race with it.
                        Duration::from_secs(5),
                        new_piece_notify
                    ))
                    .await
                    {
                        Ok(()) => debug!("woken up, new pieces might be available"),
                        Err(_) => debug!("woken up by sleep timer"),
                    }
                    continue;
                }
            };

            for chunk in self.state.lengths.iter_chunk_infos(next) {
                let request = Request {
                    index: next.get(),
                    begin: chunk.offset,
                    length: chunk.size,
                };

                aframe!(self.wait_for_request_slot()).await;

                self.state
                    .ratelimits
                    .prepare_for_download(NonZeroU32::new(request.length).unwrap())
                    .await?;

                if let Some(session) = self.state.torrent().session.upgrade() {
                    session
                        .ratelimits
                        .prepare_for_download(NonZeroU32::new(request.length).unwrap())
                        .await?;
                }

                aframe!(self.wait_for_request_slot()).await;

                match self
                    .state
                    .peers
                    .with_live_mut(handle, "add chunk request", |live| {
                        live.add_inflight_request(chunk)
                    }) {
                    Some(true) => {}
                    Some(false) => {
                        // This request was already in-flight for this peer for this chunk.
                        // This might happen in theory, but not very likely.
                        //
                        // Example:
                        // someone stole a piece from us, and then died, the piece became "needed" again, and we reserved it
                        // all before the piece request was processed by us.
                        warn!(
                            id = self.state.shared.id,
                            info_hash = ?self.state.shared.info_hash,
                            addr = ?self.addr,
                            "we already requested {:?} previously",
                            chunk
                        );
                        continue;
                    }
                    // peer died
                    None => return Ok(()),
                };

                if self
                    .tx
                    .send(WriterRequest::Message(Message::Request(request)))
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }

    fn on_i_am_choked(&self) {
        self.lock_flow_control("i_am_choked = true").i_am_choked = true;
        // A choke discards every request we have outstanding with this peer, and without
        // the fast extension nothing says which. Kept, they would hold their pieces
        // reserved to a peer that is not going to send them: the unchoke that follows
        // asks for new pieces, and the stranded ones wait for a steal - which needs a
        // faster peer to steal them, so a torrent with one peer stalled short of the end.
        // The same handback as a dying peer's, and in the same order: the table first,
        // then the pieces, never both locks at once.
        let dropped = self
            .state
            .peers
            .with_live_mut(self.addr, "forget_inflight_requests_on_choke", |live| {
                live.forget_inflight_requests_on_choke()
            })
            .unwrap_or(0);
        if dropped > 0 {
            let released = self
                .state
                .lock_write("release_choked_peer_pieces")
                .get_pieces_mut()
                .map(|pieces| pieces.release_pieces_owned_by(self.addr))
                .unwrap_or(0);
            trace!(dropped, released, "choked, handed our requests back");
            if released > 0 {
                self.state.new_pieces_notify.notify_waiters();
            }
        }
        self.notify_request_slots_changed();
    }

    fn on_peer_interested(&self) {
        trace!("peer is interested");
        self.state.peers.mark_peer_interested(self.addr, true);
    }

    fn on_peer_not_interested(&self) {
        trace!("peer is not interested");
        self.state.peers.mark_peer_interested(self.addr, false);
    }

    fn on_i_am_unchoked(&self) {
        trace!("we are unchoked");
        self.lock_flow_control("i_am_choked = false").i_am_choked = false;
        self.notify_request_slots_changed();
    }

    async fn on_received_piece(&self, piece: Piece<ByteBuf<'_>>) -> anyhow::Result<()> {
        let piece_index = self
            .state
            .lengths
            .validate_piece_index(piece.index)
            .with_context(|| format!("peer sent an invalid piece {}", piece.index))?;
        let chunk_info = match self.state.lengths.chunk_info_from_received_data(
            piece_index,
            piece.begin,
            piece.len().try_into().context("bug")?,
        ) {
            Some(i) => i,
            None => {
                anyhow::bail!("peer sent us an invalid piece {:?}", piece,);
            }
        };

        // Peer chunk/byte counters. These count what came off the wire, whether we can
        // use it or not.
        self.counters
            .fetched_bytes
            .fetch_add(piece.len() as u64, Ordering::Relaxed);
        self.counters.fetched_chunks.fetch_add(1, Ordering::Relaxed);

        let should_process = self
            .state
            .peers
            .with_live_mut(self.addr, "inflight_requests.remove", |h| {
                match h.remove_inflight_request(&chunk_info) {
                    RemoveInflightRequestResult::Expected => Ok(true),
                    RemoveInflightRequestResult::LateCanceled => {
                        trace!(?piece, "peer sent us a chunk we did not ask for");
                        Ok(false)
                    }
                    RemoveInflightRequestResult::Unexpected => anyhow::bail!(
                        "peer sent us a piece we did not ask. Inflight requests: {:?}. Got: {:?}",
                        h.inflight_requests_debug(),
                        piece,
                    ),
                }
            })
            .context("peer not found")??;

        if !should_process {
            return Ok(());
        }

        // Only now, past the check: a chunk that arrives after we cancelled the request
        // is thrown away, and a peer whose every chunk is thrown away has moved nothing.
        // A lowered cap ranks by this, and would otherwise keep exactly the peers whose
        // pipeline someone else's steal has poisoned.
        self.counters.on_bytes_moved(piece.len() as u64);

        // This one is used to calculate download speed.
        self.state
            .stats
            .fetched_bytes
            .fetch_add(piece.len() as u64, Ordering::Relaxed);
        self.state
            .session_stats
            .counters
            .fetched_bytes
            .fetch_add(piece.len() as u64, Ordering::Relaxed);

        fn write_to_disk(
            state: &TorrentStateLive,
            addr: PeerHandle,
            counters: &AtomicPeerCounters,
            piece: &Piece<ByteBuf<'_>>,
            chunk_info: &ChunkInfo,
        ) -> anyhow::Result<()> {
            let index = piece.index;

            // If someone stole the piece by now, ignore it.
            // However if they didn't, don't let them steal it while we are writing.
            // So that by the time we are done writing AND if it was the last piece,
            // we can actually checksum etc.
            // Otherwise it might get into some weird state.
            let ppl_guard = {
                let g = state.lock_read("check_steal");

                let ppl = state
                    .per_piece_locks
                    .get(piece.index as usize)
                    .map(|l| l.read());

                match g.get_pieces()?.get_inflight(chunk_info.piece_index) {
                    Some(inflight) if inflight.peer == addr => {}
                    Some(inflight) => {
                        debug!(
                            "in-flight piece {} was stolen by {}, ignoring",
                            chunk_info.piece_index, inflight.peer
                        );
                        return Ok(());
                    }
                    None => {
                        debug!(
                            "in-flight piece {} not found. it was probably completed by someone else",
                            chunk_info.piece_index
                        );
                        return Ok(());
                    }
                };

                ppl
            };

            // While we hold per piece lock, noone can steal it.
            // So we can proceed writing knowing that the piece is ours now and will still be by the time
            // the write is finished.
            //

            if !cfg!(feature = "_disable_disk_write_net_benchmark") {
                match state.file_ops().write_chunk(addr, piece, chunk_info) {
                    Ok(()) => {}
                    Err(e) => {
                        error!(
                            id = state.shared.id,
                            info_hash = ?state.shared.info_hash,
                            "FATAL: error writing chunk to disk: {e:#}"
                        );
                        return state.on_fatal_error(e);
                    }
                };
            }

            let full_piece_download_time = {
                let mut g = state.lock_write("mark_chunk_downloaded");
                let chunk_marking_result = g.get_pieces_mut()?.mark_chunk_downloaded(piece);
                trace!(?piece, chunk_marking_result=?chunk_marking_result);

                match chunk_marking_result {
                    Some(ChunkMarkingResult::Completed) => {
                        trace!("piece={} done, will write and checksum", piece.index);
                        // Remove from inflight to prevent others from stealing it during hash check.
                        g.get_pieces_mut()?.take_inflight(chunk_info.piece_index)
                    }
                    Some(ChunkMarkingResult::PreviouslyCompleted) => {
                        // TODO: we might need to send cancellations here.
                        debug!("piece={} was done by someone else, ignoring", piece.index);
                        return Ok(());
                    }
                    Some(ChunkMarkingResult::NotCompleted) => None,
                    None => {
                        anyhow::bail!(
                            "bogus data received: {:?}, cannot map this to a chunk, dropping peer",
                            piece
                        );
                    }
                }
            };

            // We don't care about per piece lock anymore, as it's removed from inflight pieces.
            // It shouldn't impact perf anyway, but dropping just in case.
            drop(ppl_guard);

            let full_piece_download_time = match full_piece_download_time {
                Some(t) => t,
                None => return Ok(()),
            };

            match state
                .file_ops()
                .check_piece(chunk_info.piece_index)
                .with_context(|| format!("error checking piece={index}"))?
            {
                true => {
                    // The storage gets the piece before anyone else hears of it. This is
                    // where a storage that answers has_piece() makes the piece visible -
                    // moves it out of a staging area, into the place a restart will look -
                    // and until it has, the piece is not ours to mark, count, advertise or
                    // serve. A storage that can't is a disk failure of the same class as a
                    // failed write, and ends the torrent the same way; the bytes it holds
                    // are checked again on restart, and has_piece() decides what is there.
                    if let Err(e) = state.files.on_piece_completed(chunk_info.piece_index) {
                        error!(
                            id = state.shared.id,
                            info_hash = ?state.shared.info_hash,
                            piece = index,
                            "FATAL: error committing piece to storage: {e:#}"
                        );
                        return state.on_fatal_error(e);
                    }

                    let piece_len = state.lengths.piece_length(chunk_info.piece_index) as u64;
                    {
                        let mut g = state.lock_write("mark_piece_downloaded");
                        g.get_pieces_mut()?
                            .mark_piece_hash_ok(chunk_info.piece_index, &state.metadata.file_infos);
                        // Under the same lock as the have-bit: drop_pieces() subtracts
                        // from this under that lock, and must not get there first.
                        state
                            .stats
                            .have_bytes
                            .fetch_add(piece_len, Ordering::Relaxed);
                    }

                    // Global piece counters.
                    state
                        .stats
                        .downloaded_and_checked_bytes
                        // This counter is used to compute "is_finished", so using
                        // stronger ordering.
                        .fetch_add(piece_len, Ordering::Release);
                    state
                        .stats
                        .downloaded_and_checked_pieces
                        // This counter is used to compute "is_finished", so using
                        // stronger ordering.
                        .fetch_add(1, Ordering::Release);
                    #[allow(clippy::cast_possible_truncation)]
                    state.stats.total_piece_download_ms.fetch_add(
                        full_piece_download_time.as_millis() as u64,
                        Ordering::Relaxed,
                    );

                    // Per-peer piece counters.
                    counters.on_piece_completed(piece_len, full_piece_download_time);
                    state.peers.reset_peer_backoff(addr);

                    trace!(piece = index, "successfully downloaded and verified");

                    state.on_piece_completed(chunk_info.piece_index)?;

                    state.transmit_haves(chunk_info.piece_index);
                }
                false => {
                    warn!(
                        id = state.shared.id,
                        info_hash = ?state.shared.info_hash,
                        ?addr,
                        "checksum for piece={} did not validate. disconnecting peer.", index
                    );
                    state
                        .lock_write("mark_piece_broken")
                        .get_pieces_mut()?
                        .mark_piece_hash_failed(chunk_info.piece_index);
                    state.new_pieces_notify.notify_waiters();
                    anyhow::bail!("i am probably a bogus peer. dying.")
                }
            };
            Ok(())
        }

        self.state
            .shared
            .spawner
            .block_in_place_with_semaphore(|| {
                write_to_disk(&self.state, self.addr, &self.counters, &piece, &chunk_info)
            })
            .await
            .with_context(|| format!("error processing received chunk {chunk_info:?}"))?;

        Ok(())
    }

    fn send_metadata_piece(&self, piece_id: u32) -> anyhow::Result<()> {
        let data = &self.state.metadata.info_bytes;
        let metadata_size = data.len();
        if metadata_size == 0 {
            anyhow::bail!("peer requested for info metadata but we don't have it")
        }
        let total_pieces: usize = (metadata_size as u64)
            .div_ceil(CHUNK_SIZE as u64)
            .try_into()?;

        if piece_id as usize > total_pieces {
            bail!("piece out of bounds")
        }

        let offset = piece_id * CHUNK_SIZE;
        let end = (offset + CHUNK_SIZE).min(data.len().try_into()?);
        let total_size: u32 = data
            .len()
            .try_into()
            .context("can't send metadata: len doesn't fit into u32")?;
        let data = data.slice(offset as usize..end as usize);

        self.tx
            .send(WriterRequest::UtMetadata(UtMetadata::Data(
                UtMetadataData::from_bytes(piece_id, total_size, data.into()),
            )))
            .context("error sending UtMetadata: channel closed")?;
        Ok(())
    }

    fn on_pex_message(&self, msg: UtPex<ByteBuf<'_>>) {
        msg.dropped_peers()
            .chain(msg.added_peers())
            .for_each(|peer| {
                self.state
                    .add_peer_if_not_seen(peer.addr)
                    .map_err(|error| {
                        warn!(
                            id = self.state.shared.id,
                            info_hash = ?self.state.shared.info_hash,
                            ?peer,
                            "failed to add peer: {error:#}"
                        );
                        error
                    })
                    .ok();
            });
    }

    fn is_choked(&self) -> bool {
        self.lock_flow_control("is_choked").i_am_choked
    }

    fn requested_inflight_count(&self) -> Option<usize> {
        self.state
            .peers
            .with_live(self.addr, |live| live.requested_inflight_count())
    }

    fn can_send_request(&self) -> bool {
        let (i_am_choked, request_window) = {
            let flow = self.lock_flow_control("can_send_request");
            (flow.i_am_choked, flow.request_window)
        };

        if i_am_choked {
            return false;
        }

        self.requested_inflight_count()
            .is_some_and(|requested| requested < request_window)
    }

    fn lock_flow_control(
        &self,
        reason: &'static str,
    ) -> TimedExistence<MutexGuard<'_, PeerFlowControl>> {
        TimedExistence::new(timeit(reason, || self.flow_control.lock()), reason)
    }

    fn request_slots_changed(&self) -> Option<Arc<Notify>> {
        self.state
            .peers
            .with_live(self.addr, |live| live.request_slots_changed())
    }

    fn notify_request_slots_changed(&self) {
        if let Some(notify) = self.request_slots_changed() {
            notify.notify_waiters();
        }
    }
}

/// Subtract up to `by` from `counter`, stopping at zero; returns how much was subtracted.
fn sub_saturating(counter: &AtomicUsize, by: usize) -> usize {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(by))
        })
        .map(|previous| previous.min(by))
        .unwrap_or(0)
}

/// A live-peer slot, held for as long as the peer connection is.
///
/// Owning the permit is not enough on its own: a cap lowered while peers hold every
/// permit cannot take the ones it wants off the semaphore, so it books them as a debt
/// (`peer_permits_to_forget`) for the returning permits to pay. Which means a permit must
/// be settled exactly once, on every way out of a peer task -- and there are many, `?`
/// operators, a panic and the task being cancelled among them. Settling it in `Drop` is
/// the only shape that covers all of them; a permit returned to the pool without paying
/// the debt lets the adder dial one more peer than the cap allows, until some unrelated
/// death happens to pay it instead.
pub(crate) struct PeerPermit {
    state: Arc<TorrentStateLive>,
    permit: Option<OwnedSemaphorePermit>,
}

impl PeerPermit {
    /// Wait for a slot.
    async fn acquire(state: &Arc<TorrentStateLive>) -> crate::Result<Self> {
        let permit = state.peer_semaphore.clone().acquire_owned().await?;
        Ok(Self {
            state: state.clone(),
            permit: Some(permit),
        })
    }

    /// Take a slot if one is free right now.
    fn try_acquire(state: &Arc<TorrentStateLive>) -> Option<Self> {
        let permit = state.peer_semaphore.clone().try_acquire_owned().ok()?;
        Some(Self {
            state: state.clone(),
            permit: Some(permit),
        })
    }
}

impl Drop for PeerPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            // Pay the debt a lowered cap left, or give the slot back to the semaphore.
            if sub_saturating(&self.state.peer_permits_to_forget, 1) == 1 {
                permit.forget();
            }
        }
    }
}

fn format_peer_client_name(value: &ByteBuf<'_>) -> Option<String> {
    let client_name = String::from_utf8_lossy(value.as_ref()).trim().to_string();
    if client_name.is_empty() {
        return None;
    }

    Some(client_name)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use librqbit_core::{hash_id::Id20, lengths::Lengths};

    use super::{
        BF, Ordering, Peer, PeerState, WriterRequest, clamp_piece_range, has_any_needed_piece,
        peer::{LivePeerState, PeerTx},
        surplus_rank,
    };
    use crate::stream_connect::ConnectionKind;

    #[test]
    fn test_clamp_piece_range() {
        // 10 pieces of 1024 bytes each.
        let lengths = Lengths::new(10 * 1024, 1024).unwrap();

        // A range inside the torrent is untouched.
        assert_eq!(clamp_piece_range(2..5, &lengths), 2..5);

        // "Everything from here on", which is how a caller asks to reclaim a tail.
        assert_eq!(clamp_piece_range(5..u32::MAX, &lengths), 5..10);
        assert_eq!(clamp_piece_range(0..u32::MAX, &lengths), 0..10);

        // Entirely out of range: empty, and not a range that panics when iterated.
        let r = clamp_piece_range(100..u32::MAX, &lengths);
        assert!(r.is_empty(), "{r:?}");
        assert_eq!(r.count(), 0);
    }

    /// The byte-wise overlap test answers exactly what a per-piece walk would, including
    /// at the ragged end of the last byte and for a peer whose bitfield has not arrived.
    #[test]
    fn has_any_needed_piece_matches_a_piece_by_piece_walk() {
        let naive = |needed: &BF, has: &BF| {
            needed
                .iter_ones()
                .any(|index| has.get(index).is_some_and(|bit| *bit))
        };
        let sized = |pieces: usize, ones: &[usize]| -> BF {
            let mut bv: bitvec::vec::BitVec<u8, bitvec::order::Msb0> =
                bitvec::vec::BitVec::repeat(false, pieces);
            for one in ones {
                bv.set(*one, true);
            }
            bv.into_boxed_bitslice()
        };

        // 20 pieces: two full bytes and a half-used third, so the masked tail is covered.
        for (needed, has) in [
            (vec![0usize], vec![0usize]),
            (vec![0], vec![1]),
            (vec![19], vec![19]),
            (vec![19], vec![18]),
            (vec![3, 11, 19], vec![11]),
            (vec![3, 11, 19], vec![2, 10, 18]),
            (vec![], vec![7]),
            (vec![7], vec![]),
        ] {
            let n = sized(20, &needed);
            let h = sized(20, &has);
            assert_eq!(
                has_any_needed_piece(&n, &h),
                naive(&n, &h),
                "needed={needed:?} has={has:?}"
            );
        }

        // A peer that has not sent its bitfield yet holds nothing, not everything.
        let n = sized(20, &[0, 19]);
        let empty = BF::default();
        assert!(!has_any_needed_piece(&n, &empty));
        assert_eq!(has_any_needed_piece(&n, &empty), naive(&n, &empty));
    }

    /// A bitfield of eight pieces holding exactly the ones listed.
    fn bitfield(has: &[usize]) -> BF {
        let mut bf = BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice());
        for index in has {
            bf.set(*index, true);
        }
        bf
    }

    /// A live peer at `port`, with `peer_interested` as given, holding `has`, that has
    /// fetched `fetched` bytes from us and been sent `uploaded`. `recent` says how much of
    /// that moved lately.
    fn live_peer(
        port: u16,
        peer_interested: bool,
        has: &[usize],
        fetched: u64,
        uploaded: u64,
        recent: u64,
    ) -> Peer {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WriterRequest>();
        let tx: PeerTx = tx;
        let mut live = LivePeerState::new(
            Id20::new([0u8; 20]),
            tx,
            peer_interested,
            ConnectionKind::Tcp,
        );
        live.bitfield = bitfield(has);
        let peer = Peer::new_in_state_for_test(addr, PeerState::Live(live));
        peer.stats
            .counters
            .fetched_bytes
            .store(fetched, Ordering::Relaxed);
        peer.stats
            .counters
            .uploaded_bytes
            .store(uploaded, Ordering::Relaxed);
        if recent > 0 {
            peer.stats.counters.on_bytes_moved(recent);
        }
        peer
    }

    /// The cut a lowered cap makes does not care which way a peer's bytes go: one we only
    /// upload to and one that only feeds us survive it together, and the two peers doing
    /// nothing either way are the ones dropped.
    #[test]
    fn the_surplus_cut_is_blind_to_direction() {
        // We still want piece 0 and nothing else.
        let has_needed_piece = |bf: &BF| bf.get(0).is_some_and(|b| *b);

        // Only feeds us: has the piece we want, wants nothing of ours.
        let feeder = live_peer(1, false, &[0], 4096, 0, 4096);
        // Only takes from us: wants what we have, holds nothing we need.
        let consumer = live_peer(2, true, &[1], 0, 4096, 4096);
        // Neither: nothing we want, wants nothing, and has moved nothing.
        let idle_a = live_peer(3, false, &[1], 0, 0, 0);
        let idle_b = live_peer(4, false, &[2], 0, 0, 0);
        // A peer we have not finished dialling has shown even less than the idle two.
        let connecting = {
            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 5));
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            Peer::new_in_state_for_test(addr, PeerState::Connecting(tx))
        };

        let rank = |p: &Peer| surplus_rank(p, &has_needed_piece).unwrap();
        let feeder_rank = rank(&feeder);
        let consumer_rank = rank(&consumer);

        // The two useful ones are worth the same: same "useful" bit, same bytes, and the
        // direction those bytes went makes no difference to either.
        assert_eq!(feeder_rank, consumer_rank);

        let mut ranked = vec![
            (rank(&idle_a), 3u16),
            (feeder_rank, 1),
            (rank(&connecting), 5),
            (consumer_rank, 2),
            (rank(&idle_b), 4),
        ];
        ranked.sort_unstable();
        let order: Vec<u16> = ranked.iter().map(|(_, port)| *port).collect();

        // Cut down to two: the peer still connecting goes first, then the idle pair.
        assert_eq!(&order[..3], &[5, 3, 4], "{ranked:?}");
        let kept: Vec<u16> = order[3..].to_vec();
        assert!(kept.contains(&1) && kept.contains(&2), "{kept:?}");
    }

    /// Between two peers that are equally useful, the one that moved bytes lately beats
    /// the one whose bytes are all in the past -- otherwise the cut just keeps whoever
    /// connected first.
    #[test]
    fn recent_bytes_outrank_a_long_dead_lifetime_total() {
        let has_needed_piece = |bf: &BF| bf.get(0).is_some_and(|b| *b);
        // An old hand: a lot of bytes, none of them recent.
        let veteran = live_peer(1, false, &[0], 50_000_000, 0, 0);
        // A newcomer that is working now, and has moved four kilobytes in its whole life.
        let newcomer = live_peer(2, false, &[0], 4096, 0, 4096);

        let veteran_rank = surplus_rank(&veteran, &has_needed_piece).unwrap();
        let newcomer_rank = surplus_rank(&newcomer, &has_needed_piece).unwrap();
        assert!(
            veteran_rank < newcomer_rank,
            "{veteran_rank:?} should rank below {newcomer_rank:?}"
        );
    }
}
