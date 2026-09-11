//! Coordinates piece download state by wrapping ChunkTracker with inflight piece tracking.
//!
//! This module provides the [`PieceTracker`] type which encapsulates the relationship between
//! queued pieces (in ChunkTracker) and in-flight pieces (being downloaded by a peer).
//!
//! The key invariant maintained is: a piece is in exactly one state at any time:
//! - HAVE (completed)
//! - QUEUED (available to download)
//! - IN_FLIGHT (currently being downloaded)
//! - NOT_NEEDED (not selected for download)
//!
//! On top of that, a piece dropped through [`PieceTracker::drop_pieces`] is RELEASING
//! until the caller reports back through [`PieceTracker::finish_release`]: it is not
//! HAVE, and nothing may make it HAVE again while the caller is deleting its storage.
//! That state lives in the ChunkTracker, because unlike the in-flight map it has to
//! survive a pause - see [`ChunkTracker::is_releasing`].

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use buffers::ByteBuf;
use librqbit_core::lengths::ValidPieceIndex;
use peer_binary_protocol::Piece;

use crate::{
    chunk_tracker::{ChunkMarkingResult, ChunkTracker, Reselected},
    type_aliases::{FileInfos, FilePriorities, PeerHandle},
};

/// Tracks a piece currently being downloaded.
#[derive(Debug, Clone)]
pub struct InflightPiece {
    pub peer: PeerHandle,
    /// Which connection to `peer` reserved it: see [`AcquireRequest::connection`].
    pub connection: u64,
    pub started: Instant,
}

/// Result of attempting to acquire a piece.
#[derive(Debug)]
pub enum AcquireResult {
    /// A new piece was reserved from the queue.
    Reserved(ValidPieceIndex),
    /// A piece was stolen from a slower peer.
    Stolen {
        piece: ValidPieceIndex,
        from_peer: PeerHandle,
    },
    /// No pieces are available for this peer.
    NoneAvailable,
}

/// Parameters for acquiring a piece.
pub struct AcquireRequest<'a, I, P, S>
where
    I: Iterator<Item = ValidPieceIndex>,
    P: Fn(ValidPieceIndex) -> bool,
    S: Fn(ValidPieceIndex) -> bool,
{
    /// The peer requesting a piece.
    pub peer: PeerHandle,
    /// Which connection to that peer is asking. Two connections can share an address in
    /// turn - a peer redialled while its old task is still winding down - and each hands
    /// back only the pieces it reserved: see [`PieceTracker::release_pieces_owned_by`].
    pub connection: u64,
    /// The peer's average piece download time (for steal calculations).
    pub peer_avg_time: Option<Duration>,
    /// Priority pieces to check first (e.g., for streaming).
    pub priority_pieces: I,
    /// File download priority ordering.
    pub file_priorities: &'a FilePriorities,
    /// File metadata for iterating pieces.
    pub file_infos: &'a FileInfos,
    /// Returns true if the peer has the given piece.
    pub peer_has_piece: P,
    /// Returns true if the piece can be stolen (e.g., not locked for writing).
    pub can_steal: S,
}

/// Coordinates piece download state.
///
/// Wraps a [`ChunkTracker`] with tracking of which pieces are currently being downloaded
/// (in-flight) and by which peer. This ensures that:
///
/// - A piece is only assigned to one peer at a time (unless stolen)
/// - Pieces are properly requeued when a peer dies
/// - State transitions maintain invariants
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
}

impl PieceTracker {
    // === CONSTRUCTION ===

    /// Create a new PieceTracker wrapping the given ChunkTracker.
    pub fn new(chunks: ChunkTracker) -> Self {
        Self {
            chunks,
            inflight: HashMap::new(),
        }
    }

    /// Read-only access to the underlying ChunkTracker.
    pub fn chunks(&self) -> &ChunkTracker {
        &self.chunks
    }

    /// Consume the PieceTracker, requeuing any in-flight pieces.
    ///
    /// This is used when pausing a torrent - any pieces that were being downloaded
    /// need to be put back in the queue so they can be re-downloaded on resume.
    pub fn into_chunks(mut self) -> ChunkTracker {
        // Requeue all in-flight pieces so they'll be re-downloaded on resume
        for piece in self.inflight.into_keys() {
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        self.chunks
    }

    // === PIECE ACQUISITION ===

    /// Attempt to acquire a piece for the requesting peer.
    ///
    /// The acquisition strategy is:
    /// 1. Reserve a priority piece (what a stream is waiting on), or take one back off a
    ///    peer 10x slower than us if the whole priority window is already spoken for
    /// 2. Reserve a queued piece
    /// 3. Steal from a peer 3x slower than us
    ///
    /// Stealing comes last, and never while there is free work left, because it is not
    /// free: the peer robbed has already asked its seeder for those chunks, and the bytes
    /// it is about to receive for them are dropped on arrival. On a slow link a request
    /// window is a minute of queued work, so a steal costs that peer a minute of its
    /// bandwidth. Paying that to start a piece nobody else wanted is a straight loss,
    /// which is why only the priority window -- where the stream waits on that piece and
    /// no other -- may steal before the queue has been drained.
    ///
    /// If `Stolen` is returned, the caller MUST call `peers.on_steal()` to notify
    /// the old peer and update counters.
    pub fn acquire_piece<I, P, S>(&mut self, mut req: AcquireRequest<I, P, S>) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        // 1. Priority pieces: what an active stream is waiting on, in playback order.
        // Reserve the first free one; if every one this peer could take is already being
        // downloaded, remember the first, which is the one a stream reaches soonest.
        let mut held_priority_piece = None;
        for piece in &mut req.priority_pieces {
            if self.chunks.is_piece_have(piece)
                || self.chunks.is_releasing(piece)
                || !(req.peer_has_piece)(piece)
            {
                continue;
            }
            match self.inflight.get(&piece) {
                None => return self.reserve_piece(piece, req.peer, req.connection),
                Some(inflight) => {
                    if held_priority_piece.is_none() && inflight.peer != req.peer {
                        held_priority_piece = Some(piece);
                    }
                }
            }
        }
        if let Some(piece) = held_priority_piece
            && let Some(result) = self.steal_piece(&req, piece, 10.0)
        {
            return result;
        }

        // 2. Then check naturally ordered queued pieces
        // Note: iter_queued_pieces only returns pieces in queue_pieces (not in-flight)
        let queued: Vec<_> = self
            .chunks
            .iter_queued_pieces(req.file_priorities, req.file_infos)
            .collect();

        for piece in queued {
            if (req.peer_has_piece)(piece) && !self.chunks.is_releasing(piece) {
                return self.reserve_piece(piece, req.peer, req.connection);
            }
        }

        // 3. Nothing left to reserve: take the piece that has been in flight longest off
        // a peer 3x slower than us, if there is one.
        if let Some(result) = self.try_steal(&req, 3.0) {
            return result;
        }

        AcquireResult::NoneAvailable
    }

    /// Reserve a piece: remove from queue, add to inflight.
    fn reserve_piece(
        &mut self,
        piece: ValidPieceIndex,
        peer: PeerHandle,
        connection: u64,
    ) -> AcquireResult {
        self.chunks.reserve_needed_piece(piece);
        self.inflight.insert(
            piece,
            InflightPiece {
                peer,
                connection,
                started: Instant::now(),
            },
        );
        AcquireResult::Reserved(piece)
    }

    /// Try to steal whichever piece has been in flight longest, from a slower peer.
    fn try_steal<I, P, S>(
        &mut self,
        req: &AcquireRequest<I, P, S>,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        // Find the slowest piece from another peer that the stealing peer actually has.
        // The threshold itself is checked by steal_piece: the piece that has been in
        // flight longest is the only candidate either way.
        let (piece, _) = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer != req.peer)
            .filter(|(p, _)| (req.peer_has_piece)(**p))
            .map(|(p, info)| (*p, info.started))
            .min_by_key(|(_, started)| *started)?;

        self.steal_piece(req, piece, threshold)
    }

    /// Take one specific piece off the peer downloading it, if that peer has held it for
    /// `threshold` times as long as a piece takes us and the piece is not being written.
    fn steal_piece<I, P, S>(
        &mut self,
        req: &AcquireRequest<I, P, S>,
        piece: ValidPieceIndex,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let my_avg = req.peer_avg_time?;
        let min_elapsed = Duration::from_secs_f64(my_avg.as_secs_f64() * threshold);

        let info = self.inflight.get(&piece)?;
        let old_peer = info.peer;
        if old_peer == req.peer || info.started.elapsed() < min_elapsed {
            return None;
        }

        // Check can_steal (e.g., per_piece_lock)
        if !(req.can_steal)(piece) {
            return None;
        }

        // Update ownership (piece stays in inflight, just changes owner)
        let info = self.inflight.get_mut(&piece)?;
        info.peer = req.peer;
        info.connection = req.connection;
        info.started = Instant::now();

        Some(AcquireResult::Stolen {
            piece,
            from_peer: old_peer,
        })
    }

    // === PIECE COMPLETION ===

    /// Remove piece from inflight tracking (e.g., after all chunks received).
    ///
    /// Returns download duration if piece was in-flight.
    /// Note: Does NOT mark the piece as downloaded - caller should do hash check
    /// and then call `mark_piece_hash_ok` or `mark_piece_hash_failed`.
    pub fn take_inflight(&mut self, piece: ValidPieceIndex) -> Option<Duration> {
        let inflight = self.inflight.remove(&piece)?;
        Some(inflight.started.elapsed())
    }

    /// Mark piece as downloaded after successful hash verification. Moves the per-file
    /// counts with it: see [`ChunkTracker::mark_piece_downloaded`].
    pub fn mark_piece_hash_ok(&mut self, piece: ValidPieceIndex, file_infos: &FileInfos) {
        self.chunks.mark_piece_downloaded(piece, file_infos);
    }

    /// Mark piece as failed after hash verification failure - requeues the piece.
    pub fn mark_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Release all pieces one connection to a peer owns (on its death, or a choke).
    ///
    /// Moves all pieces owned by the peer from IN_FLIGHT back to QUEUED.
    /// Returns the number of pieces released.
    ///
    /// By connection and not by address alone: a task that is winding down asks after its
    /// address has been dialled again, and the address alone would hand back the new
    /// connection's pieces too. It goes on asking for their chunks and throws away every
    /// one that arrives, since the piece is no longer reserved to it.
    pub fn release_pieces_owned_by(&mut self, peer: PeerHandle, connection: u64) -> usize {
        // Collect pieces to release (can't modify while iterating)
        let pieces_to_release: Vec<_> = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer == peer && info.connection == connection)
            .map(|(p, _)| *p)
            .collect();

        let count = pieces_to_release.len();
        for piece in pieces_to_release {
            self.inflight.remove(&piece);
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        count
    }

    // === QUERIES ===

    /// Get the inflight info for a piece, if it's currently being downloaded.
    pub fn get_inflight(&self, piece: ValidPieceIndex) -> Option<&InflightPiece> {
        self.inflight.get(&piece)
    }

    /// Check if a piece is currently in-flight.
    #[allow(dead_code)]
    pub fn is_inflight(&self, piece: ValidPieceIndex) -> bool {
        self.inflight.contains_key(&piece)
    }

    /// Get the number of pieces currently in-flight.
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    // === PASS-THROUGH METHODS ===

    /// Mark a chunk as downloaded. Returns the result indicating if the piece is complete.
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        self.chunks.mark_chunk_downloaded(piece)
    }

    /// Drop pieces: see [`ChunkTracker::drop_pieces`]. A piece a peer owns is left alone,
    /// so this never takes anything out of the in-flight map.
    ///
    /// The pieces it returns are claimed until [`Self::finish_release`] is called for
    /// them: the caller is about to delete their storage, and until that is done nothing
    /// may download them again.
    pub fn drop_pieces(
        &mut self,
        file_infos: &FileInfos,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
    ) -> crate::Result<Vec<ValidPieceIndex>> {
        let inflight = &self.inflight;
        self.chunks
            .drop_pieces(file_infos, pieces, |piece| inflight.contains_key(&piece))
    }

    /// The caller is done releasing the storage of these pieces, so they may be
    /// downloaded again. Returns how many of them are queued, i.e. whether anything is
    /// waiting on them.
    pub fn finish_release(&mut self, pieces: impl IntoIterator<Item = ValidPieceIndex>) -> usize {
        self.chunks.finish_release(pieces)
    }

    /// True if the piece was dropped and the caller hasn't finished releasing its storage.
    #[allow(dead_code)]
    pub fn is_releasing(&self, piece: ValidPieceIndex) -> bool {
        self.chunks.is_releasing(piece)
    }

    /// Make previously dropped pieces wanted again. A piece a peer already owns is left
    /// alone: it is being downloaded already.
    pub fn reselect_pieces(
        &mut self,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
    ) -> crate::Result<Reselected> {
        let inflight = &self.inflight;
        self.chunks
            .reselect_pieces(pieces, |piece| inflight.contains_key(&piece))
    }

    /// Update which files are selected for download.
    pub fn update_only_files(
        &mut self,
        file_infos: &FileInfos,
        new_only_files: &HashSet<usize>,
    ) -> anyhow::Result<crate::chunk_tracker::HaveNeededSelected> {
        self.chunks.update_only_files(file_infos, new_only_files)
    }

    /// Hold pieces back from what we announce, or stop holding them back: see
    /// [`ChunkTracker::set_pieces_advertised`]. Nothing to do with in-flight pieces -
    /// what we announce and what we are downloading are separate questions.
    pub fn set_pieces_advertised(
        &mut self,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
        advertised: bool,
    ) -> usize {
        self.chunks.set_pieces_advertised(pieces, advertised)
    }

    /// Flush the have pieces bitfield to disk.
    pub fn flush_have_pieces(&mut self, flush_async: bool) -> anyhow::Result<()> {
        self.chunks.get_have_pieces_mut().flush(flush_async)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bitv::BitV as BitVTrait, type_aliases::BF};
    use librqbit_core::{constants::CHUNK_SIZE, lengths::Lengths};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn peer(id: u8) -> PeerHandle {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 6881)
    }

    /// Create a simple ChunkTracker for testing.
    /// Creates a torrent with the specified number of pieces, all selected.
    fn make_test_chunk_tracker(num_pieces: u32) -> ChunkTracker {
        // Create a simple single-file torrent
        let piece_length = 16384u32; // 16KB pieces
        let total_length = piece_length as u64 * num_pieces as u64;

        let lengths = Lengths::new(total_length, piece_length).unwrap();

        let bf_len = lengths.piece_bitfield_bytes();

        // No pieces downloaded yet (empty have)
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());

        // All pieces selected
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        for i in 0..num_pieces as usize {
            selected.set(i, true);
        }

        // Single file spanning all pieces
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];

        ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap()
    }

    fn make_test_file_infos(num_pieces: u32) -> FileInfos {
        let piece_length = 16384u64;
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: piece_length * num_pieces as u64,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }]
    }

    fn make_default_file_priorities(file_infos: &FileInfos) -> FilePriorities {
        (0..file_infos.len()).collect()
    }

    // The reclaim tests need more than one chunk per piece: a piece that a peer is
    // halfway through is the whole point of them.
    const RECLAIM_CHUNKS_PER_PIECE: u32 = 4;
    const RECLAIM_PIECE_LEN: u32 = CHUNK_SIZE * RECLAIM_CHUNKS_PER_PIECE;

    fn reclaim_file_infos(num_pieces: u32) -> FileInfos {
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: RECLAIM_PIECE_LEN as u64 * num_pieces as u64,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }]
    }

    fn make_reclaim_tracker(num_pieces: u32) -> PieceTracker {
        let file_infos = reclaim_file_infos(num_pieces);
        let lengths = Lengths::new(
            RECLAIM_PIECE_LEN as u64 * num_pieces as u64,
            RECLAIM_PIECE_LEN,
        )
        .unwrap();
        let bf_len = lengths.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        selected.get_mut(0..num_pieces as usize).unwrap().fill(true);
        let mut chunks =
            ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap();
        chunks.enable_piece_reclaim();
        let mut tracker = PieceTracker::new(chunks);
        // Have everything, so nothing is queued and acquisition has to come from the
        // piece we drop below.
        for id in 0..num_pieces {
            let p = piece(&tracker, id);
            // Same order as the real thing: reserve it, then mark it good.
            tracker.chunks.reserve_needed_piece(p);
            tracker.mark_piece_hash_ok(p, &file_infos);
        }
        tracker
    }

    fn piece(tracker: &PieceTracker, id: u32) -> ValidPieceIndex {
        tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(id)
            .unwrap()
    }

    fn acquire(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        priority: Option<ValidPieceIndex>,
    ) -> AcquireResult {
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: priority.into_iter(),
            file_priorities,
            file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        })
    }

    // reselect_pieces() is the only caller of mark_piece_broken_if_not_have() that cannot
    // take the piece out of the in-flight map first: the piece is legitimately being
    // downloaded, by the very peer whose work requeuing would throw away.
    #[test]
    fn test_reselect_does_not_wipe_an_inflight_piece() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        let dropped = tracker.drop_pieces(&file_infos, [p0]).unwrap();
        assert_eq!(tracker.finish_release(dropped), 0);

        // A reader seeks back into the reclaimed range: the priority path picks the piece
        // up on its own, without it ever going through the queue.
        let res = acquire(&mut tracker, &file_infos, &file_priorities, Some(p0));
        assert!(
            matches!(res, AcquireResult::Reserved(p) if p == p0),
            "{res:?}"
        );

        // The peer delivers all but the last chunk of it.
        let block = vec![0u8; CHUNK_SIZE as usize];
        let deliver = |t: &mut PieceTracker, chunk: u32| {
            t.mark_chunk_downloaded(&Piece::from_data(p0.get(), chunk * CHUNK_SIZE, &block))
        };
        for chunk in 0..RECLAIM_CHUNKS_PER_PIECE - 1 {
            assert!(matches!(
                deliver(&mut tracker, chunk),
                Some(ChunkMarkingResult::NotCompleted)
            ));
        }

        // Now the caller reselects the range the piece is in. It is already being
        // downloaded, which is exactly what reselecting wants.
        tracker.reselect_pieces([p0]).unwrap();

        // The last chunk completes the piece - unless the ones before it were thrown away.
        assert!(
            matches!(
                deliver(&mut tracker, RECLAIM_CHUNKS_PER_PIECE - 1),
                Some(ChunkMarkingResult::Completed)
            ),
            "the chunks the peer already delivered were thrown away"
        );
        assert!(tracker.is_inflight(p0));
        assert!(
            !tracker.chunks().is_piece_queued(p0),
            "the piece a peer owns was put back in the queue, so a second peer can take it too"
        );
    }

    // Between its last chunk arriving and its hash passing, a piece is neither queued,
    // in-flight nor have. A reselect_pieces() landing in that window finds a dropped piece
    // nobody owns and queues it; the hash then passes, and without the fix the piece is
    // have AND queued, and the next peer to ask is handed a piece we have - a redundant
    // download, and a completion counted twice.
    #[test]
    fn test_reselect_during_the_hash_check_does_not_leave_a_have_piece_queued() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        let dropped = tracker.drop_pieces(&file_infos, [p0]).unwrap();
        assert_eq!(tracker.finish_release(dropped), 0);

        // A reader's priority window pulls the dropped piece back in, and the peer
        // delivers all of it. It comes out of the in-flight map for the hash check.
        let res = acquire(&mut tracker, &file_infos, &file_priorities, Some(p0));
        assert!(
            matches!(res, AcquireResult::Reserved(p) if p == p0),
            "{res:?}"
        );
        let block = vec![0u8; CHUNK_SIZE as usize];
        for chunk in 0..RECLAIM_CHUNKS_PER_PIECE {
            tracker.mark_chunk_downloaded(&Piece::from_data(p0.get(), chunk * CHUNK_SIZE, &block));
        }
        assert!(tracker.take_inflight(p0).is_some());

        // The caller reselects the same range while the hash is being checked.
        tracker.reselect_pieces([p0]).unwrap();

        // The hash passes.
        tracker.mark_piece_hash_ok(p0, &file_infos);
        assert!(tracker.chunks().is_piece_have(p0));
        assert!(
            !tracker.chunks().is_piece_queued(p0),
            "a piece we have is still queued"
        );
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::NoneAvailable),
            "a second peer was handed a piece we have: {res:?}"
        );
    }

    // A piece handed to the caller so it can delete the storage behind it must not be
    // downloaded again until the caller says the deletion is done. Otherwise we set the
    // have-bit back and the deletion removes a piece we have and are advertising.
    #[test]
    fn test_a_dropped_piece_is_not_reacquired_until_released() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        assert_eq!(tracker.drop_pieces(&file_infos, [p0]).unwrap(), [p0]);
        assert!(tracker.is_releasing(p0));

        // The picker's priority path deliberately ignores "dropped" - a reader that seeks
        // backwards into a reclaimed range gets the piece back on its own. It must not do
        // that while the storage behind it is being deleted.
        let res = acquire(&mut tracker, &file_infos, &file_priorities, Some(p0));
        assert!(
            matches!(res, AcquireResult::NoneAvailable),
            "acquired a piece whose storage is being released: {res:?}"
        );

        // Neither may the queue path, even once the piece is wanted again.
        tracker.reselect_pieces([p0]).unwrap();
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::NoneAvailable),
            "acquired a piece whose storage is being released: {res:?}"
        );

        // The caller is done deleting: the piece is queued, so this is worth a wake-up,
        // and now it can be downloaded again.
        assert_eq!(tracker.finish_release([p0]), 1);
        assert!(!tracker.is_releasing(p0));
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::Reserved(p) if p == p0),
            "{res:?}"
        );
    }

    // Pausing takes the PieceTracker apart and keeps the ChunkTracker, and unpausing
    // builds a new PieceTracker around it. A claim that didn't survive that would let an
    // unpaused torrent download pieces the caller is still deleting - and a pause is an
    // ordinary user action, and also what stop_with_error() does.
    #[test]
    fn test_a_claim_survives_a_pause() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        assert_eq!(tracker.drop_pieces(&file_infos, [p0]).unwrap(), [p0]);

        // Pause, then unpause.
        let mut tracker = PieceTracker::new(tracker.into_chunks());
        assert!(tracker.is_releasing(p0), "the claim died with the pause");

        // Same as without the pause: the piece is wanted again but nothing may take it
        // until the caller says the deletion is done.
        tracker.reselect_pieces([p0]).unwrap();
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::NoneAvailable),
            "acquired a piece whose storage is being released: {res:?}"
        );

        assert_eq!(tracker.finish_release([p0]), 1);
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::Reserved(p) if p == p0),
            "{res:?}"
        );
    }

    #[test]
    fn test_new_piece_tracker() {
        let chunks = make_test_chunk_tracker(10);
        let tracker = PieceTracker::new(chunks);
        assert_eq!(tracker.inflight_count(), 0);
    }

    #[test]
    fn test_reserve_piece_from_queue() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true, // Peer has all pieces
            can_steal: |_| true,
        });

        // Should reserve piece 0 (first in queue)
        match result {
            AcquireResult::Reserved(piece) => {
                assert_eq!(piece.get(), 0);
                assert!(tracker.is_inflight(piece));
                assert_eq!(tracker.inflight_count(), 1);
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_reserve_filters_by_peer_has_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer only has piece 2 and later
        // Note: iter_queued_pieces iterates in order: first, last, middle
        // So for 0..5: 0, 4, 1, 2, 3
        // With filter >= 2, we get 4 first (skips 0, takes 4)
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() >= 2,
            can_steal: |_| true,
        });

        match result {
            AcquireResult::Reserved(piece) => {
                // Got piece 4 (first one peer has in iteration order)
                assert!(piece.get() >= 2, "Should have gotten a piece >= 2");
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_complete_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve a piece first
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        let piece = match result {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        // Complete the piece (take_inflight + hash check + mark_piece_hash_ok)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        assert!(!tracker.is_inflight(piece));
        // Simulate successful hash check
        tracker.mark_piece_hash_ok(piece, &file_infos);
        assert!(tracker.chunks().is_piece_have(piece));
    }

    #[test]
    fn test_fail_piece_requeues() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        let piece = match result {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        assert!(tracker.is_inflight(piece));

        // Fail the piece (take_inflight + hash check fails + mark_piece_hash_failed)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        // Simulate failed hash check
        tracker.mark_piece_hash_failed(piece);

        // Should no longer be in-flight
        assert!(!tracker.is_inflight(piece));
        // Should not be in have
        assert!(!tracker.chunks().is_piece_have(piece));
        // Should be back in queue - verify by trying to reserve it again
        let result2 = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p == piece, // Only has the failed piece
            can_steal: |_| true,
        });

        match result2 {
            AcquireResult::Reserved(p) => assert_eq!(p, piece),
            _ => panic!("Expected piece to be re-reservable after fail"),
        }
    }

    // Two connections to one address in turn, the first still winding down when the
    // second reserves: the first one's death hands back what it reserved and nothing else.
    #[test]
    fn test_release_pieces_owned_by_one_connection() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut acquire = |connection| match tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            other => panic!("expected Reserved, got {other:?}"),
        };
        let old = acquire(1);
        let new = acquire(2);

        assert_eq!(tracker.release_pieces_owned_by(peer(1), 1), 1);
        assert!(!tracker.is_inflight(old));
        assert!(
            tracker.is_inflight(new),
            "the new connection lost its piece"
        );

        // A steal hands the piece to the thief's connection, whose death hands it back.
        let stolen = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 3,
            peer_avg_time: Some(Duration::ZERO),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p == new,
            can_steal: |_| true,
        });
        assert!(
            matches!(stolen, AcquireResult::Stolen { piece, .. } if piece == new),
            "{stolen:?}"
        );
        assert_eq!(tracker.release_pieces_owned_by(peer(2), 3), 1);
        assert!(
            !tracker.is_inflight(new),
            "the thief's death kept the piece"
        );
    }

    #[test]
    fn test_release_pieces_owned_by_peer() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves first two pieces (order: 0, 4, 1, 2, 3)
        // So peer A gets pieces 0 and 4
        let piece_a1 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };
        let piece_a2 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        // Peer B reserves next piece
        let piece_b = match tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        assert_eq!(tracker.inflight_count(), 3);
        assert!(tracker.is_inflight(piece_a1));
        assert!(tracker.is_inflight(piece_a2));
        assert!(tracker.is_inflight(piece_b));

        // Peer A dies
        let released = tracker.release_pieces_owned_by(peer_a, 0);
        assert_eq!(released, 2);
        assert_eq!(tracker.inflight_count(), 1); // Only peer B's piece remains

        // Verify peer B's piece is still in-flight
        assert!(tracker.is_inflight(piece_b));
        // Verify peer A's pieces are no longer in-flight
        assert!(!tracker.is_inflight(piece_a1));
        assert!(!tracker.is_inflight(piece_a2));
    }

    #[test]
    fn test_into_chunks_requeues_inflight() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve pieces 0 and 1
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        assert_eq!(tracker.inflight_count(), 2);

        // Convert back to chunks (simulates pause)
        let chunks = tracker.into_chunks();

        // Create a new tracker and verify pieces are back in queue
        let mut new_tracker = PieceTracker::new(chunks);
        let result = new_tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        // Should get piece 0 again (was requeued)
        match result {
            AcquireResult::Reserved(p) => assert_eq!(p.get(), 0),
            _ => panic!("Expected to reserve piece 0 after into_chunks"),
        }
    }

    #[test]
    fn test_priority_pieces_checked_first() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Priority pieces: piece 3, then piece 2
        let piece3 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(3)
            .unwrap();
        let piece2 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(2)
            .unwrap();
        let priority = vec![piece3, piece2];

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: priority.into_iter(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        // Should get piece 3 (first priority piece)
        match result {
            AcquireResult::Reserved(p) => assert_eq!(p.get(), 3),
            _ => panic!("Expected Reserved(3), got {:?}", result),
        }
    }

    #[test]
    fn test_none_available_when_no_pieces() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer has no pieces
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| false, // Peer has nothing
            can_steal: |_| true,
        });

        match result {
            AcquireResult::NoneAvailable => {}
            _ => panic!("Expected NoneAvailable, got {:?}", result),
        }
    }

    #[test]
    fn test_take_inflight_nonexistent_piece_returns_none() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let piece = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(0)
            .unwrap();

        // Try to take a piece that's not in-flight
        let result = tracker.take_inflight(piece);
        assert!(result.is_none());
    }

    /// A steal is not free: the peer robbed has already asked its seeder for the chunks
    /// of that piece, and everything that arrives for it after the cancel is dropped. So
    /// while there is anything left to reserve, reserve it -- however slow the incumbent
    /// looks. This is what a raised peer limit runs into: eight peers re-dial at once and
    /// would otherwise rob the two that carried the torrent while the cap was low, each
    /// of which then spends its whole link on bytes that go nowhere.
    #[test]
    fn a_free_piece_is_reserved_rather_than_stolen_from_a_slow_peer() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // The incumbent is sitting on piece 0, and has been for an age by our standards.
        tracker.reserve_piece(piece(&tracker, 0), peer(1), 0);
        tracker
            .inflight
            .get_mut(&piece(&tracker, 0))
            .unwrap()
            .started = Instant::now() - Duration::from_secs(600);

        let acquire = |tracker: &mut PieceTracker| {
            tracker.acquire_piece(AcquireRequest {
                peer: peer(2),
                connection: 0,
                // Fast: the incumbent is a thousand times over the 10x bar.
                peer_avg_time: Some(Duration::from_millis(600)),
                priority_pieces: std::iter::empty(),
                file_priorities: &file_priorities,
                file_infos: &file_infos,
                peer_has_piece: |_| true,
                can_steal: |_| true,
            })
        };

        // Four pieces are still queued, so all four come back reserved, not stolen.
        for _ in 0..4 {
            match acquire(&mut tracker) {
                AcquireResult::Reserved(p) => assert_ne!(p.get(), 0),
                other => panic!("expected a free piece to be reserved, got {other:?}"),
            }
        }
        // Only now, with nothing left to reserve, is the slow peer's piece taken.
        match acquire(&mut tracker) {
            AcquireResult::Stolen { piece, from_peer } => {
                assert_eq!(piece.get(), 0);
                assert_eq!(from_peer, peer(1));
            }
            other => panic!("expected the last piece to be stolen, got {other:?}"),
        }
    }

    /// The exception: a stream waits on one particular piece and no other, so if every
    /// piece in its window is already spoken for, the peer that can deliver soonest takes
    /// the head of the window off whoever is dawdling over it -- even with free pieces
    /// elsewhere, which would not help the stream at all.
    #[test]
    fn a_stalled_stream_piece_is_stolen_even_with_free_pieces_left() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let stream_piece = piece(&tracker, 3);
        tracker.reserve_piece(stream_piece, peer(1), 0);
        tracker.inflight.get_mut(&stream_piece).unwrap().started =
            Instant::now() - Duration::from_secs(600);

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            peer_avg_time: Some(Duration::from_millis(600)),
            priority_pieces: std::iter::once(stream_piece),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        match result {
            AcquireResult::Stolen { piece, from_peer } => {
                assert_eq!(piece.get(), 3);
                assert_eq!(from_peer, peer(1));
            }
            other => panic!("expected the stream's piece to be stolen, got {other:?}"),
        }
    }

    #[test]
    fn test_steal_only_pieces_peer_has() {
        // This test verifies the fix for a bug where try_steal didn't check
        // if the stealing peer actually has the piece in their bitfield.
        // Without this check, a peer could "steal" a piece they can't download,
        // leaving it stuck in inflight forever.

        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves pieces 0 and 4 (first two in iteration order)
        let piece_0 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => {
                assert_eq!(p.get(), 0);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        let piece_4 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved(p) => {
                assert_eq!(p.get(), 4);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        // Sleep briefly so pieces become stealable
        std::thread::sleep(Duration::from_millis(5));

        // Peer B tries to acquire with:
        // - Very short avg_time (1ms) so 3x threshold = 3ms < 5ms elapsed
        // - peer_has_piece returns true ONLY for piece 4, NOT piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            connection: 0,
            peer_avg_time: Some(Duration::from_millis(1)),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() == 4, // Peer B only has piece 4
            can_steal: |_| true,
        });

        // Should steal piece 4 (which peer B has), NOT piece 0 (which peer B doesn't have)
        match result {
            AcquireResult::Stolen { piece, from_peer } => {
                assert_eq!(piece, piece_4, "Should steal piece 4 (the one peer B has)");
                assert_eq!(from_peer, peer_a);
                // Verify piece 0 is still owned by peer A (wasn't stolen)
                assert_eq!(tracker.get_inflight(piece_0).unwrap().peer, peer_a);
            }
            _ => panic!("Expected Stolen, got {:?}", result),
        }
    }
}
