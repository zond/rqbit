use std::collections::HashSet;

use anyhow::Context;
use buffers::ByteBuf;
use librqbit_core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};
use peer_binary_protocol::Piece;
use tracing::{debug, trace};

use crate::{
    Error,
    bitv::{BitV, BoxBitV},
    file_info::FileInfo,
    type_aliases::{BF, BS, FileInfos, FilePriorities},
};

// State that only a torrent using the piece-level want-set has, boxed so that a torrent
// that has never heard of dropping carries one pointer for all of it.
struct PieceReclaim {
    // The pieces we dropped after having had them: we no longer have them, and we no
    // longer want them back. Dropping is deselecting at piece granularity, so this is
    // subtracted from "selected" everywhere below.
    dropped: BF,

    // Pieces that drop_pieces() handed to the caller so it can release their storage, and
    // that the caller hasn't reported back on through finish_release() yet. Nothing may
    // download one of these in the meantime: we would set the have-bit back, and the
    // deletion the caller is in the middle of would then remove a piece we have.
    //
    // It lives here, and not next to the in-flight map in PieceTracker, so that it
    // survives a pause: pausing takes the PieceTracker apart and keeps the ChunkTracker,
    // and a claim that died there would let an unpaused torrent download pieces whose
    // storage the caller is still deleting.
    releasing: HashSet<ValidPieceIndex>,
}

pub struct ChunkTracker {
    // This forms the basis of a "queue" to pull from.
    // It's set to 1 if we need a piece, but the moment we start requesting a peer,
    // it's set to 0.
    //
    // Initially this is the opposite of "have", until we start making requests.
    // An in-flight request is not in in the queue, and not in "have".
    //
    // needed initial value = selected & !have
    queue_pieces: BF,

    // This has a bit set per each chunk (block) that we have written to the output file.
    // It doesn't mean it's valid yet. Used to track how much is left in each piece.
    chunk_status: BF,

    // These are the pieces that we actually have, fully checked and downloaded.
    have: BoxBitV,

    // The pieces that the user selected. This doesn't change unless update_only_files
    // was called.
    selected: BF,

    // The piece-level want-set. None unless the torrent opted into piece reclaim, and
    // while it is None every path in here does exactly what it did before.
    reclaim: Option<Box<PieceReclaim>>,

    // How many bytes do we have per each file.
    per_file_bytes: Vec<u64>,

    lengths: Lengths,

    // Quick to retrieve stats, that MUST be in sync with the BFs
    // above (have/selected).
    hns: HaveNeededSelected,
}

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct HaveNeededSelected {
    // How many bytes we have downloaded and verified.
    pub have_bytes: u64,
    // How many bytes do we need to download for selected to be
    // a subset of have.
    pub needed_bytes: u64,
    // How many bytes the user selected (by picking files).
    pub selected_bytes: u64,
}

impl HaveNeededSelected {
    pub const fn progress(&self) -> u64 {
        self.selected_bytes - self.needed_bytes
    }

    pub const fn total(&self) -> u64 {
        self.selected_bytes
    }

    pub const fn finished(&self) -> bool {
        self.needed_bytes == 0
    }
}

/// How much of a single piece has been downloaded, counted in chunks (blocks) of
/// `CHUNK_SIZE` (16 KiB).
///
/// Whole pieces are already visible through the have-bitfield, but a piece can be
/// many megabytes, so "have / don't have" is too coarse to show progress to a user
/// waiting on one specific piece. This is the finer-grained view of a single piece.
///
/// See [`crate::ManagedTorrent::piece_chunk_progress`] for how to obtain it, and for the
/// caveats that come with it.
#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct PieceChunkProgress {
    /// How many chunks of the piece have been written to storage.
    ///
    /// This is downloaded, NOT verified: see [`crate::ManagedTorrent::piece_chunk_progress`].
    pub downloaded_chunks: u32,
    /// How many chunks the piece has in total. The last piece of a torrent usually has
    /// fewer chunks than the rest.
    pub total_chunks: u32,
    /// True if the piece is fully downloaded AND has passed its hash check, i.e. it is in
    /// the have-bitfield.
    ///
    /// While this is false, `downloaded_chunks == total_chunks` only means the piece is
    /// complete enough to be hashed, not that it is good.
    pub verified: bool,
}

/// What [`ChunkTracker::reselect_pieces`] changed.
#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct Reselected {
    /// How many pieces stopped being dropped, i.e. are wanted again.
    pub reselected: usize,
    /// How many of those actually went back into the download queue. Fewer than
    /// `reselected` when the user has deselected the file a piece lives in: it is wanted
    /// again, but there is nothing a peer can do about it, so waking peers for it would
    /// be a wake-up with no work behind it.
    pub queued: usize,
}

// Compute the have-status of chunks.
//
// Save as "have_pieces", but there's one bit per chunk (not per piece).
fn compute_chunk_have_status(lengths: &Lengths, have_pieces: &BS) -> anyhow::Result<BF> {
    if have_pieces.len() < lengths.total_pieces() as usize {
        anyhow::bail!(
            "bug: have_pieces.len() < lengths.total_pieces(); {} < {}",
            have_pieces.len(),
            lengths.total_pieces()
        );
    }

    let required_size = lengths.chunk_bitfield_bytes();
    let vec = vec![0u8; required_size];
    let mut chunk_bf = BF::from_boxed_slice(vec.into_boxed_slice());

    for piece in lengths.iter_piece_infos() {
        let chunks = lengths.chunks_per_piece(piece.piece_index) as usize;
        let offset = (lengths.default_chunks_per_piece() * piece.piece_index.get()) as usize;
        let range = offset..(offset + chunks);
        if have_pieces[piece.piece_index.get() as usize] {
            chunk_bf
                .get_mut(range.clone())
                .with_context(|| {
                    format!("bug in bitvec: error getting range {range:?} from chunk_bf")
                })?
                .fill(true);
        }
    }
    Ok(chunk_bf)
}

fn compute_queued_pieces_unchecked(have_pieces: &BS, selected_pieces: &BS) -> BF {
    // it's needed ONLY if it's selected and we don't have it.
    use core::ops::BitAnd;
    use core::ops::Not;

    have_pieces
        .to_bitvec()
        .not()
        .bitand(selected_pieces)
        .into_boxed_bitslice()
}

fn compute_queued_pieces(have_pieces: &BS, selected_pieces: &BS) -> anyhow::Result<BF> {
    if have_pieces.len() != selected_pieces.len() {
        anyhow::bail!(
            "have_pieces.len() != selected_pieces.len(), {} != {}",
            have_pieces.len(),
            selected_pieces.len()
        );
    }

    Ok(compute_queued_pieces_unchecked(
        have_pieces,
        selected_pieces,
    ))
}

pub(crate) fn compute_selected_pieces(
    lengths: &Lengths,
    only_files_is_empty_or_contains: impl Fn(usize) -> bool,
    file_infos: &FileInfos,
) -> BF {
    let mut bf = BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into_boxed_slice());
    for (_, fi) in file_infos
        .iter()
        .enumerate()
        .filter(|(_, fi)| !fi.attrs.padding)
        .filter(|(id, _)| only_files_is_empty_or_contains(*id))
    {
        if let Some(r) = bf.get_mut(fi.piece_range_usize()) {
            r.fill(true);
        }
    }
    bf
}

#[derive(Debug)]
pub enum ChunkMarkingResult {
    PreviouslyCompleted,
    NotCompleted,
    Completed,
}

impl ChunkTracker {
    pub fn new(
        // Have pieces are the ones we have already downloaded and verified.
        have_pieces: BoxBitV,
        // Selected pieces are the ones the user has selected
        selected_pieces: BF,
        lengths: Lengths,
        file_infos: &FileInfos,
    ) -> anyhow::Result<Self> {
        let needed_pieces = compute_queued_pieces(have_pieces.as_slice(), &selected_pieces)
            .context("error computing needed pieces")?;

        // TODO: ideally this needs to be a list based on needed files, e.g.
        // last needed piece for each file. But let's keep simple for now.

        let mut ct = Self {
            chunk_status: compute_chunk_have_status(&lengths, have_pieces.as_slice())
                .context("error computing chunk status")?,
            queue_pieces: needed_pieces,
            selected: selected_pieces,
            lengths,
            have: have_pieces,
            hns: HaveNeededSelected::default(),
            per_file_bytes: vec![0; file_infos.len()],
            reclaim: None,
        };
        ct.recalculate_per_file_bytes(file_infos);
        ct.hns = ct.calc_hns();
        Ok(ct)
    }

    fn recalculate_per_file_bytes(&mut self, file_infos: &FileInfos) {
        for (slot, fi) in self.per_file_bytes.iter_mut().zip(file_infos.iter()) {
            *slot = fi
                .piece_range
                .clone()
                .filter(|p| self.have.as_slice()[*p as usize])
                .map(|id| {
                    self.lengths
                        .size_of_piece_in_file(id, fi.offset_in_torrent, fi.len)
                })
                .sum();
        }
    }

    // The user's selection, minus what we dropped. This is what the stats are computed
    // from; with reclaim disabled it is just self.selected.
    fn is_selected(&self, id: usize) -> bool {
        self.selected[id] && !self.reclaim.as_ref().is_some_and(|r| r.dropped[id])
    }

    /// Opt this torrent into the piece-level want-set, which makes [`Self::drop_pieces`]
    /// and [`Self::reselect_pieces`] work. Until this is called nothing in here behaves
    /// differently from a tracker that has never heard of dropping.
    pub fn enable_piece_reclaim(&mut self) {
        if self.reclaim.is_none() {
            self.reclaim = Some(Box::new(PieceReclaim {
                dropped: BF::from_boxed_slice(
                    vec![0u8; self.lengths.piece_bitfield_bytes()].into_boxed_slice(),
                ),
                releasing: HashSet::new(),
            }));
        }
    }

    pub(crate) fn is_piece_dropped(&self, index: ValidPieceIndex) -> bool {
        self.reclaim
            .as_ref()
            .is_some_and(|r| r.dropped[index.get() as usize])
    }

    /// Drop the pieces we have out of the given ones: forget that we have them and stop
    /// wanting them back. Returns the pieces that were actually dropped, in the order
    /// given, so the caller can release the storage behind them.
    ///
    /// This is bookkeeping only - it does not touch storage. Releasing a dropped piece is
    /// the caller's job, and takes a storage that can let one piece go; see
    /// [`crate::ManagedTorrent::drop_pieces`].
    ///
    /// Pieces we don't have are skipped. It is the caller's job not to pass pieces that a
    /// live stream still needs.
    pub fn drop_pieces(
        &mut self,
        file_infos: &FileInfos,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
    ) -> crate::Result<Vec<ValidPieceIndex>> {
        if self.reclaim.is_none() {
            return Err(Error::PieceReclaimDisabled);
        }
        let dropped: Vec<ValidPieceIndex> = pieces
            .into_iter()
            .filter(|id| self.drop_piece(file_infos, *id))
            .collect();
        if let Some(r) = self.reclaim.as_mut() {
            r.releasing.extend(dropped.iter().copied());
        }
        Ok(dropped)
    }

    /// The caller is done releasing the storage of these pieces, so they may be
    /// downloaded again. Returns how many of them are queued, i.e. whether anything is
    /// waiting on them.
    pub fn finish_release(&mut self, pieces: impl IntoIterator<Item = ValidPieceIndex>) -> usize {
        let mut queued = 0;
        for piece in pieces {
            let was_releasing = self
                .reclaim
                .as_mut()
                .is_some_and(|r| r.releasing.remove(&piece));
            if was_releasing && self.is_piece_queued(piece) {
                queued += 1;
            }
        }
        queued
    }

    /// True if the piece was dropped and the caller hasn't finished releasing its storage.
    pub fn is_releasing(&self, piece: ValidPieceIndex) -> bool {
        self.reclaim
            .as_ref()
            .is_some_and(|r| r.releasing.contains(&piece))
    }

    /// Make previously dropped pieces wanted again, e.g. after seeking backwards into a
    /// range we reclaimed. Pieces that weren't dropped are left alone. Returns what
    /// changed: see [`Reselected`].
    ///
    /// `is_inflight` says whether a peer already owns the piece; the tracker has no view
    /// of that. Such a piece is already being downloaded and is left as it is.
    pub fn reselect_pieces(
        &mut self,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
        is_inflight: impl Fn(ValidPieceIndex) -> bool,
    ) -> crate::Result<Reselected> {
        if self.reclaim.is_none() {
            return Err(Error::PieceReclaimDisabled);
        }
        let mut res = Reselected::default();
        for id in pieces {
            if !self.undrop_piece(id) {
                continue;
            }
            res.reselected += 1;
            // Only queue it if the user still wants the file it lives in. Queuing
            // unconditionally would break "queued is a subset of selected or have" and
            // download a file the user deselected - the same invariant update_only_files
            // goes out of its way to maintain.
            // A piece a peer already owns is being downloaded, which is what reselecting
            // wants. mark_piece_broken_if_not_have() would throw away the chunks that
            // peer has already delivered and queue the piece for a second peer as well:
            // every other caller of it takes the piece out of the in-flight map first,
            // and this is the one that must not.
            if self.selected[id.get() as usize] && !is_inflight(id) {
                // Puts it back in the queue and resets its chunks.
                self.mark_piece_broken_if_not_have(id);
                res.queued += 1;
            }
        }
        Ok(res)
    }

    // Returns true if the piece was dropped, i.e. if we had it.
    fn drop_piece(&mut self, file_infos: &FileInfos, index: ValidPieceIndex) -> bool {
        let id = index.get() as usize;
        if !self.have.as_slice()[id] {
            return false;
        }
        match self.reclaim.as_mut() {
            Some(r) => r.dropped.set(id, true),
            None => return false,
        }
        self.have.as_slice_mut().set(id, false);
        if let Some(s) = self.chunk_status.get_mut(self.lengths.chunk_range(index)) {
            s.fill(false);
        }
        // Not have AND not wanted. Clearing only the have-bit would put the piece straight
        // back into the queue, and we would delete it and download it again.
        self.queue_pieces.set(id, false);

        let len = self.lengths.piece_length(index) as u64;
        self.hns.have_bytes -= len;
        if self.selected[id] {
            // needed_bytes doesn't move: the piece wasn't needed (we had it), and it still
            // isn't (we don't want it). That keeps the torrent "finished" and progress at
            // 100% instead of re-opening a torrent the user already finished.
            self.hns.selected_bytes -= len;
        }

        // Without this a file that had completed is never looked at by
        // iter_queued_pieces() again, and re-selecting inside it can't be serviced.
        self.add_to_per_file_bytes(file_infos, index, |slot, in_file| slot - in_file);

        debug!("dropped piece={index}");
        true
    }

    // Returns true if the piece was dropped and is now wanted again.
    fn undrop_piece(&mut self, index: ValidPieceIndex) -> bool {
        let id = index.get() as usize;
        match self.reclaim.as_mut() {
            Some(r) => {
                if !r.dropped.replace(id, false) {
                    return false;
                }
            }
            None => return false,
        }
        if self.selected[id] {
            let len = self.lengths.piece_length(index) as u64;
            self.hns.selected_bytes += len;
            if !self.have.as_slice()[id] {
                self.hns.needed_bytes += len;
            }
        }
        true
    }

    pub fn get_lengths(&self) -> &Lengths {
        &self.lengths
    }

    pub fn get_have_pieces(&self) -> &dyn BitV {
        &*self.have
    }

    pub fn get_have_pieces_mut(&mut self) -> &mut dyn BitV {
        &mut *self.have
    }

    pub fn reserve_needed_piece(&mut self, index: ValidPieceIndex) {
        self.queue_pieces.set(index.get() as usize, false)
    }

    pub fn get_hns(&self) -> &HaveNeededSelected {
        &self.hns
    }

    fn calc_hns(&self) -> HaveNeededSelected {
        let mut hns = HaveNeededSelected::default();
        for piece in self.lengths.iter_piece_infos() {
            let id = piece.piece_index.get() as usize;
            let len = piece.len as u64;
            let is_have = self.have.as_slice()[id];
            let is_selected = self.is_selected(id);
            let is_needed = is_selected && !is_have;
            hns.have_bytes += len * (is_have as u64);
            hns.selected_bytes += len * (is_selected as u64);
            hns.needed_bytes += len * (is_needed as u64);
        }
        hns
    }

    pub(crate) fn iter_queued_pieces<'a>(
        &'a self,
        file_priorities: &'a FilePriorities,
        file_infos: &'a FileInfos,
    ) -> impl Iterator<Item = ValidPieceIndex> + 'a {
        file_priorities
            .iter()
            .filter_map(|p| Some((*p, file_infos.get(*p)?)))
            .filter(|(id, f)| self.per_file_bytes[*id] != f.len)
            .flat_map(|(_id, f)| f.iter_piece_priorities())
            .filter(|id| self.queue_pieces[*id])
            .filter_map(|id| id.try_into().ok())
            .filter_map(|id| self.lengths.validate_piece_index(id))
    }

    pub(crate) fn is_piece_have(&self, id: ValidPieceIndex) -> bool {
        self.have.as_slice()[id.get() as usize]
    }

    pub(crate) fn is_piece_queued(&self, id: ValidPieceIndex) -> bool {
        self.queue_pieces[id.get() as usize]
    }

    pub fn mark_piece_broken_if_not_have(&mut self, index: ValidPieceIndex) {
        if self
            .have
            .as_slice()
            .get(index.get() as usize)
            .map(|r| *r)
            .unwrap_or_default()
        {
            return;
        }
        debug!("marking piece={} as broken", index);
        // A dropped piece is not wanted. This is reached on hash failure, on every peer
        // disconnect and on every pause, so requeuing here would be the "delete it and
        // download it again" loop that the want-set exists to prevent. Its chunks are
        // still reset below: a live stream's priority window can pull the piece back in
        // without going through the queue, and if it does it must start from scratch.
        if !self.is_piece_dropped(index) {
            self.queue_pieces.set(index.get() as usize, true);
        }
        if let Some(s) = self.chunk_status.get_mut(self.lengths.chunk_range(index)) {
            s.fill(false);
        }
    }

    /// The piece passed its hash check: it is ours. Sets the have-bit and moves every
    /// count that is derived from it - the totals and the per-file bytes - in the same
    /// call, so that nothing holding the lock between two calls can see a piece that is
    /// have but not counted, or counted but not have.
    pub fn mark_piece_downloaded(&mut self, idx: ValidPieceIndex, file_infos: &FileInfos) {
        // A piece we have is by definition not dropped. A live stream's priority window
        // can download a dropped piece without it ever going through the queue.
        self.undrop_piece(idx);
        let id = idx.get() as usize;
        // While its hash was being checked the piece was neither in-flight nor have, and
        // anything that queues "selected and not have" - update_only_files(), or
        // reselect_pieces() - queued it. A piece we have is not one a peer should be
        // handed, so whatever queued it in that window is undone here.
        self.queue_pieces.set(id, false);
        if !self.have.as_slice()[id] {
            self.have.as_slice_mut().set(id, true);
            let len = self.lengths.piece_length(idx) as u64;
            self.hns.have_bytes += len;
            if self.selected[id] {
                self.hns.needed_bytes -= len;
            }
            self.add_to_per_file_bytes(file_infos, idx, |slot, in_file| slot + in_file);
        }
    }

    // Apply `f(current, bytes of the piece in this file)` to the per-file count of every
    // file the piece overlaps. A filter over all files and not a scan that stops at the
    // first non-overlapping one: a zero-length file sitting inside a piece has an empty
    // piece range, and would stop such a scan short of the files after it.
    fn add_to_per_file_bytes(
        &mut self,
        file_infos: &FileInfos,
        index: ValidPieceIndex,
        f: impl Fn(u64, u64) -> u64,
    ) {
        for (file_id, fi) in file_infos
            .iter()
            .enumerate()
            .filter(|(_, fi)| fi.piece_range.contains(&index.get()))
        {
            let in_file =
                self.lengths
                    .size_of_piece_in_file(index.get(), fi.offset_in_torrent, fi.len);
            let slot = &mut self.per_file_bytes[file_id];
            *slot = f(*slot, in_file);
        }
    }

    pub fn is_chunk_ready_to_upload(&self, chunk: &ChunkInfo) -> bool {
        self.have
            .as_slice()
            .get(chunk.piece_index.get() as usize)
            .map(|b| *b)
            .unwrap_or(false)
    }

    pub fn get_remaining_bytes(&self) -> u64 {
        self.hns.needed_bytes
    }

    /// How much of the given piece has been downloaded, in chunks.
    ///
    /// Returns None if the piece index is out of range for this torrent.
    ///
    /// NOTE: this is "downloaded", not "verified". A chunk is counted as soon as it has
    /// been written to storage; a piece's hash is only checked once all of its chunks are
    /// in. If that check fails the piece is thrown away and its count drops back to zero,
    /// so this value CAN GO BACKWARDS, and a progress bar driven by it must be prepared
    /// for that. [`PieceChunkProgress::verified`] tells a piece that is merely fully
    /// downloaded from one that is known good.
    ///
    /// Cheap: this counts bits in an existing bitfield and allocates nothing.
    pub fn piece_chunk_progress(&self, piece_index: u32) -> Option<PieceChunkProgress> {
        let index = self.lengths.validate_piece_index(piece_index)?;
        let chunks = self.chunk_status.get(self.lengths.chunk_range(index))?;
        // count_ones() is bounded by the number of chunks in a piece, which is a u32.
        let downloaded_chunks = chunks.count_ones().try_into().ok()?;
        Some(PieceChunkProgress {
            downloaded_chunks,
            total_chunks: self.lengths.chunks_per_piece(index),
            verified: self.is_piece_have(index),
        })
    }

    // return true if the whole piece is marked downloaded
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        let chunk_info = self.lengths.chunk_info_from_received_data(
            self.lengths.validate_piece_index(piece.index)?,
            piece.begin,
            piece.len().try_into().unwrap(),
        )?;
        let chunk_range = self.lengths.chunk_range(chunk_info.piece_index);
        let chunk_range = self.chunk_status.get_mut(chunk_range).unwrap();
        if chunk_range.all() {
            return Some(ChunkMarkingResult::PreviouslyCompleted);
        }
        chunk_range.set(chunk_info.chunk_index as usize, true);
        trace!(
            "piece={}, chunk_info={:?}, bits={:?}",
            piece.index, chunk_info, chunk_range,
        );

        if chunk_range.all() {
            return Some(ChunkMarkingResult::Completed);
        }
        Some(ChunkMarkingResult::NotCompleted)
    }

    pub fn update_only_files(
        &mut self,
        file_infos: &FileInfos,
        new_only_files: &HashSet<usize>,
    ) -> anyhow::Result<HaveNeededSelected> {
        let selected = compute_selected_pieces(
            &self.lengths,
            |idx| new_only_files.contains(&idx),
            file_infos,
        );
        let prev_selected = std::mem::replace(&mut self.selected, selected);

        // prev_selected=false and selected=true and have=false: requeue the piece
        {
            let mut b = BF::from_boxed_slice(
                vec![0u8; self.lengths.piece_bitfield_bytes()].into_boxed_slice(),
            );
            for idx in self
                .selected
                .iter_ones()
                .filter(|idx| !prev_selected[*idx] && !self.have.as_slice()[*idx])
            {
                b.set(idx, true);
            }

            for idx in b.iter_ones() {
                #[allow(clippy::cast_possible_truncation)]
                if let Some(idx) = self.lengths.validate_piece_index(idx as u32) {
                    // The user just asked back for a file we had dropped pieces of. That
                    // outranks the drop, and it has to happen before the requeue below,
                    // which refuses to queue a dropped piece.
                    self.undrop_piece(idx);
                    self.mark_piece_broken_if_not_have(idx);
                }
            }
        }

        // selected=false, have=false: don't need the piece, and don't have it - cancel downloading it
        {
            // TODO: is there a better way to write this?
            // self.queue_pieces &= self.have | self.selected;
            let mut have_or_selected: BF = self.selected.clone();
            have_or_selected |= self.have.as_slice();
            self.queue_pieces &= have_or_selected;
        }

        self.hns = self.calc_hns();
        Ok(self.hns)
    }

    pub(crate) fn get_selected_pieces(&self) -> &BF {
        &self.selected
    }

    pub fn is_file_finished(&self, file_info: &FileInfo) -> bool {
        self.have
            .as_slice()
            .get(file_info.piece_range_usize())
            .map(|r| r.all())
            .unwrap_or(true)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.get_hns().finished()
    }

    pub fn per_file_have_bytes(&self) -> &[u64] {
        &self.per_file_bytes
    }
}

#[cfg(test)]
mod tests {
    use librqbit_core::{constants::CHUNK_SIZE, lengths::Lengths};
    use std::collections::HashSet;

    use peer_binary_protocol::Piece;

    use crate::{
        bitv::BitV, chunk_tracker::HaveNeededSelected, file_info::FileInfo, type_aliases::BF,
    };

    use super::{ChunkTracker, PieceChunkProgress, compute_chunk_have_status};

    #[test]
    fn test_compute_chunk_status() {
        // Create the most obnoxious lengths, and ensure it doesn't break in that case.
        let piece_length = CHUNK_SIZE * 2 + 1;
        let l = Lengths::new(piece_length as u64 * 2 + 1, piece_length).unwrap();

        assert_eq!(l.total_pieces(), 3);
        assert_eq!(l.default_chunks_per_piece(), 3);
        assert_eq!(l.total_chunks(), 7);

        {
            let mut have_pieces =
                BF::from_boxed_slice(vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice());
            have_pieces.set(0, false);

            let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
            assert!(!chunks[0]);
            assert!(!chunks[1]);
            assert!(!chunks[2]);
            assert!(chunks[3]);
            assert!(chunks[4]);
            assert!(chunks[5]);
            assert!(chunks[6]);
        }

        {
            let mut have_pieces =
                BF::from_boxed_slice(vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice());
            have_pieces.set(1, false);

            let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
            dbg!(&chunks);
            assert!(chunks[0]);
            assert!(chunks[1]);
            assert!(chunks[2]);
            assert!(!chunks[3]);
            assert!(!chunks[4]);
            assert!(!chunks[5]);
            assert!(chunks[6]);
        }

        {
            let mut have_pieces =
                BF::from_boxed_slice(vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice());
            have_pieces.set(2, false);

            let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
            dbg!(&chunks);
            assert!(chunks[0]);
            assert!(chunks[1]);
            assert!(chunks[2]);
            assert!(chunks[3]);
            assert!(chunks[4]);
            assert!(chunks[5]);
            assert!(!chunks[6]);
        }

        {
            // A more reasonable case.
            let piece_length = CHUNK_SIZE * 2;
            let l = Lengths::new(piece_length as u64 * 2 + 1, piece_length).unwrap();

            assert_eq!(l.total_pieces(), 3);
            assert_eq!(l.default_chunks_per_piece(), 2);
            assert_eq!(l.total_chunks(), 5);

            {
                let mut have_pieces = BF::from_boxed_slice(
                    vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice(),
                );
                have_pieces.set(1, false);

                let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
                dbg!(&chunks);
                assert!(chunks[0]);
                assert!(chunks[1]);
                assert!(!chunks[2]);
                assert!(!chunks[3]);
                assert!(chunks[4]);
            }

            {
                let mut have_pieces = BF::from_boxed_slice(
                    vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice(),
                );
                have_pieces.set(2, false);

                let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
                dbg!(&chunks);
                assert!(chunks[0]);
                assert!(chunks[1]);
                assert!(chunks[2]);
                assert!(chunks[3]);
                assert!(!chunks[4]);
            }
        }
    }

    // Four files over 3 pieces of 2 chunks + 1 byte: file 0 is piece 0 exactly; piece 1
    // holds the 1-byte file 1, the zero-length file 2 and the start of file 3, which runs
    // to the end of the torrent.
    fn four_files(piece_len: u32) -> Vec<FileInfo> {
        vec![
            FileInfo {
                relative_filename: "0".into(),
                offset_in_torrent: 0,
                piece_range: 0..1,
                len: piece_len as u64,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "1".into(),
                offset_in_torrent: piece_len as u64,
                piece_range: 1..2,
                len: 1,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "2".into(),
                offset_in_torrent: piece_len as u64 + 1,
                piece_range: 1..1,
                len: 0,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "3".into(),
                offset_in_torrent: piece_len as u64 + 1,
                piece_range: 1..3,
                len: piece_len as u64,
                attrs: Default::default(),
            },
        ]
    }

    #[test]
    fn test_update_only_files() {
        let piece_len = CHUNK_SIZE * 2 + 1;
        let total_len = piece_len as u64 * 2 + 1;
        let l = Lengths::new(total_len, piece_len).unwrap();
        assert_eq!(l.total_pieces(), 3);
        assert_eq!(l.total_chunks(), 7);

        let all_files = four_files(piece_len);

        let bf_len = l.piece_bitfield_bytes();
        let initial_have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let initial_selected = {
            let mut bf = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
            bf.get_mut(0..3).unwrap().fill(true);
            bf
        };

        // Initially, we need all files and all pieces.
        let mut ct = ChunkTracker::new(
            initial_have.clone().into_dyn(),
            initial_selected.clone(),
            l,
            &Default::default(),
        )
        .unwrap();

        // Select all file, no changes.
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([0, 1, 2, 3]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: total_len,
                needed_bytes: total_len,
            }
        );
        assert_eq!(ct.have.as_slice(), initial_have.as_bitslice());
        assert_eq!(ct.queue_pieces, initial_selected);

        // Select only the first file.
        println!("Select only the first file.");
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([0]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: all_files[0].len,
                needed_bytes: all_files[0].len,
            }
        );
        assert!(ct.queue_pieces[0]);
        assert!(!ct.queue_pieces[1]);
        assert!(!ct.queue_pieces[2]);

        // Select only the second file.
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([1]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: piece_len as u64,
                needed_bytes: piece_len as u64,
            }
        );
        assert!(!ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(!ct.queue_pieces[2]);

        // Select only the third file (zero sized one!).
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([2]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: 0,
                needed_bytes: 0,
            }
        );
        assert!(!ct.queue_pieces[0]);
        assert!(!ct.queue_pieces[1]);
        assert!(!ct.queue_pieces[2]);

        // Select only the fourth file.
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([3]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: (piece_len + 1) as u64,
                needed_bytes: (piece_len + 1) as u64,
            }
        );
        assert!(!ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(ct.queue_pieces[2]);

        // Select first and last file
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([0, 3]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: all_files[0].len + all_files[3].len + 1,
                needed_bytes: all_files[0].len + all_files[3].len + 1,
            }
        );
        assert!(ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(ct.queue_pieces[2]);

        // Select all files
        assert_eq!(
            ct.update_only_files(&all_files, &HashSet::from_iter([0, 1, 2, 3]))
                .unwrap(),
            HaveNeededSelected {
                have_bytes: 0,
                selected_bytes: total_len,
                needed_bytes: total_len
            }
        );
        assert!(ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(ct.queue_pieces[2]);
    }

    // The per-file count moves with the have-bit, in the same call. It used to be added
    // in a second step, under a second lock, and a drop_pieces() landing in between saw a
    // piece that was have but not counted; it also stopped at the zero-length file 2 and
    // never counted piece 1 into file 3.
    #[test]
    fn test_per_file_bytes_follow_the_have_bit() {
        let piece_len = CHUNK_SIZE * 2 + 1;
        let total_len = piece_len as u64 * 2 + 1;
        let l = Lengths::new(total_len, piece_len).unwrap();
        let files = four_files(piece_len);

        let bf_len = l.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        selected.get_mut(0..3).unwrap().fill(true);
        let mut ct = ChunkTracker::new(have.into_dyn(), selected, l, &files).unwrap();
        assert_eq!(ct.per_file_have_bytes(), [0, 0, 0, 0]);

        // Piece 1 is shared by three files, one of them empty.
        ct.mark_piece_downloaded(l.validate_piece_index(1).unwrap(), &files);
        assert_eq!(
            ct.per_file_have_bytes(),
            [0, 1, 0, piece_len as u64 - 1],
            "the bytes of piece 1 in every file it overlaps"
        );

        // Marking a piece we already have counts nothing twice.
        ct.mark_piece_downloaded(l.validate_piece_index(1).unwrap(), &files);
        assert_eq!(ct.per_file_have_bytes(), [0, 1, 0, piece_len as u64 - 1]);

        ct.mark_piece_downloaded(l.validate_piece_index(0).unwrap(), &files);
        ct.mark_piece_downloaded(l.validate_piece_index(2).unwrap(), &files);
        assert_eq!(
            ct.per_file_have_bytes(),
            [piece_len as u64, 1, 0, piece_len as u64],
            "every file is complete"
        );
        for fi in &files {
            assert!(ct.is_file_finished(fi));
        }
    }

    // A 3-piece torrent where every piece but the last is 2 full chunks + 1 byte, i.e.
    // 3 chunks. The last piece is 1 byte, i.e. a single (short) chunk.
    fn tracker_for_chunk_progress_tests() -> (Lengths, ChunkTracker) {
        let piece_len = CHUNK_SIZE * 2 + 1;
        let l = Lengths::new(piece_len as u64 * 2 + 1, piece_len).unwrap();
        assert_eq!(l.total_pieces(), 3);
        assert_eq!(l.total_chunks(), 7);

        let bf_len = l.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let selected = {
            let mut bf = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
            bf.get_mut(0..3).unwrap().fill(true);
            bf
        };
        let ct = ChunkTracker::new(have.into_dyn(), selected, l, &Default::default()).unwrap();
        (l, ct)
    }

    fn recv_chunk(ct: &mut ChunkTracker, piece: u32, chunk: u32, len: usize) {
        let block = vec![0u8; len];
        ct.mark_chunk_downloaded(&Piece::from_data(piece, chunk * CHUNK_SIZE, &block))
            .unwrap();
    }

    #[test]
    fn test_piece_chunk_progress_empty_piece() {
        let (_l, ct) = tracker_for_chunk_progress_tests();
        assert_eq!(
            ct.piece_chunk_progress(0),
            Some(PieceChunkProgress {
                downloaded_chunks: 0,
                total_chunks: 3,
                verified: false,
            })
        );
        // The last piece is short: one chunk, not three.
        assert_eq!(
            ct.piece_chunk_progress(2),
            Some(PieceChunkProgress {
                downloaded_chunks: 0,
                total_chunks: 1,
                verified: false,
            })
        );
    }

    #[test]
    fn test_piece_chunk_progress_partial_piece() {
        let (_l, mut ct) = tracker_for_chunk_progress_tests();

        recv_chunk(&mut ct, 1, 0, CHUNK_SIZE as usize);
        assert_eq!(
            ct.piece_chunk_progress(1),
            Some(PieceChunkProgress {
                downloaded_chunks: 1,
                total_chunks: 3,
                verified: false,
            })
        );

        recv_chunk(&mut ct, 1, 1, CHUNK_SIZE as usize);
        assert_eq!(
            ct.piece_chunk_progress(1),
            Some(PieceChunkProgress {
                downloaded_chunks: 2,
                total_chunks: 3,
                verified: false,
            })
        );

        // Neighbouring pieces are unaffected.
        assert_eq!(ct.piece_chunk_progress(0).unwrap().downloaded_chunks, 0);
        assert_eq!(ct.piece_chunk_progress(2).unwrap().downloaded_chunks, 0);
    }

    #[test]
    fn test_piece_chunk_progress_full_piece() {
        let (l, mut ct) = tracker_for_chunk_progress_tests();

        recv_chunk(&mut ct, 1, 0, CHUNK_SIZE as usize);
        recv_chunk(&mut ct, 1, 1, CHUNK_SIZE as usize);
        recv_chunk(&mut ct, 1, 2, 1);

        // All chunks are in, but the hash has not been checked yet.
        assert_eq!(
            ct.piece_chunk_progress(1),
            Some(PieceChunkProgress {
                downloaded_chunks: 3,
                total_chunks: 3,
                verified: false,
            })
        );

        // Now it passes verification.
        ct.mark_piece_downloaded(l.validate_piece_index(1).unwrap(), &Default::default());
        assert_eq!(
            ct.piece_chunk_progress(1),
            Some(PieceChunkProgress {
                downloaded_chunks: 3,
                total_chunks: 3,
                verified: true,
            })
        );
    }

    #[test]
    fn test_piece_chunk_progress_out_of_range() {
        let (_l, ct) = tracker_for_chunk_progress_tests();
        assert_eq!(ct.piece_chunk_progress(3), None);
        assert_eq!(ct.piece_chunk_progress(u32::MAX), None);
    }

    #[test]
    fn test_piece_chunk_progress_regresses_on_failed_verification() {
        let (l, mut ct) = tracker_for_chunk_progress_tests();
        let idx = l.validate_piece_index(1).unwrap();

        recv_chunk(&mut ct, 1, 0, CHUNK_SIZE as usize);
        recv_chunk(&mut ct, 1, 1, CHUNK_SIZE as usize);
        recv_chunk(&mut ct, 1, 2, 1);
        assert_eq!(ct.piece_chunk_progress(1).unwrap().downloaded_chunks, 3);

        // The hash check failed: the piece goes back into the queue and the progress
        // honestly drops to zero rather than staying at 100%.
        ct.mark_piece_broken_if_not_have(idx);
        assert_eq!(
            ct.piece_chunk_progress(1),
            Some(PieceChunkProgress {
                downloaded_chunks: 0,
                total_chunks: 3,
                verified: false,
            })
        );

        // Re-download it, this time the hash checks out. A piece we already have is not
        // broken by a later call, so its progress stays put.
        recv_chunk(&mut ct, 1, 0, CHUNK_SIZE as usize);
        recv_chunk(&mut ct, 1, 1, CHUNK_SIZE as usize);
        recv_chunk(&mut ct, 1, 2, 1);
        ct.mark_piece_downloaded(idx, &Default::default());
        ct.mark_piece_broken_if_not_have(idx);
        assert_eq!(
            ct.piece_chunk_progress(1),
            Some(PieceChunkProgress {
                downloaded_chunks: 3,
                total_chunks: 3,
                verified: true,
            })
        );
    }

    // Between its last chunk arriving and its hash passing, a piece is neither queued,
    // in-flight nor have. update_only_files() sees "selected and not have" and queues it,
    // and the hash then passes: without this, the piece is have AND queued, and the next
    // peer to ask is handed a piece we have.
    #[test]
    fn test_a_piece_queued_during_its_hash_check_is_dequeued_when_it_passes() {
        let piece_len = CHUNK_SIZE * 2;
        let l = Lengths::new(piece_len as u64 * 3, piece_len).unwrap();
        let files = vec![FileInfo {
            relative_filename: "0".into(),
            offset_in_torrent: 0,
            piece_range: 0..3,
            len: piece_len as u64 * 3,
            attrs: Default::default(),
        }];
        let bf_len = l.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        selected.get_mut(0..3).unwrap().fill(true);
        let mut ct = ChunkTracker::new(have.into_dyn(), selected, l, &files).unwrap();
        let p0 = l.validate_piece_index(0).unwrap();

        // A peer takes the piece, delivers all of it, and the hash check starts: it is
        // out of the queue and, in the real thing, just out of the in-flight map too.
        ct.reserve_needed_piece(p0);
        assert!(!ct.is_piece_queued(p0));

        // Meanwhile the user deselects the file and selects it again.
        ct.update_only_files(&files, &HashSet::new()).unwrap();
        ct.update_only_files(&files, &HashSet::from_iter([0]))
            .unwrap();
        assert!(ct.is_piece_queued(p0));

        // The hash passes.
        ct.mark_piece_downloaded(p0, &files);
        assert!(ct.is_piece_have(p0));
        assert!(
            !ct.is_piece_queued(p0),
            "a piece we have is still queued, so a second peer will download it again"
        );
    }
}

#[cfg(test)]
mod piece_reclaim_tests {
    use librqbit_core::{constants::CHUNK_SIZE, lengths::Lengths};
    use std::collections::HashSet;

    use crate::{
        Error,
        bitv::BitV,
        chunk_tracker::{HaveNeededSelected, Reselected},
        file_info::FileInfo,
        type_aliases::BF,
    };

    use super::ChunkTracker;

    // Two files over 3 whole pieces: file 0 is pieces 0 and 1, file 1 is piece 2.
    const PIECE_LEN: u32 = CHUNK_SIZE * 2;

    fn lengths() -> Lengths {
        let l = Lengths::new(PIECE_LEN as u64 * 3, PIECE_LEN).unwrap();
        assert_eq!(l.total_pieces(), 3);
        l
    }

    fn file_infos() -> Vec<FileInfo> {
        vec![
            FileInfo {
                relative_filename: "0".into(),
                offset_in_torrent: 0,
                piece_range: 0..2,
                len: PIECE_LEN as u64 * 2,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "1".into(),
                offset_in_torrent: PIECE_LEN as u64 * 2,
                piece_range: 2..3,
                len: PIECE_LEN as u64,
                attrs: Default::default(),
            },
        ]
    }

    fn tracker_with_have(l: Lengths, file_infos: &[FileInfo], have: BF) -> ChunkTracker {
        let mut selected =
            BF::from_boxed_slice(vec![0u8; l.piece_bitfield_bytes()].into_boxed_slice());
        selected.get_mut(0..3).unwrap().fill(true);
        ChunkTracker::new(have.into_dyn(), selected, l, &file_infos.to_vec()).unwrap()
    }

    fn tracker(l: Lengths, file_infos: &[FileInfo]) -> ChunkTracker {
        let have = BF::from_boxed_slice(vec![0u8; l.piece_bitfield_bytes()].into_boxed_slice());
        tracker_with_have(l, file_infos, have)
    }

    fn queued(ct: &ChunkTracker) -> Vec<usize> {
        ct.queue_pieces.iter_ones().collect()
    }

    fn have(ct: &ChunkTracker) -> Vec<usize> {
        ct.have.as_slice().iter_ones().collect()
    }

    fn selected(ct: &ChunkTracker) -> Vec<usize> {
        ct.get_selected_pieces().iter_ones().collect()
    }

    // Everything the want-set could possibly touch, so a "nothing changed" assertion is
    // actually about everything.
    fn snapshot(ct: &ChunkTracker) -> (Vec<usize>, Vec<usize>, HaveNeededSelected, Vec<u64>) {
        (
            queued(ct),
            have(ct),
            *ct.get_hns(),
            ct.per_file_have_bytes().to_vec(),
        )
    }

    fn piece(l: &Lengths, id: u32) -> librqbit_core::lengths::ValidPieceIndex {
        l.validate_piece_index(id).unwrap()
    }

    fn download(ct: &mut ChunkTracker, l: &Lengths, id: u32) {
        // Same order as the real thing: reserve it, then mark it good.
        ct.reserve_needed_piece(piece(l, id));
        ct.mark_piece_downloaded(piece(l, id), &file_infos());
    }

    // The regression test that guards the default: with nobody opting in, the tracker is
    // the upstream tracker. If a want-set check ever leaks into a default code path, this
    // fails.
    #[test]
    fn test_without_opt_in_behaviour_is_unchanged() {
        let l = lengths();
        let fi = file_infos();
        let mut ct = tracker(l, &fi);

        // The API is refused, and refusing it touches nothing.
        let before = snapshot(&ct);
        assert!(matches!(
            ct.drop_pieces(&fi, [piece(&l, 0)]),
            Err(Error::PieceReclaimDisabled)
        ));
        assert!(matches!(
            ct.reselect_pieces([piece(&l, 0)], |_| false),
            Err(Error::PieceReclaimDisabled)
        ));
        assert_eq!(snapshot(&ct), before);

        // queued = selected & !have, from the start.
        assert_eq!(queued(&ct), vec![0, 1, 2]);
        assert_eq!(
            *ct.get_hns(),
            HaveNeededSelected {
                have_bytes: 0,
                needed_bytes: PIECE_LEN as u64 * 3,
                selected_bytes: PIECE_LEN as u64 * 3,
            }
        );

        // Reserving takes a piece out of the queue, completing it puts it in "have".
        ct.reserve_needed_piece(piece(&l, 0));
        assert_eq!(queued(&ct), vec![1, 2]);
        download(&mut ct, &l, 0);
        assert_eq!(have(&ct), vec![0]);
        assert_eq!(queued(&ct), vec![1, 2]);
        assert_eq!(ct.per_file_have_bytes(), [PIECE_LEN as u64, 0]);

        // A peer dying (or a pause, or a hash failure) requeues what we don't have...
        ct.reserve_needed_piece(piece(&l, 1));
        assert_eq!(queued(&ct), vec![2]);
        ct.mark_piece_broken_if_not_have(piece(&l, 1));
        assert_eq!(queued(&ct), vec![1, 2]);

        // ...and leaves what we do have alone.
        ct.mark_piece_broken_if_not_have(piece(&l, 0));
        assert_eq!(queued(&ct), vec![1, 2]);
        assert_eq!(have(&ct), vec![0]);

        // Deselecting a file cancels its pieces, reselecting it requeues them.
        ct.update_only_files(&fi, &HashSet::from_iter([0])).unwrap();
        assert_eq!(queued(&ct), vec![1]);
        assert_eq!(
            *ct.get_hns(),
            HaveNeededSelected {
                have_bytes: PIECE_LEN as u64,
                needed_bytes: PIECE_LEN as u64,
                selected_bytes: PIECE_LEN as u64 * 2,
            }
        );
        ct.update_only_files(&fi, &HashSet::from_iter([0, 1]))
            .unwrap();
        assert_eq!(queued(&ct), vec![1, 2]);
        assert_eq!(
            *ct.get_hns(),
            HaveNeededSelected {
                have_bytes: PIECE_LEN as u64,
                needed_bytes: PIECE_LEN as u64 * 2,
                selected_bytes: PIECE_LEN as u64 * 3,
            }
        );
    }

    #[test]
    fn test_dropped_piece_is_not_have_not_wanted_not_advertised() {
        let l = lengths();
        let fi = file_infos();
        let mut ct = tracker(l, &fi);
        ct.enable_piece_reclaim();

        for id in 0..3 {
            download(&mut ct, &l, id);
        }
        assert!(ct.is_finished());
        assert_eq!(queued(&ct), Vec::<usize>::new());

        assert_eq!(ct.drop_pieces(&fi, [piece(&l, 0)]).unwrap(), [piece(&l, 0)]);

        // Not have. This is what stops it being advertised in the bitfield we send, and
        // what makes on_download_request() refuse a request for it: both read the
        // have-bitfield and nothing else.
        assert!(!ct.is_piece_have(piece(&l, 0)));
        assert_eq!(have(&ct), vec![1, 2]);
        assert!(!ct.get_have_pieces().as_slice()[0]);

        // A request for a dropped piece is refused: is_chunk_ready_to_upload() is the
        // predicate the peer request path bails on, and the one it re-checks after rate
        // limiting in case the piece went away in between.
        let chunk = l
            .chunk_info_from_received_data(piece(&l, 0), 0, CHUNK_SIZE)
            .unwrap();
        assert!(!ct.is_chunk_ready_to_upload(&chunk));
        let kept = l
            .chunk_info_from_received_data(piece(&l, 1), 0, CHUNK_SIZE)
            .unwrap();
        assert!(ct.is_chunk_ready_to_upload(&kept));

        // Not wanted.
        assert_eq!(queued(&ct), Vec::<usize>::new());
        assert!(ct.is_piece_dropped(piece(&l, 0)));

        // The torrent is still finished and still 100%: a piece we deliberately threw
        // away is not a piece we're missing.
        assert!(ct.is_finished());
        assert_eq!(
            *ct.get_hns(),
            HaveNeededSelected {
                have_bytes: PIECE_LEN as u64 * 2,
                needed_bytes: 0,
                selected_bytes: PIECE_LEN as u64 * 2,
            }
        );

        // The file it's in is no longer complete, so iter_queued_pieces() will look at it
        // again if we ever re-select inside it.
        assert_eq!(
            ct.per_file_have_bytes(),
            [PIECE_LEN as u64, PIECE_LEN as u64]
        );

        // A peer dying, a pause, or a hash failure must not put it back: that is the
        // delete-it-and-download-it-again loop this whole thing exists to avoid.
        ct.mark_piece_broken_if_not_have(piece(&l, 0));
        assert_eq!(queued(&ct), Vec::<usize>::new());

        // Neither may a no-op selection change.
        ct.update_only_files(&fi, &HashSet::from_iter([0, 1]))
            .unwrap();
        assert_eq!(queued(&ct), Vec::<usize>::new());
        assert!(ct.is_piece_dropped(piece(&l, 0)));
        assert!(ct.is_finished());

        // Dropping what we don't have is a no-op, so a policy can be sloppy about it.
        assert!(ct.drop_pieces(&fi, [piece(&l, 0)]).unwrap().is_empty());
    }

    #[test]
    fn test_reselecting_a_dropped_range_makes_it_wanted_again() {
        let l = lengths();
        let fi = file_infos();
        let mut ct = tracker(l, &fi);
        ct.enable_piece_reclaim();

        for id in 0..3 {
            download(&mut ct, &l, id);
        }
        ct.drop_pieces(&fi, [piece(&l, 0), piece(&l, 1)]).unwrap();
        assert_eq!(queued(&ct), Vec::<usize>::new());

        assert_eq!(
            ct.reselect_pieces([piece(&l, 0)], |_| false).unwrap(),
            Reselected {
                reselected: 1,
                queued: 1
            }
        );
        assert!(!ct.is_piece_dropped(piece(&l, 0)));
        assert_eq!(queued(&ct), vec![0]);
        assert!(!ct.is_finished());
        assert_eq!(
            *ct.get_hns(),
            HaveNeededSelected {
                have_bytes: PIECE_LEN as u64,
                needed_bytes: PIECE_LEN as u64,
                selected_bytes: PIECE_LEN as u64 * 2,
            }
        );

        // Reselecting something that wasn't dropped does nothing.
        assert_eq!(
            ct.reselect_pieces([piece(&l, 0), piece(&l, 2)], |_| false)
                .unwrap(),
            Reselected::default()
        );
        assert_eq!(queued(&ct), vec![0]);

        // Downloading it again restores everything.
        download(&mut ct, &l, 0);
        assert_eq!(have(&ct), vec![0, 2]);
        assert_eq!(
            ct.per_file_have_bytes(),
            [PIECE_LEN as u64, PIECE_LEN as u64]
        );
        assert_eq!(
            *ct.get_hns(),
            HaveNeededSelected {
                have_bytes: PIECE_LEN as u64 * 2,
                needed_bytes: 0,
                selected_bytes: PIECE_LEN as u64 * 2,
            }
        );
    }

    // Undropping must not queue a piece the user has deselected: "queued" has to stay a
    // subset of "selected | have", which is the invariant update_only_files() goes out of
    // its way to maintain.
    #[test]
    fn test_reselecting_a_dropped_piece_in_a_deselected_file_does_not_queue_it() {
        let l = lengths();
        let fi = file_infos();
        let mut ct = tracker(l, &fi);
        ct.enable_piece_reclaim();

        for id in 0..3 {
            download(&mut ct, &l, id);
        }
        ct.drop_pieces(&fi, [piece(&l, 2)]).unwrap();

        // Deselect file 1, which is where the dropped piece lives.
        ct.update_only_files(&fi, &HashSet::from_iter([0])).unwrap();
        assert_eq!(selected(&ct), vec![0, 1]);
        assert!(ct.is_piece_dropped(piece(&l, 2)));

        // Undropping it must not make it wanted: the user doesn't want that file. It is
        // also not queued, and the caller has to be able to tell: waking every peer for a
        // piece that nobody can download is a wake-up with no work behind it.
        assert_eq!(
            ct.reselect_pieces([piece(&l, 2)], |_| false).unwrap(),
            Reselected {
                reselected: 1,
                queued: 0
            }
        );
        assert!(!ct.is_piece_dropped(piece(&l, 2)));
        assert_eq!(selected(&ct), vec![0, 1]);
        assert_eq!(queued(&ct), Vec::<usize>::new());
        assert!(ct.is_finished());

        // Asking for the file back is what makes it wanted.
        ct.update_only_files(&fi, &HashSet::from_iter([0, 1]))
            .unwrap();
        assert_eq!(queued(&ct), vec![2]);
        assert!(!ct.is_finished());
    }

    // Asking for a file back outranks a drop inside it: otherwise the user re-selects a
    // file and it silently never completes.
    #[test]
    fn test_reselecting_a_file_undrops_its_pieces() {
        let l = lengths();
        let fi = file_infos();
        let mut ct = tracker(l, &fi);
        ct.enable_piece_reclaim();

        for id in 0..3 {
            download(&mut ct, &l, id);
        }
        ct.drop_pieces(&fi, [piece(&l, 2)]).unwrap();
        assert!(ct.is_piece_dropped(piece(&l, 2)));

        ct.update_only_files(&fi, &HashSet::from_iter([0])).unwrap();
        assert!(ct.is_piece_dropped(piece(&l, 2)));
        assert_eq!(queued(&ct), Vec::<usize>::new());

        ct.update_only_files(&fi, &HashSet::from_iter([0, 1]))
            .unwrap();
        assert!(!ct.is_piece_dropped(piece(&l, 2)));
        assert_eq!(queued(&ct), vec![2]);
        assert!(!ct.is_finished());
    }

    // Defect 3, pinned: the want-set is per-session. The have-bitfield is the only
    // per-piece state that crosses a restart, and `piece_reclaim` is not part of
    // SerializedTorrent, so a restored torrent wants its dropped pieces back. That is the
    // right default - the storage behind them is gone, so "missing and wanted" is the
    // truth - and a caller that wants them to stay dropped re-supplies both the flag and
    // its own dropped set, exactly as it re-supplies every other AddTorrentOptions field.
    #[test]
    fn test_the_want_set_does_not_survive_a_restart() {
        let l = lengths();
        let fi = file_infos();
        let mut ct = tracker(l, &fi);
        ct.enable_piece_reclaim();

        for id in 0..3 {
            download(&mut ct, &l, id);
        }
        ct.drop_pieces(&fi, [piece(&l, 0)]).unwrap();
        assert_eq!(queued(&ct), Vec::<usize>::new());

        // Restart: everything that is rebuilt from persisted state is rebuilt, and the
        // have-bitfield is all of it.
        let persisted_have = BF::from_bitslice(ct.get_have_pieces().as_slice());
        let restarted = tracker_with_have(l, &fi, persisted_have);

        assert_eq!(have(&restarted), vec![1, 2]);
        assert_eq!(queued(&restarted), vec![0]);
        assert!(!restarted.is_finished());
        assert!(!restarted.is_piece_dropped(piece(&l, 0)));
    }
}
