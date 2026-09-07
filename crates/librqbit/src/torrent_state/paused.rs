use std::{collections::HashSet, ops::Range, sync::Arc};

use crate::{
    chunk_tracker::{ChunkTracker, HaveNeededSelected},
    type_aliases::FileStorage,
};

use super::{
    ManagedTorrentShared, TorrentMetadata, live::clamp_piece_range, streaming::TorrentStreams,
};

pub struct TorrentStatePaused {
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) files: FileStorage,
    pub(crate) chunk_tracker: ChunkTracker,
    pub(crate) streams: Arc<TorrentStreams>,
}

impl TorrentStatePaused {
    pub(crate) fn update_only_files(&mut self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        self.chunk_tracker
            .update_only_files(&self.metadata.file_infos, only_files)?;
        Ok(())
    }

    pub(crate) fn hns(&self) -> &HaveNeededSelected {
        self.chunk_tracker.get_hns()
    }

    /// Drop pieces: see [`crate::ManagedTorrent::drop_pieces`]. This is how a want-set is
    /// re-applied to a restored torrent before it goes live. Nothing is in flight on a
    /// paused torrent and there is no have counter to keep in step; a stream outlives a
    /// pause, so its lookahead still guards. The have-bitfield isn't flushed here - it is
    /// when the torrent is dropped or goes live and completes something - and what keeps
    /// it honest across a crash is the storage's has_piece(), as it is for a live drop.
    pub(crate) fn drop_pieces(&mut self, pieces: Range<u32>) -> anyhow::Result<Vec<u32>> {
        let lengths = *self.chunk_tracker.get_lengths();
        let wanted = self.streams.wanted_ranges(&lengths);
        let candidates = clamp_piece_range(pieces, &lengths)
            .filter(|id| !wanted.iter().any(|r| r.contains(id)))
            .filter_map(|id| lengths.validate_piece_index(id));
        let dropped =
            self.chunk_tracker
                .drop_pieces(&self.metadata.file_infos, candidates, |_| false)?;
        Ok(dropped.into_iter().map(|id| id.get()).collect())
    }

    /// Make dropped pieces wanted again: see [`crate::ManagedTorrent::reselect_pieces`].
    /// Nothing to wake up: unpausing picks the queue up as it finds it.
    pub(crate) fn reselect_pieces(&mut self, pieces: Range<u32>) -> anyhow::Result<usize> {
        let lengths = *self.chunk_tracker.get_lengths();
        let pieces =
            clamp_piece_range(pieces, &lengths).filter_map(|id| lengths.validate_piece_index(id));
        Ok(self
            .chunk_tracker
            .reselect_pieces(pieces, |_| false)?
            .reselected)
    }

    /// The caller is done releasing the storage of these pieces. Nothing to wake up: a
    /// paused torrent has no peers, and unpausing picks the queue up as it finds it.
    pub(crate) fn finish_release(&mut self, pieces: &[u32]) {
        let lengths = *self.chunk_tracker.get_lengths();
        self.chunk_tracker.finish_release(
            pieces
                .iter()
                .filter_map(|id| lengths.validate_piece_index(*id)),
        );
    }
}
