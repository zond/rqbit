use std::{collections::HashSet, sync::Arc};

use crate::{
    chunk_tracker::{ChunkTracker, HaveNeededSelected},
    type_aliases::FileStorage,
};

use super::{ManagedTorrentShared, TorrentMetadata, streaming::TorrentStreams};

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
