// A storage that records what reached it, to check the wrappers around one against.
//
// A middleware is transparent: anything it has no opinion about has to reach what it
// wraps. The methods that make that easy to get wrong are the ones [`TorrentStorage`]
// gives a default, because forgetting one still compiles and then answers something
// plausible of its own instead of asking the storage underneath.

use std::{io::IoSlice, path::Path};

use librqbit_core::{
    constants::CHUNK_SIZE,
    lengths::{Lengths, ValidPieceIndex},
};
use parking_lot::Mutex;

use crate::{ManagedTorrentShared, TorrentMetadata, storage::TorrentStorage};

pub(crate) const PIECE_LEN: u32 = CHUNK_SIZE * 2;
pub(crate) const NUM_PIECES: u32 = 2;

pub(crate) fn lengths() -> Lengths {
    Lengths::new(PIECE_LEN as u64 * NUM_PIECES as u64, PIECE_LEN).unwrap()
}

pub(crate) fn piece(id: u32) -> ValidPieceIndex {
    lengths().validate_piece_index(id).unwrap()
}

#[derive(Default)]
pub(crate) struct Probe {
    /// The pieces on_piece_completed() was called for, in order.
    pub completed: Mutex<Vec<u32>>,
    /// The vectored writes that arrived as one call, as (offset, total length).
    pub vectored: Mutex<Vec<(u64, usize)>>,
}

impl TorrentStorage for Probe {
    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, _file_id: usize, _offset: u64, _buf: &mut [u8]) -> anyhow::Result<()> {
        Ok(())
    }

    fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        anyhow::bail!("not used")
    }

    fn pwrite_all_vectored(
        &self,
        _file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let len = bufs[0].len() + bufs[1].len();
        self.vectored.lock().push((offset, len));
        Ok(len)
    }

    fn on_piece_completed(&self, piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        self.completed.lock().push(piece_index.get());
        Ok(())
    }

    // The default is "yes, and I would know otherwise". This one knows otherwise, so a
    // middleware that doesn't forward is caught by the answer coming back as the default.
    fn has_piece(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
        Ok(false)
    }
}

// Everything a wrapper has to pass through to the storage it wraps and can't decide on
// its own. Called from each middleware's own tests, where its private fields are.
pub(crate) fn assert_forwards_defaults<S: TorrentStorage>(storage: &S, probe: &Probe) {
    storage.on_piece_completed(piece(1)).unwrap();
    assert_eq!(
        *probe.completed.lock(),
        vec![1],
        "on_piece_completed() didn't reach the storage: a store that makes a piece \
         visible there never gets told"
    );
    assert!(
        !storage.has_piece(piece(1)).unwrap(),
        "has_piece() didn't reach the storage: it answered the default yes over a piece \
         the storage says is gone"
    );

    let (a, b) = (&[1u8; 4][..], &[2u8; 6][..]);
    let written = storage
        .pwrite_all_vectored(0, 0, [IoSlice::new(a), IoSlice::new(b)])
        .unwrap();
    assert_eq!(written, a.len() + b.len());
    assert_eq!(
        *probe.vectored.lock(),
        vec![(0, a.len() + b.len())],
        "pwrite_all_vectored() didn't reach the storage whole: the default split it into \
         two pwrite_all()s, which is the one call this exists to avoid"
    );
}
