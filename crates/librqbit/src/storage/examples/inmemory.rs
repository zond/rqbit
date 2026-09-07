use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Context;
use librqbit_core::lengths::{Lengths, ValidPieceIndex};
use parking_lot::RwLock;

use crate::type_aliases::FileInfos;
use crate::{ManagedTorrentShared, TorrentMetadata};

use crate::storage::{StorageFactory, StorageFactoryExt, TorrentStorage};

struct InMemoryPiece {
    bytes: Box<[u8]>,
}

impl InMemoryPiece {
    fn new(l: &Lengths) -> Self {
        let v = vec![0; l.default_piece_length() as usize].into_boxed_slice();
        Self { bytes: v }
    }
}

// Which piece a (file_id, offset) lands in, and where inside that piece. Both storages
// in here keep one entry per piece, so this is the whole address translation they need.
fn piece_and_offset(
    lengths: &Lengths,
    file_infos: &FileInfos,
    file_id: usize,
    offset: u64,
) -> anyhow::Result<(ValidPieceIndex, usize)> {
    let fi = file_infos.get(file_id).context("no such file")?;
    let abs_offset = fi.offset_in_torrent + offset;
    let piece_id: u32 = (abs_offset / lengths.default_piece_length() as u64).try_into()?;
    let piece_offset: usize = (abs_offset % lengths.default_piece_length() as u64).try_into()?;
    let piece_id = lengths.validate_piece_index(piece_id).context("bug")?;
    Ok((piece_id, piece_offset))
}

#[derive(Default, Clone)]
pub struct InMemoryExampleStorageFactory {}

impl StorageFactory for InMemoryExampleStorageFactory {
    type Storage = InMemoryExampleStorage;

    fn create(
        &self,
        _shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<InMemoryExampleStorage> {
        InMemoryExampleStorage::new(*metadata.lengths(), metadata.file_infos.clone())
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        self.clone().boxed()
    }
}

pub struct InMemoryExampleStorage {
    lengths: Lengths,
    file_infos: FileInfos,
    map: RwLock<HashMap<ValidPieceIndex, InMemoryPiece>>,
}

impl InMemoryExampleStorage {
    fn new(lengths: Lengths, file_infos: FileInfos) -> anyhow::Result<Self> {
        // Max memory 128MiB. Make it tunable
        let max_pieces = 128 * 1024 * 1024 / lengths.default_piece_length();
        if max_pieces == 0 {
            anyhow::bail!("pieces too large");
        }

        Ok(Self {
            lengths,
            file_infos,
            map: RwLock::new(HashMap::new()),
        })
    }
}

impl TorrentStorage for InMemoryExampleStorage {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let (piece_id, piece_offset) =
            piece_and_offset(&self.lengths, &self.file_infos, file_id, offset)?;

        let g = self.map.read();
        let inmp = g.get(&piece_id).context("piece expired")?;
        buf.copy_from_slice(&inmp.bytes[piece_offset..(piece_offset + buf.len())]);
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let (piece_id, piece_offset) =
            piece_and_offset(&self.lengths, &self.file_infos, file_id, offset)?;
        let mut g = self.map.write();
        let inmp = g
            .entry(piece_id)
            .or_insert_with(|| InMemoryPiece::new(&self.lengths));
        inmp.bytes[piece_offset..(piece_offset + buf.len())].copy_from_slice(buf);
        Ok(())
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        let map = {
            let mut g = self.map.write();
            let mut repl = HashMap::new();
            std::mem::swap(&mut *g, &mut repl);
            repl
        };
        Ok(Box::new(Self {
            lengths: self.lengths,
            map: RwLock::new(map),
            file_infos: self.file_infos.clone(),
        }))
    }

    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }
}

// What the piece storage holds. The split is the point: see InMemoryPieceStorageFactory.
#[derive(Default)]
struct Pieces {
    // Pieces that are all here: fully written, and hash-checked by the time they landed.
    // This is what has_piece() answers from, and what a reader may be served.
    complete: HashMap<ValidPieceIndex, InMemoryPiece>,

    // Pieces being written. A piece appears here on its first 16 KiB chunk and moves to
    // `complete` in on_piece_completed(). Keeping it out of `complete` until then is what
    // makes "the storage has this piece" mean "complete" rather than "started".
    partial: HashMap<ValidPieceIndex, InMemoryPiece>,
}

/// An in-memory storage that can release individual pieces, for use with
/// [`crate::AddTorrentOptions::piece_reclaim`].
///
/// It keeps one entry per piece, which is what makes releasing one a meaningful thing to
/// do: a piece is either there in full or not there at all, so what the storage holds is
/// exactly the have-set. A filesystem storage that writes into whole files can't do this
/// - it can only punch holes, and the have-bitfield and the disk drift apart.
///
/// A piece is written into a staging area and moved into place in `on_piece_completed`,
/// so that its presence means "complete" and not "started" - see
/// [`crate::storage::TorrentStorage::has_piece`], where a wrong yes is silent corruption.
/// On a filesystem that move is writing to a temporary name and renaming it.
///
/// The factory is the caller's handle to those pieces: it shares the map with the storage
/// it creates, so the policy that decides what to reclaim can call
/// [`crate::ManagedTorrent::drop_pieces`] and then delete exactly what it was handed:
///
/// ```ignore
/// let dropped = torrent.drop_pieces(0..100)?;
/// for id in dropped.pieces() {
///     storage.release_piece(lengths.validate_piece_index(*id).unwrap());
/// }
/// drop(dropped); // the claim goes once the storage is gone
/// ```
///
/// One torrent per factory: the map is shared with everything it creates.
#[derive(Default, Clone)]
pub struct InMemoryPieceStorageFactory {
    pieces: Arc<RwLock<Pieces>>,
}

impl InMemoryPieceStorageFactory {
    /// Forget the data of a piece. Returns true if it was there, complete.
    ///
    /// This is the "release the storage" half of the reclaim loop, and the caller only
    /// gets to call it for pieces [`crate::ManagedTorrent::drop_pieces`] handed over.
    pub fn release_piece(&self, piece_id: ValidPieceIndex) -> bool {
        let mut g = self.pieces.write();
        // A half-written copy of a piece being released is worth just as little as the
        // finished one: it is what the caller asked us to forget.
        g.partial.remove(&piece_id);
        g.complete.remove(&piece_id).is_some()
    }

    /// Whether the data of a piece is still here, complete.
    pub fn has_piece(&self, piece_id: ValidPieceIndex) -> bool {
        self.pieces.read().complete.contains_key(&piece_id)
    }

    /// How many whole pieces are held right now.
    pub fn piece_count(&self) -> usize {
        self.pieces.read().complete.len()
    }
}

impl StorageFactory for InMemoryPieceStorageFactory {
    type Storage = InMemoryPieceStorage;

    fn create(
        &self,
        _shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<InMemoryPieceStorage> {
        Ok(InMemoryPieceStorage {
            lengths: *metadata.lengths(),
            file_infos: metadata.file_infos.clone(),
            pieces: self.pieces.clone(),
        })
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        self.clone().boxed()
    }
}

pub struct InMemoryPieceStorage {
    lengths: Lengths,
    file_infos: FileInfos,
    pieces: Arc<RwLock<Pieces>>,
}

impl TorrentStorage for InMemoryPieceStorage {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let (piece_id, piece_offset) =
            piece_and_offset(&self.lengths, &self.file_infos, file_id, offset)?;
        let g = self.pieces.read();
        // A piece being written is readable: hash checking it is a read of what was just
        // written, and it happens before the piece is complete.
        let piece = g
            .complete
            .get(&piece_id)
            .or_else(|| g.partial.get(&piece_id))
            .context("piece was released")?;
        buf.copy_from_slice(&piece.bytes[piece_offset..(piece_offset + buf.len())]);
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let (piece_id, piece_offset) =
            piece_and_offset(&self.lengths, &self.file_infos, file_id, offset)?;
        let mut g = self.pieces.write();
        let piece = g
            .partial
            .entry(piece_id)
            .or_insert_with(|| InMemoryPiece::new(&self.lengths));
        piece.bytes[piece_offset..(piece_offset + buf.len())].copy_from_slice(buf);
        Ok(())
    }

    // The piece is written and hash-checked: move it into place. This is the rename in
    // "write to a temporary name and rename it when it's done", and until it happens
    // has_piece() says no.
    fn on_piece_completed(&self, piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        let mut g = self.pieces.write();
        if let Some(piece) = g.partial.remove(&piece_index) {
            g.complete.insert(piece_index, piece);
        }
        Ok(())
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        // The pieces stay where they are: a pause must not lose them, and the factory is
        // the caller's handle to them.
        Ok(Box::new(Self {
            lengths: self.lengths,
            file_infos: self.file_infos.clone(),
            pieces: self.pieces.clone(),
        }))
    }

    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    // The whole point: a piece that was released is gone, and startup has to believe the
    // storage over the resume data. Only whole pieces count - a piece half-written when
    // the process died is not one we have.
    fn has_piece(&self, piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
        Ok(self.pieces.read().complete.contains_key(&piece_index))
    }
}

#[cfg(test)]
mod tests {
    use librqbit_core::{constants::CHUNK_SIZE, lengths::Lengths};

    use super::*;
    use crate::storage::TorrentStorage;

    const PIECE_LEN: u32 = CHUNK_SIZE * 2;
    const NUM_PIECES: u32 = 2;

    fn storage() -> (InMemoryPieceStorageFactory, InMemoryPieceStorage) {
        let len = PIECE_LEN as u64 * NUM_PIECES as u64;
        let lengths = Lengths::new(len, PIECE_LEN).unwrap();
        let file_infos: FileInfos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len,
            piece_range: 0..NUM_PIECES,
            attrs: Default::default(),
        }];
        let factory = InMemoryPieceStorageFactory::default();
        let storage = InMemoryPieceStorage {
            lengths,
            file_infos,
            pieces: factory.pieces.clone(),
        };
        (factory, storage)
    }

    // has_piece() must mean "complete", not "started". Chunks arrive 16 KiB at a time,
    // and a storage that answers yes as soon as the first one lands leaves a have-bit
    // over a half-written piece if the process dies in between - and startup's
    // intersection only ever clears bits, so nothing takes it back and we serve garbage.
    #[test]
    fn test_a_piece_is_not_there_until_it_is_complete() {
        let (factory, storage) = storage();
        let piece = storage.lengths.validate_piece_index(0).unwrap();
        let chunk = vec![1u8; CHUNK_SIZE as usize];

        storage.pwrite_all(0, 0, &chunk).unwrap();
        assert!(
            !storage.has_piece(piece).unwrap(),
            "half a piece is not a piece"
        );
        assert!(!factory.has_piece(piece));
        assert_eq!(factory.piece_count(), 0);

        // It is readable while it is being written: that is how it gets hash-checked.
        let mut buf = vec![0u8; CHUNK_SIZE as usize];
        storage.pread_exact(0, 0, &mut buf).unwrap();
        assert_eq!(buf, chunk);

        storage.pwrite_all(0, CHUNK_SIZE as u64, &chunk).unwrap();
        assert!(
            !storage.has_piece(piece).unwrap(),
            "a piece that hasn't been hash-checked yet is not a piece we have"
        );

        // Written and hash-checked: now it is here.
        storage.on_piece_completed(piece).unwrap();
        assert!(storage.has_piece(piece).unwrap());
        assert!(factory.has_piece(piece));
        assert_eq!(factory.piece_count(), 1);
        storage.pread_exact(0, 0, &mut buf).unwrap();
        assert_eq!(buf, chunk);

        // And released it is gone again.
        assert!(factory.release_piece(piece));
        assert!(!storage.has_piece(piece).unwrap());
        assert_eq!(factory.piece_count(), 0);
        assert!(storage.pread_exact(0, 0, &mut buf).is_err());
    }
}
