//! Storage engine for torrent data.
//!
//! This is deliberately sync, not async by design, for several reasons.
//!
//! Reason 1. Performance: avoiding copying.
//!
//! Torrent files are large so memcpy costs can compound. Tokio FS does all file writes in a thread
//! pool. To do those writes it has a buffer per file. When you call e.g. "write", your request is first
//! memcpy'ed to that buffer, and only then written to the file.
//!
//! On the write path (download), we write straight from the peer's socket buffer into the file.
//! On the read path (upload), we read straight into the peer's socket buffer also.
//!
//! Reason 2. Memory use: memory bloat would be a problem if tokio::fs was used.
//!
//! The said buffers above default to 2MB. We have a lot of files open, so this can compound into a pretty large
//! memory use.
//!
//! Reason 3. Performance: advanced FS APIs.
//!
//! We use positioned vectored writes (pwritev). Tokio doesn't support that.
//! Positioned so that writing can be done to files in parallel without locks.
//! Vectored so that we issue 1 write call for a potentially non-contiguous chunk.

pub mod filesystem;

#[cfg(feature = "storage_examples")]
pub mod examples;

#[cfg(feature = "storage_middleware")]
pub mod middleware;

use std::{
    any::{Any, TypeId},
    io::IoSlice,
    path::Path,
};

use librqbit_core::lengths::ValidPieceIndex;

use crate::torrent_state::{ManagedTorrentShared, TorrentMetadata};

pub trait StorageFactory: Send + Sync + Any {
    type Storage: TorrentStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<Self::Storage>;
    fn create_and_init(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        let mut storage = self.create(shared, metadata)?;
        storage.init(shared, metadata)?;
        Ok(storage)
    }

    fn is_type_id(&self, type_id: TypeId) -> bool {
        Self::type_id(self) == type_id
    }
    fn clone_box(&self) -> BoxStorageFactory;
}

pub type BoxStorageFactory = Box<dyn StorageFactory<Storage = Box<dyn TorrentStorage>>>;

pub trait StorageFactoryExt {
    fn boxed(self) -> BoxStorageFactory;
}

impl<SF: StorageFactory> StorageFactoryExt for SF {
    fn boxed(self) -> BoxStorageFactory {
        struct Wrapper<SF> {
            sf: SF,
        }

        impl<SF: StorageFactory> StorageFactory for Wrapper<SF> {
            type Storage = Box<dyn TorrentStorage>;

            fn create(
                &self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<Self::Storage> {
                let s = self.sf.create(shared, metadata)?;
                Ok(Box::new(s))
            }

            fn is_type_id(&self, type_id: TypeId) -> bool {
                self.sf.type_id() == type_id
            }

            fn clone_box(&self) -> BoxStorageFactory {
                self.sf.clone_box()
            }
        }

        Box::new(Wrapper { sf: self })
    }
}

impl<U: StorageFactory + ?Sized> StorageFactory for Box<U> {
    type Storage = U::Storage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<U::Storage> {
        (**self).create(shared, metadata)
    }

    fn clone_box(&self) -> BoxStorageFactory {
        (**self).clone_box()
    }
}

pub trait TorrentStorage: Send + Sync {
    // Create/open files etc.
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()>;

    /// Given a file_id (which you can get more info from in init_storage() through torrent info)
    /// read buf.len() bytes into buf at offset.
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()>;

    /// Given a file_id (which you can get more info from in init_storage() through torrent info)
    /// write buf.len() bytes into the file at offset.
    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()>;

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let mut offset = offset;
        let mut size = 0;

        for ioslice in bufs {
            self.pwrite_all(file_id, offset, &ioslice)?;
            offset += ioslice.len() as u64;
            size += ioslice.len();
        }

        Ok(size)
    }

    /// Remove a file from the storage. If not supported, or it doesn't matter, just return Ok(())
    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()>;

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()>;

    /// E.g. for filesystem backend ensure that the file has a certain length, and grow/shrink as needed.
    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()>;

    /// Replace the current storage with a dummy, and return a new one that should be used instead.
    /// This is used to make the underlying object useless when e.g. pausing the torrent.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>>;

    /// Callback called every time a piece has completed and has been validated.
    /// Default implementation does nothing, but can be override in trait implementations.
    ///
    /// This is where a storage that implements [`Self::has_piece`] makes the piece
    /// visible: until it returns, the piece is still being written.
    fn on_piece_completed(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        Ok(())
    }

    /// Whether the data of this piece is still here, **complete**.
    ///
    /// Called at startup for every piece the resume data claims we have, and the have-set
    /// we start with is the intersection of the two - so this can only take a piece away,
    /// never add one. It exists for storages that can lose a single piece behind our back:
    /// with [`crate::AddTorrentOptions::piece_reclaim`] the caller deletes the storage of
    /// a dropped piece itself, and if the process dies before the bitfield reaches the
    /// disk, the resume data outlives the data it describes.
    ///
    /// Complete is the whole of the contract, and the errors are not symmetric. A wrong
    /// "no" costs a re-download. A wrong "yes" is silent corruption: the intersection
    /// only clears bits, so a "yes" over a piece that is half-written leaves the have-bit
    /// standing, nothing downstream looks again (the fastresume hash check samples ~65
    /// pieces of a torrent however large), and rqbit advertises and serves that piece as
    /// good.
    ///
    /// The trap is that the obvious answer is about presence, not completeness. Chunks
    /// arrive 16 KiB at a time, so whatever a piece's first chunk creates - a map entry,
    /// a file - is there long before the piece is whole, and a `try_exists()` on it says
    /// yes the entire time it is being downloaded. Make existence mean complete instead:
    /// write the piece under a temporary name and move it into place in
    /// [`Self::on_piece_completed`], which runs after the hash check. The example storage
    /// in `storage::examples::inmemory` does exactly that.
    ///
    /// The default answers "yes, and I would know otherwise", which is right for a
    /// storage that cannot lose one piece on its own - the filesystem one grows files, it
    /// never punches holes in them. Implement it if a piece of yours can go away.
    fn has_piece(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
        Ok(true)
    }
}

impl<U: TorrentStorage + ?Sized> TorrentStorage for Box<U> {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        (**self).pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        (**self).pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        (**self).remove_file(file_id, filename)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        (**self).ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        (**self).take()
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        (**self).remove_directory_if_empty(path)
    }

    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        (**self).init(shared, metadata)
    }

    fn on_piece_completed(&self, piece_id: ValidPieceIndex) -> anyhow::Result<()> {
        (**self).on_piece_completed(piece_id)
    }

    fn has_piece(&self, piece_id: ValidPieceIndex) -> anyhow::Result<bool> {
        (**self).has_piece(piece_id)
    }
}
