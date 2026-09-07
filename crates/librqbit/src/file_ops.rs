use std::{
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::{Context, bail};
use buffers::{ByteBuf, ByteBufOwned};
use librqbit_core::{
    lengths::{ChunkInfo, ValidPieceIndex},
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use peer_binary_protocol::{DoubleBufHelper, Piece};
use sha1w::{ISha1, Sha1};
use tracing::{debug, trace, warn};

use crate::{
    file_info::FileInfo,
    storage::TorrentStorage,
    type_aliases::{BF, FileInfos, PeerHandle},
};

pub fn update_hash_from_file<Sha1: ISha1>(
    file_id: usize,
    file_info: &FileInfo,
    mut pos: u64,
    files: &dyn TorrentStorage,
    hash: &mut Sha1,
    buf: &mut [u8],
    mut bytes_to_read: usize,
) -> anyhow::Result<()> {
    let mut read = 0;
    while bytes_to_read > 0 {
        let chunk = std::cmp::min(buf.len(), bytes_to_read);
        if file_info.attrs.padding {
            buf[..chunk].fill(0);
        } else {
            files
                .pread_exact(file_id, pos, &mut buf[..chunk])
                .with_context(|| {
                    format!("failed reading chunk of size {chunk}, read so far {read}")
                })?;
        }
        bytes_to_read -= chunk;
        read += chunk;
        pos += chunk as u64;
        hash.update(&buf[..chunk]);
    }
    Ok(())
}

pub(crate) struct FileOps<'a> {
    torrent: &'a ValidatedTorrentMetaV1Info<ByteBufOwned>,
    files: &'a dyn TorrentStorage,
    file_infos: &'a FileInfos,
    phantom_data: PhantomData<Sha1>,
}

impl<'a> FileOps<'a> {
    pub fn new(
        torrent: &'a ValidatedTorrentMetaV1Info<ByteBufOwned>,
        files: &'a dyn TorrentStorage,
        file_infos: &'a FileInfos,
    ) -> Self {
        Self {
            torrent,
            files,
            file_infos,
            phantom_data: PhantomData,
        }
    }

    // Returns the bitvector with pieces we have.
    pub fn initial_check(
        &self,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
    ) -> anyhow::Result<BF> {
        let mut have_pieces =
            BF::from_boxed_slice(vec![0u8; self.torrent.lengths().piece_bitfield_bytes()].into());
        let mut piece_files = Vec::<usize>::new();

        #[derive(Debug)]
        struct CurrentFile<'a> {
            index: usize,
            fi: &'a FileInfo,
            processed_bytes: u64,
            is_broken: bool,
        }
        impl CurrentFile<'_> {
            fn remaining(&self) -> u64 {
                self.fi.len - self.processed_bytes
            }
            fn mark_processed_bytes(&mut self, bytes: u64) {
                self.processed_bytes += bytes
            }
        }
        let mut file_iterator = self
            .file_infos
            .iter()
            .enumerate()
            .map(|(idx, fi)| CurrentFile {
                index: idx,
                fi,
                processed_bytes: 0,
                is_broken: false,
            });

        let mut current_file = file_iterator.next().context("empty input file list")?;

        let mut read_buffer = vec![0u8; 65536];

        for piece_info in self.torrent.lengths().iter_piece_infos() {
            if pause_requested.load(Ordering::Relaxed) {
                bail!("initial check paused");
            }

            piece_files.clear();
            let mut computed_hash = Sha1::new();
            let mut piece_remaining = piece_info.len as usize;
            let mut some_files_broken = false;
            progress.fetch_add(piece_info.len as u64, Ordering::Relaxed);

            // Ask the storage before reading anything. A storage that can lose a single
            // piece (see AddTorrentOptions::piece_reclaim) answers for what it holds, and
            // that answer is the have-set: not the bytes, which may still be there after a
            // release, and not a read error, which is per file below and would write off
            // every later piece of the file over one hole. The default says yes, so a
            // storage that can't lose a piece is checked the way it always was.
            let storage_has_piece =
                self.files
                    .has_piece(piece_info.piece_index)
                    .with_context(|| {
                        format!(
                            "error asking the storage if it has piece {}",
                            piece_info.piece_index
                        )
                    })?;

            while piece_remaining > 0 {
                let mut to_read_in_file: usize =
                    std::cmp::min(current_file.remaining(), piece_remaining as u64).try_into()?;

                // Keep changing the current file to next until we find a file that has greater than 0 length.
                while to_read_in_file == 0 {
                    current_file = file_iterator.next().context("broken torrent metadata")?;

                    to_read_in_file =
                        std::cmp::min(current_file.remaining(), piece_remaining as u64)
                            .try_into()?;
                }

                piece_files.push(current_file.index);

                let pos = current_file.processed_bytes;
                piece_remaining -= to_read_in_file;
                current_file.mark_processed_bytes(to_read_in_file as u64);

                if current_file.is_broken || !storage_has_piece {
                    // no need to read.
                    continue;
                }

                if let Err(err) = update_hash_from_file(
                    current_file.index,
                    current_file.fi,
                    pos,
                    self.files,
                    &mut computed_hash,
                    &mut read_buffer,
                    to_read_in_file,
                ) {
                    debug!(
                        "error reading from file {} ({:?}) at {}: {:#}",
                        current_file.index, current_file.fi.relative_filename, pos, &err
                    );
                    current_file.is_broken = true;
                    some_files_broken = true;
                }
            }

            if !storage_has_piece {
                trace!(
                    "piece {} is not in the storage, marking as needed",
                    piece_info.piece_index
                );
                continue;
            }

            if some_files_broken {
                trace!(
                    "piece {} had errors, marking as needed",
                    piece_info.piece_index
                );
                continue;
            }

            if self
                .torrent
                .info()
                .compare_hash(piece_info.piece_index.get(), computed_hash.finish())
                .context("bug: either torrent info broken or we have a bug - piece index invalid")?
            {
                have_pieces.set(piece_info.piece_index.get() as usize, true);
            }
        }

        Ok(have_pieces)
    }

    pub fn check_piece(&self, piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
        if cfg!(feature = "_disable_disk_write_net_benchmark") {
            return Ok(true);
        }

        let mut h = Sha1::new();
        let piece_length = self.torrent.lengths().piece_length(piece_index);
        let mut absolute_offset = self.torrent.lengths().piece_offset(piece_index);
        let mut buf = vec![0u8; std::cmp::min(65536, piece_length as usize)];

        let mut piece_remaining_bytes = piece_length as usize;

        for (file_idx, fi) in self.file_infos.iter().enumerate() {
            let file_len = fi.len;
            if absolute_offset > file_len {
                absolute_offset -= file_len;
                continue;
            }
            let file_remaining_len = file_len - absolute_offset;

            let to_read_in_file: usize =
                std::cmp::min(file_remaining_len, piece_remaining_bytes as u64).try_into()?;
            trace!(
                "piece={}, file_idx={}, seeking to {}",
                piece_index, file_idx, absolute_offset,
            );
            update_hash_from_file(
                file_idx,
                fi,
                absolute_offset,
                self.files,
                &mut h,
                &mut buf,
                to_read_in_file,
            )
            .with_context(|| {
                format!(
                    "error reading {to_read_in_file} bytes, file_id: {file_idx} (\"{:?}\")",
                    fi.relative_filename
                )
            })?;

            piece_remaining_bytes -= to_read_in_file;

            if piece_remaining_bytes == 0 {
                break;
            }

            absolute_offset = 0;
        }

        match self
            .torrent
            .info()
            .compare_hash(piece_index.get(), h.finish())
        {
            Some(true) => {
                trace!("piece={} hash matches", piece_index);
                Ok(true)
            }
            Some(false) => {
                let piece_length = self.torrent.lengths().piece_length(piece_index);
                let absolute_offset = self.torrent.lengths().piece_offset(piece_index);
                warn!(
                    piece_length,
                    absolute_offset, "the piece={} hash does not match", piece_index
                );
                Ok(false)
            }
            None => {
                // this is probably a bug?
                warn!("compare_hash() did not find the piece");
                anyhow::bail!("compare_hash() did not find the piece");
            }
        }
    }

    pub fn read_chunk(
        &self,
        who_sent: PeerHandle,
        chunk_info: &ChunkInfo,
        result_buf: &mut [u8],
    ) -> anyhow::Result<()> {
        if result_buf.len() < chunk_info.size as usize {
            anyhow::bail!("read_chunk(): not enough capacity in the provided buffer")
        }
        let mut absolute_offset = self.torrent.lengths().chunk_absolute_offset(chunk_info);
        let mut buf = result_buf;

        for (file_idx, file_info) in self.file_infos.iter().enumerate() {
            let file_len = file_info.len;
            if absolute_offset > file_len {
                absolute_offset -= file_len;
                continue;
            }
            let file_remaining_len = file_len - absolute_offset;
            let to_read_in_file = std::cmp::min(file_remaining_len, buf.len() as u64).try_into()?;

            trace!(
                "piece={}, handle={}, file_idx={}, seeking to {}. To read chunk: {:?}",
                chunk_info.piece_index, who_sent, file_idx, absolute_offset, &chunk_info
            );
            if file_info.attrs.padding {
                buf[..to_read_in_file].fill(0);
            } else {
                self.files
                    .pread_exact(file_idx, absolute_offset, &mut buf[..to_read_in_file])
                    .with_context(|| {
                        format!("error reading {file_idx} bytes, file_id: {to_read_in_file}")
                    })?;
            }

            buf = &mut buf[to_read_in_file..];

            if buf.is_empty() {
                break;
            }

            absolute_offset = 0;
        }

        Ok(())
    }

    pub fn write_chunk(
        &self,
        who_sent: PeerHandle,
        data: &Piece<ByteBuf<'a>>,
        chunk_info: &ChunkInfo,
    ) -> anyhow::Result<()> {
        let mut absolute_offset = self.torrent.lengths().chunk_absolute_offset(chunk_info);
        let mut data = DoubleBufHelper::new(data.data().0, data.data().1);

        for (file_idx, file_info) in self.file_infos.iter().enumerate() {
            let file_len = file_info.len;
            if absolute_offset > file_len {
                absolute_offset -= file_len;
                continue;
            }

            let remaining_len = file_len - absolute_offset;
            let to_write = std::cmp::min(data.len() as u64, remaining_len).try_into()?;

            trace!(
                "piece={}, chunk={:?}, handle={}, begin={}, file={}, writing {} bytes at {}",
                chunk_info.piece_index,
                chunk_info,
                who_sent,
                chunk_info.offset,
                file_idx,
                to_write,
                absolute_offset
            );
            let slices = data.as_ioslices(to_write);
            debug_assert_eq!(slices[0].len() + slices[1].len(), to_write);
            if !file_info.attrs.padding {
                let written = self
                    .files
                    .pwrite_all_vectored(file_idx, absolute_offset, slices)
                    .with_context(|| {
                        format!(
                            "error writing to file {file_idx} (\"{:?}\")",
                            file_info.relative_filename
                        )
                    })?;
                debug_assert_eq!(written, to_write);
            }
            data.advance(to_write);
            if data.is_empty() {
                break;
            }

            absolute_offset = 0;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        path::Path,
        sync::atomic::{AtomicBool, AtomicU64},
    };

    use anyhow::bail;
    use librqbit_core::{constants::CHUNK_SIZE, lengths::ValidPieceIndex};

    use super::FileOps;
    use crate::{
        CreateTorrentOptions, ManagedTorrentShared, create_torrent, spawn_utils::BlockingSpawner,
        storage::TorrentStorage, tests::test_util::create_default_random_dir_with_torrents,
        torrent_state::TorrentMetadata,
    };

    const PIECE_LEN: u32 = CHUNK_SIZE;
    const NUM_PIECES: u32 = 8;

    // A single-file torrent held whole in memory, minus the pieces the storage says it no
    // longer has - the shape of a storage whose pieces the caller can release.
    struct HoleyStorage {
        bytes: Vec<u8>,
        missing: HashSet<u32>,
        // Whether a missing piece still reads back. On a filesystem the bytes of a
        // released piece are there until the caller deletes them; in a store with one
        // entry per piece they are gone, and a read of them is an error.
        readable_when_missing: bool,
    }

    impl HoleyStorage {
        fn new(bytes: Vec<u8>, missing: impl IntoIterator<Item = u32>, readable: bool) -> Self {
            Self {
                bytes,
                missing: missing.into_iter().collect(),
                readable_when_missing: readable,
            }
        }
    }

    impl TorrentStorage for HoleyStorage {
        fn init(
            &mut self,
            _shared: &ManagedTorrentShared,
            _metadata: &TorrentMetadata,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn pread_exact(&self, _file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            let piece: u32 = (offset / PIECE_LEN as u64).try_into()?;
            if self.missing.contains(&piece) && !self.readable_when_missing {
                bail!("piece {piece} was released");
            }
            let offset: usize = offset.try_into()?;
            buf.copy_from_slice(&self.bytes[offset..offset + buf.len()]);
            Ok(())
        }

        fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> anyhow::Result<()> {
            bail!("not used")
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
            bail!("not used")
        }

        fn has_piece(&self, piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
            Ok(!self.missing.contains(&piece_index.get()))
        }
    }

    // A storage that can't say what it has.
    struct Clueless;

    impl TorrentStorage for Clueless {
        fn init(
            &mut self,
            _shared: &ManagedTorrentShared,
            _metadata: &TorrentMetadata,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn pread_exact(&self, _file_id: usize, _offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            buf.fill(0);
            Ok(())
        }

        fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> anyhow::Result<()> {
            bail!("not used")
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
            bail!("not used")
        }

        fn has_piece(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
            bail!("no idea")
        }
    }

    // A real torrent, so that the hashes are real: NUM_PIECES pieces of random bytes in
    // one file, and the bytes themselves.
    async fn torrent() -> (TorrentMetadata, Vec<u8>) {
        let dir = create_default_random_dir_with_torrents(
            1,
            (PIECE_LEN * NUM_PIECES) as usize,
            Some("test_initial_check"),
        );
        let torrent = create_torrent(
            dir.path(),
            CreateTorrentOptions {
                name: None,
                piece_length: Some(PIECE_LEN),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await
        .unwrap();
        let bytes = std::fs::read(dir.path().join("0.data")).unwrap();
        let torrent_bytes = torrent.as_bytes().unwrap();
        let metadata = TorrentMetadata::new(
            torrent.meta.info.data.validate().unwrap(),
            torrent_bytes,
            Default::default(),
        )
        .unwrap();
        (metadata, bytes)
    }

    fn initial_check(
        metadata: &TorrentMetadata,
        storage: &dyn TorrentStorage,
    ) -> anyhow::Result<Vec<usize>> {
        let have = FileOps::new(&metadata.info, storage, &metadata.file_infos)
            .initial_check(&AtomicU64::new(0), &AtomicBool::new(false))?;
        Ok(have.iter_ones().collect())
    }

    // The full check is what startup falls back to whenever there is no resume data to
    // intersect with the storage: fastresume off (the default), a torrent restarted
    // after a fatal error, a bitfield that didn't match. It has to ask the storage too,
    // or a storage with holes in it is checked as if it were a whole file.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_initial_check_asks_the_storage_before_reading() {
        let (metadata, bytes) = torrent().await;
        let all: Vec<usize> = (0..NUM_PIECES as usize).collect();

        // Nothing missing, nothing to ask about: the check is the hash check.
        let whole = HoleyStorage::new(bytes.clone(), [], false);
        assert_eq!(initial_check(&metadata, &whole).unwrap(), all);

        // One released piece whose bytes are gone. A read of it fails, and a read error
        // used to write the whole file off from there on - every later piece marked
        // needed without a read. The hole is what the storage says it is: one piece.
        let hole = HoleyStorage::new(bytes.clone(), [3], false);
        assert_eq!(
            initial_check(&metadata, &hole).unwrap(),
            vec![0, 1, 2, 4, 5, 6, 7],
            "a hole in the storage wrote off every piece of the file after it"
        );

        // The same hole with the bytes still readable, as on a filesystem where the
        // caller hasn't deleted them yet. They would hash fine, and the storage is still
        // the one that decides: a piece it says is gone is not one we have.
        let stale = HoleyStorage::new(bytes.clone(), [3], true);
        assert_eq!(
            initial_check(&metadata, &stale).unwrap(),
            vec![0, 1, 2, 4, 5, 6, 7],
            "the check believed the bytes over the storage"
        );

        // has_piece is a claim about presence, not a hash check: a piece the storage
        // holds but whose bytes are wrong still fails the check.
        let mut corrupt = bytes.clone();
        corrupt[(5 * PIECE_LEN) as usize] ^= 0xff;
        let corrupt = HoleyStorage::new(corrupt, [3], false);
        assert_eq!(
            initial_check(&metadata, &corrupt).unwrap(),
            vec![0, 1, 2, 4, 6, 7]
        );

        // A storage that can't answer fails the check rather than have it guess: the
        // full check is the last resort, and there is nothing to fall back to from here.
        assert!(initial_check(&metadata, &Clueless).is_err());
    }
}
