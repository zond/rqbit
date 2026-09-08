use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::Context;

use rand::{RngExt, seq::IteratorRandom};
use size_format::SizeFormatterBinary as SF;
use tracing::{info, trace, warn};

use librqbit_core::lengths::{Lengths, ValidPieceIndex};

use crate::{
    api::TorrentIdOrHash,
    bitv::BitV,
    bitv_factory::BitVFactory,
    chunk_tracker::{ChunkTracker, compute_selected_pieces},
    file_ops::FileOps,
    type_aliases::{BF, BS, FileStorage},
};

const MAX_FASTRESUME_CHECKS: usize = 64;

// Intersect a have-set loaded from resume data with what the storage still holds.
//
// The resume data is a claim, the storage is the fact, and this can only clear bits,
// never set them - so a storage that can't answer (the default says "yes") leaves it
// exactly as it was. Returns how many pieces it took away.
fn intersect_have_with_storage(
    have: &mut BS,
    lengths: &Lengths,
    has_piece: impl Fn(ValidPieceIndex) -> anyhow::Result<bool>,
) -> anyhow::Result<usize> {
    let mut cleared = 0;
    for id in 0..lengths.total_pieces() {
        let idx = id as usize;
        if !have[idx] {
            continue;
        }
        let Some(piece_id) = lengths.validate_piece_index(id) else {
            continue;
        };
        if !has_piece(piece_id)
            .with_context(|| format!("error asking the storage if it has piece {id}"))?
        {
            have.set(idx, false);
            cleared += 1;
        }
    }
    Ok(cleared)
}

use super::{ManagedTorrentShared, TorrentMetadata, paused::TorrentStatePaused};

pub struct TorrentStateInitializing {
    pub(crate) files: FileStorage,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) only_files: Option<Vec<usize>>,
    pub(crate) checked_bytes: AtomicU64,
    pause_requested: AtomicBool,
    check_running: AtomicBool,
    previously_errored: bool,
}

impl TorrentStateInitializing {
    pub fn new(
        shared: Arc<ManagedTorrentShared>,
        metadata: Arc<TorrentMetadata>,
        only_files: Option<Vec<usize>>,
        files: FileStorage,
        previously_errored: bool,
    ) -> Self {
        Self {
            shared,
            metadata,
            only_files,
            files,
            checked_bytes: AtomicU64::new(0),
            pause_requested: AtomicBool::new(false),
            check_running: AtomicBool::new(false),
            previously_errored,
        }
    }

    pub fn get_checked_bytes(&self) -> u64 {
        self.checked_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn request_pause(&self) {
        self.pause_requested.store(true, Ordering::Relaxed);
    }

    pub(crate) fn clear_pause_request(&self) {
        self.pause_requested.store(false, Ordering::Relaxed);
    }

    pub(crate) fn is_pause_requested(&self) -> bool {
        self.pause_requested.load(Ordering::Relaxed)
    }

    pub(crate) fn is_check_running(&self) -> bool {
        self.check_running.load(Ordering::Acquire)
    }

    pub(crate) fn try_start_check(&self) -> bool {
        self.check_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn finish_check(&self) {
        self.check_running.store(false, Ordering::Release);
    }

    async fn validate_fastresume(
        &self,
        bitv_factory: &dyn BitVFactory,
        have_pieces: Option<Box<dyn BitV>>,
    ) -> Option<Box<dyn BitV>> {
        let mut hp = have_pieces?;
        let actual = hp.as_bytes().len();
        let expected = self.metadata.lengths().piece_bitfield_bytes();
        if actual != expected {
            warn!(
                actual,
                expected,
                "the bitfield loaded isn't of correct length, ignoring it, will do full check"
            );
            return None;
        }

        // Ask the storage before hashing anything. A storage whose pieces the caller can
        // release (see AddTorrentOptions::piece_reclaim) can have lost one behind our
        // back: the bitfield is flushed lazily, so a crash between the deletion and the
        // flush leaves resume data claiming a piece whose bytes are gone. Doing this
        // first also means we don't hash-check a piece that isn't there and throw the
        // whole fastresume away over it.
        let cleared = self
            .shared
            .spawner
            .block_in_place_with_semaphore(|| {
                intersect_have_with_storage(hp.as_slice_mut(), self.metadata.lengths(), |id| {
                    self.files.has_piece(id)
                })
            })
            .await;
        match cleared {
            Ok(0) => {}
            Ok(cleared) => warn!(
                cleared,
                "the resume data claimed pieces the storage no longer has"
            ),
            Err(e) => {
                warn!("error checking what the storage has, will do full check: {e:#}");
                return None;
            }
        }

        let is_broken = self
            .shared
            .spawner
            .block_in_place_with_semaphore(|| {
                let fo = crate::file_ops::FileOps::new(
                    &self.metadata.info,
                    &self.files,
                    &self.metadata.file_infos,
                );

                let mut to_validate = BF::from_boxed_slice(
                    vec![0u8; self.metadata.lengths().piece_bitfield_bytes()].into_boxed_slice(),
                );
                let mut queue = hp.as_slice().to_owned();

                // Validate at least one piece from each file, if we claim we have it.
                for fi in self.metadata.file_infos.iter() {
                    let prange = fi.piece_range_usize();
                    let offset = prange.start;
                    for piece_id in hp
                        .as_slice()
                        .get(fi.piece_range_usize())
                        .into_iter()
                        .flat_map(|s| s.iter_ones())
                        .map(|pid| pid + offset)
                        .take(1)
                    {
                        to_validate.set(piece_id, true);
                        queue.set(piece_id, false);
                    }
                }

                // For all the remaining pieces we claim we have, validate them with decreasing probability.
                let queue = queue
                    .iter_ones()
                    .sample(&mut rand::rng(), MAX_FASTRESUME_CHECKS);

                for (tmp_id, piece_id) in queue.into_iter().enumerate() {
                    let denom: u32 = (tmp_id + 1).min(50).try_into().unwrap();
                    if rand::rng().random_ratio(1, denom) {
                        to_validate.set(piece_id, true);
                    }
                }

                let to_validate_count = to_validate.count_ones();
                for (id, piece_id) in to_validate
                    .iter_ones()
                    .filter_map(|id| {
                        self.metadata
                            .lengths()
                            .validate_piece_index(id.try_into().ok()?)
                    })
                    .enumerate()
                {
                    if fo.check_piece(piece_id).is_err() {
                        return true;
                    }

                    #[allow(clippy::cast_possible_truncation)]
                    let progress = (self.metadata.lengths().total_length() as f64
                        / to_validate_count as f64
                        * (id + 1) as f64) as u64;
                    let progress = progress.min(self.metadata.lengths().total_length());
                    self.checked_bytes.store(progress, Ordering::Relaxed);
                }

                false
            })
            .await;

        if is_broken {
            warn!(
                id = ?self.shared.id,
                info_hash = ?self.shared.info_hash,
                "data corrupted, ignoring fastresume data"
            );
            if let Err(e) = bitv_factory.clear(self.shared.id.into()).await {
                warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, "error clearing bitfield: {e:#}");
            }
            self.checked_bytes.store(0, Ordering::Relaxed);
            return None;
        }

        Some(hp)
    }

    pub async fn check(&self) -> anyhow::Result<TorrentStatePaused> {
        let id: TorrentIdOrHash = self.shared.info_hash.into();
        let bitv_factory = self
            .shared
            .session
            .upgrade()
            .context("session is dead")?
            .bitv_factory
            .clone();
        let have_pieces = if self.previously_errored {
            if let Err(e) = bitv_factory.clear(id).await {
                warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, error=?e, "error clearing bitfield");
            }
            None
        } else {
            bitv_factory
                .load(id)
                .await
                .context("error loading have_pieces")?
        };

        let have_pieces = self.validate_fastresume(&*bitv_factory, have_pieces).await;

        let have_pieces = match have_pieces {
            Some(h) => h,
            None => {
                info!("Doing initial checksum validation, this might take a while...");
                let have_pieces = self
                    .shared
                    .spawner
                    .block_in_place_with_semaphore(|| {
                        FileOps::new(&self.metadata.info, &self.files, &self.metadata.file_infos)
                            .initial_check(&self.checked_bytes, &self.pause_requested)
                    })
                    .await?;
                bitv_factory
                    .store_initial_check(id, have_pieces)
                    .await
                    .context("error storing initial check bitfield")?
            }
        };

        let selected_pieces = compute_selected_pieces(
            self.metadata.lengths(),
            |idx| {
                self.only_files
                    .as_ref()
                    .map(|o| o.contains(&idx))
                    .unwrap_or(true)
            },
            &self.metadata.file_infos,
        );

        let mut chunk_tracker = ChunkTracker::new(
            have_pieces.into_dyn(),
            selected_pieces,
            *self.metadata.lengths(),
            &self.metadata.file_infos,
        )
        .context("error creating chunk tracker")?;

        if self.shared.options.piece_reclaim {
            chunk_tracker.enable_piece_reclaim();
        }

        let hns = chunk_tracker.get_hns();

        info!(
            torrent=?self.shared.id,
            "Initial check results: have {}, needed {}, total selected {}",
            SF::new(hns.have_bytes),
            SF::new(hns.needed_bytes),
            SF::new(hns.selected_bytes)
        );

        // Ensure file lengths are correct, and reopen read-only.
        self.shared
            .spawner
            .block_in_place_with_semaphore(|| {
                for (idx, fi) in self.metadata.file_infos.iter().enumerate() {
                    if self
                        .only_files
                        .as_ref()
                        .map(|v| v.contains(&idx))
                        .unwrap_or(true)
                    {
                        let now = Instant::now();
                        if fi.attrs.padding {
                            continue;
                        }
                        if let Err(err) = self.files.ensure_file_length(idx, fi.len) {
                            warn!(
                                id=?self.shared.id, info_hash = ?self.shared.info_hash,
                                "Error setting length for file {:?} to {}: {:#?}",
                                fi.relative_filename, fi.len, err
                            );
                        } else {
                            trace!(
                                "Set length for file {:?} to {} in {:?}",
                                fi.relative_filename,
                                SF::new(fi.len),
                                now.elapsed()
                            );
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;

        let paused = TorrentStatePaused {
            shared: self.shared.clone(),
            metadata: self.metadata.clone(),
            files: self.files.take()?,
            chunk_tracker,
            streams: Arc::new(Default::default()),
        };
        Ok(paused)
    }
}

#[cfg(test)]
mod tests {
    use super::intersect_have_with_storage;
    use crate::type_aliases::BF;
    use librqbit_core::lengths::Lengths;

    #[test]
    fn test_intersect_have_with_storage() {
        // 10 pieces of 1024 bytes each.
        let lengths = Lengths::new(10 * 1024, 1024).unwrap();
        let mut have =
            BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into_boxed_slice());
        for id in [0usize, 3, 7] {
            have.set(id, true);
        }

        // A storage that can't lose a piece on its own says so, and nothing moves. This
        // is every storage that doesn't implement has_piece().
        assert_eq!(
            intersect_have_with_storage(&mut have, &lengths, |_| Ok(true)).unwrap(),
            0
        );
        assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 3, 7]);

        // The piece the storage no longer has goes, and only that one: the resume data
        // claimed it, but the storage is what decides.
        assert_eq!(
            intersect_have_with_storage(&mut have, &lengths, |id| Ok(id.get() != 3)).unwrap(),
            1
        );
        assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 7]);

        // It can only take pieces away, never add them: a storage that has a piece we
        // never claimed doesn't get to claim it for us, because it can't say whether the
        // bytes are good.
        assert_eq!(
            intersect_have_with_storage(&mut have, &lengths, |_| Ok(true)).unwrap(),
            0
        );
        assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 7]);

        // Pieces we don't claim are not asked about at all - that is what keeps this
        // proportional to what we have, not to the torrent.
        intersect_have_with_storage(&mut have, &lengths, |id| {
            assert!(matches!(id.get(), 0 | 7), "asked about piece {id}");
            Ok(true)
        })
        .unwrap();

        // An error is an error: the caller falls back to a full check rather than making
        // a have-set up.
        assert!(
            intersect_have_with_storage(&mut have, &lengths, |_| anyhow::bail!("no idea")).is_err()
        );
    }
}
