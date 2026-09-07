use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};

/// How far back "recently" reaches. A peer that has moved nothing for this long has
/// nothing to show for itself; one that moved bytes within it is working. Long enough
/// that a peer between pieces, or choked for a moment, does not read as idle; short
/// enough that the number means now and not "at some point today".
const RECENT_WINDOW: Duration = Duration::from_secs(10);

/// Which [`RECENT_WINDOW`]-long window we are in. The origin is arbitrary and shared by
/// every peer of every torrent -- only differences matter. Whole seconds, so the window
/// has to be whole seconds too.
fn recent_window_index() -> u64 {
    static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
    ORIGIN.elapsed().as_secs() / RECENT_WINDOW.as_secs()
}

/// Bytes a peer moved lately, in whichever direction, kept without a timer, a periodic
/// scan or a lock: two buckets and the index of the window the newer one belongs to.
/// A write that finds the index stale rolls the buckets over itself; a read returns the
/// newer bucket plus whatever is left of the older one, so the number does not fall to
/// zero the instant a window turns -- it spans one to two windows. Two writers racing at
/// a boundary put a few chunks in the neighbouring bucket, which is of no consequence to
/// the ranking this exists for.
#[derive(Default, Debug)]
pub(crate) struct RecentBytes {
    window: AtomicU64,
    current: AtomicU64,
    previous: AtomicU64,
}

impl RecentBytes {
    fn add(&self, bytes: u64) {
        let now = recent_window_index();
        let seen = self.window.load(Ordering::Relaxed);
        if seen != now
            && self
                .window
                .compare_exchange(seen, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let carried = self.current.swap(0, Ordering::Relaxed);
            // Only the window right before this one is still "recent".
            let carried = if now == seen + 1 { carried } else { 0 };
            self.previous.store(carried, Ordering::Relaxed);
        }
        self.current.fetch_add(bytes, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        let now = recent_window_index();
        let seen = self.window.load(Ordering::Relaxed);
        if seen == now {
            self.current.load(Ordering::Relaxed) + self.previous.load(Ordering::Relaxed)
        } else if now == seen + 1 {
            // Nothing written this window yet; the last one still counts.
            self.current.load(Ordering::Relaxed)
        } else {
            0
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct PeerCountersAtomic {
    pub fetched_bytes: AtomicU64,
    pub uploaded_bytes: AtomicU64,
    pub total_time_connecting_ms: AtomicU64,
    pub incoming_connections: AtomicU32,
    pub outgoing_connection_attempts: AtomicU32,
    pub outgoing_connections: AtomicU32,
    pub errors: AtomicU32,
    pub fetched_chunks: AtomicU32,
    pub downloaded_and_checked_pieces: AtomicU32,
    pub downloaded_and_checked_bytes: AtomicU64,
    pub total_piece_download_ms: AtomicU64,
    pub times_stolen_from_me: AtomicU32,
    pub times_i_stole: AtomicU32,
    /// Both directions at once: see [`Self::bytes_moved_recently`].
    recent_bytes: RecentBytes,
}

impl PeerCountersAtomic {
    /// Count bytes that just went one way or the other into the recent window. The
    /// direction is deliberately not kept: what reads this -- the cut a lowered peer
    /// limit makes -- weighs a byte we sent exactly as much as one we received.
    pub(crate) fn on_bytes_moved(&self, bytes: u64) {
        self.recent_bytes.add(bytes);
    }

    /// Bytes moved either way in roughly the last [`RECENT_WINDOW`] (one to two of them,
    /// see [`RecentBytes`]). Unlike `fetched_bytes + uploaded_bytes` this does not simply
    /// favour whoever connected first.
    pub(crate) fn bytes_moved_recently(&self) -> u64 {
        self.recent_bytes.get()
    }

    pub(crate) fn on_piece_completed(&self, piece_len: u64, elapsed: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let elapsed = elapsed.as_millis() as u64;
        self.total_piece_download_ms
            .fetch_add(elapsed, Ordering::Release);
        self.downloaded_and_checked_pieces
            .fetch_add(1, Ordering::Release);
        self.downloaded_and_checked_bytes
            .fetch_add(piece_len, Ordering::Relaxed);
    }

    pub(crate) fn average_piece_download_time(&self) -> Option<Duration> {
        let downloaded_pieces = self.downloaded_and_checked_pieces.load(Ordering::Acquire);
        let total_download_time = self.total_piece_download_ms.load(Ordering::Acquire);
        if total_download_time == 0 || downloaded_pieces == 0 {
            return None;
        }
        Some(Duration::from_millis(
            total_download_time / downloaded_pieces as u64,
        ))
    }
}

fn backoff() -> ExponentialBackoff {
    ExponentialBuilder::new()
        .with_min_delay(Duration::from_secs(10))
        .with_factor(6.)
        .with_jitter()
        .with_max_delay(Duration::from_secs(3600))
        .with_total_delay(Some(Duration::from_secs(86400)))
        .without_max_times()
        .build()
}

#[derive(Debug)]
pub(crate) struct PeerStats {
    pub counters: Arc<PeerCountersAtomic>,
    pub backoff: ExponentialBackoff,
}

impl Default for PeerStats {
    fn default() -> Self {
        Self {
            counters: Arc::new(Default::default()),
            backoff: backoff(),
        }
    }
}

impl PeerStats {
    pub fn reset_backoff(&mut self) {
        self.backoff = backoff();
    }
}
