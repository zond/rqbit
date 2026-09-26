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
    ///
    /// Count only bytes that were of use. A chunk arriving after we cancelled the request
    /// for it is discarded, and a peer whose pipeline is full of such chunks has moved
    /// nothing however busy its link looks.
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
    /// When the wait scheduled by the last death ends, while the peer is `Dead`; `None`
    /// once it has been re-queued or dropped. What a stall report reads to say how long
    /// until the next dial.
    pub retry_at: Option<Instant>,
    /// Bumped on every death. The wait task spawned by a death carries the value it saw
    /// and re-queues only if it is unchanged, so a peer brought forward and dead again
    /// since is not re-queued twice by a stale sleep.
    pub retry_generation: u64,
}

impl Default for PeerStats {
    fn default() -> Self {
        Self {
            counters: Arc::new(Default::default()),
            backoff: backoff(),
            retry_at: None,
            retry_generation: 0,
        }
    }
}

impl PeerStats {
    pub fn reset_backoff(&mut self) {
        self.backoff = backoff();
    }

    /// Whether this peer has ever handed us a piece that verified. The one fact that
    /// separates an address that is a source from one that is a guess: a tracker's list
    /// is mostly peers behind NAT and peers long gone, and a schedule that treats a peer
    /// that served us like one that never answered is what leaves a thin swarm silent
    /// for the third step of the backoff -- six minutes -- after its one source hangs up.
    pub fn proven(&self) -> bool {
        self.counters
            .downloaded_and_checked_pieces
            .load(Ordering::Relaxed)
            > 0
    }

    /// The wait before this peer is dialled again, and the bookkeeping of it.
    ///
    /// With `starving_retry` set -- the torrent still wants pieces and has fewer live
    /// peers than its floor -- a proven peer waits exactly that long, every time, and its
    /// exponential schedule is left where it was: the flat retry is a state of the
    /// *torrent*, and when the torrent is fed again the peer resumes the schedule it had.
    /// Anything else -- an unproven peer, or a torrent with peers enough -- takes the
    /// next step of the exponential schedule, and `None` when that is exhausted.
    pub fn next_wait(&mut self, starving_retry: Option<Duration>) -> Option<Duration> {
        let wait = match starving_retry {
            Some(retry) if self.proven() => Some(retry),
            _ => self.backoff.next(),
        };
        self.retry_generation = self.retry_generation.wrapping_add(1);
        self.retry_at = wait.map(|wait| Instant::now() + wait);
        wait
    }

    /// The peer is being dialled again: nothing is scheduled any more.
    pub fn retry_taken(&mut self) {
        self.retry_at = None;
    }
}

#[cfg(test)]
mod starving_schedule_tests {
    use super::*;

    fn deliver(stats: &PeerStats) {
        stats
            .counters
            .downloaded_and_checked_pieces
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A proven peer under starvation waits the flat retry, every death, and the
    /// exponential schedule underneath is untouched by it.
    #[test]
    fn a_proven_peer_of_a_starving_torrent_waits_the_flat_retry() {
        let retry = Duration::from_secs(60);
        let mut stats = PeerStats::default();
        deliver(&stats);
        for _ in 0..4 {
            assert_eq!(stats.next_wait(Some(retry)), Some(retry));
        }
        // Fed again: the first exponential step, not the fifth.
        let first = stats.next_wait(None).expect("the schedule has steps left");
        assert!(first < Duration::from_secs(20), "{first:?}");
    }

    /// A peer that never delivered keeps the exponential schedule, starving or not:
    /// those are the addresses that cost a thin swarm its dials.
    #[test]
    fn an_unproven_peer_keeps_the_exponential_schedule_while_starving() {
        let retry = Duration::from_secs(60);
        let mut stats = PeerStats::default();
        let first = stats.next_wait(Some(retry)).unwrap();
        let second = stats.next_wait(Some(retry)).unwrap();
        let third = stats.next_wait(Some(retry)).unwrap();
        assert!(
            first < second && second < third,
            "{first:?} {second:?} {third:?}"
        );
        assert!(
            third > Duration::from_secs(120),
            "the third step is minutes: {third:?}"
        );
    }

    /// A proven peer of a torrent that is not starving is on the ordinary schedule.
    #[test]
    fn a_proven_peer_of_a_fed_torrent_keeps_the_exponential_schedule() {
        let mut stats = PeerStats::default();
        deliver(&stats);
        let first = stats.next_wait(None).unwrap();
        let second = stats.next_wait(None).unwrap();
        assert!(second > first, "{first:?} {second:?}");
    }

    /// Every death is a new generation, and the wait is written down for the report.
    #[test]
    fn a_death_bumps_the_generation_and_records_the_wait() {
        let mut stats = PeerStats::default();
        let before = stats.retry_generation;
        let wait = stats.next_wait(None).unwrap();
        assert_eq!(stats.retry_generation, before + 1);
        let until = stats.retry_at.expect("a wait is recorded");
        assert!(until <= Instant::now() + wait);
        stats.retry_taken();
        assert!(stats.retry_at.is_none());
    }
}
