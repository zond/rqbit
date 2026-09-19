//! Coordinates piece download state by wrapping ChunkTracker with inflight piece tracking.
//!
//! This module provides the [`PieceTracker`] type which encapsulates the relationship between
//! queued pieces (in ChunkTracker) and in-flight pieces (being downloaded by a peer).
//!
//! The key invariant maintained is: a piece is in exactly one state at any time:
//! - HAVE (completed)
//! - QUEUED (available to download)
//! - IN_FLIGHT (currently being downloaded)
//! - NOT_NEEDED (not selected for download)
//!
//! IN_FLIGHT does not mean one peer. A piece a stream is parked on is split
//! into claims and several peers fetch it at once -- see [`InflightPiece`].
//!
//! On top of that, a piece dropped through [`PieceTracker::drop_pieces`] is RELEASING
//! until the caller reports back through [`PieceTracker::finish_release`]: it is not
//! HAVE, and nothing may make it HAVE again while the caller is deleting its storage.
//! That state lives in the ChunkTracker, because unlike the in-flight map it has to
//! survive a pause - see [`ChunkTracker::is_releasing`].

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Range,
    time::{Duration, Instant},
};

use buffers::ByteBuf;
use librqbit_core::lengths::ValidPieceIndex;
use peer_binary_protocol::Piece;

use crate::{
    chunk_tracker::{ChunkMarkingResult, ChunkTracker, Reselected},
    type_aliases::{FileInfos, FilePriorities, PeerHandle},
};

/// How many consecutive chunks one peer claims of a split piece at a time.
///
/// Small enough that several peers get a share of one piece, large enough
/// that each keeps its request pipeline full between trips back for the
/// next claim: at 16 KiB a chunk this is 256 KiB, which is over a second of
/// work for the 209 kB/s peer that held a 4 MB piece for twenty seconds in
/// the field while seventeen seeders were turned away from it.
const CLAIM_CHUNKS: u32 = 16;

/// How many claims of one piece a single peer may hold while other peers
/// could be taking them.
///
/// **A peer comes back for another claim when it has *sent* the last one's
/// requests, not when they have arrived.** Its window is
/// `DEFAULT_PEER_REQUEST_WINDOW` -- 128 chunks against a 16-chunk claim --
/// so without this one peer takes eight claims in a few milliseconds and
/// two peers take all sixteen of a 4 MiB piece. The piece is then "the
/// slowest of two", which is the thing splitting exists to prevent: the
/// field of 2026-09-14 blocked 13.4 s on one piece while the swarm was
/// delivering 12-16 MB/s from seventeen seeders.
///
/// **It is a preference and not a limit, and what lifts it is the piece's
/// age.** "Other peers could be taking the rest" cannot be read off a piece
/// -- a peer that has not come round its request loop is nobody's
/// participant -- so the question asked instead is the one comparison
/// ([`Activity::outpaces`]): has the piece been in flight longer than this
/// peer's last chunk took? If it has, anyone coming for the pool would have
/// been here by now, and the share is this peer's; if it has not, the peer
/// is merely round its loop again, and it is sent to look elsewhere and to
/// come back when a chunk of its own lands. Fast peers qualify sooner and
/// so take more; slow ones take less; a peer alone on a piece takes all of
/// it, a round trip at a time; and no constant says how long anyone waits.
/// It used to be lifted by whether the rest of the lookahead held anything
/// for the peer, which in steady state it never does, so the first visitor
/// took every share within a millisecond.
///
/// And "come back" is not "come back when the whole piece it went to is
/// requested": a peer sent away from the head is offered the head again
/// before every chunk it sends elsewhere ([`PieceTracker::acquire_head_share`]).
const CLAIMS_PER_PEER: usize = 2;

/// What a peer has to show for itself: the latency of its own last
/// chunk, read off the live peer by the request loop and handed in with
/// the request ([`AcquireRequest::last_latency`]). The asker's half of the
/// one comparison in `CLAIMS.md`.
#[derive(Debug, Clone, Copy)]
struct Activity {
    /// How long this peer's most recent chunk took, request to arrival;
    /// `None` before the first.
    last_latency: Option<Duration>,
    /// The one reading of the clock this ask is made at; see
    /// [`AcquireRequest::now`].
    now: Instant,
}

impl Activity {
    /// **The one comparison.** Whether this peer may take over work on a
    /// piece the swarm has been at since `since`: its last chunk took less
    /// time than the piece has been in flight. *Had I been asked when this
    /// piece was, I would have delivered by now, and it is not done.* A
    /// piece reserved a moment ago is nobody's to cut, double or share
    /// beyond one's own; one that has been in flight for seconds loses to
    /// any live peer; a peer that has never delivered outpaces nothing.
    ///
    /// The piece's age and not the holder's last delivery: a holder with
    /// seconds of latency and a hundred requests in flight lands a chunk
    /// every few milliseconds and never looked quiet, while its piece took
    /// ten seconds. What the reader waits on is the piece.
    fn outpaces(&self, since: Instant) -> bool {
        let waited = self.now.saturating_duration_since(since);
        self.last_latency.is_some_and(|latency| latency < waited)
    }

    /// The instant [`Self::outpaces`] over `since` turns true -- when a
    /// peer refused now would be admitted -- or `None` for a peer that has
    /// never delivered, which outpaces nothing however long it waits. The
    /// comparison is strict, so one millisecond past the round trip.
    fn ready_at(&self, since: Instant) -> Option<Instant> {
        self.last_latency
            .map(|latency| since + latency + Duration::from_millis(1))
    }
}

/// **When a peer refused on time grounds should ask again**: the earliest
/// of the instants at which each refusal it met -- a cut it did not yet
/// outpace the piece for, a share beyond its two, a claim whose newest
/// holder it did not yet outpace -- would have gone the other way. The
/// walk collects it as it refuses, and the request loop sleeps until it
/// rather than for a fixed spell; see CLAIMS.md, "When a refused peer
/// asks again". `None` when nothing time-based was refused: a fresh peer,
/// or a lookahead with nothing this peer could ever join.
#[derive(Debug, Default, Clone, Copy)]
struct Retry(Option<Instant>);

impl Retry {
    fn refused(&mut self, activity: Activity, since: Instant) {
        if let Some(at) = activity.ready_at(since) {
            self.0 = Some(self.0.map_or(at, |best| best.min(at)));
        }
    }
}

/// The answer to a peer over its share of a piece whose remaining shares
/// other peers may still be coming for: come back when you have delivered
/// something. See [`CLAIMS_PER_PEER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Crowded;

/// One peer's share of a piece: which chunks it claimed, and since when.
#[derive(Debug, Clone)]
pub struct Participant {
    pub peer: PeerHandle,
    /// Which connection to `peer` claimed it: see [`AcquireRequest::connection`].
    pub connection: u64,
    /// Chunk indices *within the piece* this participant is fetching.
    pub chunks: Range<u32>,
    /// When this participant took the claim. How long we have waited on a
    /// holder that has delivered nothing here yet ([`Activity::outpaces`]),
    /// and the tie-break in [`InflightPiece::stalled_claim`], which ranks
    /// on how much of a claim is still missing first.
    started: Instant,
    /// When this holder last delivered a chunk of this piece, stamped by
    /// the write path ([`PieceTracker::note_delivery`]); `None` until it
    /// has. **Diagnostics only**: no rule reads it -- every takeover is
    /// measured against the piece's age or the claim's hand-out
    /// ([`Activity::outpaces`]) -- and what it feeds is
    /// [`ClaimSnapshot::waited`] in the blocked-read line.
    last_delivery: Option<Instant>,
}

impl Participant {
    /// When this holder last delivered a chunk of the piece, or `None` if
    /// it never has. For the wiring test of the write path's stamp.
    #[cfg(test)]
    pub(crate) fn last_delivery(&self) -> Option<Instant> {
        self.last_delivery
    }
}

/// One holder of one claim, as a diagnostic line reads it: who, which
/// chunks, how many are still missing, how long we have waited on its last
/// delivery (or since it was asked, if it has delivered nothing), and the
/// latency of its own last chunk, so a log of a read that blocked says why
/// nobody took the claim over. Note that `waited` is not what the rules
/// compare: a takeover is measured against the piece's age (a cut, a share
/// beyond two) or the claim's newest hand-out (a join), never against a
/// holder'"'"'s last delivery.
#[derive(Debug, Clone)]
pub struct ClaimSnapshot {
    pub peer: PeerHandle,
    pub chunks: Range<u32>,
    pub missing: u32,
    pub waited: Duration,
    /// Filled in by the live state, which is where the peer's latency
    /// lives; `None` for a peer that has never delivered, or that is gone.
    pub latency: Option<Duration>,
}

/// How far a peer will go for a share of a piece already in flight.
///
/// **Double work is worth paying for where a deadline is, and nowhere
/// else.** A piece is split the moment a stream waits on it, and a peer's
/// request window is 128 chunks against a 16-chunk claim -- so two peers
/// can hold every claim of a 4 MiB piece before either has delivered a
/// byte. A piece at the head of the window is therefore parked on two
/// peers, and if either is slow the read blocks on it. Further out there
/// is a whole piece of playback for one copy to arrive in, and the field
/// measured what fetching the whole lookahead twice costs: 415 MB fetched
/// against 281 MB verified on 2026-09-14.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Share {
    /// Only work nobody holds.
    Unclaimed,
    /// Failing that, another copy of a claim whose newest holder this
    /// peer outpaces.
    OrDuplicate,
}

/// The default for [`PieceTracker::deadline_pieces`] -- how many pieces at
/// the head of the lookahead are split, and may have a claim of theirs
/// fetched twice; see [`Share`]. Two, because the reader is blocked on the
/// first and about to be blocked on the second, and every piece after
/// those has a whole piece of playback to arrive in -- **when a piece
/// arrives in less than that.** On a swarm where one does not, the reader
/// walks into pieces still in flight and waits for their slowest claim;
/// the embedder that sees the stall raises the count
/// ([`PieceTracker::set_deadline_pieces`]), and this tracker tells it how
/// long its split pieces have been taking
/// ([`PieceTracker::median_completion`]) so it knows where to
/// start.
pub const DEFAULT_DEADLINE_PIECES: usize = 2;

/// How many completed pieces the completion median is over.
///
/// Sixteen was a seek's worth: after one, the ring filled with the split
/// pieces at the new head -- fast, several peers on each -- and the median
/// fell to under three seconds, then the whole pieces reserved deep in the
/// window by slow peers came in at eight, and the depth an embedder sized
/// from it flapped between three and eight within forty seconds (field,
/// 2026-09-17 12:22). Sixty-four is about seventy-five seconds of a 4 MiB
/// piece film: one seek's burst is a sixth of the sample and moves the
/// median, not swings it.
const COMPLETION_SAMPLES: usize = 64;

/// Tracks a piece currently being downloaded.
///
/// **A piece may have more than one peer on it.** A piece a stream is
/// parked on is split into [`CLAIM_CHUNKS`]-sized claims so that several
/// peers fetch it at once, because the wire protocol asks for chunks --
/// `Request { index, begin, length }` -- and `chunk_status` records them
/// globally, so two peers filling different chunks of one piece was always
/// safe. Only this map made it exclusive, and exclusivity is what left a
/// blocked read waiting twenty seconds on one slow peer.
///
/// A piece nobody is waiting on is still claimed whole by one peer: it has
/// no deadline, splitting it would only cost lock round-trips, and single
/// ownership is what lets a bad piece be blamed on the peer that sent it.
#[derive(Debug)]
pub struct InflightPiece {
    /// Every peer fetching part of this piece. Never empty: the piece
    /// leaves the map when the last participant goes.
    ///
    /// A peer can hold more than one entry -- the request loop comes back
    /// for another claim as soon as it has sent the requests for the last,
    /// not when they have arrived. What it must not hold is an entry for a
    /// claim that is entirely on disk; [`Self::claim`] retires those, and
    /// [`Self::stalled_claim`] and [`Self::release`] ignore any it has not
    /// got to yet.
    participants: Vec<Participant>,
    /// Claims nobody has taken yet, in ascending order.
    unclaimed: VecDeque<Range<u32>>,
    /// When the first participant started: how long the piece has been in
    /// flight, which is what every takeover on it is measured against
    /// ([`Activity::outpaces`]) and what the completion median samples. A
    /// steal does not reset it; the steal rule reads the holder's own
    /// [`Participant::started`] instead.
    started: Instant,
}

impl InflightPiece {
    /// The first participant, with the rest of the piece left unclaimed
    /// when `split`, or claimed whole when not.
    ///
    /// **A claim already on disk is nobody's to take.** A piece put back
    /// with its chunk marks kept (`Chunks::Keep`: a release that broke
    /// nothing, a pause) comes here with some of its claims entirely
    /// landed. Handed out, such a claim costs its holder a trip round the
    /// request loop to find nothing to ask for; left in the pool, it would
    /// be that trip for every peer in turn. So claims with nothing
    /// `missing` are never made.
    fn new(
        peer: PeerHandle,
        connection: u64,
        chunks_in_piece: u32,
        split: bool,
        missing: impl Fn(&Range<u32>) -> u32,
        now: Instant,
    ) -> (Self, Range<u32>) {
        let mut unclaimed = VecDeque::new();
        let first = if split {
            let mut start = 0;
            while start < chunks_in_piece {
                let end = (start + CLAIM_CHUNKS).min(chunks_in_piece);
                if missing(&(start..end)) > 0 {
                    unclaimed.push_back(start..end);
                }
                start = end;
            }
            unclaimed.pop_front().unwrap_or(0..chunks_in_piece)
        } else {
            0..chunks_in_piece
        };
        let started = now;
        (
            Self {
                participants: vec![Participant {
                    peer,
                    connection,
                    chunks: first.clone(),
                    started,
                    last_delivery: None,
                }],
                unclaimed,
                started,
            },
            first,
        )
    }

    /// Hand this peer the next unclaimed share; failing that, another copy
    /// of whichever claim is lagging. `None` when the piece is entirely
    /// spoken for and every claim still missing chunks was handed to its
    /// newest holder too recently for this peer to have out-delivered it.
    ///
    /// `missing` answers how many chunks of a claim have not arrived --
    /// [`ChunkTracker::chunks_missing`], which is the only place that knows.
    /// Nothing in here records chunk arrival, and nothing should: two peers
    /// filling different chunks of one piece is safe precisely because
    /// arrival is recorded once, globally, and not per peer.
    fn claim(
        &mut self,
        peer: PeerHandle,
        connection: u64,
        missing: impl Fn(&Range<u32>) -> u32,
        share: Share,
        activity: Activity,
        retry: &mut Retry,
        retired: &mut Vec<Range<u32>>,
    ) -> Result<Option<Range<u32>>, Crowded> {
        // This connection's finished shares are over: every chunk of them
        // is on disk, and it owes the piece nothing more for them. Left
        // in, they are what `stalled_claim` ranks and `release` puts back
        // -- a peer four claims into a piece carried four entries, of which
        // three were bytes long since on disk.
        //
        // Only this connection's, and only here. It is back here because it
        // has *sent* every request of them, not because they have landed: a
        // holder that lost a duplicate race is sitting on a finished claim
        // with its requests still out, and once the claim is gone
        // `overtaken_by` cannot find it to cancel them when the piece
        // completes. So what retires is handed back in `retired`, and the
        // caller cancels whatever this connection still has out for it
        // (review #28); for a claim it delivered itself that is nothing.
        self.participants.retain(|p| {
            let finished =
                p.peer == peer && p.connection == connection && missing(&p.chunks) == 0;
            if finished {
                retired.push(p.chunks.clone());
            }
            !finished
        });
        let chunks = match self.unclaimed.front() {
            // Room for this peer on this piece, so take the next share.
            Some(_) if self.held_by(peer) < CLAIMS_PER_PEER => {
                self.unclaimed.pop_front().expect("front() just answered")
            }
            // **Over its share, and somebody else may be coming for the
            // rest.** The share is this peer's if the piece has been in
            // flight longer than its own last chunk took: had the others
            // been coming they would have taken it by now. Twenty peers
            // arriving within milliseconds find a piece younger than any
            // of their round trips and spread it; three peers leave shares
            // in the pool, and whoever returns half a second later takes
            // them. See [`CLAIMS_PER_PEER`].
            Some(_) => {
                if !activity.outpaces(self.started) {
                    retry.refused(activity, self.started);
                    return Err(Crowded);
                }
                self.unclaimed.pop_front().expect("front() just answered")
            }
            None if share == Share::Unclaimed => return Ok(None),
            None => match self.stalled_claim(peer, &missing, activity, retry) {
                Some(chunks) => chunks,
                None => return Ok(None),
            },
        };
        self.participants.push(Participant {
            peer,
            connection,
            chunks: chunks.clone(),
            started: activity.now,
            last_delivery: None,
        });
        Ok(Some(chunks))
    }

    /// **A piece reserved whole is split when a stream reaches it.**
    ///
    /// Splitting happens where a piece is reserved, and only at the head of
    /// the lookahead; a piece reserved whole while deeper, or from the
    /// ordinary queue beyond the lookahead, keeps its single claim as the
    /// stream advances onto it -- and in steady-state playback that is
    /// nearly every piece the stream ever reaches, since the swarm has
    /// reserved the pieces ahead of the window long before the window gets
    /// there. Left alone, the head piece is then one peer's whole claim
    /// with `unclaimed` empty, nothing for a second copy to double, and the
    /// steal wants 10x: the field's read blocked on one slow peer's piece
    /// while sixteen others were turned away, with the whole splitting
    /// machinery standing unused beside it.
    ///
    /// So the piece is cut here, as it would have been at the head: the
    /// holder keeps the claim it is currently delivering into -- the first
    /// with anything missing, since it requests in order -- and every claim
    /// after that goes to the pool for the peers now arriving. Claims
    /// already on disk need nobody. The holder's requests for chunks it no
    /// longer holds are still on the wire and still land; that is up to a
    /// window's worth of duplication, paid once per piece, which is why it
    /// is not cut for just anyone.
    ///
    /// **Cut by a peer that outpaces the holder** ([`Activity::outpaces`]).
    /// The arriving peer is taking over chunks the holder has requests out
    /// for, and the one honest ground for that is the same one a second
    /// copy stands on: this peer's last chunk took less time than we have
    /// been waiting on the holder. A healthy holder is nearly impossible to
    /// outpace, so it is nearly never cut; a stalled one loses to anyone
    /// live; a peer that has never delivered cuts nothing and goes to the
    /// ordinary queue to prove itself.
    ///
    /// `true` when anything was cut.
    fn split_whole(
        &mut self,
        peer: PeerHandle,
        missing: impl Fn(&Range<u32>) -> u32,
        activity: Activity,
    ) -> bool {
        let since = self.started;
        let [holder] = self.participants.as_mut_slice() else {
            return false;
        };
        // Never by its own holder. The holder outpacing its own piece says nothing
        // about anybody coming for the rest of it, and the cut would only move the
        // tail it already has requests out for into the pool -- where it takes it
        // back itself, one claim at a time, or leaves it to be fetched twice.
        if holder.peer == peer {
            return false;
        }
        // A refusal here names no instant of its own: the holder of a whole
        // piece is the newest holder of its one claim, and `stalled_claim`
        // records that claim's instant for the same asker a moment later.
        if !self.unclaimed.is_empty()
            || holder.chunks.end.saturating_sub(holder.chunks.start) <= CLAIM_CHUNKS
            || !activity.outpaces(since)
        {
            return false;
        }
        let whole = holder.chunks.clone();
        let mut claims = Vec::new();
        let mut start = whole.start;
        while start < whole.end {
            let end = (start + CLAIM_CHUNKS).min(whole.end);
            claims.push(start..end);
            start = end;
        }
        let Some(current) = claims.iter().position(|claim| missing(claim) > 0) else {
            // Every chunk is on disk: nothing to cut, and the piece is about
            // to complete.
            return false;
        };
        holder.chunks = claims[current].clone();
        // Only what is still missing goes to the pool. A holder requests in
        // order, but a piece handed out again keeps the chunks earlier peers
        // left (`release_pieces_owned_by`), so a claim past the current one
        // can be on disk already -- and a peer handed one would walk it,
        // find nothing to ask for, and come back.
        self.unclaimed.extend(
            claims
                .into_iter()
                .skip(current + 1)
                .filter(|claim| missing(claim) > 0),
        );
        true
    }

    /// The claim most worth another peer, or `None`.
    ///
    /// **Only a claim whose newest holder this peer outpaces**
    /// ([`Activity::outpaces`], measured from that holder's hand-out): its
    /// own last chunk took less time than the claim has now been with
    /// whoever got it last. *Had I been handed this when they were, I would
    /// have delivered it by now, and it is not done.* That is the one rule
    /// for every takeover, and it is what makes this a rescue and not a
    /// duplicate: a healthy holder finishes sixteen chunks within one of
    /// its round trips, so a claim still open that long is joined only by
    /// somebody faster than its newest holder, and a silent one is joined
    /// by anyone live. It replaces a cap of two holders, which the field of
    /// 2026-09-15 found full on a claim held by a peer that had delivered
    /// nothing in fourteen seconds and a doubler with five seconds of
    /// latency, with every faster peer turned away from it for six seconds.
    /// Never this peer's own. Of those, the one with the most left to
    /// fetch, then the oldest -- what gates the piece is the work remaining
    /// on its slowest claim.
    ///
    /// **Whatever the claim has delivered so far.** A first version doubled
    /// only claims that had delivered nothing, on the argument that a
    /// claim with chunks landing is being fetched. The field of 2026-09-15
    /// showed what that misses: a holder trickling a chunk a second is
    /// "being fetched" and never rescued, and the piece completes when it
    /// finishes its sixteen chunks -- 24 and 30 seconds, with fifteen and
    /// twenty-two seeders connected. Two healthy peers still never double
    /// each other: a holder with a window of requests out lands a chunk
    /// every few milliseconds, so the wait on it never reaches anyone's
    /// round trip.
    fn stalled_claim(
        &self,
        peer: PeerHandle,
        missing: impl Fn(&Range<u32>) -> u32,
        activity: Activity,
        retry: &mut Retry,
    ) -> Option<Range<u32>> {
        let mut best: Option<(u32, Instant, Range<u32>)> = None;
        for candidate in &self.participants {
            let left = missing(&candidate.chunks);
            if left == 0 {
                continue;
            }
            // Every holder of this claim: the newest is who the asker is
            // measured against, and one of them must not be the asker.
            let (newest, mine) = self
                .participants
                .iter()
                .filter(|p| p.chunks == candidate.chunks)
                .fold((candidate.started, false), |(newest, mine), p| {
                    (newest.max(p.started), mine || p.peer == peer)
                });
            if mine {
                continue;
            }
            if !activity.outpaces(newest) {
                retry.refused(activity, newest);
                continue;
            }
            let better = match best.as_ref() {
                None => true,
                Some((best_left, best_started, _)) => {
                    left > *best_left || (left == *best_left && candidate.started < *best_started)
                }
            };
            if better {
                best = Some((left, candidate.started, candidate.chunks.clone()));
            }
        }
        best.map(|(_, _, chunks)| chunks)
    }

    /// How many claims of this piece `peer` holds.
    fn held_by(&self, peer: PeerHandle) -> usize {
        self.participants.iter().filter(|p| p.peer == peer).count()
    }

    /// Whether this peer already has a share.
    pub fn has_peer(&self, peer: PeerHandle) -> bool {
        self.participants.iter().any(|p| p.peer == peer)
    }

    /// Take one connection's shares back, returning their chunks to the
    /// unclaimed pool. True when it held any.
    ///
    /// A share goes back whole, including chunks the connection had already
    /// delivered: `chunk_status` knows which those are and re-fetching one
    /// costs a duplicate, where wiping the piece -- what a single-owner
    /// release does -- costs everything every *other* peer delivered.
    ///
    /// A share it had delivered *entirely* does not go back at all. There
    /// is nothing left in it to fetch, so the pool would be handing the
    /// next peer a quarter of a megabyte that is already on disk; and the
    /// piece does not need it back to finish, because finishing is every
    /// chunk marked, which those already are.
    fn release(
        &mut self,
        peer: PeerHandle,
        connection: u64,
        missing: impl Fn(&Range<u32>) -> u32,
    ) -> bool {
        let mut freed = Vec::new();
        self.participants.retain(|p| {
            if p.peer == peer && p.connection == connection {
                freed.push(p.chunks.clone());
                false
            } else {
                true
            }
        });
        if freed.is_empty() {
            return false;
        }
        // Not a claim somebody still holds, and not one the pool already
        // has. `stalled_claim`'s comparison does not make that safe,
        // because `claim` pops the unclaimed pool without looking at who
        // holds what, so an entry in the pool is a hand-out whatever else
        // is going on. A claim put back while its other holder is still
        // fetching it therefore goes out again immediately -- to a third
        // peer nobody measured, or, when the holder is the next to ask,
        // straight back to the holder, which then finds every chunk of it
        // already in flight with itself and sends nothing. That is the field log's "we already
        // requested ChunkInfo { piece_index: 5563, chunk_index: 0 }" and
        // its fifteen siblings: one whole claim, handed to one peer twice.
        for chunks in freed {
            if missing(&chunks) == 0
                || self.participants.iter().any(|p| p.chunks == chunks)
                || self.unclaimed.contains(&chunks)
            {
                continue;
            }
            self.unclaimed.push_back(chunks);
        }
        self.unclaimed
            .make_contiguous()
            .sort_by_key(|range| range.start);
        true
    }
}

/// What [`PieceTracker::walk_lookahead`] came back with.
struct Walk {
    /// The share it took, if any.
    taken: Option<AcquireResult>,
    /// When to ask again, if every refusal on the way was one that time
    /// would lift; see [`Retry`].
    retry: Retry,
    /// The first in-flight piece it could not join: the one a stream
    /// reaches soonest, and the steal candidate.
    held_priority_piece: Option<ValidPieceIndex>,
    /// Whether a piece turned this peer away for being over its share
    /// while shares remain; see [`CLAIMS_PER_PEER`].
    crowded: bool,
}

impl Walk {
    fn taken(result: AcquireResult) -> Self {
        Walk {
            taken: Some(result),
            retry: Retry::default(),
            held_priority_piece: None,
            crowded: false,
        }
    }
}

/// Result of attempting to acquire a piece.
#[derive(Debug)]
pub enum AcquireResult {
    /// A share of a piece was reserved: its chunk indices within the piece.
    Reserved {
        piece: ValidPieceIndex,
        chunks: Range<u32>,
    },
    /// A piece was stolen from a slower peer.
    Stolen {
        piece: ValidPieceIndex,
        chunks: Range<u32>,
        from_peer: PeerHandle,
    },
    /// No pieces are available for this peer. `retry_at` is when a
    /// refusal it met on the way would be lifted by time alone -- a claim
    /// or a whole head piece it will outpace once they have been in flight
    /// for its own round trip -- or `None` when only an event can change
    /// the answer; see [`Retry`].
    NoneAvailable { retry_at: Option<Instant> },
    /// Nothing for this peer right now, but a piece it is over its share of
    /// still has shares other peers may be coming for: ask again once a
    /// chunk of its own has landed, which is what would make the share its
    /// ([`CLAIMS_PER_PEER`]), or at `retry_at`, when the piece will have
    /// been in flight longer than this peer's round trip and the shares
    /// nobody came for are its.
    Crowded { retry_at: Option<Instant> },
}

/// Parameters for acquiring a piece.
pub struct AcquireRequest<'a, I, P, S>
where
    I: Iterator<Item = ValidPieceIndex>,
    P: Fn(ValidPieceIndex) -> bool,
    S: Fn(ValidPieceIndex) -> bool,
{
    /// The peer requesting a piece.
    pub peer: PeerHandle,
    /// Which connection to that peer is asking. Two connections can share an address in
    /// turn - a peer redialled while its old task is still winding down - and each hands
    /// back only the pieces it reserved: see [`PieceTracker::release_pieces_owned_by`].
    pub connection: u64,
    /// The peer's average piece download time (for steal calculations).
    pub peer_avg_time: Option<Duration>,
    /// How long this peer's most recently arrived chunk took, request to
    /// arrival, or `None` before the first: its side of the one comparison
    /// that lets it take over another peer's work (`CLAIMS.md`).
    pub last_latency: Option<Duration>,
    /// **Now, handed in rather than read.** What this module decides about
    /// who is fetching what turns on how long a claim has been outstanding,
    /// so the clock is a parameter: a test that could not put two claims a
    /// measured distance apart could not test the rule that one peer has
    /// out-delivered another.
    pub now: Instant,
    /// Priority pieces to check first (e.g., for streaming).
    pub priority_pieces: I,
    /// File download priority ordering.
    pub file_priorities: &'a FilePriorities,
    /// File metadata for iterating pieces.
    pub file_infos: &'a FileInfos,
    /// Returns true if the peer has the given piece.
    pub peer_has_piece: P,
    /// Returns true if the piece can be stolen (e.g., not locked for writing).
    pub can_steal: S,
}

/// Coordinates piece download state.
///
/// Wraps a [`ChunkTracker`] with tracking of which pieces are currently being downloaded
/// (in-flight) and by which peer. This ensures that:
///
/// - A piece no stream waits on is assigned to one peer at a time (unless
///   stolen); one a stream waits on is split between the peers that have it
/// - Pieces are properly requeued when a peer dies
/// - State transitions maintain invariants
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
    /// How deep into the lookahead pieces are split; see
    /// [`DEFAULT_DEADLINE_PIECES`] and [`Self::set_deadline_pieces`].
    deadline_pieces: usize,
    /// How long the last [`COMPLETION_SAMPLES`] pieces took from their
    /// first claim to their last chunk, oldest first; see
    /// [`Self::median_completion`].
    completions: VecDeque<Duration>,
    /// Whether an acquisition since the last [`Self::take_pool_changed`]
    /// put shares in a pool -- a piece reserved split at the head, or a
    /// whole one cut there. What an idle peer waiting for something to
    /// join has to be told about, since nothing else it waits on says it.
    pool_changed: bool,
    /// Finished claims the asking connection was retired from since the
    /// last [`Self::take_retired_claims`]; see [`InflightPiece::claim`].
    retired: Vec<(ValidPieceIndex, Range<u32>)>,
    /// Who has written into each piece that is not on disk yet, stamped by
    /// the write path ([`Self::note_delivery`]). Outlives the in-flight
    /// entry, because the chunks do: a piece re-queued keeping its chunks
    /// carries what earlier peers put in it. Read when the piece's hash
    /// fails, which is the only thing that asks who filled it; see
    /// [`Self::take_writers`].
    writers: HashMap<ValidPieceIndex, Vec<PeerHandle>>,
}

impl PieceTracker {
    /// Hands `piece` to `peer` whole, whatever state it is in -- the one
    /// thing no route in the tracker is meant to do to a finished piece,
    /// for the test that a chunk delivered on such a share is not written.
    #[cfg(test)]
    pub(crate) fn reserve_whole_for_test(
        &mut self,
        piece: ValidPieceIndex,
        peer: PeerHandle,
        now: Instant,
    ) {
        let _ = self.reserve_piece(piece, peer, 0, false, now);
    }

    // === CONSTRUCTION ===

    /// Create a new PieceTracker wrapping the given ChunkTracker.
    pub fn new(chunks: ChunkTracker) -> Self {
        Self {
            chunks,
            inflight: HashMap::new(),
            deadline_pieces: DEFAULT_DEADLINE_PIECES,
            completions: VecDeque::new(),
            pool_changed: false,
            retired: Vec::new(),
            writers: HashMap::new(),
        }
    }

    /// Whether shares were put in a pool since this was last asked -- a
    /// piece reserved split at the head of the lookahead, or a whole one
    /// cut there -- and clears it. The caller wakes the idle request loops
    /// on `true`: they wait for exactly this, and nothing else they wait on
    /// announces it (CLAIMS.md, "When a refused peer asks again").
    pub fn take_pool_changed(&mut self) -> bool {
        std::mem::take(&mut self.pool_changed)
    }

    /// Whether `connection` to `peer` should still ask for chunk
    /// `chunk_index` of `piece`: it holds a claim covering it, and the chunk
    /// is not on disk. The request loop asks before each request it sends,
    /// because what it was handed can be taken from under it -- a cut at
    /// the head, a steal, a choke's handback, the piece completing -- and it
    /// sends a share one chunk at a time over as long as the peer takes.
    pub fn still_to_request(
        &self,
        piece: ValidPieceIndex,
        peer: PeerHandle,
        connection: u64,
        chunk_index: u32,
    ) -> bool {
        let chunk = chunk_index..chunk_index + 1;
        self.inflight.get(&piece).is_some_and(|inflight| {
            inflight.participants.iter().any(|p| {
                p.peer == peer && p.connection == connection && p.chunks.contains(&chunk_index)
            })
        }) && self.chunks.chunks_missing(piece, &chunk) > 0
    }

    /// The finished claims the asking connection was retired from since
    /// this was last asked, and clears them. Its requests for them may
    /// still be out -- a holder that lost a duplicate race -- and nothing
    /// else will cancel them once the claim is gone; see
    /// [`InflightPiece::claim`].
    pub fn take_retired_claims(&mut self) -> Vec<(ValidPieceIndex, Range<u32>)> {
        std::mem::take(&mut self.retired)
    }

    /// Every peer that wrote into `piece` since it was last empty, and
    /// forgets them. One of them sent the bytes a failed hash is about --
    /// and if there is more than one, nothing here can say which, which is
    /// what decides whether anybody is blamed for it.
    pub fn take_writers(&mut self, piece: ValidPieceIndex) -> Vec<PeerHandle> {
        self.writers.remove(&piece).unwrap_or_default()
    }

    /// How many pieces at the head of the lookahead are split between the
    /// peers that have them.
    pub fn deadline_pieces(&self) -> usize {
        self.deadline_pieces
    }

    /// Sets how many pieces at the head of the lookahead are split. At
    /// least one: the piece the reader is on is always a deadline piece.
    ///
    /// This is the embedder's knob and this tracker holds no opinion about
    /// where it should sit: it knows how long its pieces take
    /// ([`Self::median_completion`]) but not how fast the reader
    /// consumes them, nor when the reader has actually stalled. What it
    /// guarantees is the mechanics -- pieces inside the count are split,
    /// joinable by anyone who outpaces the newest holder, and asked for
    /// before any chunk sent deeper in ([`Self::acquire_head_share`]).
    pub fn set_deadline_pieces(&mut self, pieces: usize) {
        self.deadline_pieces = pieces.max(1);
    }

    /// How long a piece has been taking, from its first claim to its last
    /// chunk: the median of the last [`COMPLETION_SAMPLES`] completed, or
    /// `None` before any has.
    ///
    /// **Every piece is a sample**, whole or split. The question a horizon
    /// sized from this answers is "how far ahead must the split start for
    /// the piece to be whole when the reader arrives", and until the split
    /// reaches it a piece *is* one peer's whole reservation -- so what a
    /// whole piece takes is the time to cover, and the split pieces, which
    /// several peers fill at once, are the fast end of the sample. A median
    /// of split pieces alone measured the mode it was sizing: a deeper
    /// split made pieces faster, which made the depth shallower, which put
    /// the cut back to a second before the reader.
    ///
    /// The upper of the two middles when the count is even, so a horizon
    /// sized from it errs towards starting a piece earlier rather than
    /// later.
    pub fn median_completion(&self) -> Option<Duration> {
        if self.completions.is_empty() {
            return None;
        }
        let mut sorted: Vec<Duration> = self.completions.iter().copied().collect();
        sorted.sort_unstable();
        Some(sorted[sorted.len() / 2])
    }

    /// Read-only access to the underlying ChunkTracker.
    pub fn chunks(&self) -> &ChunkTracker {
        &self.chunks
    }

    /// Consume the PieceTracker, requeuing any in-flight pieces.
    ///
    /// This is used when pausing a torrent - any pieces that were being downloaded
    /// need to be put back in the queue so they can be re-downloaded on resume.
    pub fn into_chunks(mut self) -> ChunkTracker {
        // Requeue all in-flight pieces so they'll be re-downloaded on resume
        for piece in self.inflight.into_keys() {
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        // **And every piece whose hash check the pause interrupted.** Such a
        // piece is in none of the three sets -- `take_inflight` took it out
        // of the in-flight map before the check, and the check never came
        // back to mark it have -- so it is not requeued above and it is not
        // have, with every chunk marked. It would be carried into the paused
        // tracker like that and never fetched again: `acquire_piece` skips a
        // fully-downloaded piece precisely because one is being checked, and
        // nothing is checking this one any more. Wiped and queued, it is an
        // ordinary piece on resume.
        let total = self.chunks.get_lengths().total_pieces();
        for index in 0..total {
            let Some(piece) = self.chunks.get_lengths().validate_piece_index(index) else {
                continue;
            };
            if !self.chunks.is_piece_have(piece) && self.chunks.is_piece_fully_downloaded(piece) {
                self.chunks.mark_piece_broken_if_not_have(piece);
            }
        }
        self.chunks
    }

    // === PIECE ACQUISITION ===

    /// Attempt to acquire a piece for the requesting peer.
    ///
    /// The acquisition strategy is:
    /// 1. Reserve a priority piece (what a stream is waiting on), or take one back off a
    ///    peer 10x slower than us if the whole priority window is already spoken for
    /// 2. Reserve a queued piece
    /// 3. Steal from a peer 3x slower than us
    ///
    /// Stealing comes last, and never while there is free work left, because it is not
    /// free: the peer robbed has already asked its seeder for those chunks, and the bytes
    /// it is about to receive for them are dropped on arrival. On a slow link a request
    /// window is a minute of queued work, so a steal costs that peer a minute of its
    /// bandwidth. Paying that to start a piece nobody else wanted is a straight loss,
    /// which is why only the priority window -- where the stream waits on that piece and
    /// no other -- may steal before the queue has been drained.
    ///
    /// If `Stolen` is returned, the caller MUST call `peers.on_steal()` to notify
    /// the old peer and update counters.
    pub fn acquire_piece<I, P, S>(&mut self, mut req: AcquireRequest<I, P, S>) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let Walk {
            taken,
            retry,
            held_priority_piece,
            crowded,
        } = self.walk_lookahead(&mut req, usize::MAX);
        if let Some(result) = taken {
            return result;
        }

        if let Some(piece) = held_priority_piece
            && let Some(result) = self.steal_piece(&req, piece, 10.0)
        {
            return result;
        }

        // 2. Then check naturally ordered queued pieces
        // Note: iter_queued_pieces only returns pieces in queue_pieces (not in-flight)
        let queued: Vec<_> = self
            .chunks
            .iter_queued_pieces(req.file_priorities, req.file_infos)
            .collect();

        for piece in queued {
            if (req.peer_has_piece)(piece) && !self.chunks.is_releasing(piece) {
                return self.reserve_piece(piece, req.peer, req.connection, false, req.now);
            }
        }

        // 3. Nothing left to reserve: take the piece that has been in flight longest off
        // a peer 3x slower than us, if there is one.
        if let Some(result) = self.try_steal(&req, 3.0) {
            return result;
        }

        if crowded {
            AcquireResult::Crowded { retry_at: retry.0 }
        } else {
            AcquireResult::NoneAvailable { retry_at: retry.0 }
        }
    }

    /// **The head of the lookahead, offered before every chunk sent
    /// elsewhere.** The two pieces a read is blocked on or about to block
    /// on ([`Self::deadline_pieces`]) -- reserved split if nobody has them, cut if
    /// one peer holds one whole, a pool share or another copy of a lagging
    /// claim if they are in flight; the same rules as [`Self::acquire_piece`]
    /// over the same two pieces, and nothing past them: no whole piece
    /// deeper in, nothing off the ordinary queue, no steal.
    ///
    /// The request loop asks this before each chunk of work it took from
    /// deeper in, because a slot freeing is a chunk landing, which is the
    /// moment the peer's latency was re-measured and the event the one
    /// comparison is about. Without it a peer turned away from a head piece
    /// a few milliseconds old -- which nobody outpaces -- was handed a whole
    /// piece and sent every one of its 256 chunks before asking again, two
    /// request windows away; by the time the head piece was old enough to
    /// share out, every peer that could share it was committed elsewhere.
    /// The field of 2026-09-15: ten of sixteen shares of the head piece in
    /// the pool for two seconds, two peers that outpaced it many times over
    /// each on a whole piece deeper in, and a 5.4 s read.
    pub fn acquire_head_share<I, P, S>(&mut self, mut req: AcquireRequest<I, P, S>) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let deadline_pieces = self.deadline_pieces;
        let walk = self.walk_lookahead(&mut req, deadline_pieces);
        walk.taken.unwrap_or(AcquireResult::NoneAvailable {
            retry_at: walk.retry.0,
        })
    }

    /// The walk over a stream's lookahead in playback order, `depth` pieces
    /// deep at most, counting only the pieces this peer could take.
    fn walk_lookahead<I, P, S>(&mut self, req: &mut AcquireRequest<I, P, S>, depth: usize) -> Walk
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        // 1. Priority pieces: what an active stream is waiting on, in playback order.
        // Reserve the first free one; if every one this peer could take is already being
        // downloaded, remember the first, which is the one a stream reaches soonest.
        let mut held_priority_piece = None;
        // How far into the lookahead this walk has got. Only the pieces a
        // read is blocked on or about to block on are split, or worth
        // fetching twice; see [`Share`] and [`Self::deadline_pieces`]. Counted
        // over the pieces this peer could actually take -- one it does not
        // have, or that is being hash-checked, is not a piece the reader is
        // waiting on us for.
        let mut deep = 0usize;
        let deadline_pieces = self.deadline_pieces;
        // Whether a piece turned this peer away for being over its share
        // while shares remain -- which decides what it waits for before
        // asking again if nothing else turns up; see [`CLAIMS_PER_PEER`].
        let mut crowded = false;
        let activity = Activity {
            last_latency: req.last_latency,
            now: req.now,
        };
        let mut retry = Retry::default();
        for piece in &mut req.priority_pieces {
            if deep >= depth {
                break;
            }
            if self.chunks.is_piece_have(piece)
                || self.chunks.is_releasing(piece)
                // Nor one whose chunks are all in, which is a piece being
                // hash-checked: `take_inflight` drops it from the in-flight
                // map before the check so nobody can steal it, and until
                // the check comes back it is not have, not queued and not
                // in flight. Only this loop can reserve such a piece --
                // `iter_queued_pieces` cannot, its bit is long gone -- and
                // reserving it is pure damage: every chunk fetched for it
                // comes back `PreviouslyCompleted` and is dropped, but only
                // after being written to storage, over a piece the check
                // has by then handed to the storage as complete. It cannot
                // even heal a failed check, because `mark_chunk_downloaded`
                // short-circuits on a piece whose chunks are already all
                // marked and never reports the piece complete again.
                //
                // A dropped piece is deliberately still reachable here --
                // that is how a stream re-fetches what the reclaim took --
                // and its chunks are reset when it goes, so it does not
                // look like this.
                || self.chunks.is_piece_fully_downloaded(piece)
                || !(req.peer_has_piece)(piece)
            {
                continue;
            }
            match self.inflight.get_mut(&piece) {
                // Split from the first peer on: a stream is waiting on this
                // one, and every later peer that turns up takes a share
                // rather than being sent away.
                // **Split it, or hand it whole to one peer.** The same
                // depth decides this and whether a claim may be copied,
                // because they are one question.
                //
                // Handing the deeper ones whole is not only about what
                // they cost. A split piece can never be stolen --
                // `steal_piece` refuses any piece with more than one
                // participant -- and it poisons the only per-peer speed
                // number there is, because `on_piece_completed` credits a
                // whole piece's bytes and elapsed time to whichever peer
                // delivered its last chunk. Splitting everything turned
                // both of those off for the whole lookahead.
                None => {
                    return Walk::taken(self.reserve_piece(
                        piece,
                        req.peer,
                        req.connection,
                        deep < deadline_pieces,
                        req.now,
                    ));
                }
                Some(inflight) => {
                    let tracker = &self.chunks;
                    let share = if deep < deadline_pieces {
                        // A piece that reached the head whole is cut here,
                        // as it would have been had it been reserved here;
                        // see [`InflightPiece::split_whole`].
                        if inflight
                            .split_whole(
                                req.peer,
                                |claim| tracker.chunks_missing(piece, claim),
                                activity,
                            )
                        {
                            self.pool_changed = true;
                        }
                        Share::OrDuplicate
                    } else {
                        Share::Unclaimed
                    };
                    let mut retired = Vec::new();
                    let claimed = inflight.claim(
                        req.peer,
                        req.connection,
                        |claim| tracker.chunks_missing(piece, claim),
                        share,
                        activity,
                        &mut retry,
                        &mut retired,
                    );
                    self.retired
                        .extend(retired.into_iter().map(|chunks| (piece, chunks)));
                    match claimed {
                        Ok(Some(chunks)) => {
                            return Walk::taken(AcquireResult::Reserved { piece, chunks });
                        }
                        Ok(None) => {}
                        // Shares are left that other peers may be coming
                        // for. Remembered, not taken: this peer looks for
                        // work elsewhere first, and if there is none it
                        // asks again once a chunk of its own has landed;
                        // see [`CLAIMS_PER_PEER`].
                        Err(Crowded) => crowded = true,
                    }
                    if held_priority_piece.is_none() && !inflight.has_peer(req.peer) {
                        held_priority_piece = Some(piece);
                    }
                }
            }
            deep += 1;
        }
        Walk {
            taken: None,
            retry,
            held_priority_piece,
            crowded,
        }
    }

    /// Reserve a piece: remove from queue, add to inflight.
    ///
    /// `split` leaves all but the first claim unclaimed, so other peers can
    /// join this piece and this one comes back for more when its share is
    /// done. Without it the single claim covers the whole piece, which is
    /// what every piece no stream is waiting on gets.
    fn reserve_piece(
        &mut self,
        piece: ValidPieceIndex,
        peer: PeerHandle,
        connection: u64,
        split: bool,
        now: Instant,
    ) -> AcquireResult {
        // Should be unreachable: the walk and the queue both refuse a
        // piece that is have or mid-check. Said out loud if it is not,
        // because a peer on a finished piece is how a write lands in a
        // piece the storage has already committed (piece 4342, 2026-09-19)
        // and this names the route that got it here.
        let have = self.chunks.is_piece_have(piece);
        if have || self.chunks.is_piece_fully_downloaded(piece) {
            tracing::warn!(
                piece = piece.get(),
                %peer,
                have,
                "reserving a piece that is already complete"
            );
        }
        self.chunks.reserve_needed_piece(piece);
        let chunks_in_piece = self.chunks.get_lengths().chunks_per_piece(piece);
        let tracker = &self.chunks;
        let (inflight, chunks) = InflightPiece::new(
            peer,
            connection,
            chunks_in_piece,
            split,
            |claim| tracker.chunks_missing(piece, claim),
            now,
        );
        if !inflight.unclaimed.is_empty() {
            self.pool_changed = true;
        }
        self.inflight.insert(piece, inflight);
        AcquireResult::Reserved { piece, chunks }
    }

    /// Try to steal whichever piece has been in flight longest, from a slower peer.
    fn try_steal<I, P, S>(
        &mut self,
        req: &AcquireRequest<I, P, S>,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        // Find the slowest piece from another peer that the stealing peer actually has.
        // The threshold itself is checked by steal_piece: the piece that has been in
        // flight longest is the only candidate either way.
        let (piece, _) = self
            .inflight
            .iter()
            .filter(|(_, info)| !info.has_peer(req.peer))
            // Only a piece one peer holds whole. A split piece has several
            // peers on it and no single owner to take it from, and it is
            // already getting the parallelism a steal would be buying.
            .filter(|(_, info)| info.participants.len() == 1 && info.unclaimed.is_empty())
            .filter(|(p, _)| (req.peer_has_piece)(**p))
            // Ranked by how long its holder has had it, which is what the threshold
            // below reads: a piece keeps its own start across a steal (see there).
            .map(|(p, info)| (*p, info.participants[0].started))
            .min_by_key(|(_, started)| *started)?;

        self.steal_piece(req, piece, threshold)
    }

    /// Take one specific piece off the peer downloading it, if that peer has held it for
    /// `threshold` times as long as a piece takes us and the piece is not being written.
    fn steal_piece<I, P, S>(
        &mut self,
        req: &AcquireRequest<I, P, S>,
        piece: ValidPieceIndex,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let my_avg = req.peer_avg_time?;
        let min_elapsed = Duration::from_secs_f64(my_avg.as_secs_f64() * threshold);

        let info = self.inflight.get(&piece)?;
        // Nothing to take from a piece several peers share, and nothing to
        // take from ourselves.
        let [only] = info.participants.as_slice() else {
            return None;
        };
        let old_peer = only.peer;
        // How long this holder has had it, not how long the piece has been in flight: a
        // piece stolen once is old, and measured on its age the next peer along could
        // take it off the thief at once.
        if old_peer == req.peer || req.now.saturating_duration_since(only.started) < min_elapsed {
            return None;
        }

        // Check can_steal (e.g., per_piece_lock)
        if !(req.can_steal)(piece) {
            return None;
        }

        // Update ownership (piece stays in inflight, just changes owner)
        let info = self.inflight.get_mut(&piece)?;
        let chunks = info.participants[0].chunks.clone();
        info.participants[0] = Participant {
            peer: req.peer,
            connection: req.connection,
            chunks: chunks.clone(),
            started: req.now,
            last_delivery: None,
        };
        // The piece keeps its start. It is what a reader has waited on it for, which is
        // the clock every takeover is measured against (`Activity::outpaces`) and the
        // sample the completion median takes; restarted here, a piece stolen from a
        // stalled peer looked fresh to both.

        Some(AcquireResult::Stolen {
            piece,
            chunks,
            from_peer: old_peer,
        })
    }

    // === PIECE COMPLETION ===

    /// Remove piece from inflight tracking (e.g., after all chunks received).
    ///
    /// Returns download duration if piece was in-flight.
    /// Note: Does NOT mark the piece as downloaded - caller should do hash check
    /// and then call `mark_piece_hash_ok` or `mark_piece_hash_failed`.
    pub fn take_inflight(&mut self, piece: ValidPieceIndex) -> Option<Duration> {
        self.take_inflight_at(piece, Instant::now())
    }

    /// [`Self::take_inflight`] with the clock handed in, which is how a
    /// test measures a completion without waiting for it.
    pub fn take_inflight_at(&mut self, piece: ValidPieceIndex, now: Instant) -> Option<Duration> {
        let inflight = self.inflight.remove(&piece)?;
        let took = now.saturating_duration_since(inflight.started);
        if self.completions.len() == COMPLETION_SAMPLES {
            self.completions.pop_front();
        }
        self.completions.push_back(took);
        Some(took)
    }

    /// Every holder of `piece` as of `now`, for a diagnostic line; empty
    /// for a piece nobody holds. Latency is left for the caller to fill.
    pub fn claims(&self, piece: ValidPieceIndex, now: Instant) -> Vec<ClaimSnapshot> {
        self.participants(piece)
            .iter()
            .map(|holder| ClaimSnapshot {
                peer: holder.peer,
                chunks: holder.chunks.clone(),
                missing: self.chunks.chunks_missing(piece, &holder.chunks),
                waited: now
                    .saturating_duration_since(holder.last_delivery.unwrap_or(holder.started)),
                latency: None,
            })
            .collect()
    }

    /// A chunk of `piece` from `peer` has landed at `now`: the holder's
    /// half of the one comparison ([`Activity::outpaces`]). Every claim the
    /// peer holds on the piece is stamped, since it requests them in order
    /// and a landing on any says it is not stalled here.
    pub fn note_delivery(&mut self, piece: ValidPieceIndex, peer: PeerHandle, now: Instant) {
        // Who wrote into the piece, for the hash check to read if it fails.
        let writers = self.writers.entry(piece).or_default();
        if !writers.contains(&peer) {
            writers.push(peer);
        }
        if let Some(inflight) = self.inflight.get_mut(&piece) {
            for participant in inflight.participants.iter_mut().filter(|p| p.peer == peer) {
                participant.last_delivery = Some(now);
            }
        }
    }

    /// The peers left holding requests for `piece` that `winner` just
    /// finished, and whose outstanding chunks are now bytes we have.
    ///
    /// A split piece ends when the last of its claims arrives, and a
    /// duplicated claim ends when the first of its two copies does -- so
    /// completion routinely leaves other peers mid-claim, asking for what
    /// is already on disk. Everyone but the peer that finished it is
    /// overtaken, including one holding the same claim as the winner, which
    /// is the copy that lost the race and the whole reason to cancel.
    pub fn overtaken_by(&self, piece: ValidPieceIndex, winner: PeerHandle) -> Vec<PeerHandle> {
        self.participants(piece)
            .iter()
            .map(|participant| participant.peer)
            .filter(|peer| *peer != winner)
            .collect()
    }

    /// Every peer with a share of `piece`, for cancelling what the others
    /// still have outstanding once it is complete.
    pub fn participants(&self, piece: ValidPieceIndex) -> &[Participant] {
        self.inflight
            .get(&piece)
            .map(|info| info.participants.as_slice())
            .unwrap_or_default()
    }

    /// Mark piece as downloaded after successful hash verification. Moves the per-file
    /// counts with it: see [`ChunkTracker::mark_piece_downloaded`].
    pub fn mark_piece_hash_ok(&mut self, piece: ValidPieceIndex, file_infos: &FileInfos) {
        self.chunks.mark_piece_downloaded(piece, file_infos);
    }

    /// Mark piece as failed after hash verification failure - requeues the piece.
    pub fn mark_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Release all pieces one connection to a peer owns (on its death, or a choke).
    ///
    /// Moves all pieces owned by the peer from IN_FLIGHT back to QUEUED.
    /// Returns the number of pieces released.
    ///
    /// By connection and not by address alone: a task that is winding down asks after its
    /// address has been dialled again, and the address alone would hand back the new
    /// connection's pieces too. It goes on asking for their chunks and throws away every
    /// one that arrives, since the piece is no longer reserved to it.
    pub fn release_pieces_owned_by(&mut self, peer: PeerHandle, connection: u64) -> usize {
        // A share goes back to the unclaimed pool; the piece itself is only
        // broken when the last peer on it leaves. Breaking it while others
        // are still filling it would throw away their chunks too.
        let mut count = 0;
        let mut abandoned = Vec::new();
        let chunks = &self.chunks;
        for (piece, info) in self.inflight.iter_mut() {
            if info.release(peer, connection, |claim| {
                chunks.chunks_missing(*piece, claim)
            }) {
                count += 1;
                if info.participants.is_empty() {
                    abandoned.push(*piece);
                }
            }
        }
        for piece in abandoned {
            self.inflight.remove(&piece);
            // **An empty participant list is not an empty piece.** It was
            // once: every peer on a piece stayed listed until it left, so
            // the list emptying meant nothing had been delivered by anyone
            // and the wipe was free. Claims retire from the list now -- a
            // connection's fully-delivered shares go when it comes back for
            // more -- so the list can empty over a piece that is most of
            // the way to disk, and the last live holder merely being choked
            // is enough to reach here. Wiping then throws away every chunk
            // every other peer delivered, all of it already paid for, up to
            // a whole piece an event; and it is a *choke*, not a death, so
            // the same peers are still there to be asked for it again.
            //
            // The piece is re-queued either way. What it keeps is the work:
            // requests are filtered against `chunk_status`
            // (`PeerConnection`'s request loop), so whoever picks it up
            // next asks for the chunks that are missing rather than for the
            // piece. A dropped piece is the exception: it goes back to no
            // queue, so nobody picks it up, and its chunks are wiped after all
            // (`ChunkTracker::requeue_piece`).
            if self.chunks.any_chunk_arrived(piece) {
                self.chunks.requeue_piece_keeping_chunks(piece);
            } else {
                self.chunks.mark_piece_broken_if_not_have(piece);
            }
        }
        count
    }

    // === QUERIES ===

    /// Get the inflight info for a piece, if it's currently being downloaded.
    pub fn get_inflight(&self, piece: ValidPieceIndex) -> Option<&InflightPiece> {
        self.inflight.get(&piece)
    }

    /// Check if a piece is currently in-flight.
    #[allow(dead_code)]
    pub fn is_inflight(&self, piece: ValidPieceIndex) -> bool {
        self.inflight.contains_key(&piece)
    }

    /// Get the number of pieces currently in-flight.
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    // === PASS-THROUGH METHODS ===

    /// Mark a chunk as downloaded. Returns the result indicating if the piece is complete.
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        self.chunks.mark_chunk_downloaded(piece)
    }

    /// Drop pieces: see [`ChunkTracker::drop_pieces`]. A piece a peer owns is left alone,
    /// so this never takes anything out of the in-flight map.
    ///
    /// The pieces it returns are claimed until [`Self::finish_release`] is called for
    /// them: the caller is about to delete their storage, and until that is done nothing
    /// may download them again.
    pub fn drop_pieces(
        &mut self,
        file_infos: &FileInfos,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
    ) -> crate::Result<Vec<ValidPieceIndex>> {
        let inflight = &self.inflight;
        self.chunks
            .drop_pieces(file_infos, pieces, |piece| inflight.contains_key(&piece))
    }

    /// The caller is done releasing the storage of these pieces, so they may be
    /// downloaded again. Returns how many of them are queued, i.e. whether anything is
    /// waiting on them.
    pub fn finish_release(&mut self, pieces: impl IntoIterator<Item = ValidPieceIndex>) -> usize {
        self.chunks.finish_release(pieces)
    }

    /// True if the piece was dropped and the caller hasn't finished releasing its storage.
    #[allow(dead_code)]
    pub fn is_releasing(&self, piece: ValidPieceIndex) -> bool {
        self.chunks.is_releasing(piece)
    }

    /// Make previously dropped pieces wanted again. A piece a peer already owns is left
    /// alone: it is being downloaded already.
    pub fn reselect_pieces(
        &mut self,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
    ) -> crate::Result<Reselected> {
        let inflight = &self.inflight;
        self.chunks
            .reselect_pieces(pieces, |piece| inflight.contains_key(&piece))
    }

    /// Update which files are selected for download.
    pub fn update_only_files(
        &mut self,
        file_infos: &FileInfos,
        new_only_files: &HashSet<usize>,
    ) -> anyhow::Result<crate::chunk_tracker::HaveNeededSelected> {
        let inflight = &self.inflight;
        self.chunks
            .update_only_files(file_infos, new_only_files, |piece| {
                inflight.contains_key(&piece)
            })
    }

    /// Hold pieces back from what we announce, or stop holding them back: see
    /// [`ChunkTracker::set_pieces_advertised`]. Nothing to do with in-flight pieces -
    /// what we announce and what we are downloading are separate questions.
    pub fn set_pieces_advertised(
        &mut self,
        pieces: impl IntoIterator<Item = ValidPieceIndex>,
        advertised: bool,
    ) -> usize {
        self.chunks.set_pieces_advertised(pieces, advertised)
    }

    /// Flush the have pieces bitfield to disk.
    pub fn flush_have_pieces(&mut self, flush_async: bool) -> anyhow::Result<()> {
        self.chunks.get_have_pieces_mut().flush(flush_async)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bitv::BitV as BitVTrait, type_aliases::BF};
    use librqbit_core::{constants::CHUNK_SIZE, lengths::Lengths};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn peer(id: u8) -> PeerHandle {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 6881)
    }

    /// Create a simple ChunkTracker for testing.
    /// Creates a torrent with the specified number of pieces, all selected.
    fn make_test_chunk_tracker(num_pieces: u32) -> ChunkTracker {
        // Create a simple single-file torrent
        let piece_length = 16384u32; // 16KB pieces
        let total_length = piece_length as u64 * num_pieces as u64;

        let lengths = Lengths::new(total_length, piece_length).unwrap();

        let bf_len = lengths.piece_bitfield_bytes();

        // No pieces downloaded yet (empty have)
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());

        // All pieces selected
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        for i in 0..num_pieces as usize {
            selected.set(i, true);
        }

        // Single file spanning all pieces
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];

        ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap()
    }

    fn make_test_file_infos(num_pieces: u32) -> FileInfos {
        let piece_length = 16384u64;
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: piece_length * num_pieces as u64,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }]
    }

    fn make_default_file_priorities(file_infos: &FileInfos) -> FilePriorities {
        (0..file_infos.len()).collect()
    }

    // The reclaim tests need more than one chunk per piece: a piece that a peer is
    // halfway through is the whole point of them.
    const RECLAIM_CHUNKS_PER_PIECE: u32 = 4;
    const RECLAIM_PIECE_LEN: u32 = CHUNK_SIZE * RECLAIM_CHUNKS_PER_PIECE;

    fn reclaim_file_infos(num_pieces: u32) -> FileInfos {
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: RECLAIM_PIECE_LEN as u64 * num_pieces as u64,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }]
    }

    fn make_reclaim_tracker(num_pieces: u32) -> PieceTracker {
        let file_infos = reclaim_file_infos(num_pieces);
        let lengths = Lengths::new(
            RECLAIM_PIECE_LEN as u64 * num_pieces as u64,
            RECLAIM_PIECE_LEN,
        )
        .unwrap();
        let bf_len = lengths.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        selected.get_mut(0..num_pieces as usize).unwrap().fill(true);
        let mut chunks =
            ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap();
        chunks.enable_piece_reclaim();
        let mut tracker = PieceTracker::new(chunks);
        // Have everything, so nothing is queued and acquisition has to come from the
        // piece we drop below.
        for id in 0..num_pieces {
            let p = piece(&tracker, id);
            // Same order as the real thing: reserve it, then mark it good.
            tracker.chunks.reserve_needed_piece(p);
            tracker.mark_piece_hash_ok(p, &file_infos);
        }
        tracker
    }

    /// A tracker whose pieces are big enough to split: 64 chunks each,
    /// four [`CLAIM_CHUNKS`] claims. Every other fixture here has one or
    /// four chunks to a piece, which is a single claim, which is why
    /// nothing else in this file exercises splitting at all.
    const SPLIT_CHUNKS_PER_PIECE: u32 = 64;

    fn make_split_tracker(num_pieces: u32) -> (PieceTracker, FileInfos, FilePriorities) {
        make_split_tracker_of(num_pieces, SPLIT_CHUNKS_PER_PIECE)
    }

    /// The same with the piece size spelled out, for the tests that want a
    /// piece of exactly two claims.
    fn make_split_tracker_of(
        num_pieces: u32,
        chunks_per_piece: u32,
    ) -> (PieceTracker, FileInfos, FilePriorities) {
        let piece_len = CHUNK_SIZE * chunks_per_piece;
        let total = piece_len as u64 * num_pieces as u64;
        let file_infos: FileInfos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];
        let lengths = Lengths::new(total, piece_len).unwrap();
        let bf_len = lengths.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        selected.get_mut(0..num_pieces as usize).unwrap().fill(true);
        let chunks = ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap();
        let priorities = make_default_file_priorities(&file_infos);
        (PieceTracker::new(chunks), file_infos, priorities)
    }

    /// Acquire as a named peer, with `priority` offered as the stream's
    /// waiting piece.
    fn acquire_as(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        who: u8,
        priority: Option<ValidPieceIndex>,
    ) -> AcquireResult {
        acquire_at(
            tracker,
            file_infos,
            file_priorities,
            who,
            priority,
            Instant::now(),
        )
    }

    /// [`acquire_as`] with the clock handed in, for the rules that turn on
    /// how long a claim has been outstanding.
    fn acquire_at(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        who: u8,
        priority: Option<ValidPieceIndex>,
        now: Instant,
    ) -> AcquireResult {
        let window: Vec<ValidPieceIndex> = priority.into_iter().collect();
        acquire_with(
            tracker,
            file_infos,
            file_priorities,
            who,
            &window,
            now,
            None,
        )
    }

    /// A peer that has never had a chunk land, asking for `priority` at
    /// `now`, with `latency` as its last chunk's time -- for a peer whose
    /// speed matters but whose delivery timing does not.
    fn acquire_fast(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        who: u8,
        priority: Option<ValidPieceIndex>,
        now: Instant,
        latency: Duration,
    ) -> AcquireResult {
        let window: Vec<ValidPieceIndex> = priority.into_iter().collect();
        acquire_with(
            tracker,
            file_infos,
            file_priorities,
            who,
            &window,
            now,
            Some(latency),
        )
    }

    /// The general form: a peer whose last chunk took `last_latency`,
    /// asking with `window` as the stream's lookahead at `now`.
    fn acquire_with(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        who: u8,
        window: &[ValidPieceIndex],
        now: Instant,
        last_latency: Option<Duration>,
    ) -> AcquireResult {
        tracker.acquire_piece(AcquireRequest {
            peer: peer(who),
            connection: 0,
            peer_avg_time: None,
            last_latency,
            now,
            priority_pieces: window.iter().copied(),
            file_priorities,
            file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        })
    }

    /// Acquire with a whole lookahead offered in playback order, which is
    /// what production hands in and the only way to reach anything that
    /// depends on how deep in the window a piece is.
    fn acquire_in_window(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        who: u8,
        window: &[ValidPieceIndex],
        now: Instant,
    ) -> AcquireResult {
        acquire_with(tracker, file_infos, file_priorities, who, window, now, None)
    }

    /// The head-only ask ([`PieceTracker::acquire_head_share`]) as a fresh
    /// peer, with `window` as the stream's lookahead.
    fn head_share(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        who: u8,
        window: &[ValidPieceIndex],
        now: Instant,
    ) -> AcquireResult {
        tracker.acquire_head_share(AcquireRequest {
            peer: peer(who),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now,
            priority_pieces: window.iter().copied(),
            file_priorities,
            file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        })
    }

    fn reserved_piece(result: AcquireResult) -> ValidPieceIndex {
        match result {
            AcquireResult::Reserved { piece, .. } => piece,
            other => panic!("expected a reservation, got {other:?}"),
        }
    }

    /// The piece a result reserved a share of, or `None` for anything else.
    fn reserved_or_none(result: AcquireResult) -> Option<ValidPieceIndex> {
        match result {
            AcquireResult::Reserved { piece, .. } => Some(piece),
            _ => None,
        }
    }

    fn claimed(result: AcquireResult) -> Range<u32> {
        match result {
            AcquireResult::Reserved { chunks, .. } => chunks,
            other => panic!("expected a reservation, got {other:?}"),
        }
    }

    /// **The piece a stream is parked on is fetched by every peer that has
    /// it, not by one.**
    ///
    /// The field case: a read blocked on one 4 MB piece waited twenty
    /// seconds while seventeen seeders were connected. The piece was
    /// reserved to a 209 kB/s peer, and `acquire_piece` turns every other
    /// peer away from a priority piece already in flight unless it is ten
    /// times faster. Nothing about that was forced by the protocol -- the
    /// wire asks for chunks and `chunk_status` records them globally -- so
    /// here four peers take a quarter of the piece each, and the four
    /// claims are disjoint and cover it exactly.
    #[test]
    fn a_piece_a_stream_waits_on_is_split_between_the_peers_that_have_it() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);

        let shares: Vec<Range<u32>> = (1..=4)
            .map(|who| {
                claimed(acquire_as(
                    &mut tracker,
                    &file_infos,
                    &priorities,
                    who,
                    Some(waited_on),
                ))
            })
            .collect();

        assert_eq!(
            shares,
            vec![0..16, 16..32, 32..48, 48..64],
            "each peer took the next claim of the piece the stream is waiting on"
        );
        assert_eq!(
            tracker.participants(waited_on).len(),
            4,
            "and all four are on it at once"
        );
    }

    /// **A peer alone covers the piece, a share at a time, once the piece is
    /// older than its own round trip.**
    ///
    /// Two shares come freely. Beyond that a share is this peer's only if
    /// the piece has been in flight longer than its last chunk took: had
    /// other peers been coming for the pool, they would have taken it by
    /// then. So a peer coming straight round its loop on a fresh piece is
    /// sent to look elsewhere, and the same peer half a second later takes
    /// the rest, share by share.
    #[test]
    fn a_peer_alone_covers_the_piece_a_share_at_a_time() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let latency = Some(Duration::from_millis(300));
        let ask = |tracker: &mut PieceTracker, now: Instant| {
            acquire_with(
                tracker,
                &file_infos,
                &priorities,
                1,
                &[waited_on],
                now,
                latency,
            )
        };

        assert_eq!(claimed(ask(&mut tracker, t0)), 0..16);
        assert_eq!(claimed(ask(&mut tracker, t0)), 16..32);
        assert!(
            matches!(ask(&mut tracker, ms(100)), AcquireResult::Crowded { .. }),
            "over its share on a piece younger than its own round trip: others may be coming"
        );
        assert_eq!(
            claimed(ask(&mut tracker, ms(400))),
            32..48,
            "the piece has now been in flight longer than this peer's last chunk took, \
             and nobody else came"
        );
        assert_eq!(claimed(ask(&mut tracker, ms(500))), 48..64);
    }

    /// **A peer that outpaces a holder doubles up on its claim.**
    ///
    /// Splitting alone turns "one peer's speed" into "the slowest of four
    /// peers' speed": the piece is not done until its last claim is, so one
    /// straggler still gates a read. A second copy is the rescue -- from a
    /// peer whose last chunk took less time than we have now waited on the
    /// holder, against a claim that has produced nothing in that time.
    /// *Had I been asked when they were, I would have delivered by now.*
    ///
    /// **One comparison, two durations, no constant** (`CLAIMS.md`): the
    /// asker's latency is what its own chunks measured on the wire, and the
    /// wait is on this piece -- nothing a split piece poisons
    /// (`on_piece_completed` credits a whole piece to whoever delivers its
    /// last chunk).
    #[test]
    fn a_peer_that_outpaces_a_holder_doubles_up_on_its_claim() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }

        // Peer 1 fetches the whole of its claim, its chunks taking a
        // tenth of a second each; the other three have delivered nothing
        // ten seconds in.
        deliver(&mut tracker, waited_on, 0..16);
        let second = claimed(acquire_fast(
            &mut tracker,
            &file_infos,
            &priorities,
            1,
            Some(waited_on),
            t0 + Duration::from_secs(10),
            Duration::from_millis(100),
        ));

        assert_eq!(
            second,
            16..32,
            "the oldest claim that has delivered nothing is the one worth a second copy"
        );
        assert_eq!(
            tracker.participants(waited_on).len(),
            4,
            "peer 1's finished claim retired and its second one took its place"
        );
    }

    /// **A peer that has delivered nothing takes no second copy.**
    ///
    /// The waste this cost is what the field measured: 415 MB fetched
    /// against 281 MB verified on 2026-09-14. A peer's request window is
    /// 128 chunks against a 16-chunk claim, so two peers can hold every
    /// claim of a piece before either has delivered a byte -- and every
    /// peer arriving after them used to take a second copy of a claim
    /// nobody could yet call slow, on a piece nobody could yet call
    /// stalled. A peer with no latency to its name outpaces nobody,
    /// however long the holders have sat: it is sent to fetch the next
    /// piece the stream needs, and proves itself there.
    #[test]
    fn a_peer_with_nothing_to_show_for_itself_takes_no_second_copy() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }

        // A fresh peer, an hour later, with every claim still outstanding:
        // it has still proved nothing about this piece.
        assert_ne!(
            reserved_piece(acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                5,
                Some(waited_on),
                t0 + Duration::from_secs(3600),
            )),
            waited_on,
            "a peer that has delivered none of this piece took a second copy of \
             somebody else's claim, instead of fetching a piece nobody had"
        );
    }

    /// **A piece younger than the asker's own latency is left alone.**
    ///
    /// The comparison is *latency < the piece's age*, and the second half
    /// is what keeps a fresh piece from being doubled, cut or over-shared
    /// by a peer that has merely come round its loop. Had the asker been
    /// asked when the piece was, it would not have delivered yet either.
    #[test]
    fn a_piece_younger_than_the_askers_latency_is_left_alone() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }
        deliver(&mut tracker, waited_on, 0..16);

        // Fifty milliseconds in, peer 1's chunks take a hundred: the other
        // claims are nobody's to double yet, and peer 1 goes elsewhere.
        assert_ne!(
            reserved_piece(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                t0 + Duration::from_millis(50),
                Duration::from_millis(100),
            )),
            waited_on,
            "a claim on a fifty-millisecond-old piece was doubled by a peer whose \
             chunks take a hundred"
        );
    }

    /// **A holder's own deliveries do not shield its claims.**
    ///
    /// The piece's age is what a takeover is measured against, not how
    /// recently the holder landed a chunk. A holder with seconds of latency
    /// and a hundred requests in flight lands a chunk every few
    /// milliseconds and would never have looked quiet, while its piece
    /// took ten seconds -- the field's 24 and 30 seconds on a head piece
    /// with a fast swarm. On a ten-second-old piece, a claim that still has
    /// chunks missing is doubled whatever its holder did fifty milliseconds
    /// ago.
    #[test]
    fn a_holders_own_deliveries_do_not_shield_its_claims() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in [1u8, 1, 2, 3] {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }
        deliver(&mut tracker, waited_on, 32..64);
        let later = t0 + Duration::from_secs(10);
        delivered_by(
            &mut tracker,
            waited_on,
            0..8,
            1,
            later - Duration::from_millis(50),
        );

        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                2,
                Some(waited_on),
                later,
                Duration::from_millis(100),
            )),
            16..32,
            "on a ten-second-old piece the claim with the most missing is doubled, \
             whatever its holder landed a moment ago"
        );
    }

    /// **A peer does not double its own claim.**
    ///
    /// A peer fast elsewhere and stalled here -- a tenth of a second a
    /// chunk on the last piece, nothing on this one in ten seconds --
    /// outpaces its own claim's holder by the numbers, since the holder is
    /// itself. A second copy from the same peer is the same bytes queued
    /// twice behind the same stall.
    #[test]
    fn a_peer_does_not_double_its_own_claim() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        // Peer 1 takes one claim; peers 2, 3 and 4 take the rest and
        // finish them. Peer 1's is the one claim left, and it is stalled.
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }
        deliver(&mut tracker, waited_on, 16..64);

        assert_ne!(
            reserved_or_none(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                t0 + Duration::from_secs(10),
                Duration::from_millis(100),
            )),
            Some(waited_on),
            "a peer took a second copy of its own stalled claim"
        );
    }

    /// **A claim already entirely on disk is not handed to anybody.**
    ///
    /// This was the defect. A `Participant` was removed when its peer left
    /// or when the piece ended, never when its claim arrived, so a peer
    /// that had worked through four claims carried four entries and three
    /// of them were finished. `stalled_claim` ranked on `started` over that
    /// list, so "the claim outstanding longest" was, nearly always, one
    /// whose every chunk had landed long ago -- and every free peer that
    /// asked was sent to fetch a quarter of a megabyte we already had.
    ///
    /// Worse than the waste: it is what put several peers on one piece's
    /// writes at the tail of every split piece, which is the precondition
    /// for a chunk landing in a piece the storage has already finished.
    ///
    /// Two claims here, and by the time the fourth peer asks the only one
    /// that is not full is the one that is done. There is nothing left for
    /// it on this piece, and it must be told so rather than sent after
    /// bytes on disk.
    #[test]
    fn a_claim_already_on_disk_is_not_offered_for_duplication() {
        let (mut tracker, file_infos, priorities) = make_split_tracker_of(4, CLAIM_CHUNKS * 2);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();

        assert_eq!(
            claimed(acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                t0
            )),
            0..16
        );
        assert_eq!(
            claimed(acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                2,
                Some(waited_on),
                t0
            )),
            16..32
        );
        // Peer 1 delivers the whole of its claim, a tenth of a second a
        // chunk, and comes back ten seconds in: its own claim is finished
        // and retires, and what is left to double is the one that has
        // delivered nothing in all that time.
        deliver(&mut tracker, waited_on, 0..16);
        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                t0 + Duration::from_secs(10),
                Duration::from_millis(100),
            )),
            16..32
        );

        // And nothing is ever sent after a claim entirely on disk: peer 2
        // finishes the piece, so there is nothing left of it to fetch and
        // the next asker is sent elsewhere.
        deliver(&mut tracker, waited_on, 16..32);
        match acquire_at(
            &mut tracker,
            &file_infos,
            &priorities,
            2,
            Some(waited_on),
            t0 + Duration::from_secs(20),
        ) {
            AcquireResult::Reserved { piece: got, chunks } => assert_ne!(
                got, waited_on,
                "a peer was sent to re-fetch a claim already on disk: {chunks:?}"
            ),
            AcquireResult::NoneAvailable { .. } => panic!("the other pieces are still free"),
            other => panic!("expected a reservation elsewhere, got {other:?}"),
        }
    }

    /// **A piece that comes back with chunks on disk is not split over
    /// them.**
    ///
    /// A pause, or a release that broke nothing, puts a piece back in the
    /// queue with its chunk marks kept. Split again at the head, its first
    /// claims are then entirely landed -- and the first peer to reserve it
    /// was handed one, requested nothing of it, and came round for another;
    /// every peer after found the next such claim in the pool. The claims
    /// made are the ones with something left to fetch.
    #[test]
    fn a_piece_that_comes_back_with_chunks_on_disk_is_not_split_over_them() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        // The first claim and a half landed before the piece went back.
        deliver(&mut tracker, waited_on, 0..24);

        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on)
            )),
            16..32,
            "the first peer was handed a claim entirely on disk"
        );
        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                2,
                Some(waited_on)
            )),
            32..48
        );
    }

    /// **A claim on a piece older than the asker's latency is doubled,
    /// whatever it has delivered.**
    ///
    /// The one rule: the asker's last chunk took less time than the piece
    /// has been in flight. A first version refused any claim with a chunk
    /// on disk as "being fetched", and the field's 24- and 30-second
    /// blocked reads were slow holders nobody was allowed to rescue.
    #[test]
    fn a_claim_on_an_old_piece_is_doubled_whatever_it_has_delivered() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }
        deliver(&mut tracker, waited_on, 0..16);
        deliver(&mut tracker, waited_on, 16..17);
        deliver(&mut tracker, waited_on, 32..33);
        deliver(&mut tracker, waited_on, 48..49);

        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                t0 + Duration::from_secs(10),
                Duration::from_millis(100),
            )),
            16..32,
            "a claim on a ten-second-old piece was left to its holder because it had \
             a chunk on disk"
        );
    }

    /// **A peer that finished a claim stops being one of its holders.**
    ///
    /// It has sent every request of that claim and every one has landed, so
    /// it holds nothing of it on the wire and owes the piece nothing more
    /// for it. Left in the list it is a ghost: something for
    /// `stalled_claim` to rank, something for `release` to hand back,
    /// a hand-out for `stalled_claim` to measure the next joiner against,
    /// and a second cancellation sent to a peer that needs one.
    #[test]
    fn a_peer_that_finished_a_claim_stops_holding_it() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);

        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on)
            )),
            0..16
        );
        deliver(&mut tracker, waited_on, 0..16);
        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on)
            )),
            16..32,
            "and it still comes back for the next claim"
        );

        assert_eq!(
            tracker.participants(waited_on).len(),
            1,
            "one peer, one claim it is actually fetching"
        );
        assert_eq!(
            tracker.overtaken_by(waited_on, peer(9)),
            vec![peer(1)],
            "and it is cancelled once, not once per claim it has been through"
        );
    }

    /// **A claim its holder delivered whole does not go back to the pool
    /// when that holder leaves.**
    ///
    /// `release` hands a departing connection's shares back so somebody
    /// else can finish them. There is nothing to finish in a share whose
    /// every chunk is already marked: the piece completes on those marks
    /// whoever put them there, so handing the share out again buys the
    /// piece nothing and costs the next peer the whole of it over the wire.
    #[test]
    fn a_claim_its_holder_delivered_whole_does_not_come_back() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);

        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on)
            )),
            0..16
        );
        // A second peer, so the piece is not abandoned when the first goes.
        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                2,
                Some(waited_on)
            )),
            16..32
        );
        deliver(&mut tracker, waited_on, 0..16);

        // Peer 1 is choked or dies, having delivered all of its share.
        assert_eq!(tracker.release_pieces_owned_by(peer(1), 0), 1);

        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                3,
                Some(waited_on)
            )),
            32..48,
            "the next peer was handed back a claim that was already on disk"
        );
    }

    /// **A split piece worked down to its last claim is one peer's piece
    /// again.**
    ///
    /// `try_steal` takes a piece off a slower peer only when one peer holds
    /// it and nothing is unclaimed, and retiring finished claims is what
    /// makes that true of a piece a single peer has carried: before, its
    /// four spent entries made it look like four peers sharing, and it was
    /// passed over. It is not sharing -- three of those claims are on disk
    /// and one peer is sitting on the fourth.
    ///
    /// The thief is handed that claim and nothing else, so what the slow
    /// peer already delivered still counts. This only ever fires for a
    /// piece no stream is parked on: while one is, the priority loop offers
    /// a second copy of that same claim first, which costs the incumbent
    /// nothing.
    #[test]
    fn a_split_piece_down_to_its_last_claim_can_be_taken_off_a_slow_peer() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);

        // One peer carries the piece claim by claim, delivering each but
        // the last, which it is still fetching.
        let mut start = 0;
        while start < SPLIT_CHUNKS_PER_PIECE {
            let got = claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
            ));
            assert_eq!(got, start..start + CLAIM_CHUNKS);
            start += CLAIM_CHUNKS;
            if start < SPLIT_CHUNKS_PER_PIECE {
                deliver(&mut tracker, waited_on, got);
            }
        }
        assert_eq!(
            tracker.participants(waited_on).len(),
            1,
            "one peer, on the one claim that is left"
        );

        // A second peer, with no stream parked on the piece and nothing
        // else queued -- the only route to `try_steal`.
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            peer_avg_time: Some(Duration::ZERO),
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        match result {
            AcquireResult::Stolen {
                piece: got,
                chunks,
                from_peer,
            } => {
                assert_eq!(got, waited_on);
                assert_eq!(from_peer, peer(1));
                assert_eq!(
                    chunks,
                    SPLIT_CHUNKS_PER_PIECE - CLAIM_CHUNKS..SPLIT_CHUNKS_PER_PIECE,
                    "the thief takes the unfinished claim, not the whole piece"
                );
            }
            other => panic!("expected the last claim to be taken, got {other:?}"),
        }
    }

    /// **Another peer joins a claim when it outpaces the claim's newest
    /// holder, and not before.**
    ///
    /// Measured from the newest hand-out, not the piece's start: a healthy
    /// second holder finishes sixteen chunks within one of its round trips,
    /// so a third copy is only taken by a peer faster than that -- and
    /// there is no count that stops it, because the field found a claim
    /// held by a silent peer and a five-second doubler, with every faster
    /// peer turned away for six seconds. A peer never joins a claim it
    /// holds a copy of itself, however fast it is.
    #[test]
    fn a_claim_is_joined_by_whoever_outpaces_its_newest_holder() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }

        // Peers 1, 2 and 3 finish their claims, fast; peer 4 has delivered
        // nothing in ten seconds, so its claim is the only candidate.
        deliver(&mut tracker, waited_on, 0..16);
        deliver(&mut tracker, waited_on, 16..32);
        deliver(&mut tracker, waited_on, 32..48);
        let later = t0 + Duration::from_secs(10);
        let fast = Duration::from_millis(100);

        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                later,
                fast,
            )),
            48..64,
            "the one claim that has delivered nothing is the one doubled"
        );

        // Peer 2 outpaces peer 4 just as well, but peer 1 was handed the
        // claim fifty milliseconds ago and peer 2's own chunks take a
        // hundred: peer 1 may still deliver it first.
        match acquire_fast(
            &mut tracker,
            &file_infos,
            &priorities,
            2,
            Some(waited_on),
            later + Duration::from_millis(50),
            fast,
        ) {
            AcquireResult::Reserved { piece: other, .. } => assert_ne!(
                other, waited_on,
                "a claim handed out more recently than the asker's latency is left alone"
            ),
            other => panic!("expected a different piece, got {other:?}"),
        }

        // Two hundred milliseconds after peer 1 took it, the claim is still
        // open: peer 1 has had longer than peer 2 needs, and peer 2 joins.
        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                2,
                Some(waited_on),
                later + Duration::from_millis(200),
                fast,
            )),
            48..64,
            "a third copy, because the second holder has been outpaced too"
        );

        // And peer 1 does not join the claim it is itself a copy of.
        match acquire_fast(
            &mut tracker,
            &file_infos,
            &priorities,
            1,
            Some(waited_on),
            later + Duration::from_secs(10),
            fast,
        ) {
            AcquireResult::Reserved { piece: other, .. } => {
                assert_ne!(
                    other, waited_on,
                    "a peer took a second copy of its own copy"
                )
            }
            other => panic!("expected a different piece, got {other:?}"),
        }
    }

    /// **The head ask offers the two head pieces and nothing past them.**
    ///
    /// It runs before every chunk a peer sends for work it took from
    /// deeper in, so it must hand out only what the reader is waiting on:
    /// a share of a piece at the head, or nothing. The whole pieces beyond
    /// the deadline, the ordinary queue and the steal are the full ask's.
    #[test]
    fn the_head_ask_stops_at_the_deadline_pieces() {
        let (mut tracker, file_infos, priorities) = make_split_tracker_of(4, 2 * CLAIM_CHUNKS);
        let window: Vec<ValidPieceIndex> = (0..4).map(|i| piece(&tracker, i)).collect();
        let t0 = Instant::now();

        // Peer 1 takes both claims of piece 0 and peer 2 both of piece 1:
        // the head is spoken for, by holders nobody outpaces yet.
        let taken: Vec<ValidPieceIndex> = [1u8, 1, 2, 2]
            .into_iter()
            .map(|who| {
                reserved_piece(head_share(
                    &mut tracker,
                    &file_infos,
                    &priorities,
                    who,
                    &window,
                    t0,
                ))
            })
            .collect();
        assert_eq!(
            taken,
            vec![window[0], window[0], window[1], window[1]],
            "the head ask reserves the head pieces, split"
        );

        // Peer 3 asks the head and is offered nothing, though piece 2 is
        // free and the queue is full of pieces.
        assert!(
            matches!(
                head_share(&mut tracker, &file_infos, &priorities, 3, &window, t0),
                AcquireResult::NoneAvailable { .. }
            ),
            "the head ask reached past the deadline pieces"
        );
        assert_eq!(
            reserved_piece(acquire_in_window(
                &mut tracker,
                &file_infos,
                &priorities,
                3,
                &window,
                t0
            )),
            window[2],
            "which the full ask then hands it whole"
        );
    }

    /// **Finishing a piece cancels what everyone else still has out for
    /// it.**
    ///
    /// The point of duplicating a claim is that two peers race it, and the
    /// loser is then asking a seeder for bytes already on our disk. It is
    /// not only the loser: a split piece completes on its last claim, so
    /// every peer still mid-claim is overtaken too, however far along it
    /// was. Only the peer that finished it is spared.
    #[test]
    fn completing_a_piece_overtakes_every_other_peer_on_it() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        for who in 1..=4 {
            acquire_as(&mut tracker, &file_infos, &priorities, who, Some(waited_on));
        }
        // Peer 5 doubles up on peer 1's claim and wins the race.
        acquire_as(&mut tracker, &file_infos, &priorities, 5, Some(waited_on));

        let mut overtaken = tracker.overtaken_by(waited_on, peer(5));
        overtaken.sort();

        assert_eq!(
            overtaken,
            vec![peer(1), peer(2), peer(3), peer(4)],
            "the losing copy of the winner's own claim is cancelled with the rest"
        );
        assert!(
            !overtaken.contains(&peer(5)),
            "and the peer that finished it is not asked to cancel itself"
        );
    }

    /// **A piece nobody is waiting on is still one peer's.**
    ///
    /// Splitting buys parallelism on a deadline and costs a lock round-trip
    /// per claim; a piece no stream is parked on has no deadline to spend
    /// that on, and single ownership is what lets a failed hash be blamed
    /// on the peer that sent it.
    #[test]
    fn an_ordinary_queued_piece_is_claimed_whole() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);

        let share = claimed(acquire_as(&mut tracker, &file_infos, &priorities, 1, None));

        assert_eq!(
            share,
            0..SPLIT_CHUNKS_PER_PIECE,
            "no stream is waiting, so the whole piece goes to one peer"
        );
    }

    /// **One peer leaving a split piece does not throw away what the others
    /// fetched.**
    ///
    /// A single-owner release breaks the piece, which wipes every chunk of
    /// it. That is right when the departing peer was the only one on it and
    /// catastrophic when it was not: the other peers' chunks go too, and
    /// they are still fetching into it.
    #[test]
    fn releasing_one_peer_returns_its_share_and_leaves_the_rest_alone() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        for who in 1..=3 {
            acquire_as(&mut tracker, &file_infos, &priorities, who, Some(waited_on));
        }

        assert_eq!(tracker.release_pieces_owned_by(peer(2), 0), 1);

        assert_eq!(
            tracker.participants(waited_on).len(),
            2,
            "the piece is still in flight, with the peers that did not leave"
        );
        assert_eq!(
            claimed(acquire_as(
                &mut tracker,
                &file_infos,
                &priorities,
                4,
                Some(waited_on)
            )),
            16..32,
            "and the share it abandoned is the next one handed out"
        );
    }

    /// **A claim goes back to the pool only when nobody is fetching it.**
    ///
    /// `stalled_claim`'s comparison is not what keeps a claim from being
    /// handed out too often: `claim` pops the unclaimed pool without
    /// consulting it, so anything in the pool goes out, measured or not. Put a claim
    /// back while its other holder is still on it and the holder itself can
    /// be the next peer to ask -- it is handed the very chunks it has in
    /// flight, finds every one of them already requested, and sends
    /// nothing. That is the field log's whole-claim "we already requested"
    /// run on piece 5563.
    #[test]
    fn a_claim_its_other_holder_left_does_not_come_back_to_the_peer_fetching_it() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        for who in 1..=4 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }
        // Peer 2 finishes its own claim fast and doubles peer 1's, which
        // has delivered nothing in ten seconds.
        deliver(&mut tracker, waited_on, 16..32);
        let later = t0 + Duration::from_secs(10);
        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                2,
                Some(waited_on),
                later,
                Duration::from_millis(100),
            )),
            0..16
        );

        // Peer 2 is choked or dies. Peer 1 is still fetching 0..16.
        assert_eq!(tracker.release_pieces_owned_by(peer(2), 0), 1);

        assert_ne!(
            claimed(acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                Some(waited_on),
                later
            )),
            0..16,
            "peer 1 was handed back the claim it still has on the wire"
        );
    }

    /// **One peer does not take a whole split piece while other peers
    /// could be helping with it.**
    ///
    /// A peer comes back for another claim when it has *sent* the last
    /// one's requests, not when they have arrived, and its window is eight
    /// claims wide -- so two peers took all sixteen claims of a 4 MiB piece
    /// within milliseconds and the piece was back to "the slowest of two".
    /// The field of 2026-09-14 blocked 13.4 s on one piece while the swarm
    /// delivered 12-16 MB/s from seventeen seeders; every other blocked
    /// read in that log was under three seconds.
    ///
    /// So a peer takes its share and goes to fetch the next piece the
    /// stream needs, which is work either way.
    #[test]
    fn a_peer_takes_its_share_of_a_piece_and_moves_on() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let window: Vec<ValidPieceIndex> = (0..2).map(|id| piece(&tracker, id)).collect();
        let t0 = Instant::now();
        let take = |tracker: &mut PieceTracker| {
            reserved_piece(acquire_in_window(
                tracker,
                &file_infos,
                &priorities,
                1,
                &window,
                t0,
            ))
        };

        assert_eq!(take(&mut tracker), window[0]);
        assert_eq!(
            take(&mut tracker),
            window[0],
            "its share is more than one claim"
        );
        assert_eq!(
            take(&mut tracker),
            window[1],
            "one peer took a third claim of a piece other peers could be helping with"
        );
    }

    /// **A peer over its share yields on a fresh piece and takes on an old
    /// one, and a fresh peer takes its share whatever the piece's age.**
    #[test]
    fn a_peer_over_its_share_yields_on_a_fresh_piece() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let ask = |tracker: &mut PieceTracker, who: u8, now: Instant, latency: u64| {
            acquire_with(
                tracker,
                &file_infos,
                &priorities,
                who,
                &[waited_on],
                now,
                Some(Duration::from_millis(latency)),
            )
        };

        assert_eq!(claimed(ask(&mut tracker, 1, t0, 300)), 0..16);
        assert_eq!(claimed(ask(&mut tracker, 1, t0, 300)), 16..32);
        assert!(
            matches!(
                ask(&mut tracker, 1, ms(50), 300),
                AcquireResult::Crowded { .. }
            ),
            "fifty milliseconds into the piece, a peer with a 300 ms round trip waits"
        );
        assert_eq!(
            claimed(ask(&mut tracker, 2, ms(60), 300)),
            32..48,
            "a peer under its share takes freely, whatever the piece's age"
        );
        assert_eq!(
            claimed(ask(&mut tracker, 1, ms(350), 300)),
            48..64,
            "and past its own round trip the first peer takes the shares nobody came for"
        );

        // A peer whose claim has finished holds nothing of the piece any
        // more, and is under its share again: the question is not asked.
        let (mut tracker2, file_infos2, priorities2) = make_split_tracker(1);
        let piece2 = piece(&tracker2, 0);
        for who in [1u8, 1, 2] {
            acquire_at(
                &mut tracker2,
                &file_infos2,
                &priorities2,
                who,
                Some(piece2),
                t0,
            );
        }
        deliver(&mut tracker2, piece2, 32..48);
        assert_eq!(
            claimed(acquire_at(
                &mut tracker2,
                &file_infos2,
                &priorities2,
                2,
                Some(piece2),
                t0
            )),
            48..64,
            "a peer whose claim of this piece is done is not over its share"
        );
    }

    /// **The head of the lookahead is split; the rest is vanilla.**
    ///
    /// One depth decides both halves, because they are one question. A
    /// piece a read is blocked on -- or about to block on -- is worth
    /// several peers fetching it at once and worth a second copy of a
    /// stalled claim. A piece further out has a whole piece of playback to
    /// arrive in, so it goes to one peer as it did before the fork.
    ///
    /// **Handing the deeper ones whole is not only about what they cost.**
    /// `steal_piece` refuses any piece with more than one participant, and
    /// `on_piece_completed` credits a whole piece's bytes and elapsed time
    /// to whichever peer delivered its last chunk -- so splitting every
    /// piece of the lookahead turned off stealing across the whole of it
    /// and made the only per-peer speed number there is meaningless exactly
    /// where it was being used.
    #[test]
    fn only_the_head_of_the_lookahead_is_split() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let window: Vec<ValidPieceIndex> = (0..6).map(|id| piece(&tracker, id)).collect();
        let t0 = Instant::now();

        // One peer walks the window, taking a claim at a time. On a split
        // piece it takes its share and moves on (the rest is other peers',
        // until a delivery of its own says otherwise); on an unsplit one the
        // single claim is the whole piece.
        let mut reserved = Vec::new();
        for _ in 0..8 {
            match acquire_in_window(&mut tracker, &file_infos, &priorities, 1, &window, t0) {
                AcquireResult::Reserved { piece, chunks } => reserved.push((piece, chunks)),
                other => panic!("expected a reservation, got {other:?}"),
            }
        }
        assert!(
            matches!(
                acquire_in_window(&mut tracker, &file_infos, &priorities, 1, &window, t0),
                AcquireResult::Crowded { .. }
            ),
            "with its share of both head pieces and every deeper piece whole, the \
             peer is told to come back"
        );

        let of = |id: usize| -> Vec<Range<u32>> {
            reserved
                .iter()
                .filter(|(p, _)| *p == window[id])
                .map(|(_, c)| c.clone())
                .collect()
        };
        assert_eq!(
            (of(0), of(1)),
            (vec![0..16, 16..32], vec![0..16, 16..32]),
            "the two pieces at the head are split into claims, and this peer has \
             its share of each"
        );
        for id in 2..6 {
            assert_eq!(
                of(id),
                vec![0..64],
                "piece {id}, further out, goes to one peer whole, as it did before \
                 there was any splitting"
            );
        }
    }

    /// **The depth is the embedder's to set, and a deeper one splits
    /// deeper.** With three, the third piece of the window is cut into
    /// claims like the first two, and the fourth is still one peer's whole.
    #[test]
    fn a_deeper_deadline_splits_more_of_the_lookahead() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        tracker.set_deadline_pieces(3);
        let window: Vec<ValidPieceIndex> = (0..6).map(|id| piece(&tracker, id)).collect();
        let t0 = Instant::now();

        let mut reserved = Vec::new();
        for _ in 0..9 {
            match acquire_in_window(&mut tracker, &file_infos, &priorities, 1, &window, t0) {
                AcquireResult::Reserved { piece, chunks } => reserved.push((piece, chunks)),
                other => panic!("expected a reservation, got {other:?}"),
            }
        }
        let of = |id: usize| -> Vec<Range<u32>> {
            reserved
                .iter()
                .filter(|(p, _)| *p == window[id])
                .map(|(_, c)| c.clone())
                .collect()
        };
        for id in 0..3 {
            assert_eq!(
                of(id),
                vec![0..16, 16..32],
                "piece {id} is inside the deadline and split into claims"
            );
        }
        assert_eq!(of(3), vec![0..64], "piece 3 is past it and goes whole");
        assert_eq!(tracker.deadline_pieces(), 3);
        tracker.set_deadline_pieces(0);
        assert_eq!(
            tracker.deadline_pieces(),
            1,
            "never fewer than the piece the reader is on"
        );
    }

    /// **The median is of every piece, whole or split.** A piece one peer
    /// fetched whole deep in the lookahead is what a piece is until the
    /// split reaches it, so its time is the time a horizon has to cover;
    /// the split pieces are the fast end of the sample, not the sample.
    #[test]
    fn the_completion_median_counts_every_piece() {
        let (mut tracker, _file_infos, _priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let secs = |n: u64| t0 + Duration::from_secs(n);
        assert_eq!(tracker.median_completion(), None, "nothing has completed");

        // Two split at the head, one whole and slow deep in.
        tracker.reserve_piece(piece(&tracker, 0), peer(1), 0, true, t0);
        tracker.reserve_piece(piece(&tracker, 1), peer(2), 0, true, t0);
        tracker.reserve_piece(piece(&tracker, 5), peer(3), 0, false, t0);
        assert_eq!(
            tracker.take_inflight_at(piece(&tracker, 5), secs(40)),
            Some(Duration::from_secs(40)),
        );
        assert_eq!(
            tracker.median_completion(),
            Some(Duration::from_secs(40)),
            "the whole piece is a sample: it is what a piece takes on one peer"
        );

        tracker.take_inflight_at(piece(&tracker, 0), secs(4));
        assert_eq!(
            tracker.median_completion(),
            Some(Duration::from_secs(40)),
            "the upper middle of an even count: the horizon errs towards earlier"
        );
        tracker.take_inflight_at(piece(&tracker, 1), secs(8));
        assert_eq!(tracker.median_completion(), Some(Duration::from_secs(8)));
    }

    /// A piece reserved whole that the stream then reaches and cuts counts
    /// from its first claim -- the reader waited for all of it, not just
    /// the part after the cut.
    #[test]
    fn a_piece_cut_at_the_head_counts_from_its_first_claim() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let secs = |n: u64| t0 + Duration::from_secs(n);
        let reached = piece(&tracker, 2);
        tracker.reserve_piece(reached, peer(1), 0, false, t0);
        delivered_by(&mut tracker, reached, 0..16, 1, secs(1));
        // The stream reaches piece 2; a faster peer arrives and cuts it.
        let window: Vec<ValidPieceIndex> = (2..6).map(|id| piece(&tracker, id)).collect();
        let _ = acquire_with(
            &mut tracker,
            &file_infos,
            &priorities,
            2,
            &window,
            secs(10),
            Some(Duration::from_secs(1)),
        );
        tracker.take_inflight_at(reached, secs(12));
        assert_eq!(
            tracker.median_completion(),
            Some(Duration::from_secs(12)),
            "twelve seconds from the first claim, not two from the cut"
        );
    }

    /// **A refusal says when to ask again.** A peer over its share is
    /// turned away from a piece younger than its own round trip, and told
    /// the instant the piece will be that old; a peer that has never
    /// delivered is told nothing, since it outpaces nothing however long it
    /// waits. See CLAIMS.md, "When a refused peer asks again".
    #[test]
    fn a_refusal_says_when_the_share_becomes_the_askers() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let ask = |tracker: &mut PieceTracker, who: u8, now: Instant, latency: Option<u64>| {
            acquire_with(
                tracker,
                &file_infos,
                &priorities,
                who,
                &[waited_on],
                now,
                latency.map(Duration::from_millis),
            )
        };

        assert_eq!(claimed(ask(&mut tracker, 1, t0, Some(300))), 0..16);
        assert_eq!(claimed(ask(&mut tracker, 1, t0, Some(300))), 16..32);
        match ask(&mut tracker, 1, ms(50), Some(300)) {
            AcquireResult::Crowded { retry_at } => assert_eq!(
                retry_at,
                Some(ms(301)),
                "the piece started at t0 and this peer's round trip is 300 ms"
            ),
            other => panic!("over its share on a fresh piece: {other:?}"),
        }

        // A fresh peer takes its two freely, and is then told nothing: it
        // has no round trip to outpace anything with, however long it
        // waits. (The pool is empty by now, so the refusal is a join's.)
        assert_eq!(claimed(ask(&mut tracker, 2, ms(60), None)), 32..48);
        assert_eq!(claimed(ask(&mut tracker, 2, ms(60), None)), 48..64);
        match ask(&mut tracker, 2, ms(70), None) {
            AcquireResult::Crowded { retry_at } | AcquireResult::NoneAvailable { retry_at } => {
                assert_eq!(retry_at, None)
            }
            other => panic!("a fresh peer with nothing left to take: {other:?}"),
        }
    }

    /// **A join refused says when the newest holder is outpaced.** Every
    /// claim of the piece is held, two from the start and two from later;
    /// a peer with a half-second round trip is told the earliest instant
    /// any of them is a round trip old.
    #[test]
    fn a_refusal_to_join_says_when_the_newest_holder_is_outpaced() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let ask = |tracker: &mut PieceTracker, who: u8, now: Instant, latency: u64| {
            acquire_with(
                tracker,
                &file_infos,
                &priorities,
                who,
                &[waited_on],
                now,
                Some(Duration::from_millis(latency)),
            )
        };
        assert_eq!(claimed(ask(&mut tracker, 1, t0, 300)), 0..16);
        assert_eq!(claimed(ask(&mut tracker, 1, t0, 300)), 16..32);
        assert_eq!(claimed(ask(&mut tracker, 2, ms(200), 300)), 32..48);
        assert_eq!(claimed(ask(&mut tracker, 2, ms(200), 300)), 48..64);

        match ask(&mut tracker, 3, ms(300), 500) {
            AcquireResult::NoneAvailable { retry_at } => assert_eq!(
                retry_at,
                Some(ms(501)),
                "the first peer's claims are the oldest; five hundred milliseconds after them"
            ),
            other => panic!("every claim is held by someone not yet outpaced: {other:?}"),
        }
    }

    /// **And at the head, when the cut becomes possible.** A piece one peer
    /// reserved whole that the stream then reaches: a faster peer arriving
    /// before the piece is a round trip old is refused, told when it may
    /// cut, and cuts when it comes back then.
    #[test]
    fn a_refusal_at_the_head_says_when_the_cut_becomes_possible() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let reached = piece(&tracker, 2);
        tracker.reserve_piece(reached, peer(1), 0, false, t0);
        let latency = Some(Duration::from_millis(500));
        let head_ask = |tracker: &mut PieceTracker, now: Instant| {
            tracker.acquire_head_share(AcquireRequest {
                peer: peer(2),
                connection: 0,
                peer_avg_time: None,
                last_latency: latency,
                now,
                priority_pieces: std::iter::once(reached),
                file_priorities: &priorities,
                file_infos: &file_infos,
                peer_has_piece: |_| true,
                can_steal: |_| true,
            })
        };

        match head_ask(&mut tracker, ms(100)) {
            AcquireResult::NoneAvailable { retry_at } => assert_eq!(
                retry_at,
                Some(ms(501)),
                "the whole piece is a hundred milliseconds old; this peer cuts at five hundred"
            ),
            other => panic!("a hundred milliseconds in, nothing is this peer's: {other:?}"),
        }
        assert_eq!(
            claimed(head_ask(&mut tracker, ms(600))),
            16..32,
            "back at the instant it was told, it cuts the piece and takes the next claim"
        );
    }

    /// **Shares put in a pool are announced, once.** A piece reserved split
    /// at the head, or a whole one cut there, is what an idle peer waiting
    /// for something to join needs to hear about; a whole reservation deep
    /// in the window is not.
    #[test]
    fn shares_put_in_a_pool_are_announced_once() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let window: Vec<ValidPieceIndex> = (0..6).map(|id| piece(&tracker, id)).collect();
        assert!(!tracker.take_pool_changed(), "nothing has happened");

        // The head piece, reserved split: shares in the pool.
        assert_eq!(
            claimed(acquire_in_window(
                &mut tracker,
                &file_infos,
                &priorities,
                1,
                &window,
                t0
            )),
            0..16
        );
        assert!(tracker.take_pool_changed());
        assert!(!tracker.take_pool_changed(), "and it is reported once");

        // A whole piece deep in the window: nobody else's to share yet.
        tracker.reserve_piece(piece(&tracker, 5), peer(2), 0, false, t0);
        assert!(!tracker.take_pool_changed());

        // Cut when the stream reaches it: shares in the pool again.
        let tail: Vec<ValidPieceIndex> = vec![piece(&tracker, 5)];
        let _ = acquire_with(
            &mut tracker,
            &file_infos,
            &priorities,
            3,
            &tail,
            t0 + Duration::from_secs(10),
            Some(Duration::from_secs(1)),
        );
        assert!(
            tracker.take_pool_changed(),
            "the cut put the rest of the piece in the pool"
        );
    }

    /// **The ring holds sixty-four completions, and the oldest leaves
    /// first.** Sixty-four one-second pieces, then seventeen slow ones:
    /// the slow ones are seventeen samples of sixty-four and the median
    /// stays at a second -- a sixteen-deep ring would by then hold nothing
    /// but slow ones. Then enough slow ones to fill the ring: the fast ones
    /// are gone and the median is the slow time. A shorter ring let one
    /// seek's burst of fast split pieces swing the median, and the depth
    /// with it.
    #[test]
    fn the_completion_ring_is_sixty_four_deep_and_forgets_the_oldest() {
        let (mut tracker, _file_infos, _priorities) = make_split_tracker(200);
        let t0 = Instant::now();
        fn complete(tracker: &mut PieceTracker, id: u32, took: u64, t0: Instant) {
            let index = piece(tracker, id);
            tracker.reserve_piece(index, peer(1), 0, false, t0);
            tracker.take_inflight_at(index, t0 + Duration::from_secs(took));
        }
        let samples = u32::try_from(COMPLETION_SAMPLES).unwrap();
        for id in 0..samples {
            complete(&mut tracker, id, 1, t0);
        }
        for id in samples..samples + 17 {
            complete(&mut tracker, id, 40, t0);
        }
        assert_eq!(
            tracker.median_completion(),
            Some(Duration::from_secs(1)),
            "seventeen slow pieces in sixty-four do not move the median"
        );
        for id in samples + 17..2 * samples + 17 {
            complete(&mut tracker, id, 40, t0);
        }
        assert_eq!(
            tracker.median_completion(),
            Some(Duration::from_secs(40)),
            "a ring of slow pieces later the fast ones are forgotten"
        );
    }

    /// **A piece reserved whole is split when the stream reaches it.**
    ///
    /// Splitting happens where a piece is reserved and only at the head of
    /// the lookahead, so a piece that entered the in-flight map whole --
    /// reserved while deeper, or from the ordinary queue beyond the window
    /// -- kept its single claim as the stream advanced onto it. In
    /// steady-state playback that is nearly every piece the stream reaches:
    /// the swarm has reserved the pieces ahead of the window long before
    /// the window gets there. The head piece was then one peer's whole
    /// claim, nothing for a second copy to double, and the steal wanted
    /// 10x -- the field's read blocked on one slow peer while sixteen
    /// others were sent past it, with the splitting machinery unused.
    ///
    /// **Cut by whoever outpaces the holder**, and by nobody else: cutting
    /// duplicates the holder's already-sent tail, so it is for a peer whose
    /// last chunk took less time than the holder has now been silent. A
    /// fresh peer, and a peer slower than that silence, are sent on to the
    /// next piece.
    #[test]
    fn a_piece_reserved_whole_is_split_when_the_stream_reaches_it() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let secs = |n: u64| t0 + Duration::from_secs(n);
        let deep = piece(&tracker, 2);
        // Reserved whole by peer 1, deep in the window, and a quarter of it
        // delivered within the first second -- then nothing.
        tracker.reserve_piece(deep, peer(1), 0, false, t0);
        delivered_by(&mut tracker, deep, 0..16, 1, secs(1));

        // The stream advances: piece 2 is the head now, ten seconds in.
        let window: Vec<ValidPieceIndex> = (2..6).map(|id| piece(&tracker, id)).collect();
        let ask = |tracker: &mut PieceTracker, who: u8, latency: Option<Duration>| {
            acquire_with(
                tracker,
                &file_infos,
                &priorities,
                who,
                &window,
                secs(10),
                latency,
            )
        };
        assert_ne!(
            reserved_or_none(ask(&mut tracker, 3, None)),
            Some(deep),
            "a peer that has never delivered cut into a holder's piece"
        );
        assert_ne!(
            reserved_or_none(ask(&mut tracker, 5, Some(Duration::from_secs(20)))),
            Some(deep),
            "a peer slower than the holder's silence cut into its piece"
        );
        assert_eq!(
            tracker.participants(deep).len(),
            1,
            "the holder is still alone on it"
        );
        // Peer 2's last chunk took a tenth of a second; the holder has been
        // silent nine. The piece is cut for it.
        match ask(&mut tracker, 2, Some(Duration::from_millis(100))) {
            AcquireResult::Reserved { piece: p, chunks } => {
                assert_eq!(p, deep, "the piece the stream is on, not the one after it");
                assert_eq!(
                    chunks,
                    32..48,
                    "the first claim after the one the holder is delivering into"
                );
            }
            other => panic!("expected a share of the head piece, got {other:?}"),
        }
        let holder = tracker
            .participants(deep)
            .iter()
            .find(|p| p.peer == peer(1))
            .expect("the original holder is still on it");
        assert_eq!(
            holder.chunks,
            16..32,
            "the holder keeps the claim it is currently delivering into"
        );
        assert_eq!(
            claimed(ask(&mut tracker, 4, None)),
            48..64,
            "and the rest is in the pool for whoever comes next, no latency needed \
             for a share nobody holds"
        );
    }

    /// **A whole piece is cut by the piece's age, not the holder's last
    /// delivery.**
    ///
    /// A piece reserved whole fifty milliseconds ago is nobody's to cut; one
    /// reserved ten seconds ago is cut for any peer whose chunks take less,
    /// however steadily its holder has been landing chunks -- a steady
    /// trickle is exactly what a slow pipelined holder looks like.
    #[test]
    fn a_whole_piece_is_cut_by_its_age_not_its_holders_last_delivery() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let deep = piece(&tracker, 2);
        tracker.reserve_piece(deep, peer(1), 0, false, t0);
        let window: Vec<ValidPieceIndex> = (2..6).map(|id| piece(&tracker, id)).collect();
        let ask = |tracker: &mut PieceTracker, who: u8, now: Instant| {
            acquire_with(
                tracker,
                &file_infos,
                &priorities,
                who,
                &window,
                now,
                Some(Duration::from_millis(100)),
            )
        };

        assert_ne!(
            reserved_or_none(ask(&mut tracker, 2, t0 + Duration::from_millis(50))),
            Some(deep),
            "a fifty-millisecond-old piece was cut by a peer whose chunks take a hundred"
        );
        let later = t0 + Duration::from_secs(10);
        delivered_by(
            &mut tracker,
            deep,
            0..16,
            1,
            later - Duration::from_millis(50),
        );
        assert_eq!(
            reserved_or_none(ask(&mut tracker, 3, later)),
            Some(deep),
            "a ten-second-old piece was not cut because its holder landed a chunk \
             fifty milliseconds ago"
        );
    }

    /// **A cut pools only the claims still missing something** (review
    /// #30). A piece handed out again keeps the chunks earlier peers left,
    /// so a claim past the one its holder is delivering into can be on
    /// disk already; the peer that cuts takes the next claim with work in
    /// it, not that one.
    #[test]
    fn a_cut_does_not_pool_a_claim_already_on_disk() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let reached = piece(&tracker, 2);
        tracker.reserve_piece(reached, peer(1), 0, false, t0);
        // What a previous holder left: the second claim, whole.
        deliver(&mut tracker, reached, 16..32);
        let window: Vec<ValidPieceIndex> = (2..6).map(|id| piece(&tracker, id)).collect();
        let cutter = acquire_with(
            &mut tracker,
            &file_infos,
            &priorities,
            2,
            &window,
            t0 + Duration::from_secs(10),
            Some(Duration::from_millis(100)),
        );
        match cutter {
            AcquireResult::Reserved { piece, chunks } => {
                assert_eq!(piece, reached, "the head piece was cut");
                assert_eq!(
                    chunks,
                    32..48,
                    "the holder keeps 0..16 and 16..32 is on disk: 32..48 is the next with work"
                );
            }
            other => panic!("expected a share of the cut piece, got {other:?}"),
        }
    }

    /// **A holder does not cut its own whole piece** (review #29). It is
    /// the one peer whose outpacing the piece says nothing about who else
    /// is coming, and a cut would only pool the tail it has requests out
    /// for.
    #[test]
    fn a_holder_does_not_cut_its_own_whole_piece() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(6);
        let t0 = Instant::now();
        let reached = piece(&tracker, 2);
        tracker.reserve_piece(reached, peer(1), 0, false, t0);
        let window: Vec<ValidPieceIndex> = (2..6).map(|id| piece(&tracker, id)).collect();
        let asked = acquire_with(
            &mut tracker,
            &file_infos,
            &priorities,
            1,
            &window,
            t0 + Duration::from_secs(10),
            Some(Duration::from_millis(100)),
        );
        assert_ne!(
            reserved_or_none(asked),
            Some(reached),
            "the holder was handed a share of its own piece"
        );
        let holders = tracker.participants(reached);
        assert_eq!(holders.len(), 1);
        assert_eq!(holders[0].chunks, 0..64, "the holder cut its own piece");
    }

    /// **A claim handed out again is doubled like any other when its new
    /// holder goes quiet.**
    ///
    /// `release` puts a claim back whole while any chunk of it is missing,
    /// and the previous holder's chunks stay on disk. The next holder is
    /// judged on the one rule -- how long since it last delivered, against
    /// the asker's latency -- so a chunk the last peer left is neither
    /// credit nor debt.
    #[test]
    fn a_re_handed_claim_is_doubled_when_its_new_holder_goes_quiet() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        let secs = |n: u64| t0 + Duration::from_secs(n);
        for who in 1..=3 {
            acquire_at(
                &mut tracker,
                &file_infos,
                &priorities,
                who,
                Some(waited_on),
                t0,
            );
        }
        // Peer 1 delivers one chunk of its claim and leaves; peer 5 picks
        // that claim up with the chunk already on disk, and then stalls.
        deliver(&mut tracker, waited_on, 0..1);
        tracker.release_pieces_owned_by(peer(1), 0);
        let re_handed = claimed(acquire_at(
            &mut tracker,
            &file_infos,
            &priorities,
            5,
            Some(waited_on),
            secs(1),
        ));
        assert_eq!(re_handed, 0..16);

        // Peer 4 takes the last free claim, delivers it fast and comes back
        // ten seconds later; peers 2 and 3 have delivered theirs, so the
        // re-handed claim is the only one with anything missing, and peer 5
        // has been silent on it for eleven seconds.
        let own = claimed(acquire_at(
            &mut tracker,
            &file_infos,
            &priorities,
            4,
            Some(waited_on),
            secs(2),
        ));
        deliver(&mut tracker, waited_on, 16..32);
        deliver(&mut tracker, waited_on, 32..48);
        deliver(&mut tracker, waited_on, own);
        assert_eq!(
            claimed(acquire_fast(
                &mut tracker,
                &file_infos,
                &priorities,
                4,
                Some(waited_on),
                secs(12),
                Duration::from_millis(100),
            )),
            re_handed,
            "the re-handed claim's holder has been silent for eleven seconds and \
             was not doubled"
        );
    }

    /// **The claims of a piece can be read for a diagnostic line.**
    #[test]
    fn the_claims_of_a_piece_say_who_holds_what_and_how_long_we_have_waited() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        let t0 = Instant::now();
        acquire_at(
            &mut tracker,
            &file_infos,
            &priorities,
            1,
            Some(waited_on),
            t0,
        );
        acquire_at(
            &mut tracker,
            &file_infos,
            &priorities,
            2,
            Some(waited_on),
            t0,
        );
        deliver(&mut tracker, waited_on, 0..4);
        tracker.note_delivery(waited_on, peer(1), t0 + Duration::from_secs(1));

        let claims = tracker.claims(waited_on, t0 + Duration::from_secs(3));
        assert_eq!(claims.len(), 2);
        assert_eq!((claims[0].peer, claims[0].chunks.clone()), (peer(1), 0..16));
        assert_eq!(claims[0].missing, 12);
        assert_eq!(
            claims[0].waited,
            Duration::from_secs(2),
            "since its last delivery"
        );
        assert_eq!(claims[1].missing, 16);
        assert_eq!(
            claims[1].waited,
            Duration::from_secs(3),
            "since it was asked, having delivered nothing"
        );
        assert!(
            tracker
                .claims(piece(&tracker, 0), t0)
                .iter()
                .all(|c| c.latency.is_none())
        );
    }

    /// **A piece whose hash check a pause interrupted is queued again.**
    ///
    /// Between `take_inflight` and `mark_piece_hash_ok` a piece is in none
    /// of the three sets -- not have, not queued, not in flight -- with
    /// every chunk marked. A pause takes the tracker apart right there and
    /// carries the piece into the paused tracker like that, where nothing
    /// will ever fetch it again: `acquire_piece` skips a fully-downloaded
    /// piece precisely because one is being checked, and nothing is
    /// checking this one any more.
    #[test]
    fn a_piece_whose_check_a_pause_interrupted_is_queued_again() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(2);
        let checking = piece(&tracker, 0);
        acquire_as(&mut tracker, &file_infos, &priorities, 1, Some(checking));
        deliver(&mut tracker, checking, 0..64);
        assert!(tracker.take_inflight(checking).is_some());
        assert!(tracker.chunks().is_piece_fully_downloaded(checking));

        let chunks = tracker.into_chunks();
        assert!(
            chunks.is_piece_queued(checking),
            "the piece the pause caught mid-check is stranded: not have, not queued, \
             every chunk marked"
        );
        assert!(!chunks.is_piece_fully_downloaded(checking));
    }

    /// **Selecting a file back must not reset the pieces of it that peers
    /// are already fetching.**
    ///
    /// `update_only_files` was the one caller of
    /// `mark_piece_broken_if_not_have` that neither took the piece out of
    /// the in-flight map first nor asked whether anybody was on it --
    /// `drop_pieces` and `reselect_pieces` both take an `is_inflight`
    /// predicate for exactly this. On a live piece it did two things
    /// nothing downstream allows for: it grew `chunks_missing` under live
    /// claims, which that function's own contract says cannot happen, so a
    /// finished claim read as unfinished and was offered for duplication
    /// and handed back on release; and it set the queue bit on a piece
    /// still in the in-flight map, which `acquire_piece` assumes is
    /// impossible.
    #[test]
    fn selecting_a_file_back_leaves_its_in_flight_pieces_alone() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        let claim = claimed(acquire_as(
            &mut tracker,
            &file_infos,
            &priorities,
            1,
            Some(waited_on),
        ));
        deliver(&mut tracker, waited_on, claim.clone());
        assert_eq!(
            tracker.chunks().chunks_missing(waited_on, &claim),
            0,
            "the fixture did not deliver the claim"
        );

        // The user deselects the file and asks for it back, while peer 1 is
        // still on the piece.
        tracker
            .update_only_files(&file_infos, &HashSet::new())
            .unwrap();
        tracker
            .update_only_files(&file_infos, &HashSet::from_iter([0]))
            .unwrap();

        assert_eq!(
            tracker.chunks().chunks_missing(waited_on, &claim),
            0,
            "a delivered claim became unfinished under its holder"
        );
        assert!(
            !tracker.chunks().is_piece_queued(waited_on),
            "a piece still in the in-flight map was put back in the queue"
        );
    }

    /// **A choke must not throw away what every other peer delivered.**
    ///
    /// The field cost, in the smallest shape that has it. A piece a stream
    /// waits on is split between peers; most of it lands; the one peer
    /// still fetching the last claim is choked. `release_pieces_owned_by`
    /// decided the piece was abandoned from an empty participant list and
    /// wiped `chunk_status` for the whole piece -- discarding every chunk
    /// already on disk and already paid a peer for, up to four megabytes an
    /// event, on a *choke*, with the same seeders still connected.
    ///
    /// The list stopped meaning "nobody has delivered anything" when claims
    /// began retiring from it: a peer whose shares are all on disk is
    /// retired when it comes back for more, so the piece can be most of the
    /// way home with one name left on it.
    #[test]
    fn a_choke_on_the_last_holder_keeps_what_the_others_delivered() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);

        // Three peers take three claims of the piece and deliver two of
        // them entirely.
        let first = claimed(acquire_as(
            &mut tracker,
            &file_infos,
            &priorities,
            1,
            Some(waited_on),
        ));
        let second = claimed(acquire_as(
            &mut tracker,
            &file_infos,
            &priorities,
            2,
            Some(waited_on),
        ));
        acquire_as(&mut tracker, &file_infos, &priorities, 3, Some(waited_on));
        deliver(&mut tracker, waited_on, first.clone());
        deliver(&mut tracker, waited_on, second.clone());
        assert!(
            tracker.chunks().any_chunk_arrived(waited_on),
            "the fixture delivered nothing"
        );

        // Peers 1 and 2 come back for more work, which retires their
        // finished claims from the participant list -- so peer 3 is the
        // only name left on a piece that is two thirds on disk.
        acquire_as(&mut tracker, &file_infos, &priorities, 1, Some(waited_on));
        acquire_as(&mut tracker, &file_infos, &priorities, 2, Some(waited_on));
        tracker.release_pieces_owned_by(peer(1), 0);
        tracker.release_pieces_owned_by(peer(2), 0);

        // And peer 3 is choked.
        tracker.release_pieces_owned_by(peer(3), 0);

        assert!(
            tracker.chunks().any_chunk_arrived(waited_on),
            "a choke threw away every chunk the other peers had delivered"
        );
        for claim in [first, second] {
            assert_eq!(
                tracker.chunks().chunks_missing(waited_on, &claim),
                0,
                "claim {claim:?} was delivered and is now missing again"
            );
        }
    }

    /// **And it comes back once, not once per holder that left.**
    ///
    /// Two holders leaving used to push two entries, on the reasoning that
    /// the cap would bind on the way out. It does not -- see above -- so
    /// the same sixteen chunks went to the same peer twice in a row.
    #[test]
    fn a_claim_both_its_holders_left_comes_back_once() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        for who in 1..=4 {
            acquire_as(&mut tracker, &file_infos, &priorities, who, Some(waited_on));
        }
        acquire_as(&mut tracker, &file_infos, &priorities, 5, Some(waited_on));

        tracker.release_pieces_owned_by(peer(1), 0);
        tracker.release_pieces_owned_by(peer(5), 0);

        let first = claimed(acquire_as(
            &mut tracker,
            &file_infos,
            &priorities,
            6,
            Some(waited_on),
        ));
        let second = claimed(acquire_as(
            &mut tracker,
            &file_infos,
            &priorities,
            6,
            Some(waited_on),
        ));
        assert_eq!(first, 0..16, "the abandoned claim is the one handed out");
        assert_ne!(second, first, "and it was handed out twice: {first:?}");
    }

    /// **A piece being hash-checked is not handed to anyone.**
    ///
    /// Completion takes the piece out of the in-flight map so nothing can
    /// steal it while it is checked, and leaves it in no set at all: not
    /// have, not queued, not in flight. The priority loop is the one place
    /// that reserves without asking the queue, so it is the one place that
    /// can hand out a piece that is already entirely on disk -- and every
    /// peer on the piece is woken into exactly that moment, by the
    /// cancellations completion sends them.
    ///
    /// What it costs: the chunks come back, get written over a piece the
    /// check has already handed to the storage as complete, and are then
    /// dropped as `PreviouslyCompleted`. The piece also never leaves the
    /// in-flight map again, because nothing reports it complete a second
    /// time.
    #[test]
    fn a_piece_in_its_hash_check_is_not_reserved_again() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(4);
        let waited_on = piece(&tracker, 0);
        for who in 1..=4 {
            acquire_as(&mut tracker, &file_infos, &priorities, who, Some(waited_on));
        }
        // Every chunk arrives, and the piece leaves the map for its check.
        for chunk in 0..SPLIT_CHUNKS_PER_PIECE {
            tracker
                .chunks
                .mark_chunk_downloaded(&fake_piece(waited_on, chunk));
        }
        assert!(tracker.take_inflight(waited_on).is_some());

        // An overtaken peer, woken by the cancellation, asks for work while
        // the stream is still parked on that piece.
        match acquire_as(&mut tracker, &file_infos, &priorities, 2, Some(waited_on)) {
            AcquireResult::Reserved { piece: got, chunks } => assert_ne!(
                got, waited_on,
                "the piece being checked was reserved again, chunks {chunks:?}"
            ),
            AcquireResult::NoneAvailable { .. } => panic!("the other pieces are still free"),
            other => panic!("expected a reservation elsewhere, got {other:?}"),
        }
    }

    /// Mark every chunk of `claim` as arrived, as the write path does when
    /// the bytes land -- from nobody in particular, so no holder is
    /// credited with a delivery on the piece.
    fn deliver(tracker: &mut PieceTracker, piece: ValidPieceIndex, claim: Range<u32>) {
        for chunk in claim {
            tracker.mark_chunk_downloaded(&fake_piece(piece, chunk));
        }
    }

    /// [`deliver`], as the write path does it for a chunk `who` sent: the
    /// bytes land and `who` is stamped as having delivered on the piece
    /// at `at`.
    fn delivered_by(
        tracker: &mut PieceTracker,
        piece: ValidPieceIndex,
        claim: Range<u32>,
        who: u8,
        at: Instant,
    ) {
        deliver(tracker, piece, claim);
        tracker.note_delivery(piece, peer(who), at);
    }

    /// A chunk-sized `Piece` message for `mark_chunk_downloaded`, whose
    /// payload it never looks at.
    fn fake_piece(index: ValidPieceIndex, chunk: u32) -> Piece<ByteBuf<'static>> {
        const ZEROES: [u8; CHUNK_SIZE as usize] = [0u8; CHUNK_SIZE as usize];
        Piece::from_data(index.get(), chunk * CHUNK_SIZE, &ZEROES)
    }

    fn piece(tracker: &PieceTracker, id: u32) -> ValidPieceIndex {
        tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(id)
            .unwrap()
    }

    fn acquire(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        file_priorities: &FilePriorities,
        priority: Option<ValidPieceIndex>,
    ) -> AcquireResult {
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: priority.into_iter(),
            file_priorities,
            file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        })
    }

    // reselect_pieces() is the only caller of mark_piece_broken_if_not_have() that cannot
    // take the piece out of the in-flight map first: the piece is legitimately being
    // downloaded, by the very peer whose work requeuing would throw away.
    #[test]
    fn test_reselect_does_not_wipe_an_inflight_piece() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        let dropped = tracker.drop_pieces(&file_infos, [p0]).unwrap();
        assert_eq!(tracker.finish_release(dropped), 0);

        // A reader seeks back into the reclaimed range: the priority path picks the piece
        // up on its own, without it ever going through the queue.
        let res = acquire(&mut tracker, &file_infos, &file_priorities, Some(p0));
        assert!(
            matches!(res, AcquireResult::Reserved { piece: p, .. } if p == p0),
            "{res:?}"
        );

        // The peer delivers all but the last chunk of it.
        let block = vec![0u8; CHUNK_SIZE as usize];
        let deliver = |t: &mut PieceTracker, chunk: u32| {
            t.mark_chunk_downloaded(&Piece::from_data(p0.get(), chunk * CHUNK_SIZE, &block))
        };
        for chunk in 0..RECLAIM_CHUNKS_PER_PIECE - 1 {
            assert!(matches!(
                deliver(&mut tracker, chunk),
                Some(ChunkMarkingResult::NotCompleted)
            ));
        }

        // Now the caller reselects the range the piece is in. It is already being
        // downloaded, which is exactly what reselecting wants.
        tracker.reselect_pieces([p0]).unwrap();

        // The last chunk completes the piece - unless the ones before it were thrown away.
        assert!(
            matches!(
                deliver(&mut tracker, RECLAIM_CHUNKS_PER_PIECE - 1),
                Some(ChunkMarkingResult::Completed)
            ),
            "the chunks the peer already delivered were thrown away"
        );
        assert!(tracker.is_inflight(p0));
        assert!(
            !tracker.chunks().is_piece_queued(p0),
            "the piece a peer owns was put back in the queue, so a second peer can take it too"
        );
    }

    // Between its last chunk arriving and its hash passing, a piece is neither queued,
    // in-flight nor have. A reselect_pieces() landing in that window finds a dropped piece
    // nobody owns and queues it; the hash then passes, and without the fix the piece is
    // have AND queued, and the next peer to ask is handed a piece we have - a redundant
    // download, and a completion counted twice.
    #[test]
    fn test_reselect_during_the_hash_check_does_not_leave_a_have_piece_queued() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        let dropped = tracker.drop_pieces(&file_infos, [p0]).unwrap();
        assert_eq!(tracker.finish_release(dropped), 0);

        // A reader's priority window pulls the dropped piece back in, and the peer
        // delivers all of it. It comes out of the in-flight map for the hash check.
        let res = acquire(&mut tracker, &file_infos, &file_priorities, Some(p0));
        assert!(
            matches!(res, AcquireResult::Reserved { piece: p, .. } if p == p0),
            "{res:?}"
        );
        let block = vec![0u8; CHUNK_SIZE as usize];
        for chunk in 0..RECLAIM_CHUNKS_PER_PIECE {
            tracker.mark_chunk_downloaded(&Piece::from_data(p0.get(), chunk * CHUNK_SIZE, &block));
        }
        assert!(tracker.take_inflight(p0).is_some());

        // The caller reselects the same range while the hash is being checked.
        tracker.reselect_pieces([p0]).unwrap();

        // The hash passes.
        tracker.mark_piece_hash_ok(p0, &file_infos);
        assert!(tracker.chunks().is_piece_have(p0));
        assert!(
            !tracker.chunks().is_piece_queued(p0),
            "a piece we have is still queued"
        );
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::NoneAvailable { .. }),
            "a second peer was handed a piece we have: {res:?}"
        );
    }

    // A piece handed to the caller so it can delete the storage behind it must not be
    // downloaded again until the caller says the deletion is done. Otherwise we set the
    // have-bit back and the deletion removes a piece we have and are advertising.
    #[test]
    fn test_a_dropped_piece_is_not_reacquired_until_released() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        assert_eq!(tracker.drop_pieces(&file_infos, [p0]).unwrap(), [p0]);
        assert!(tracker.is_releasing(p0));

        // The picker's priority path deliberately ignores "dropped" - a reader that seeks
        // backwards into a reclaimed range gets the piece back on its own. It must not do
        // that while the storage behind it is being deleted.
        let res = acquire(&mut tracker, &file_infos, &file_priorities, Some(p0));
        assert!(
            matches!(res, AcquireResult::NoneAvailable { .. }),
            "acquired a piece whose storage is being released: {res:?}"
        );

        // Neither may the queue path, even once the piece is wanted again.
        tracker.reselect_pieces([p0]).unwrap();
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::NoneAvailable { .. }),
            "acquired a piece whose storage is being released: {res:?}"
        );

        // The caller is done deleting: the piece is queued, so this is worth a wake-up,
        // and now it can be downloaded again.
        assert_eq!(tracker.finish_release([p0]), 1);
        assert!(!tracker.is_releasing(p0));
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::Reserved { piece: p, .. } if p == p0),
            "{res:?}"
        );
    }

    // Pausing takes the PieceTracker apart and keeps the ChunkTracker, and unpausing
    // builds a new PieceTracker around it. A claim that didn't survive that would let an
    // unpaused torrent download pieces the caller is still deleting - and a pause is an
    // ordinary user action, and also what stop_with_error() does.
    #[test]
    fn test_a_claim_survives_a_pause() {
        let file_infos = reclaim_file_infos(3);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut tracker = make_reclaim_tracker(3);
        let p0 = piece(&tracker, 0);

        assert_eq!(tracker.drop_pieces(&file_infos, [p0]).unwrap(), [p0]);

        // Pause, then unpause.
        let mut tracker = PieceTracker::new(tracker.into_chunks());
        assert!(tracker.is_releasing(p0), "the claim died with the pause");

        // Same as without the pause: the piece is wanted again but nothing may take it
        // until the caller says the deletion is done.
        tracker.reselect_pieces([p0]).unwrap();
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::NoneAvailable { .. }),
            "acquired a piece whose storage is being released: {res:?}"
        );

        assert_eq!(tracker.finish_release([p0]), 1);
        let res = acquire(&mut tracker, &file_infos, &file_priorities, None);
        assert!(
            matches!(res, AcquireResult::Reserved { piece: p, .. } if p == p0),
            "{res:?}"
        );
    }

    #[test]
    fn test_new_piece_tracker() {
        let chunks = make_test_chunk_tracker(10);
        let tracker = PieceTracker::new(chunks);
        assert_eq!(tracker.inflight_count(), 0);
    }

    #[test]
    fn test_reserve_piece_from_queue() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true, // Peer has all pieces
            can_steal: |_| true,
        });

        // Should reserve piece 0 (first in queue)
        match result {
            AcquireResult::Reserved { piece, .. } => {
                assert_eq!(piece.get(), 0);
                assert!(tracker.is_inflight(piece));
                assert_eq!(tracker.inflight_count(), 1);
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_reserve_filters_by_peer_has_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer only has piece 2 and later
        // Note: iter_queued_pieces iterates in order: first, last, middle
        // So for 0..5: 0, 4, 1, 2, 3
        // With filter >= 2, we get 4 first (skips 0, takes 4)
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() >= 2,
            can_steal: |_| true,
        });

        match result {
            AcquireResult::Reserved { piece, .. } => {
                // Got piece 4 (first one peer has in iteration order)
                assert!(piece.get() >= 2, "Should have gotten a piece >= 2");
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_complete_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve a piece first
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        let piece = match result {
            AcquireResult::Reserved { piece: p, .. } => p,
            _ => panic!("Expected Reserved"),
        };

        // Complete the piece (take_inflight + hash check + mark_piece_hash_ok)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        assert!(!tracker.is_inflight(piece));
        // Simulate successful hash check
        tracker.mark_piece_hash_ok(piece, &file_infos);
        assert!(tracker.chunks().is_piece_have(piece));
    }

    #[test]
    fn test_fail_piece_requeues() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        let piece = match result {
            AcquireResult::Reserved { piece: p, .. } => p,
            _ => panic!("Expected Reserved"),
        };

        assert!(tracker.is_inflight(piece));

        // Fail the piece (take_inflight + hash check fails + mark_piece_hash_failed)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        // Simulate failed hash check
        tracker.mark_piece_hash_failed(piece);

        // Should no longer be in-flight
        assert!(!tracker.is_inflight(piece));
        // Should not be in have
        assert!(!tracker.chunks().is_piece_have(piece));
        // Should be back in queue - verify by trying to reserve it again
        let result2 = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p == piece, // Only has the failed piece
            can_steal: |_| true,
        });

        match result2 {
            AcquireResult::Reserved { piece: p, .. } => assert_eq!(p, piece),
            _ => panic!("Expected piece to be re-reservable after fail"),
        }
    }

    // Two connections to one address in turn, the first still winding down when the
    // second reserves: the first one's death hands back what it reserved and nothing else.
    #[test]
    fn test_release_pieces_owned_by_one_connection() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);
        let mut acquire = |connection| match tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved { piece: p, .. } => p,
            other => panic!("expected Reserved, got {other:?}"),
        };
        let old = acquire(1);
        let new = acquire(2);

        assert_eq!(tracker.release_pieces_owned_by(peer(1), 1), 1);
        assert!(!tracker.is_inflight(old));
        assert!(
            tracker.is_inflight(new),
            "the new connection lost its piece"
        );

        // A steal hands the piece to the thief's connection, whose death hands it back.
        let stolen = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 3,
            peer_avg_time: Some(Duration::ZERO),
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p == new,
            can_steal: |_| true,
        });
        assert!(
            matches!(stolen, AcquireResult::Stolen { piece, .. } if piece == new),
            "{stolen:?}"
        );
        assert_eq!(tracker.release_pieces_owned_by(peer(2), 3), 1);
        assert!(
            !tracker.is_inflight(new),
            "the thief's death kept the piece"
        );
    }

    #[test]
    fn test_release_pieces_owned_by_peer() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves first two pieces (order: 0, 4, 1, 2, 3)
        // So peer A gets pieces 0 and 4
        let piece_a1 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved { piece: p, .. } => p,
            _ => panic!("Expected Reserved"),
        };
        let piece_a2 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved { piece: p, .. } => p,
            _ => panic!("Expected Reserved"),
        };

        // Peer B reserves next piece
        let piece_b = match tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved { piece: p, .. } => p,
            _ => panic!("Expected Reserved"),
        };

        assert_eq!(tracker.inflight_count(), 3);
        assert!(tracker.is_inflight(piece_a1));
        assert!(tracker.is_inflight(piece_a2));
        assert!(tracker.is_inflight(piece_b));

        // Peer A dies
        let released = tracker.release_pieces_owned_by(peer_a, 0);
        assert_eq!(released, 2);
        assert_eq!(tracker.inflight_count(), 1); // Only peer B's piece remains

        // Verify peer B's piece is still in-flight
        assert!(tracker.is_inflight(piece_b));
        // Verify peer A's pieces are no longer in-flight
        assert!(!tracker.is_inflight(piece_a1));
        assert!(!tracker.is_inflight(piece_a2));
    }

    #[test]
    fn test_into_chunks_requeues_inflight() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve pieces 0 and 1
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        assert_eq!(tracker.inflight_count(), 2);

        // Convert back to chunks (simulates pause)
        let chunks = tracker.into_chunks();

        // Create a new tracker and verify pieces are back in queue
        let mut new_tracker = PieceTracker::new(chunks);
        let result = new_tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        // Should get piece 0 again (was requeued)
        match result {
            AcquireResult::Reserved { piece: p, .. } => assert_eq!(p.get(), 0),
            _ => panic!("Expected to reserve piece 0 after into_chunks"),
        }
    }

    #[test]
    fn test_priority_pieces_checked_first() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Priority pieces: piece 3, then piece 2
        let piece3 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(3)
            .unwrap();
        let piece2 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(2)
            .unwrap();
        let priority = vec![piece3, piece2];

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: priority.into_iter(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });

        // Should get piece 3 (first priority piece)
        match result {
            AcquireResult::Reserved { piece: p, .. } => assert_eq!(p.get(), 3),
            _ => panic!("Expected Reserved(3), got {:?}", result),
        }
    }

    #[test]
    fn test_none_available_when_no_pieces() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer has no pieces
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| false, // Peer has nothing
            can_steal: |_| true,
        });

        match result {
            AcquireResult::NoneAvailable { .. } => {}
            _ => panic!("Expected NoneAvailable, got {:?}", result),
        }
    }

    #[test]
    fn test_take_inflight_nonexistent_piece_returns_none() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let piece = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(0)
            .unwrap();

        // Try to take a piece that's not in-flight
        let result = tracker.take_inflight(piece);
        assert!(result.is_none());
    }

    /// A steal is not free: the peer robbed has already asked its seeder for the chunks
    /// of that piece, and everything that arrives for it after the cancel is dropped. So
    /// while there is anything left to reserve, reserve it -- however slow the incumbent
    /// looks. This is what a raised peer limit runs into: eight peers re-dial at once and
    /// would otherwise rob the two that carried the torrent while the cap was low, each
    /// of which then spends its whole link on bytes that go nowhere.
    #[test]
    fn a_free_piece_is_reserved_rather_than_stolen_from_a_slow_peer() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // The incumbent is sitting on piece 0, and has been for an age by our standards.
        tracker.reserve_piece(
            piece(&tracker, 0),
            peer(1),
            0,
            false,
            Instant::now() - Duration::from_secs(600),
        );

        let acquire = |tracker: &mut PieceTracker| {
            tracker.acquire_piece(AcquireRequest {
                peer: peer(2),
                connection: 0,
                // Fast: the incumbent is a thousand times over the 10x bar.
                now: Instant::now(),
                peer_avg_time: Some(Duration::from_millis(600)),
                last_latency: None,
                priority_pieces: std::iter::empty(),
                file_priorities: &file_priorities,
                file_infos: &file_infos,
                peer_has_piece: |_| true,
                can_steal: |_| true,
            })
        };

        // Four pieces are still queued, so all four come back reserved, not stolen.
        for _ in 0..4 {
            match acquire(&mut tracker) {
                AcquireResult::Reserved { piece: p, .. } => assert_ne!(p.get(), 0),
                other => panic!("expected a free piece to be reserved, got {other:?}"),
            }
        }
        // Only now, with nothing left to reserve, is the slow peer's piece taken.
        match acquire(&mut tracker) {
            AcquireResult::Stolen {
                piece, from_peer, ..
            } => {
                assert_eq!(piece.get(), 0);
                assert_eq!(from_peer, peer(1));
            }
            other => panic!("expected the last piece to be stolen, got {other:?}"),
        }
    }

    /// The exception: a stream waits on one particular piece and no other, so if every
    /// piece in its window is already spoken for, the peer that can deliver soonest takes
    /// the head of the window off whoever is dawdling over it -- even with free pieces
    /// elsewhere, which would not help the stream at all.
    #[test]
    fn a_stalled_stream_piece_is_stolen_even_with_free_pieces_left() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let stream_piece = piece(&tracker, 3);
        tracker.reserve_piece(
            stream_piece,
            peer(1),
            0,
            true,
            Instant::now() - Duration::from_secs(600),
        );

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            connection: 0,
            now: Instant::now(),
            peer_avg_time: Some(Duration::from_millis(600)),
            last_latency: None,
            priority_pieces: std::iter::once(stream_piece),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        // **It takes the piece rather than joining it**, which is what this
        // did before there was any splitting. For a while it joined
        // instead: a second copy races the dawdler where a steal drops
        // whatever the dawdler has in flight. But joining was available to
        // any peer that turned up, including one that had proved nothing,
        // and that is what fetched the field's lookahead twice. Cutting
        // into a holder's piece now takes a latency shorter than the
        // holder's silence, and a peer that has never delivered a chunk
        // has none -- so what is left for it is the steal, on the strength
        // of what it has delivered elsewhere (`peer_avg_time`, ten times
        // faster than the piece has been sitting there).
        //
        // What the test is here for is unchanged either way: the free
        // pieces elsewhere do not help the stream, and the peer must not go
        // and fetch one of those instead.
        match result {
            AcquireResult::Stolen {
                piece, from_peer, ..
            } => {
                assert_eq!(piece.get(), 3, "the piece the stream is waiting on");
                assert_eq!(from_peer, peer(1));
            }
            other => panic!("expected the stream's piece to be stolen, got {other:?}"),
        }
    }

    /// **A steal keeps the piece's start** (review #32). The reader has
    /// waited on the piece since it was first handed out, so the median
    /// counts from then; and the thief is measured on its own hold, so the
    /// next peer along cannot take the piece off it at once.
    #[test]
    fn a_steal_keeps_the_pieces_start() {
        let (mut tracker, file_infos, priorities) = make_split_tracker(2);
        let t0 = Instant::now();
        let secs = |n: u64| t0 + Duration::from_secs(n);
        let stolen = piece(&tracker, 0);
        let other = piece(&tracker, 1);
        tracker.reserve_piece(stolen, peer(1), 0, false, t0);
        tracker.reserve_piece(other, peer(4), 0, false, secs(50));
        let steal = |tracker: &mut PieceTracker, who: u8, now: Instant| {
            tracker.acquire_piece(AcquireRequest {
                peer: peer(who),
                connection: 0,
                peer_avg_time: Some(Duration::from_secs(1)),
                last_latency: None,
                now,
                priority_pieces: std::iter::empty(),
                file_priorities: &priorities,
                file_infos: &file_infos,
                peer_has_piece: |_| true,
                can_steal: |_| true,
            })
        };

        match steal(&mut tracker, 2, secs(100)) {
            AcquireResult::Stolen { piece, .. } => assert_eq!(piece, stolen),
            other => panic!("the piece held a hundred seconds was not stolen: {other:?}"),
        }
        // The next peer along: piece 0 is the older piece, but its thief has
        // had it a second. What it can take is piece 1, held fifty.
        match steal(&mut tracker, 3, secs(101)) {
            AcquireResult::Stolen { piece, .. } => assert_eq!(
                piece, other,
                "the thief lost the piece a second after taking it"
            ),
            other => panic!("the piece held fifty seconds was not stolen: {other:?}"),
        }
        assert_eq!(
            tracker.take_inflight_at(stolen, secs(110)),
            Some(Duration::from_secs(110)),
            "the piece took 110 s from its first claim, not 10 from the steal"
        );

        // And a stream's piece, which is stolen by name rather than ranked: the
        // thief is measured on its own hold there too.
        let (mut tracker, file_infos, priorities) = make_split_tracker(1);
        let waited_on = piece(&tracker, 0);
        tracker.reserve_piece(waited_on, peer(1), 0, false, t0);
        let steal_head = |tracker: &mut PieceTracker, who: u8, now: Instant| {
            tracker.acquire_piece(AcquireRequest {
                peer: peer(who),
                connection: 0,
                peer_avg_time: Some(Duration::from_secs(1)),
                last_latency: None,
                now,
                priority_pieces: std::iter::once(waited_on),
                file_priorities: &priorities,
                file_infos: &file_infos,
                peer_has_piece: |_| true,
                can_steal: |_| true,
            })
        };
        match steal_head(&mut tracker, 2, secs(100)) {
            AcquireResult::Stolen { piece, .. } => assert_eq!(piece, waited_on),
            other => panic!("the stream's piece held a hundred seconds was not stolen: {other:?}"),
        }
        if let AcquireResult::Stolen { .. } = steal_head(&mut tracker, 3, secs(101)) {
            panic!("the thief lost the stream's piece a second after taking it");
        }
    }

    #[test]
    fn test_steal_only_pieces_peer_has() {
        // This test verifies the fix for a bug where try_steal didn't check
        // if the stealing peer actually has the piece in their bitfield.
        // Without this check, a peer could "steal" a piece they can't download,
        // leaving it stuck in inflight forever.

        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves pieces 0 and 4 (first two in iteration order)
        let piece_0 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved { piece: p, .. } => {
                assert_eq!(p.get(), 0);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        let piece_4 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            connection: 0,
            peer_avg_time: None,
            last_latency: None,
            now: Instant::now(),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        }) {
            AcquireResult::Reserved { piece: p, .. } => {
                assert_eq!(p.get(), 4);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        // Sleep briefly so pieces become stealable
        std::thread::sleep(Duration::from_millis(5));

        // Peer B tries to acquire with:
        // - Very short avg_time (1ms) so 3x threshold = 3ms < 5ms elapsed
        // - peer_has_piece returns true ONLY for piece 4, NOT piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            connection: 0,
            now: Instant::now(),
            peer_avg_time: Some(Duration::from_millis(1)),
            last_latency: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() == 4, // Peer B only has piece 4
            can_steal: |_| true,
        });

        // Should steal piece 4 (which peer B has), NOT piece 0 (which peer B doesn't have)
        match result {
            AcquireResult::Stolen {
                piece, from_peer, ..
            } => {
                assert_eq!(piece, piece_4, "Should steal piece 4 (the one peer B has)");
                assert_eq!(from_peer, peer_a);
                // Verify piece 0 is still owned by peer A (wasn't stolen)
                assert_eq!(tracker.participants(piece_0)[0].peer, peer_a);
            }
            _ => panic!("Expected Stolen, got {:?}", result),
        }
    }
}
