# Live Torrent State Architecture

This document describes the shared state used during live torrent downloading and the invariants that are maintained.

## Key Data Structures

### 1. `PieceTracker` (in `piece_tracker.rs`)

Coordinates piece download state by wrapping `ChunkTracker` and the in-flight map. Lives in `TorrentStateLocked`.

```rust
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
    deadline_pieces: usize,                            // how deep into the lookahead pieces are split
    completions: VecDeque<Duration>,                   // recent piece times, for median_completion()
    pool_changed: bool,                                // an ask put shares in a pool; see take_pool_changed()
    retired: Vec<(ValidPieceIndex, Range<u32>)>,       // finished claims to cancel; see take_retired_claims()
    writers: HashMap<ValidPieceIndex, Vec<PeerHandle>>, // who wrote into a piece not on disk yet
}

pub struct InflightPiece {
    participants: Vec<Participant>,   // every peer fetching part of it; never empty
    unclaimed: VecDeque<Range<u32>>,  // claims nobody has taken yet
    started: Instant,                 // when the first participant started; a steal keeps it
}

pub struct Participant {
    pub peer: PeerHandle,
    pub connection: u64,              // which connection to `peer` holds it
    pub chunks: Range<u32>,           // the claim: chunk indices within the piece
    started: Instant,                 // when this participant took the claim
    last_delivery: Option<Instant>,   // diagnostics only
}
```

Key methods that maintain invariants:
- `acquire_piece()` - The lookahead, then the queue, then a steal from a slow peer
- `acquire_head_share()` - The same over the deadline pieces only
- `take_inflight()` - Remove from inflight (before hash check)
- `mark_piece_hash_ok()` - Mark as completed after hash verification
- `mark_piece_hash_failed()` - Requeue after hash failure (or a check that could not be read)
- `release_pieces_owned_by()` - Release one connection's shares (its death, or a choke)

### 2. `ChunkTracker` (in `chunk_tracker.rs`)

Tracks piece/chunk download progress. Wrapped by `PieceTracker`.

| Field | Type | Description |
|-------|------|-------------|
| `have` | `BitVec` | Pieces fully downloaded and verified |
| `queue_pieces` | `BitVec` | Pieces needed but not currently being downloaded |
| `chunk_status` | `BitVec` | Per-chunk completion status |

### 3. `inflight_requests` (in `LivePeerState`)

```rust
HashMap<ChunkInfo, Instant>  // key aka InflightRequest; value: when it was sent

struct ChunkInfo {
    piece_index: ValidPieceIndex,
    chunk_index: u32,
    absolute_index: u32,
    size: u32,
    offset: u32,
}
```

Per-peer tracking of which chunks have been requested from this peer. Used for:
- Knowing which chunks to expect from peer
- Detecting unexpected data ("peer sent us a piece we did not ask")
- Pricing an arrival: the time since its request is the peer's `last_latency`
- Cleanup when peer dies

### 4. `AdvertisedPieces` (in `torrent_state/advertised.rs`)

Which pieces the torrent tells peers about. Lives on `ManagedTorrentShared`, not in
the chunk tracker, so it outlives every state change -- a pause, and a restart out of
an error, which builds a new tracker.

```rust
pub(crate) struct AdvertisedPieces {
    set: RwLock<Option<BF>>,  // None: every piece (upstream); Some: exactly these
    bytes: usize,             // bitfield size, to materialise the set
    explicit: bool,           // SessionOptions::explicit_piece_advertising: starts empty
}
```

A piece is **announced** when `have[p] && advertised[p]`: the handshake bitfield is
`have & advertised`, a Have goes out only for an advertised piece
(`should_advertise_have`), and a request for an unadvertised piece is dropped
(`on_download_request`). Under explicit advertising, while the torrent is live the
announced set only grows: `set_pieces_advertised(_, false)` is refused (`WithdrawRefused`)
if it would clear an announced piece -- checked under the state lock, which is what a
completion sets the have-bit under -- and `drop_pieces` skips announced pieces. Under the
default neither is refused, as upstream.

Its lock is a leaf: taken after the state lock where both are needed, and nothing is
taken while it is held.

## Piece State Invariant

A piece is in exactly ONE of these states:

```
have[piece] = true                    → COMPLETED (verified)
inflight.contains(piece)              → IN_FLIGHT (being downloaded)
queue_pieces[piece] = true            → QUEUED (needed, waiting)
none of the above                     → NOT_NEEDED (deprioritized)
```

These are **disjoint** - a piece is never in multiple states simultaneously. This invariant is maintained by `PieceTracker` methods.

### Chunk-Piece Consistency

If `inflight_requests` contains chunks for piece P, then:
- this connection holds a claim on P that covers them (`inflight[P].participants`)
- OR the claim was just taken from it -- a steal, a cut, a choke's handback,
  the piece completing, a claim retired -- and the Cancels for those
  requests are on their way (transient)

## State Transitions

### Normal Download Flow

```
QUEUED → IN_FLIGHT → COMPLETED
```

1. `PieceTracker::acquire_piece()`:
   - Finds piece in `queue_pieces` (or steals from slow peer)
   - Calls `chunks.reserve_needed_piece(p)` → clears `queue_pieces[p] = false`
   - Inserts into `inflight[p]`, whose participants are the peers on it
   - Returns `AcquireResult::Reserved { piece, chunks }` or
     `AcquireResult::Stolen { piece, chunks, from_peer }`, where `chunks` is
     the claim this peer took

2. Chunk requesting:
   - For each chunk **of the claim**, insert into `inflight_requests`
   - Send Request message to peer

### Split Pieces

The pieces at the head of a stream's lookahead -- `deadline_pieces` of them,
`DEFAULT_DEADLINE_PIECES` (two) unless the embedder sets it -- are
divided into claims of `CLAIM_CHUNKS`, and several peers hold one each: the
wire asks for chunks, and `chunk_status` records them globally, so two peers
filling different chunks of one piece is safe; only `inflight` decides who
may. Every deeper piece is reserved whole to one peer, and a whole piece the
stream reaches is cut at the head (`split_whole`).
Who may take over whose work -- cutting, doubling, taking a share beyond
`CLAIMS_PER_PEER` -- is one comparison, written up in `CLAIMS.md`.

What splitting changes in the invariants:

- **A piece may have several peers.** `inflight[p]` holds a list of
  participants, and a peer is disqualified from writing to a piece by having
  no share of it, not by not being *the* owner. A share is checked again
  before each request of it goes out (`still_to_request`): it is sent one
  chunk at a time as slots free, and a cut, a steal, a choke's handback or
  the piece completing can take part of it away in between.
- **A release is partial.** One connection leaving returns its claim to the
  unclaimed pool; the piece is re-queued when the last participant goes --
  keeping the chunks other peers delivered, unless it is a piece the
  reclaim dropped, which is wiped because nothing will pick it up. A claim
  another participant is still fetching does **not** go back: anything in
  the unclaimed pool is handed out on the next `claim()`, which pops the
  pool without asking whether anyone holds it -- only `stalled_claim`
  weighs that -- so a claim put back under its holder's feet goes straight
  out again, to anyone at all, or back to the holder itself, which then
  finds every chunk of it already in flight with itself. Nor does a claim whose chunks have **all arrived**:
  there is nothing left in it to fetch and the piece completes on those
  marks whoever set them.
- **A finished claim is over.** `chunk_status` is the only record of which
  chunks have landed, so `ChunkTracker::chunks_missing()` is what tells
  `inflight` that a claim is done. A claim with nothing missing is not
  offered to a second peer, is not put back by `release()`, and is retired
  from `participants` when its own holder next asks for work -- and
  whatever that holder still has out for it (it lost a duplicate race) is
  cancelled then (`take_retired_claims`), since nothing can find those
  requests once the claim is gone. Without the retirement, ranking on
  `started` would name a claim that landed minutes ago "outstanding
  longest", and send every free peer to fetch it again.
- **The second copy goes to a claim whose newest holder the asker
  outpaces.** `stalled_claim()` considers any claim still missing
  something, whatever it has delivered, where the asker's last chunk took
  less time than that claim has been with whoever got it last
  (`Activity::outpaces`, measured from that hand-out -- `Participant::
  started`), and never one the asker already holds. There is no cap on
  holders: among the claims that qualify it ranks by chunks still missing
  with `started` as the tie-break, because what gates the piece is the
  work remaining on its slowest claim. The same comparison lets another
  peer cut a whole piece at the head of a stream's lookahead
  (`split_whole` -- never its own holder). `Participant::last_delivery`
  is stamped by the write path (`note_delivery`) but no rule reads it: it
  is there for the diagnostic line (`ClaimSnapshot::waited`, which is
  therefore a wait since the last delivery, not since the hand-out). The
  whole of it is written up in `CLAIMS.md`.
- **A piece with several writers needs a real lock.** `per_piece_locks[p]`
  is taken **exclusively** by a chunk write, and before the state lock.
  With several writers it is the only thing between one peer's chunk and
  another peer finishing the piece and handing it to the storage as
  complete. See "Piece completion" below.
- **A split piece is never stolen.** There is no single owner to take it
  from, and it already has the parallelism a steal would buy.

A piece no stream waits on is still claimed whole by one peer: it has no
deadline to spend the extra lock round-trips on, and single ownership is
what lets a failed hash be blamed on the peer that sent it. A split piece
that fails its hash blames nobody -- see "Failed hash" below.

3. Data arrival (`on_received_piece`):
   - Remove chunk from `inflight_requests`
   - Mark chunk complete in `chunk_status`
   - If all chunks done → verify hash

4. Piece completion:
   - `PieceTracker::take_inflight(piece)` → removes from `inflight`
   - every other participant is cancelled (`overtaken_by` →
     `cancel_inflight_requests_for_piece`), which frees their request slots
     and wakes them straight into the next step
   - `per_piece_locks[p]` is released -- and only here, because a chunk
     queued behind it now reads an `inflight` that no longer has the piece,
     and goes away without writing
   - Hash check passes: `TorrentStorage::on_piece_completed(piece)` commits it,
     then `PieceTracker::mark_piece_hash_ok(piece)` → sets `have[p] = true`
   - Hash check fails: `PieceTracker::mark_piece_hash_failed(piece)` → sets `queue_pieces[p] = true`
   - Hash check cannot be *run* -- `check_piece()` returns an error, i.e. the
     read failed -- is treated the same way and then propagated. A piece left
     in the check's own state is stranded for good: nothing puts it back, and
     nothing can heal it, because `mark_chunk_downloaded()` short-circuits on
     a piece whose chunks are all marked and never reports it complete again.

**The hash check is a state of its own.** Between `take_inflight()` and
`mark_piece_hash_ok()` the piece is not `have`, not `queued` (reserving it
cleared that) and not `inflight`: it is in none of the three sets. Nothing
may hand it to a peer there, and the one thing that could -- the priority
loop of `acquire_piece()`, which reserves without asking the queue -- tests
`is_piece_fully_downloaded()` and passes over it. Reserving it is pure
damage: the chunks fetched for it come back `PreviouslyCompleted` and are
dropped, but only after being written over a piece the storage has been told
is finished, and the piece never leaves `inflight` again because nothing
reports it complete a second time.

### Piece Stealing Flow

```
IN_FLIGHT (peer A) → IN_FLIGHT (peer B)
```

`PieceTracker::acquire_piece()` with steal logic:
1. Finds a piece in `inflight` held whole by one participant -- a split
   piece is never stolen -- that has held it longer than threshold × this
   peer's average piece time (measured from that participant's `started`)
2. Replaces that participant with this peer's connection, same claim,
   `started = now`
3. Leaves `inflight[p].started` alone: the piece's age is what the reader
   has waited, and what the takeover rules and the completion median read
4. Returns `AcquireResult::Stolen { piece, chunks, from_peer }`

Caller then:
5. Calls `peers.on_steal(from_peer, to_peer, piece)`:
   - Sends Cancel messages to victim peer
   - Removes chunks from victim's `inflight_requests`
6. Stealer requests chunks:
   - Inserts into own `inflight_requests`
   - Sends Request messages

**Note:** Stealing does NOT call `reserve_needed_piece()` because the piece is already in `inflight`, not in `queue_pieces`.

### Peer Death Flow

```
IN_FLIGHT → IN_FLIGHT, a share back in the pool (others still on the piece)
IN_FLIGHT → QUEUED                               (it was the last participant)
```

1. `on_peer_died()` (and a choke, which hands back the same way):
   - Takes `LivePeerState` (consumes it)
   - Calls `PieceTracker::release_pieces_owned_by(peer_addr, connection)` --
     by connection, so a task winding down does not hand back what a fresh
     dial to the same address has since reserved

2. `release_pieces_owned_by()`: the partial release described under "Split
   Pieces" above.
   - Each of the connection's participants leaves its piece, and its claim
     goes back to the unclaimed pool unless another holder is still
     fetching it or it has all arrived
   - A piece left with no participant leaves `inflight` and is re-queued:
     `requeue_piece_keeping_chunks` if any chunk has arrived, so whoever
     picks it up asks only for what is missing; `mark_piece_broken_if_not_have`
     (which also clears `chunk_status`) if none has. A dropped piece is
     wiped either way and not queued, and its writers are forgotten with its
     chunks
   - Returns the number of pieces the connection held shares of

### Checksum Failure Flow

```
IN_FLIGHT → QUEUED
```

1. All chunks received, hash verification fails
2. `PieceTracker::take_inflight(piece)` → removes from `inflight`
3. `PieceTracker::mark_piece_hash_failed(piece)` → calls `mark_piece_broken_if_not_have(piece)` → sets `queue_pieces[p] = true`

**Failed hash: who is to blame.** The hash is over the whole piece, and
`PieceTracker::take_writers(piece)` says who wrote into it -- every peer
the write path stamped through `note_delivery`, across re-queues, since
the piece was last empty.

- **One writer**: it sent the bytes, and it is disconnected.
- **More than one**: nothing can say whose bytes were bad, so nobody is
  disconnected and nobody is written off. The writers go into
  `TorrentStateLive::hash_failure_exclusions`, and while any other peer
  has the piece, they are not offered it again
  (`pieces_to_leave_to_others`, read before `acquire_piece`). If no other
  peer has it, they may take it after all -- a piece nobody fetches is
  worse than one fetched twice. The set is cleared when the piece
  verifies.

### Pause Flow

```
IN_FLIGHT → QUEUED (for all in-flight pieces)
```

1. `pause()`:
   - Calls `PieceTracker::into_chunks()` which:
     - For each piece in `inflight`, calls `mark_piece_broken_if_not_have(piece)`
     - Returns the inner `ChunkTracker`
   - Stores the `ChunkTracker` for resume

## Architectural Notes

### State Encapsulation

Piece state is coordinated by `PieceTracker`:
- `ChunkTracker`: `have`, `queue_pieces`, `chunk_status` (wrapped)
- `PieceTracker`: `inflight` (owned)
- `LivePeerState`: `inflight_requests` (per-peer, separate)

`PieceTracker` methods ensure atomic state transitions that maintain invariants. Direct access to `ChunkTracker` is read-only via `chunks()`.

### Lock Ordering

To avoid deadlocks, locks must be acquired in a consistent order:

1. `peers` lock (via `with_live_mut` / `with_peer_mut`) - DashMap per-peer locks
2. `TorrentStateLive` lock (via `lock_write` / `lock_read`) - global torrent state

**Critical Rule: Never access other peers while holding a peer lock.**

The `peers` field is a `DashMap<PeerHandle, Peer>` which uses sharded locking. When you hold
a lock on one peer's shard via `with_live_mut` or `with_peer_mut`, you must NOT:
- Call `with_peer`, `with_live_mut`, `with_peer_mut` on a different peer
- Iterate over the peers DashMap
- Call any method that internally accesses other peers (e.g., `on_steal`)

This is because:
1. Thread A holds write lock on shard S1 (peer X)
2. Thread A tries to access peer Y which is in shard S2
3. Thread B holds/waits for shard S2 and wants shard S1
4. Deadlock!

**Example of what NOT to do:**
```rust
// BAD - accessing other peers inside with_live_mut
self.peers.with_live_mut(self.addr, "example", |live| {
    // ... do something ...
    self.peers.on_steal(other_peer, self.addr, piece);  // DEADLOCK RISK!
});
```

**Correct pattern:**
```rust
// GOOD - collect info inside closure, process outside
let steal_info = self.peers.with_live_mut(self.addr, "example", |live| {
    // ... return data needed for on_steal ...
    Some((other_peer, piece))
});
if let Some((from_peer, piece)) = steal_info {
    self.peers.on_steal(from_peer, self.addr, piece);  // Safe - no peer lock held
}
```

Care must be taken when modifying state transition logic to maintain this ordering.

### Acquire Strategy

`PieceTracker::acquire_piece()` walks, in this order:

1. **The stream's lookahead, in playback order** (`walk_lookahead`) - a
   free piece is reserved (split while within `deadline_pieces` of the
   head, whole beyond it), and one already in flight is cut, joined or
   shared out by the rules in `CLAIMS.md`.
2. **Steal (10x threshold)** - the first lookahead piece this peer could
   not take, from a holder ten times slower.
3. **The ordinary queue** (`iter_queued_pieces`), claimed whole.
4. **Steal (3x threshold)** - any piece from a holder three times slower,
   ranked by how long its holder has had it.

Reserving beats stealing, except for a piece a reader is actually waiting
on: there, nothing else this peer could fetch would help.
