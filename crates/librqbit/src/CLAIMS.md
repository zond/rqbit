# Claims: how a piece a stream waits on is shared between peers

Agreed with zond on 2026-09-14, after the read-pattern retention work and
three field logs. This is the design `piece_tracker.rs` implements; if the
code and this file disagree, one of them is wrong and it is probably the
code. Written down because the previous version of these rules was worked
out in conversation, rebuilt twice in one day, and the second rebuild made
the first one's mistake again.

## Three states of a piece

- **Whole.** One holder, one claim covering the piece. Every piece reserved
  outside the head of the lookahead -- at depth >= `DEADLINE_PIECES` (2),
  or from the ordinary queue -- is whole. This is vanilla rqbit: stealable
  at 10x, and `peer_avg_time` is honest because one peer fetched all of it.
- **Split.** Cut into `CLAIM_CHUNKS` (16) claims, each held by one peer,
  unclaimed claims in a pool. Only the two head pieces are ever split. The
  work is disjoint, so splitting costs nothing in duplication. What it costs
  is that the piece is done when its *slowest* claim is done.
- **Doubled.** One claim of a split piece with two holders
  (`MAX_HOLDERS_PER_CLAIM`). The rescue for a stalled claim: whichever copy
  lands first finishes it, and the loser's outstanding requests are
  cancelled when the piece completes (`overtaken_by`). It spends bandwidth
  by design, which is why it needs a justification and the other two do
  not.

## The one measurement

Every takeover -- cutting a whole head piece, doubling a claim -- is
justified by one comparison, and it is a comparison of two durations:

> **A peer may take over work another peer holds iff the latency of its
> last delivered chunk is shorter than the time we have been waiting on
> that holder.**

- **The asker's side**: `last_latency`, the time from sending the request
  for its most recent chunk to that chunk arriving. Recorded on the live
  peer (`inflight_requests` is a map from chunk to `sent_at` so the
  difference can be taken on arrival). A peer that has never delivered has
  none, and outpaces nobody.
- **The holder's side**: `waited`, now minus the holder's last delivery *on
  this piece* (a `Participant` field stamped by the write path), or now
  minus when it was asked (`Participant::started`) if it has delivered
  nothing here. Per piece and not global, because a holder busy delivering
  earlier claims of the same piece is not stalled on this one -- it is
  queued behind itself, and it requests in order.

`outpaces(holder) = last_latency < waited`. Read: *had I been asked when
they were, I would have delivered by now -- and they have not.* A holder
that delivered 3 ms ago is untouchable; one silent for seconds loses to any
live peer. No threshold and no constant.

The reference points differ on purpose. Cutting compares against the
*holder's* silence (am I doing more than the peer I am taking from);
taking an unclaimed share, below, compares against the *piece's* last
handout (has anyone else shown up since I last did something).

## The rules, in the order a peer meets them in `acquire_piece`

The walk goes over the lookahead in playback order, `deep` counting the
pieces this peer could actually take.

1. **Cut a whole piece at the head** (`InflightPiece::split_whole`). At
   depth < 2, a piece held whole is cut if the asker outpaces its holder:
   the holder keeps the claim it is currently delivering into (the first
   with anything missing), claims already on disk need nobody, and every
   claim after goes to the pool. Without this, splitting never engaged in
   real playback: the swarm reserves pieces far ahead of the window, so
   every piece is whole by the time the stream reaches it, and the head
   piece was one slow peer's whole claim with sixteen faster peers sent
   past it. Cost: the holder's already-sent requests for the tail still
   land -- up to one request window of duplication, once per piece, which
   is why it is not cut for just anyone.

2. **Take an unclaimed share.** Up to `CLAIMS_PER_PEER` (2) freely. Beyond
   that, only if the asker **has delivered a chunk since the last share of
   this piece went to anyone**: it is draining its window and nobody else
   has shown up. Another peer taking a share resets that mark. This is the
   one event-based rule, because there is no holder to be faster than; the
   question is whether others are arriving. It replaced "take the rest if
   the lookahead holds nothing else", which in steady state it never does,
   so the first visitor took every share within a millisecond and nothing
   was spread.

3. **Double a claim** (`stalled_claim`). Only a claim that has **delivered
   nothing of its own** (`left == missing_at_start`), only if the asker
   outpaces its holder, only up to two holders, never the asker's own. Two
   healthy peers therefore never double each other.

4. **Refused everywhere: `Crowded`.** Returned only after the whole
   lookahead, a steal attempt and the ordinary queue have all yielded
   nothing. The request loop then waits for **its own next chunk to land**
   -- the event that would make a share its -- with the existing 5 s wait
   as backstop. A peer over its share always has claims in flight to wait
   on, so this cannot deadlock.

5. **A fresh peer** outpaces nobody: two free shares if any, otherwise a
   whole piece from the ordinary queue. It proves itself there.

6. **Steal** stays as vanilla for whole pieces deeper in the window. At the
   head, cutting makes it nearly redundant. It refuses any piece with more
   than one participant.

7. **A pause during a hash check** requeues the piece (`into_chunks`): it
   was in none of the three sets and would otherwise never be fetched
   again.

## Judged on its own work

`Participant::missing_at_start` records how many chunks of the claim were
missing when it was handed out. A claim put back by a peer that left comes
back with that peer's chunks on disk, and the next holder is not credited
with them: they are not its progress, so a stalled re-handed claim can
still be doubled.

## What this replaced, so nobody rebuilds it

- **Blind doubling** (any arriving peer could double any claim with the
  most missing): fetched the whole lookahead twice, 35% of all bytes
  unverified in the field.
- **Proof by finishing a claim** (a peer earned a duration by retiring its
  own finished claim and could double claims older than that): needed a
  proof only a holder could have, so a whole piece drifting to the head
  could never be rescued; and a doubled claim credited both holders,
  including the loser. Deleted with `proven`, `contested` and the
  retire-to-earn-a-duration protocol.
- **A grace timer** (`SPREAD_GRACE`, 100 ms) for the over-share case: a
  constant standing in for an observable event. Replaced by "delivered
  since the last handout".
- **The `idle` escape** (a peer with nothing in flight could take anything):
  unnecessary, since a peer over its share has claims to wait on, and a
  fresh peer belongs on the vanilla queue.

## Cost of being wrong

Cutting a healthy holder: one window of duplicate tail, but a healthy
holder is nearly impossible to outpace. Doubling a healthy claim:
impossible by construction, it has delivered something. Refusing a fast
peer: it waits one chunk arrival, not a timer.

## Status

Written before the latency rules were built. The tree at the time carried
the grace-timer version with revert-proven tests; the rebuild deletes
`proven`, `contested`, `idle` and `SPREAD_GRACE`, adds `last_latency` on
the live peer and `last_delivery` on `Participant`, and rewrites the tests
around deliveries and latencies. Update this section when it lands.
