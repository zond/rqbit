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
- **Doubled.** One claim of a split piece with more than one holder. The
  rescue for a stalled claim: whichever copy lands first finishes it, and
  the losers' outstanding requests are cancelled when the piece completes
  (`overtaken_by`). It spends bandwidth by design, which is why it needs a
  justification and the other two do not -- and the justification is what
  bounds it, not a count.

## The one measurement

Every takeover -- cutting a whole head piece, doubling a claim, taking a
share of the pool beyond one's own -- is justified by one comparison of two
durations:

> **A peer may take over work on a piece iff the latency of its last
> delivered chunk is shorter than the time the piece has been in flight.**

- **The asker's side**: `last_latency`, the time from sending the request
  for its most recent chunk to that chunk arriving. Recorded on the live
  peer (`inflight_requests` is a map from chunk to `sent_at`). A peer that
  has never delivered has none, and outpaces nothing.
- **The piece's side**: its age, now minus when it was first reserved
  (`InflightPiece::started`). **Not the holder's last delivery** (an
  earlier version): a holder with seconds of latency and a hundred
  requests in flight lands a chunk every few milliseconds and never looks
  quiet, while its piece takes ten seconds. What the reader waits on is the
  piece, so the piece is what is measured.

`outpaces(piece) = last_latency < age(piece)`. Read: *had I been asked when
this piece was, I would have delivered by now -- and it is not done.* A
piece reserved a moment ago is nobody's to cut, double or over-share; one
in flight for seconds loses to any live peer. No threshold and no constant.

For joining a claim somebody already holds, the clock is the claim's
**newest hand-out** rather than the piece's start: *had I been handed this
claim when its latest holder was, I would have delivered it by now.* It is
the same comparison one level down, and it implies the piece's, since no
hand-out predates the piece.

**And when it is asked.** The comparison is only as good as the moments it
is made at. A peer asks whenever it has a request slot free, and a slot
frees when a chunk lands -- which is exactly when its latency was
re-measured. So before every chunk it sends for work it took from deeper
in the window, a peer offers itself to the two head pieces first
(`acquire_head_share`): the same rules over the same two pieces, and
nothing past them. What the head gives it, it sends first, then resumes
what it was on. Without this, a peer turned away from a head piece a few
milliseconds old was handed a whole piece and sent all 256 of its chunks
before asking again -- two request windows -- and by the time the head
piece was old enough to share out, everyone who could share it was
committed elsewhere (the fourth field log, 2026-09-15: ten of sixteen
shares of the head piece in the pool for two seconds, a 5.4 s read).

## The rules, in the order a peer meets them in `acquire_piece`

The walk goes over the lookahead in playback order, `deep` counting the
pieces this peer could actually take.

1. **Cut a whole piece at the head** (`InflightPiece::split_whole`). At
   depth < 2, a piece held whole is cut if the asker outpaces the piece:
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
   that, only if the asker outpaces the piece: had the other peers been
   coming for the pool, they would have taken it by now. Twenty peers
   arriving within milliseconds find a piece younger than any of their
   round trips and spread it; three peers leave shares in the pool and
   whoever returns half a second later takes them. The first field log
   (2026-09-15) had ten of sixteen shares of piece 0 untaken for ten
   seconds under the rule this replaced -- "take beyond your share only if
   you delivered since the last handout" -- because each peer took two and
   went to fetch whole pieces deeper in the window instead.

3. **Join a claim** (`stalled_claim`). Any claim with chunks still missing
   whose newest holder the asker outpaces -- its latency is shorter than
   the time since that holder was handed the claim -- never the asker's
   own, the one with the most left first. A healthy holder finishes
   sixteen chunks within one of its round trips, so a claim still open
   that long is joined only by somebody faster than its newest holder, and
   a silent one is joined by anyone live. There is no count of holders: the
   fourth field log had a claim held by a peer that delivered nothing in
   fourteen seconds and a doubler with five seconds of latency, and a cap
   of two kept every faster peer off it for six seconds.

4. **Refused everywhere: `Crowded`.** Returned only after the whole
   lookahead, a steal attempt and the ordinary queue have all yielded
   nothing. The request loop then waits for **its own next chunk to land**
   -- the event that would make a share its -- with the existing 5 s wait
   as backstop. A peer over its share always has claims in flight to wait
   on, so this cannot deadlock.

5. **A fresh peer** outpaces nothing: two free shares if any, otherwise a
   whole piece from the ordinary queue. It proves itself there.

6. **Steal** stays as vanilla for whole pieces deeper in the window. At the
   head, cutting makes it nearly redundant. It refuses any piece with more
   than one participant.

7. **A pause during a hash check** requeues the piece (`into_chunks`): it
   was in none of the three sets and would otherwise never be fetched
   again.

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
- **The holder's last delivery as the clock** (`Activity::outpaces(holder)`,
  with `Participant::last_delivery` stamped by the write path): a slow
  holder with a deep pipeline delivers steadily and was never outpaced,
  and a healthy piece's holders reset the clock every few milliseconds,
  which was the point -- but the reader waits on the piece, not on any
  holder, so the piece's age is the honest clock. The stamp stays, for the
  diagnostic line.
- **"Delivered since the last handout"** as the over-share gate: with any
  other work in the window a peer over its share went there instead, and
  the pool sat.
- **A cap of two holders per claim** (`MAX_HOLDERS_PER_CLAIM`): a constant
  standing in for the judgment "a third copy is not worth it", which is
  true of a claim whose holders are delivering and false of one whose
  holders are dead weight. The newest hand-out comparison tells them apart;
  the cap could not.
- **Asking only at piece boundaries.** The request loop acquired a share,
  sent every chunk of it, and only then asked again. Right for a 16-chunk
  claim, wrong for a 256-chunk whole piece: the rules above were correct
  and were not consulted for seconds at a time.
- **"Only a claim that has delivered nothing"** as the doubling gate, with
  `missing_at_start` to judge a re-handed claim on its own work. The first
  field log on the latency rules (2026-09-15) showed why not: a holder
  trickling a chunk a second is "being fetched" and never rescued, and the
  piece completes when it finishes its sixteen chunks -- a 24-second block
  on the head piece with fifteen seeders, a 30-second one with twenty-two.
  The outpacing rule alone already spares a healthy holder, since its
  deliveries are milliseconds apart.

## Cost of being wrong

Cutting a healthy holder: one window of duplicate tail, but a healthy
holder is nearly impossible to outpace. Doubling a healthy claim:
impossible by construction, it has delivered something. Refusing a fast
peer: it waits one chunk arrival, not a timer.

## Status

Built 2026-09-14 (`a31258c0`), rebuilt on the piece's age 2026-09-15 after
two field logs; the head-at-every-slot ask and the newest-holder rule for
joining a claim added the same day after the fourth log. Every rule and the three wiring points are proven by a test
that fails under mutation. The first log on the latency rules (xtremio
`a58f5f0`) had 24- and 30-second head-piece blocks; the second (`ef6ac8c`,
with the claims probe) showed why: ten of sixteen shares of piece 0 sitting
in the pool while three peers each held two, and whole pieces held by
high-latency pipelined peers that the holder's-last-delivery clock could
not see. Both are what the piece's age measures. The fourth log (`2272a51`) then
showed the comparison right and unasked: fast peers away on whole pieces
while the head pool sat, and a claim at the holder cap with two dead-weight
holders. Read the next log's `blocked_read_claims` lines for pieces older
than a second with shares still unclaimed, or claims open longer than any
connected peer's latency with only slow holders: there should be none.
