# 0026: Hold the single cursor until a resubscribe round is complete

**Status:** Accepted as a design, not built yet; the server close rule (D4)
is built first, then the transport part. Amends
[ADR-0002 C1](./0002-adversarial-review-corrections.md) and
[ADR-0011 D3/C1′](./0011-ssr-dehydrate-hydrate.md). Extends
[ADR-0016](./0016-reconnect-policy.md) and
[ADR-0023 D7](./0023-on-demand-watches-acquisitions-holds.md).
**Date:** 2026-09-29. Revised 2026-11-17 after the independent check and four reviews.

## Context and evidence

F1 loses data when one subscription advances the shared cursor before another
has caught up. Starting at 9, messages can finish at 18 before files receive a
delete; a second drop then resumes files past that delete. A broadcast
boundary can do the same. `tests/cursor-resubscribe-round.test.ts` reproduces
both.

Line references are at `9ea1932`; the server is `src/server/mixin.ts`. The
**check** is an independent check of the original proposal, run at `c23e1ee`
with db 0.9.2. It used hold spikes and probes P1 to P11. These were throwaway
experiments and are not kept in the repository. Each fact this ADR takes from
them is pinned in the validation contract, except P2, which motivates the
deferred export work.

The suite has 29 scenarios and one harness check. With plain assertions the
check found 16 convergence failures and 14 passes. A hold passed all 30 tests;
a hold that never released failed 29. Holding every registered subscription
froze the cursor when one never got a reply: P4 recorded a mutation at 3 with
the cursor at 1 and `awaitSeq(3)` timing out. Holding only bootstrapped
entries passed all 30 and gave P4 cursor 3 with `awaitSeq` resolved. That is
the cohort chosen here; D7 protects an unfinished subscription independently.

## Decision

### D1: Hold for entries that were bootstrapped at round start

When installing a reconnect socket, before restart callbacks or sub sends,
capture the registered entries and `appliedSeq`. The cohort is the captured
entries whose `bootstrapped` flag is true, tracked by entry identity in a
pending set scoped to this socket and round. The candidate maximum starts at
the captured cursor. An empty cohort, or an initial connection, has no hold.
Resubscribe every captured entry with one captured cursor and D7's resume
selection (D2), skipping entries a callback removed. Membership never grows
during a round. While a cohort entry is pending:

- Dispatch rows and commit callbacks normally; rendering, reset cleanup and
  subset readiness do not wait.
- A known entry's `snap-end` or scoped `uptodate`, and a broadcast `uptodate`,
  raise the candidate. None advances `appliedSeq` or resolves `awaitSeq`.
- Only a cohort entry's own terminal completes it. D3 defines retirement and
  callback failure.
- Candidate recording, handler dispatch and membership changes are one
  dispatch. Record a terminal's candidate before its callback can remove the
  entry. Release only after callbacks return, with socket and round identity
  checked again. Any callback that closes or reseeds discards the candidate.

After the last pending entry completes or retires, advance once to the
candidate through the existing path (`transport.ts:892`); later boundaries
advance normally. Never export the candidate, send it as `since`, or settle a
waiter with it. This adds round accounting, not a cursor, timer or wire field.

**Why maximum is safe.** Each retained cohort entry has its old baseline and
receives catch-up through its terminal. Once registered, live deltas cover
later writes. Before a later subscription boundary, C1′ flushes buffered
deltas on that socket (`mixin.ts:1080`); registration and terminal emission
are synchronous (`mixin.ts:1035`, `1125`, `1152`). On the ordered stream the
completing round therefore covers the greatest delivered boundary for every
retained claim. Excluded entries have no completed baseline claim; D7 protects
their next attempt. This is a proof from those code paths, supported by the
29 scenarios, not a check of every interleaving.

### D2: A drop discards the round and retains the safe cursor

On unexpected close, forced seed reconnect, explicit close or superseding
socket installation, discard the pending set and candidate without releasing.
The next round captures currently registered, bootstrapped entries anew and
starts from `appliedSeq`, or the lower seed chosen by D5. Keep ADR-0016's
scheduling-time reconnect flag, demand-driven connect, backoff (as amended,
D3 rider 1) and terminal stop (`transport.ts:514`).

Every resubscription keeps D7 (`transport.ts:552`), not the cohort test: a
bootstrapped entry uses the captured cursor; an unfinished explicit-since
catch-up uses its own `since`, or a lower nonzero shared cursor; an unfinished
snapshot uses no `since` and calls `onRestart` to undo partial state.

An excluded entry still receives rows and its own terminal during the round.
That terminal marks it bootstrapped (D3 rider 2) but does not add it to this
round; its boundary raises the candidate. Before that terminal, a later drop
repeats D7 bootstrap even if others advanced the cursor; after it, a later
round includes it and replays from the held cursor. So an interrupted round
cannot turn its candidate into a resume point. This rests on D7 and the flag
writes (`transport.ts:835,850`); `tests/reconnect-bootstrap.test.ts` pins
unfinished bootstrap recovery. The socket identity guard (`transport.ts:450`)
keeps abandoned stream frames out of the replacement round.

### D3: Joining bootstraps; leaving retires a claim; reset invalidates it

New entries, including a replacement sync run, do not join the cohort; they
follow D7. One added during a pending dial is sent once: a suspended
`subscribe()` must not resend it after resubscribe did (`transport.ts:552,679`).
Removing an entry retires its obligation. If it was last, release on the live
socket after any active dispatch finishes; teardown discards instead. Keep
today's adapter unload order (`do-collection.ts:471,498`); the earliest review
found no standalone loss from it, and this revision did not test unload.

Track entry identity as well as subId. A replacement on the same socket needs
a fresh wire subId, which ADR-0028 D4 (eager) and ADR-0023 D8 (watches)
supply. A terminal whose entry was known at dispatch start contributes under
D1 even if its callback unsubscribes. Ignore unknown or retired scoped
boundaries for advancement and the candidate. This also changes behaviour
outside a hold: today such a boundary, such as a released watch's trailing
terminal, advances the cursor (`transport.ts:834-837,848-853`). It only lowers
the claim, but the check's spike did not do it (its `advance` runs for any
boundary), so existing tests that read the cursor right after a release may
move. On db 1.0, `reload()` is a new sync run with fresh subscriptions that
keeps rows (ADR-0028 D3/D4); it need not delete kept eager rows first.

**A reset is not catch-up.** For the affected entries, clear `bootstrapped`
and explicit `since`, run their reset handlers and retire their obligations,
as one dispatch that clears eligibility before any callback. Release only
after all affected callbacks return. A socket change inside an unscoped reset
stops release and completion accounting, not cleanup: every affected callback
not removed or replaced by an earlier one still runs. A reseed dials
asynchronously (`transport.ts:384,398-404`), so the truncates land before the
new batch. A surviving entry snapshots without `since` on every attempt until
its own `snap-end`. A below-floor reset brings that snapshot on the same
socket (`mixin.ts:1118`); a deliberate refusal does not (`mixin.ts:1037`, D4).
Today reset only calls handlers (`transport.ts:884`).
P11 showed the bug: after reset and a partial snapshot the third socket sent b
with `since=50`; with this rule it sent b without `since`. On-demand reset may
retire all watches in its generation (ADR-0023 D5); each removal retires its
obligation. Reset does not imply readiness or supply a sequence. Every reset
the server sends carries a sub id (`mixin.ts:355,1040,1051,1068,1092,1118` and
the prerequisite's refusal). The client accepts an unscoped one
(`transport.ts:887`), so the unscoped rules stay, but only a fake peer can
drive their pins.

**A callback that throws during a held round abandons the round.** While a
cohort entry is pending, if a row, terminal, broadcast, reset or restart
callback throws, complete no entry and release no candidate, even if the
callback removed its entry. Discard the round as D2 does. Unless the callback
already closed or replaced the socket, replace it as the close handler does
for an unexpected drop (`transport.ts:451-471`): park in-flight mut/call and
consult the policy with no close code, or settle them under ADR-0021 if no
subscription is left. Rethrow the original error, as today; a restart
callback's error must also be reported uncaught, since its dial rejection is
swallowed on a timer-driven reconnect (`transport.ts:384-386,534-536`). Flags
are unchanged, so the next round resumes the cohort from the held cursor and
idempotent replay repairs what the callback left half done, whether it threw
before or after its commit (`do-collection.ts:338,353,364`).

The cost is liveness, not safety. A row write that throws every round, such
as a duplicate key, abandons every round at that frame, so entries whose
frames come later (batch order is registration order, `transport.ts:555`)
never get their catch-up: the rest of each socket is dropped as stale
(`:449`). The cursor stays held and the backoff grows to the cap; a custom
policy returning `null` is the app's only signal. Today the row is skipped.

Outside a held round today's behaviour stays: the error leaves through the
message listener (`transport.ts:435-450`), the rest of that frame is skipped,
and the transport and its other collections keep working. ADR-0028 D2 holds:
the error goes uncaught and the collection stays ready. No latch, lifecycle
state or app API is added; ADR-0020 is unchanged. Four riders apply:

1. **Backoff is the ADR-0016 §3 amendment of 2026-11-17** (pull request #59,
   merged): only a socket that stayed open past a threshold resets the
   attempt counter, checked at close. This ADR adds no reset rule. An
   abandonment here or under rider 4 must take the close path through the
   policy, never `forceReconnect`
   (`transport.ts:373-387`), which skips the policy and would loop unpaced. A
   round abandoned on a young socket then never resets the backoff; one on an
   older socket does, but cycles no faster than once per threshold.
2. **Set `bootstrapped` only after the terminal callback returns**, on the
   entry captured at dispatch start. Today it is set first (`transport.ts:835,850`).
   An excluded entry whose first terminal callback throws before commit would
   then join the next cohort and resume without its snapshot rows, breaking
   D1's premise. Outside a hold the entry also restarts after the next drop.
   For eager that costs one reconciling snapshot. A watch would restart
   through `dropHolds` (`do-collection.ts:706`), deleting every row it holds,
   including rows from live deltas beyond its snapshot window (`:672-676`) and
   from fetches it covered (`:515-534`). Its bounded resnapshot restores
   neither. Today, after the common throw after commit, it resumes with `since`
   and loses nothing. So a watch the adapter has accepted (`w.bootstrapped`,
   set at `:668` before the commit) treats `onRestart` as its `onReset` does
   (`afterApplied(truncateAll(), onReady, onFail)`, `:701`), and core's
   truncate replay reloads every demand. The cost is that the restart reloads
   every watch of that collection, as a below-floor reset does. Its
   unsubscribes retire those entries (D3) and drop them from the batch (D1).
   This one adapter change means the build touches
   `src/client/do-collection.ts`; the ADR is not transport only.
3. **A reset dispatch runs every affected callback even if one throws**, then
   rethrows the first error. Flags are cleared before any callback, so
   stopping early leaves entries invalidated without cleanup: a watch would
   restart through `dropHolds`, which deletes rows that covered fetches and
   does not restore them (`do-collection.ts:701,706`). In and out of a hold.
4. **A restart callback that throws abandons that socket attempt**, as a
   failed sub send does (D4), with or without a cohort. Today the throw in
   `resubscribeAll` (`transport.ts:556`) follows install (`:472,481`), so the
   rest of the batch and the replay (`:485`) never run, the dial's rejection is
   swallowed (`:534`) and `connect()` returns at once (`:390`): later
   subscriptions are dead on a live socket.

These rules are unprobed; their pins must pass before shipment.

### D4: Receipts follow the complete resubscribe batch

**Prerequisite: the server refuses or closes.** A deliberate refusal (unknown
collection, the sub cap, an unsupported predicate, a query over a known SQLite
limit) gets `reset`, with nothing of the sub left registered, in memory or
durably. That is the long in lists change, pull request #60, with the
ADR-0012 and ADR-0013 amendments of 2026-11-17. An unexpected handler failure
closes the socket with 1011, which the policy retries, as for ADR-0019's
failed restore. It never gets a bare `reset`, which would tell the client a
snapshot follows when none does. This close rule is decided but has no ADR or
code yet; the transport part of this ADR must not ship before it. At this
ADR's base `mixin.ts:1102` can throw before replying, then dedup replay at
`:918` can send a receipt.

The close needs only that no reply reaches the client on that socket after
the failure. The server may still run later requests: it checks a socket by
tag (`mixin.ts:469-470`), not by whether it closed, so a replayed `mut`
queued behind the failed `sub` may still run, and dedup answers it on the
next socket. Its reply cannot reach the client,
because an endpoint sends no data frame after its Close frame (RFC 6455
section 5.5.1); that workerd honours this is not verified.

**A refused or closed cohort entry.** A refusal runs D3: the entry is retired
and the round may release over the other claims. A close abandons the round
(D2). Neither releases over a still-valid claim. The client shows, read from
code and not run:

- **Deliberate refusal.** A resumed eager entry's reset handler truncates and
  marks ready (`do-collection.ts:354-364`), so the collection is empty and
  `ready`. The client still lists the sub but the server holds none, so it
  gets no changes until the next drop resnapshots it (D3). A refused watch
  runs `truncateAll` (`:701`), and core reloads each demand (`:466-470`).
- **Unexpected failure, so close.** Rows stay, stale but present. 1011 is not
  terminal by default (`transport.ts:156-160`), so the cohort resumes from the
  held cursor on the next socket. A failure that repeats is paced by the
  ADR-0016 amendment, as rider 1 describes.

Send all surviving captured subscriptions synchronously before replaying
parked mutations or commands, then allow new mut/call sends; restart
callbacks must not interleave one. Check socket, attempt and entry identity
after every callback, before each sub send and before parked replay. Close or
reseed invalidates the old attempt: stop its remaining callbacks and sends;
only the new attempt resubscribes and replays. A send failure or a throwing
restart callback (D3 rider 4) abandons the attempt; no mutation follows a
partial batch (`transport.ts:479,552,585`).

With the prerequisite, synchronous `handleSub` (`mixin.ts:1035`) emits a
terminal or reset, or closes, before a later mutation can produce a receipt
that reaches the client. Successful client dispatch then completes or retires
the cohort before `committed`. C1's flush before receipt remains required,
including dedup replay (`mixin.ts:798,865,918`; ADR-0025). A current-socket
receipt advances normally before its promise continuation runs
(`transport.ts:855`), so `appliedSeq >= commitSeq` at successful settlement.
It never clears a pending set. A current-socket receipt with a valid cohort
entry still pending falsifies this proof and must block shipment.

The smaller cohort removes P4's counterexample: an unanswered subscription
that never bootstrapped cannot block this receipt, nor can a new sub or an
unfinished snapshot. P5 observed `sub, sub, sub, mut`, all three terminals
before `committed`, and cursor 6 at settlement for commit 6. It did not cover
parked replay, async authorization or exceptions; the prerequisite and D3
close those by design, pending their pins. Keep ADR-0021's parked outcomes,
timeout and no-subscription fallback, and the stale-socket exception: a known
txId can settle from an abandoned socket without advancing the stream
(abandoned-receipt test, `tests/ssr-cursor.test.ts`). `connect()` still
resolves when a socket opens, not when a round completes.

### D5: A hydration seed during a hold starts another round

A valid positive seed arriving during a hold abandons the socket and round,
then reconnects from `min(appliedSeq, seed)`, keeping 0 if held at 0. Do this
even for a seed at or above the held cursor, and even before any terminal has
raised the candidate. Outside a hold keep the existing seed rules, including
cursor 0 being a no-op (`transport.ts:350`). Use the existing voluntary
reconnect and stale stream guard.

P1 explains why: at held cursor 100, A can apply 200 before hydration at 150
overwrites A; ignoring the seed then releases 200 over stale rows. The check
saw no reconnect without this rule, and a fresh socket with both subscriptions
at `since=100` with it. Comparing only against the candidate also misses rows
delivered before their terminal.

After a seed reconnect, keep that safe cursor until the cohort completes or
retires; a broadcast alone cannot release it (the changed SSR expectation
below). `appliedCursor` and `awaitSeq` use only `appliedSeq`. No adapter
export value is promised: `exportSyncMeta` can use an imported or settled
cursor (`do-collection.ts:967`), such as 150 while the transport holds 0. A
no-change round may complete at the seed, so `awaitSeq(seed + 1)` is not a
general ready signal.

## Deferred decisions

A later ADR, with no number allocated, must own the adapter export baseline
gate and active repair of late cursor-0, filter-mismatched or malformed
hydration chunks, including queued eager writes, replacement cleanup, unload
reordering and their tests. P2 showed the export gap without any hold: a
never-synced adapter and an unfinished catch-up both exported another
collection's cursor 200. The hold only lowers the transport cursor and D5
keeps seed repair. That argues for separate work; it is not a verified export
non-regression result.

Leave L1 and L2 in `tests/cursor-known-limitations.test.ts` as expected
failures; neither passed in the check's suite with the hold. L2 loses a
buffered write while the socket survives, so no round starts. L1 needs storage
incarnation evidence. Both need separate wire decisions; sequence gaps alone
do not prove missing delivery (ADR-0002 C5, `mixin.ts:1139`).

**Not covered: a callback that throws outside a held round.** A row write or
commit that throws is skipped (`transport.ts:830,842,852`), and a later
boundary advances the cursor past rows that never committed, so a later drop
resumes beyond them. Rider 2 covers only a first terminal. This gap exists
today and is not F1.

**Not covered: a commit receipt that rejects after its callback returned.**
That work did not commit, but the transport has already completed the entry.
It stays under the adapter contract: the transport sees only synchronous
callback outcomes, and the gap exists today with or without a hold.

## Validation contract

This commit changes documentation only. The builder must:

1. Change all sixteen `it.fails` cases in `cursor-resubscribe-round.test.ts`
   to ordinary tests. Remove `isRed` selection and the expected-failure
   harness guard. Keep all 29 scenarios, convergence, final cursor equality
   and the final live write, so a never-releasing hold still fails.
2. In `ssr-cursor.test.ts`, "a stale pre-regress boundary cannot re-advance
   the claim; the fresh socket replays from the seed", keep cursor 50 after
   broadcast 102, then send s1's terminal at 102 and expect 102. Keep the
   stale 101 and `since=50` assertions. This replaces the F1 expectation at
   line 220. Preserve the abandoned-receipt test unchanged.
3. In `reconnect-window.test.ts`, "a mutation-driven connect still resubscribes
   from the cursor", send s1's terminal before the receipt. Assert sub before
   mut and C1 at settlement. Its current fake skips a required terminal.
4. Leave the original proposal's three `ssr-adapter.test.ts` changes deferred
   with the baseline gate: "export round-trips through import; the eager
   where is fingerprinted", "a DIFFERENT where downgrades the cursor to the
   snapshot-reconcile path", and "exportSyncMeta never claims a boundary
   whose receipt is unsettled".

The sixteen convergence flips are the original cuts:

| Offline changes | Cuts that become ordinary passing assertions |
|---|---|
| mixed | after eager messages, eager files, on-demand a*; mid eager files, on-demand a*, on-demand c*; after a live broadcast mid-round |
| files only | after eager messages; mid eager files; after a live broadcast mid-round |
| on-demand moves | after eager messages, eager files, on-demand a*; mid on-demand a*, on-demand c*; after a live broadcast mid-round |

Add behavioral pins before shipping, with these observable outcomes:

- P4 freeze: with a healthy bootstrapped sub and an unanswered unbootstrapped
  sub, reconnect and deliver a live write at 2. Before sending any mutation,
  assert cursor 2 and `awaitSeq(2)` resolved. Then mutate at 3 and assert C1.
  Use a controlled unanswered peer even after the server prerequisite lands.
  Also pin an empty cohort and unfinished snapshot/explicit-since recovery.
- P11 reset then drop: two bootstrapped subs, below-floor reset, partial second
  snapshot and another drop. The invalidated sub sends no `since` and finally
  converges. Include refusal without a terminal and a fake-peer unscoped reset
  whose callbacks remove multiple watches; no release halfway through reset.
- Interrupted rounds at different cuts discard every old candidate; a parked
  mutation keeps its first-drop deadline and still times out typed. Last-entry
  removal, externally and within a terminal, releases once; a terminal at 15
  whose callback removes the last entry releases 15, not the earlier
  candidate. Close or reseed in a callback discards.
- Unequal boundaries: A completes at 12, B at 15; release 15. Separately, A at
  12, broadcast 20, then last pending B retires; release exactly 20 and resolve
  its waiter. An unknown scoped boundary at 99, during the hold or after,
  changes nothing.
- Add/remove/reload during a round, including real db 1.0 `reload()`: fresh
  ids reject old frames, replacements bootstrap independently, a sub added
  during dial is sent once, and kept-row reconciliation holds.
- Seeds below, at and above the held cursor, a hold at 0, and partial delivery
  before a terminal: each replaces the socket, and stale boundaries cannot
  release. Assert the transport cursor and `awaitSeq`, not adapter exports.
- New mut/call and parked replay, with async authorization and refusal: cohort
  sub sends precede mutation sends, cohort terminals or resets precede
  receipts, C1 holds. Cover a sub after a mutation send, restart-callback sends
  and a failed sub send. Close or reseed in a restart callback stops the old
  batch before its next sub or replay; only the replacement attempt continues.
- With a parked mutation and the real server, a deliberate refusal of a
  bootstrapped cohort entry (for example an unknown collection after a
  deploy): the reset precedes the replay receipt, D3 retires the entry, and an
  eager one is empty and ready. Separately, inject a handler throw for such an
  entry before its terminal: the server closes with 1011, no reply reaches the
  client on that socket, and the client reconnects from its safe cursor, rows
  intact, and settles the mutation from dedup.
- In a held round, throw before and after commit from row, terminal, broadcast
  and reset callbacks, also after removal or reseed: no release, the socket is
  replaced through the policy (not `forceReconnect`), work replays, the error
  is rethrown, and data converges. A repeating throw backs off, and a cohort
  entry behind a row that throws every round gets no catch-up while the
  cursor stays held. An excluded eager entry whose first terminal throws
  resnapshots. Outside a hold the transport keeps working. A fake-peer
  unscoped reset runs every callback after one throws or changes the socket.
  A restart callback throw, with or without a cohort, sends nothing more,
  reports the error uncaught, and retries.
- A bounded watch (`orderBy` and `limit`) whose first `snap-end` callback
  throws after its commit, then covers a fetch that returns rows outside its
  snapshot window, such as a scroll-back page (`do-collection.ts:732-750`):
  after a drop those rows are present once core's truncate replay settles.
  Restarting through `dropHolds` loses them; an unbounded watch would hide
  that, because its resnapshot brings every row back. Also assert that the
  old watch's sub id is not sent on the new socket, and that each replacement
  sub id is sent exactly once.

Run affected suites at the supported floor and locked db version (ADR-0022,
ADR-0028), keeping reconnect, bootstrap, barrier, catch-up, recovery, txId,
hibernation, SSR and on-demand coverage. The check's spike changed only the
named SSR expectation and the sixteen flips. It did not run on 0.8.6 or 1.0,
nor implement D3's unknown-boundary and callback-throw rules. No revision of
this ADR ran tests or spikes.

## Review record and consequences

A review of the original proposal, which held every registered entry, found
the deferred export and repair gaps, no loss from unsubscribe order, and no
C1 counterexample with a complete batch. The check then overturned that
cohort (P4), and established seed repair (P1), the export gap (P2) and reset
eligibility (P11).

Four reviews of this ADR followed; all read code only. Review 1, of the text
that first held only bootstrapped entries, found a throwing server handler
broke D4, callback exceptions had no policy, export wording exceeded transport
scope, and pins were missing. The next text added the server prerequisite, a
transport stop on callback failure, and the pins. Review 2 rejected the stop:
one app callback error silently froze every collection on the transport, also
with no round held, against ADR-0028 D2 and ADR-0020. The next text abandoned
only a held round, kept today's behaviour outside one, added the review's four
riders, narrowed the prerequisite, and recorded the scoped resets and the
unknown-boundary change. After review 3 an accepted watch restarts as a
reset (rider 2, an adapter change), the liveness cost of a repeating throw is
stated, an unscoped reset finishes cleanup after a socket change, lost pin
details and two wordings are fixed, and the decided server rule and the
ADR-0016 backoff amendment replace text of our own. Review 4 tightened the
watch pin to a bounded watch and restored the receipt scope note.

Partial fresh rows can stay visible while the resume cursor is older. Replay
must stay idempotent, as the full-row catch-up at `mixin.ts:1147` is. Retention
expiry can still require reset and snapshot (`mixin.ts:1113`). This design
adds no idle timers and makes no broader delivery guarantee.
