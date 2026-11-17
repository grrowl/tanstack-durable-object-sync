# 0026: Hold the single cursor until a resubscribe round is complete

**Status:** Proposed. Amends [ADR-0002 C1](./0002-adversarial-review-corrections.md)
and [ADR-0011 D3/C1′](./0011-ssr-dehydrate-hydrate.md). Extends
[ADR-0016](./0016-reconnect-policy.md) and
[ADR-0023 D7](./0023-on-demand-watches-acquisitions-holds.md).
**Date:** 2026-09-29. Revised 2026-11-17 after the independent check.

## Context and evidence

F1 loses data when one subscription advances the shared cursor before another
subscription has caught up. Starting at 9, messages can finish at 18 before
files receive a delete. A second drop then resumes files past that delete.
A broadcast boundary can cause the same omission. The convergence checks in
`tests/cursor-resubscribe-round.test.ts` reproduce both cases.

This revision starts from the proposal at `90c4bdb`. Code line references below
refer to the revision base `9ea1932`. The server implementation is in
`src/server/mixin.ts`; `src/server/sync-do.ts:29` applies that mixin.
The independent evidence is the **Call 1** section of
`/Users/tom/.bb/thread-storage/thr_6qqtzubfx3/report.md` (the **check** below).
Its sibling `evidence/` directory holds `adr0026-hold-spike.diff`,
`adr0026-hold-spike-variantB.diff`, `probes-cursor.test.ts` (P1 to P5), and
`probe-reset-then-drop.test.ts` (P11). These are experiment records, not code
to copy into production. The check ran at `c23e1ee` with db 0.9.2.

The suite has 29 scenarios and one harness check, rather than 30 scenarios.
With plain assertions, the check found 16 convergence failures and 14 passes.
A hold passed all 30 tests; a hold that never released failed 29. However,
holding every registered subscription froze the cursor when one never got a
reply. P4 recorded a successful mutation at 3 with the cursor still at 1 and
`awaitSeq(3)` timing out. Restricting the cohort to bootstrapped entries passed
all 30 and changed P4 to cursor 3 with `awaitSeq` resolved. This is the cohort
chosen here. D7 already protects an unfinished subscription independently.

## Decision

### D1: Hold for entries that were bootstrapped at round start

When installing a reconnect socket, before restart callbacks or sub sends,
capture the registered entries and the current `appliedSeq`. The cohort is
only those captured entries whose `bootstrapped` flag is true. Track their
entry identities in a pending set, scoped to this socket and round. The
candidate maximum starts at the captured cursor. An empty cohort creates no
hold. Initial connection retains ordinary D7 bootstrap behavior.

Resubscribe all captured entries, including those outside the cohort. Use
one captured cursor for the batch and the D7 resume selection in D2. Entries
removed by a callback must not be sent. Membership never grows during a round.
While any cohort entry remains pending:

- Dispatch rows and commit callbacks normally. Rendering, reset cleanup and
  subset readiness do not wait for the round.
- A known entry's `snap-end` or scoped `uptodate`, and a broadcast `uptodate`,
  can raise the candidate maximum. None advances `appliedSeq` or resolves
  `awaitSeq` through the candidate.
- Only a cohort entry's own terminal completes that entry. Broadcasts commit
  delivered deltas but complete no entries. D3 defines retirement.
- Treat candidate recording, handler dispatch and membership changes as one
  dispatch. Record the terminal's candidate before callback removal can
  release it. Release only after callbacks finish, with socket and round
  identity checked again. A callback that closes or reseeds discards the old
  candidate. This applies to broadcast callbacks too.

After the last pending entry completes or retires, advance once to the maximum
candidate. Later boundaries advance normally. Never export the candidate,
use it as `since`, or use it to settle a sequence waiter. This adds temporary
round accounting, not another acknowledgement cursor, timer or wire field.
The existing advancement and waiter path is `transport.ts:892`.

**Why maximum is safe.** Each retained cohort entry has its old baseline and
receives catch-up through its terminal. Once registered, live deltas cover
later writes. Before a later subscription boundary, C1′ flushes buffered
deltas on that socket (`mixin.ts:1080`); subscription registration and terminal
emission are synchronous (`mixin.ts:1035`, `1125`, `1152`). On the ordered
stream the completing round therefore covers the greatest delivered boundary
for all retained claims. Entries excluded from the cohort have no completed
baseline claim on the shared cursor; D7 protects their next attempt instead.
This is a proof from those code paths, supported by the 29 convergence
scenarios, not exhaustive verification of every interleaving. Dispatch
reentrancy and retirement require the additional pins below.

### D2: A drop discards the round and retains the safe cursor

On unexpected close, forced seed reconnect, explicit close or superseding
socket installation, discard the pending set and candidate without releasing.
The next round captures currently registered, bootstrapped entries anew and
starts from `appliedSeq`, or the lower seed chosen by D5. Keep ADR-0016's
scheduling-time reconnect flag, demand-driven connect, backoff and terminal
stop (`transport.ts:514`). `connect()` still means that a socket opened.

For every resubscription, preserve D7 (`transport.ts:552`): a bootstrapped
entry uses the captured shared cursor; an unfinished explicit-since catch-up
uses its own `since`, or a lower nonzero shared cursor; an unfinished snapshot
uses no `since` and calls `onRestart` to undo partial snapshot state. A reset
clears the explicit resume point as D3 specifies. Do not replace these rules
with the cohort membership test.

An excluded entry still receives rows and its own terminal during the round.
That terminal marks it bootstrapped but does not add it to this round. Its
boundary contributes to the candidate while a hold exists, or advances
normally after release. Before that terminal, a later drop repeats D7
bootstrap even if other entries advanced the shared cursor. After it, a later
round includes it and replays from the held cursor. Thus partial progress in
an interrupted round cannot turn its candidate into a resume point. This
argument rests on D7 and the terminal flag writes at `transport.ts:835,850`;
`tests/reconnect-bootstrap.test.ts` pins unfinished bootstrap recovery.

Keep the socket identity guard (`transport.ts:450`): abandoned stream frames
cannot affect the replacement round. D4 retains the separate receipt exception.
Repeated interruption and late frame cases are implementation pins below.

### D3: Joining bootstraps; leaving retires a claim; reset invalidates it

New entries, including a replacement sync run, do not join the cohort. They
follow the same D7 rules as entries unbootstrapped at round start. If added
during a pending dial, send them once on the installed socket: a suspended
`subscribe()` must not send again if resubscribe already sent that entry.
The two current send sites are `transport.ts:552` and `679`; a new pin must
exercise this overlap.

Removing an entry retires its pending obligation. If it was last, release on
the live socket after any active dispatch finishes. Teardown discards instead.
Keep today's adapter unload order (`do-collection.ts:471,498`). The earlier adversary found no standalone
loss from unsubscribing before cleanup; promise continuations run after the
callback. Reordering adapter cleanup is not needed for this transport repair
and belongs with the deferred export work. This does not weaken D1's rule
against release halfway through a terminal or reset dispatch. The earlier
review is recorded below; this revision did not independently test unload.

Track entry identity as well as subId. A replacement on the same socket must
use a fresh wire subId, since local identity cannot distinguish late frames
with a reused id. ADR-0028 D4 already supplies fresh eager ids per sync run;
ADR-0023 D8 supplies fresh watch ids. Ignore unknown or retired scoped
boundaries for both advancement and candidate accounting. A terminal whose
entry was known at dispatch start still contributes under D1 even if its
callback unsubscribes. On db 1.0, `reload()` starts a new sync run with fresh
subscriptions and keeps rows; it does not preserve the old transport entry's
bootstrap flag (ADR-0028 context and D3/D4). Do not require reload to delete
kept eager rows before unsubscribe.

**A reset is not catch-up.** For the affected entries, clear `bootstrapped`
and explicit `since`, run their reset handlers, and retire their obligations.
Do this as one dispatch, clearing eligibility before callbacks can reconnect.
For an unscoped reset, capture the affected entries and invalidate them all
before callbacks; release only after all affected callbacks finish. Recheck
identity if a callback replaces an entry or socket. A surviving entry must
snapshot without `since` until its own `snap-end`. A second drop before then
must restart that snapshot, even if the shared cursor has advanced.

Today reset only calls handlers (`transport.ts:884`). P11 showed the resulting
bug: after reset and a partial snapshot, the third socket sent b with
`since=50`; with the reset rule it sent b without `since`. A refused sub can
have reset with no terminal (`mixin.ts:1037`), while a below-floor reset has a
snapshot (`mixin.ts:1118`). Retiring the invalid baseline handles both without
stranding the hold. On-demand reset may retire all watches in its generation
(ADR-0023 D5); each removal retires its obligation. Refusal reporting remains
outside this ADR. Reset does not imply readiness or supply a sequence.

### D4: Receipts follow the complete resubscribe batch

Send all surviving captured subscriptions synchronously before replaying
parked mutations or commands, then allow new mut/call sends on the socket.
Restart callbacks must not interleave a mut/call send into that batch. A send
failure abandons the socket attempt; no mutation may follow a partially sent
batch. These are implementation constraints based on the existing order at
`transport.ts:479` and `585`, not a claim that every failure path is tested.

For valid cohort entries the server's synchronous `handleSub` emits a terminal
or reset before processing a later mutation frame (`mixin.ts:1035`). The
ordered incoming stream therefore completes or retires those obligations
before `committed`. C1's flush before receipt remains required, including
dedup replay (`mixin.ts:798,865,918`; ADR-0025). A current-socket receipt advances
normally before its promise continuation runs (`transport.ts:855`), preserving
`appliedSeq >= commitSeq` at successful settlement. It must never clear a
pending set as a shortcut. A current-socket receipt with a valid cohort entry
still pending falsifies this proof and must block shipment.

The smaller cohort removes P4's counterexample: the unanswered subscription
never bootstrapped, so it cannot block this receipt. A newly added sub or an
unfinished snapshot likewise cannot hold an earlier mutation hostage. P5
observed `sub, sub, sub, mut`, all three terminals before `committed`, and
cursor 6 at settlement for commit 6. Parked replay, async authorization and
an unexpected server exception for a previously valid cohort entry were not
verified by that probe. The proof assumes such entries receive a terminal or
reset; this ADR does not fix the separate server exception/refusal gap.

Keep ADR-0021's parked outcomes, timeout and no-subscription fallback. Also
keep the accepted stale-socket exception: a known txId can settle from an
abandoned socket without advancing the current stream. The abandoned-receipt
test in `tests/ssr-cursor.test.ts` pins it. C1 above is explicitly scoped to a
current-socket receipt, not that older outcome.

### D5: A hydration seed during a hold starts another round

A valid positive seed arriving during a hold abandons the socket and round,
then reconnects from `min(appliedSeq, seed)`, including keeping 0 if already
held at 0. Do this even for a seed equal to or above the held cursor, and even
before any terminal has raised the candidate. Outside a hold retain the
existing seed rules, including cursor 0 being a no-op (`transport.ts:350`).
Use the existing voluntary reconnect policy and stale stream guard.

P1 explains why: at held cursor 100, A can apply 200 before hydration at 150
overwrites A. Ignoring the seed then releases 200 over stale rows. The check
observed no reconnect without this rule, versus a fresh socket with both
subscriptions at `since=100` with it. Comparing only against the candidate
also misses rows delivered before their terminal; pin that case below.

After a seed reconnect, retain that safe cursor until the bootstrapped cohort
completes or retires. A broadcast alone cannot release it. `awaitSeq` and
syncMeta observe only `appliedSeq`. This is a sequence guarantee, not a
baseline/readiness guarantee for excluded entries. A no-change round may
complete at the seed, so `awaitSeq(seed + 1)` is not a general ready signal.
The changed broadcast expectation in the validation contract pins this rule.

## Deferred decisions

A later ADR must own the adapter export baseline gate and active repair of
late cursor-0, filter-mismatched or malformed hydration chunks, including
queued eager writes, replacement cleanup and their tests. No new ADR number
is allocated here. P2 showed the export gap without any hold: both a
never-synced adapter and an unfinished catch-up exported another collection's
cursor 200. Deferring it leaves that existing behavior in place. This design
adds no new cursor-0 exports; holding lowers the exposed cursor and D5 keeps
the seed repair that would otherwise regress. That argument supports no new
export regression, but is not an independently tested end-to-end export proof.

Leave L1 and L2 in `tests/cursor-known-limitations.test.ts` as expected failures.
L2 loses a buffered write while the socket survives, so no round starts. L1
needs storage incarnation evidence. Neither pin passed in the check's full
suite with the hold. Delivery continuity and incarnation epochs need separate
wire decisions; global sequence gaps alone do not prove missing delivery
(ADR-0002 C5 and `mixin.ts:1139` latest-per-key catch-up).

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
4. Leave the three `ssr-adapter.test.ts` changes from the original proposal
   deferred: "export round-trips through import; the eager where is
   fingerprinted", "a DIFFERENT where downgrades the cursor to the
   snapshot-reconcile path", and "exportSyncMeta never claims a boundary
   whose receipt is unsettled". Their baseline gate is not part of this work.

The sixteen convergence flips are the original cuts:

| Offline changes | Cuts that become ordinary passing assertions |
|---|---|
| mixed | after eager messages, eager files, on-demand a*; mid eager files, on-demand a*, on-demand c*; after a live broadcast mid-round |
| files only | after eager messages; mid eager files; after a live broadcast mid-round |
| on-demand moves | after eager messages, eager files, on-demand a*; mid on-demand a*, on-demand c*; after a live broadcast mid-round |

Add behavioral pins before shipping, with these observable outcomes:

- P4 freeze: a healthy bootstrapped sub and an unanswered unbootstrapped sub,
  reconnect, live write and mutation. The cursor advances, `awaitSeq` resolves
  and C1 holds; the unanswered sub never joins the cohort. Also pin an empty
  cohort and unfinished snapshot/explicit-since recovery after release.
- P11 reset then drop: two bootstrapped subs, below-floor reset, partial second
  snapshot and another drop. The invalidated sub sends no `since` and finally
  converges. Include refusal without a terminal and unscoped reset whose
  callbacks remove multiple watches; no release occurs halfway through reset.
- Multiple interrupted rounds at different cuts discard every old candidate.
  Last-entry removal externally and within a terminal releases once; callback
  close/reseed discards instead. A completing terminal retains its candidate.
- Add/remove/reload during a round, including real db 1.0 `reload()`. Fresh ids
  reject old data and terminals, replacement entries bootstrap independently,
  and a sub added during dial is sent once. Preserve kept-row reconciliation.
- Seeds below, equal to and above the held cursor, a hold at 0, and partial
  delivery before a terminal. Each positive seed replaces the socket; stale
  boundaries cannot release. Export and `awaitSeq` see only the held cursor.
- New mut/call and parked replay, including async authorization and refusal:
  every cohort sub send precedes mutation sends; cohort terminals/resets
  precede receipts; C1 holds at settlement. Add a sub after mutation send,
  restart-callback sends and a failed sub send to exercise batch integrity.

Run affected suites at the supported floor and locked db version under
ADR-0022/0028, preserving reconnect, bootstrap, cursor-barrier, catchup,
recovery, in-flight txId, hibernation, SSR and on-demand lifecycle coverage.
The check's spike passed 30 tests and otherwise changed only the named SSR
broadcast expectation plus the sixteen expected-failure flips. It did not
run the hold on 0.8.6 or 1.0. This revision ran no runtime tests or spikes.

## Review record and consequences

The earlier read-only review by `thr_g97yu5bmbz` (codex, gpt-6-sol, high)
reported incomplete adapter exports, late no-resume hydration repair, and
queued eager inserts surviving replacement cleanup. The original D5 expanded
to address them. It also raised unsubscribe ordering, then refined that to a
constraint/test gap with no standalone permanent loss demonstrated. It upheld
maximum release and seed repair, and found no C1 counterexample under the
complete batch and synchronous handler assumptions. That historical verdict
was not rerun here; its export and repair findings remain deferred, not fixed.

The independent check overturned the all-entry cohort with P4, preserved all
30 tests with the smaller cohort, proved D5's seed rule necessary with P1,
showed the existing export gap with P2, and exposed the separate reset bug
with P11. Those findings caused this revision. The reset rule stays, the
adapter unload reorder is dropped, and the export/repair work moves out.
ADR-0028 now supplies fresh eager ids and defines reload as a new sync run.
The coordinator arranges adversarial review of this revised proposal.

Partial fresh rows can remain visible while the resume cursor stays older.
Replay must remain idempotent, as the current full-row catch-up at
`mixin.ts:1147` permits; future column patches must preserve that property.
Retention expiry can still require reset and snapshot (`mixin.ts:1113`).
This design adds no idle timers and makes no broader delivery guarantee.
