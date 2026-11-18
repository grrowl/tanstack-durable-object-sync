# 0028 — `@tanstack/db` 1.0: the peer range admits the release candidates, and the adapter follows the 1.0 sync contract

**Status:** Accepted. Amends [ADR-0022](./0022-db-version-support-policy.md):
the peer range gains a second clause (§2), the contract table gains the rows
below (§1), and the `locked` CI leg moves to the release candidate (§3). Updates
the upstream behaviour that [ADR-0002](./0002-adversarial-review-corrections.md)
C2 verified.
**Date:** 2026-11-17

The first-load decision in D2 was subsequently amended by [ADR-0029](./0029-first-load-recovery.md).

## Context

The weekly `Upstream latest` job (ADR-0022 §3) was red from 2026-10-12. The job
stopped at its first failing step, so each run showed one problem and hid the
rest. There were five.

1. **0.12.0 drops a write's optimistic row when its mutation function settles**
   (TanStack/db#2030). Before, the row of a completed direct write stayed until
   the next sync commit. `tests/empty-commit-probe.test.ts` asserted that it
   stayed, so that one test failed. The adapter was not affected. It issues an
   empty sync commit after an on-demand mutation (ADR-0002 C2). That commit
   is still needed on 0.11 and older, and it has nothing to do on 0.12 and
   newer.
2. **1.0.0-rc.1 removed `Transaction.isPersisted`** in favour of
   `when("settled")`. Twenty-one test lines used it, so the typecheck step
   failed and the job never ran the tests again. No shipped code uses it.
3. **1.0.0-rc.1 renamed the sync control `markError` to `fail`.** The adapter
   called `markError?.(e)`, which does nothing when `markError` is absent. On
   1.0 a collection whose first connect failed stayed `loading` forever.
   ADR-0022 §1 names this as the degraded behaviour below 0.8.2. The suite
   passed anyway, because no test drove a failed first connect through a real
   collection.
4. **npm refuses to install this package beside the release candidate.** The
   `latest` tag of `@tanstack/db` is `1.0.0-rc.1.5`, and `@tanstack/react-db`
   pins that exact version. In semver a range such as `>=0.8.6` matches no
   prerelease. So `npm install @tanstack/react-db tanstack-durable-object-sync`
   fails with `ERESOLVE` for every new app.
5. **1.0's own declarations need DOM types for IndexedDB.** Core ships its
   IndexedDB collection from 0.13, and `indexed-db.d.ts` names types such as
   `IDBDatabase`. A Worker program that checks library types
   (`skipLibCheck: false`) reports them, as it already did for `Storage` in
   `local-storage.d.ts`.

1.0 also adds `collection.reload()`. A reload starts a new sync run and keeps
the rows the previous run wrote. Upstream's rule for adapters is that core
never deletes a kept row, so removing a row the server deleted in between is
the adapter's job.

- An eager collection already does this. Every eager snapshot is reconciled
  against the synced rows (ADR-0011 D4).
- An on-demand collection did not. Its watches and holds (ADR-0023) belong to
  one sync run. The new run holds none of the kept rows, so nothing removes
  them. Reproduced on 1.0.0-rc.1.5: drop the socket, delete a row on the
  server, call `reload()`, and the row stays for good. Without the reload the
  transport's reconnect catch-up delivers that delete.

Reviewing the restart path found one more problem, and it is older than 1.0.
An eager collection used one subscription id for its whole life. If it is
started over while a snapshot is arriving, the frames the server already sent
for the old subscription reach the new handler under the same id. Their
`snap-end` then passes for the end of a whole snapshot. The transport marks
the new subscription as bootstrapped, so after a drop it resumes from the
cursor, and the rows the old handler had already consumed are never sent
again. Reproduced on 0.8.6 and on 1.0.0-rc.1.5.

## Decision

### D1 — The peer range is `>=0.8.6 || >=1.0.0-rc.1`

The floor stays 0.8.6. The second clause admits the 1.0 release candidates and
nothing else, because 1.0.0 and every later release already match the first
clause. ADR-0022 §2 decided that the range has no upper cap. The semver rule
for prereleases had put one there by accident.

`scripts/db-floor.mjs` accepts this one extra shape and still takes the floor
from the first clause, so CI's floor leg is unchanged. It rejects a second
clause that names a version at or below the floor.

### D2 — A failed first load is reported through `fail` or `markError`, and nothing is reported after the first ready

`sync` calls `params.fail ?? params.markError`. Each branch runs on a CI leg:
`floor` (0.8.6) has only `markError`, and `locked` (the release candidate)
has only `fail`. ADR-0022 §1 rejected version branches that no CI leg runs.
This one is run at both ends.

The adapter calls it only while the sync run has not been ready yet. Once the
run is ready the collection is usable and the transport retries by itself, so
there is no load left to fail, and a report would strand the collection.

- From 1.0, `fail` ends the sync run. After the first ready it leaves an
  error on the collection's health that no later frame can clear.
- Before 1.0, `markError` puts the collection in `error` until a later
  `markReady()`. A hydrated eager collection is ready at once and then
  resumes from its cursor. A catch-up brings no snapshot, so nothing called
  `markReady()` again, and a first dial that failed left the collection in
  `error` for good while its rows converged. That was a defect in 0.7.0.

What a failed first load means afterwards differs, and the adapter does not
hide the difference.

- **Before 1.0** a later `markReady()` recovers the collection. An eager
  collection therefore recovers with no call from the app: its subscription
  keeps the transport retrying, and the snapshot that arrives marks it ready.
- **From 1.0** a failed sync run is over. `markReady()` after `fail()` does
  nothing, and a retried `preload()` rejects with the same error. The app
  calls `collection.reload()`, which starts a new run.

An on-demand collection that was not hydrated has no subscription when its
first connect fails, so the transport does not retry (ADR-0016), and it never
recovered by itself on any version. Its recovery is the call upstream gives an
app: `reload()` from 1.0, `cleanup()` then `preload()` before it. The comment
in `do-collection.ts` and the 0.7.0 changelog said a retried `preload()`
recovers it. That was true only for eager. An on-demand collection hydrated
with a resume point has its catch-up subscription, so the transport does retry
for it.

Only the connect or the subscribe can fail a load. What the adapter writes
once the connection is open is not reported this way. Applying a truncate
makes the collection ready in core, which runs the app's ready callbacks, and
a callback that throws surfaces from that commit. That is the app's error. It
goes uncaught and the collection stays ready, as it does when the callback
throws from `markReady()`.

### D3 — An on-demand sync run that replaces another starts with a truncate

The adapter remembers that a sync run has started. A later run of an
on-demand collection takes the path ADR-0011 D3 already defines for hydrated
rows with no resume point. It connects, then truncates. Core replays every
live demand, and the new run loads each one with a fresh subscription.

One later run is different. If hydration imported a resume point after the
previous run started, the new run consumes it and catches up from that cursor
instead, as a first run would.

The truncate does not first look for rows in the synced store. Core holds a
sync commit back while a user write is persisting, so rows of the replaced run
can still be queued when the new run starts, and they land later. A check for
present rows missed them, and the new run's snapshot could then collide with a
queued insert (`DuplicateKeySyncError`).

Before 1.0 a restart goes through `cleanup()`, which drops the rows. The
truncate is then of an empty collection.

Eager needs no change here. Every eager snapshot is reconciled.

### D4 — An eager subscription id is fresh for each sync run

`sync` mints the id when it starts, as an on-demand watch already does
(ADR-0023 D8). Frames of a replaced run find no handler and are dropped, and
the new subscription is bootstrapped only by its own snapshot.

### D5 — `locked` moves to the release candidate

The root devDependency is `^1.0.0-rc.1.5`, as ADR-0022 §2 prescribes when the
scheduled job is green on a newer release. PR CI now runs the suite on both
dialects: 0.8.6 and 1.0.0-rc.1.5. The versions between them are not on a CI
leg. They were run once for this change (see Verification).

### D6 — Tests await a write through one helper

`tests/settled.ts` calls `when("settled")` where it exists and
`isPersisted.promise` where it does not. Tests for behaviour that only one
dialect has say so with `runIf`, so the report shows them as skipped on the
other leg.

### D7 — The weekly job runs every check

`Test`, `Build` and `Pack smoke` run even when an earlier step failed. One run
then shows every problem.

### D8 — The pack smoke test allowlists the IndexedDB diagnostics

They are allowlisted by file, code and name, like the local-storage ones
(ADR-0022 §3). Any other diagnostic in upstream's declarations still fails the
test.

## Contracts added to ADR-0022 §1

| Version | Contract |
|---|---|
| 0.12.0 | a write's optimistic row is dropped when its mutation function settles; `begin({ immediate })` is gone (we never passed it) |
| 1.0.0-rc.1 | `fail(error)` replaces `markError`; `when("settled")` replaces `isPersisted`; `reload()` starts a new sync run over kept rows |

None of these raises the floor.

## Verification

Typecheck and the full suite, on the tree of this change:

| `@tanstack/db` | Result |
|---|---|
| 0.8.6 (floor), 0.9.2, 0.11.3, 0.12.3, 0.13.0, 0.15.2 | pass; the three `reload()` tests are skipped |
| 1.0.0-rc.1, 1.0.0-rc.1.5 | pass; the one test of recovery before 1.0 is skipped |

The pack smoke test passes on 0.8.6 and 1.0.0-rc.1.5, including the install
that failed with `ERESOLVE`.

Each new test was red before its fix:

| Test | Before the fix |
|---|---|
| a failed first connect fails `preload()` (D2) | on 1.0.0-rc.1.5, still pending after 1.5 s |
| a hydrated eager collection survives a failed first dial (D2) | on 0.8.6, `status` was `error` after the rows had converged; on 1.0.0-rc.1.5, `health` kept the error |
| a row deleted while disconnected is gone after an on-demand reload (D3) | the row was still there 3 s later |
| rows queued behind a persisting write are not kept by a reload (D3) | the deleted row came back when the write settled |
| a restart during the first snapshot (D4) | on 0.8.6 and 1.0.0-rc.1.5, the consumed row was still missing 3 s after the reconnect |
| a ready callback that throws on a restarted on-demand collection (D2) | the error was reported as the collection's failure and never reached the app uncaught |

A codex adversary (gpt-6.1-sol) reviewed this change twice. The first pass
found the queued rows of D3 and the report after ready of D2, and it confirmed
the subscription id hazard of D4 by reading. The second pass found the ready
callback case of D2. All four are fixed and pinned above.

## Alternatives considered

- **Raise the floor to 1.0.** Rejected. It would drop every app on 0.8.6 to
  0.15, and 1.0 is still a release candidate.
- **Wait for 1.0.0 to match `>=0.8.6` by itself.** Rejected. `latest` is the
  release candidate today, and react-db pins it, so new installs fail now.
- **On 1.0, do not fail the first load while the transport is still
  retrying.** Upstream's guidance to an adapter that is retrying is to call
  nothing, and to call `fail` when it has stopped. That would let an eager
  collection that starts offline become ready by itself on 1.0, as it does
  before 1.0, at the price of a `preload()` that waits for as long as the
  server is unreachable. It needs the transport to tell the adapter whether it
  will retry, which is a new member of the `Transport` interface. Not done
  here. D2 keeps the behaviour this project documented (ADR-0022 §1: a failed
  first connect fails `preload()` with its cause) and leaves this as a
  follow-up.
- **In D3, truncate only when the synced store holds rows.** This was the
  first version. Rejected after review: it misses rows that are still queued.
- **In D3, also truncate a first run that starts with rows and no cursor.**
  It is the same reasoning as ADR-0011 D3, and it would cover rows an app
  seeds through `DbClient`'s `initialData`, which no watch holds either.
  Rejected for this change. It changes what those apps see on every supported
  version, and nothing here needs that. Recorded as a follow-up.
- **In D3, delete only the rows the new run does not load, instead of
  truncating.** Rejected. The adapter cannot know when every current demand
  has loaded, and the truncate path already exists and is tested.

## Consequences

- A 1.0 app must call `reload()` to recover a collection whose first connect
  failed. Before 1.0 an eager collection recovered by itself.
- A hydrated eager collection whose first dial fails stays `ready` with its
  hydrated rows, on every version. In 0.7.0 it went to `error` and stayed
  there.
- After the first ready the adapter reports no failure to the collection, on
  any version. The transport retries, and `onClosed` reports a terminal close
  to the app (ADR-0016). See the health follow-up.
- An on-demand `reload()` refetches every live subset. Live queries keep their
  last complete result while that happens (1.0's truncate replay).
- Before 1.0, restarting an on-demand collection with `cleanup()` then
  `preload()` now issues one truncate of the empty collection.
- The examples still pin `@tanstack/db` 0.9.2 and `@tanstack/react-db` 0.4.1
  (ADR-0024). They are not moved here.

## Follow-ups (not in this change)

- **Health reporting.** 1.0 separates "can the data be shown" (`status`) from
  "is it still kept up to date" (`health`), and gives adapters
  `reportError()` and `reportHealthy()` for a connection that drops after the
  collection is ready. The adapter reports nothing there today.
- **The retrying first load** (third alternative above).
- **Rows seeded with no cursor into an on-demand collection** (`initialData`,
  or a wrapper that restores cached rows before `sync` runs). No watch holds
  them, so one the server has deleted stays until a subset that covers it is
  released. See the fifth alternative above.
- **A snapshot row whose key is still in a queued commit.** The adapter
  decides between insert and update from the synced store, which does not
  show a commit that core is holding behind a persisting write. D3 removes the
  case a restart added. Two watches of one run can still meet it.
- **Fixed sleeps in `tests/on-demand-contracts.test.ts`.** The commit receipt
  test waits 60 ms for a snapshot to be queued. On a slow run it can pass
  without a queued receipt. `tests/reload.test.ts` waits on the cursor
  instead, which that test could do too.
- **Upstream.** `@tanstack/db`'s declarations could keep DOM-only types out of
  a Worker program's way. Worth a report. It is not a defect in this package.
