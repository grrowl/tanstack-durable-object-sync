# 0009 — Changelog time-based retention; reset stale reconnects

**Status:** Accepted. Refines [ADR-0002](./0002-adversarial-review-corrections.md)
C5 (the changelog-retention floor); supersedes its per-connection-cursor
proposal for the as-built push architecture.

## Context

`_sync_changes` is the one ordered stream (ADR-0001 D12). Today it is bounded by
**only one** mechanism: compaction (`compactChanges`), which collapses the log to
the latest op per `(tbl, key)`. That bounds the log by **distinct-key
cardinality**, not by **age**. A collection that keeps inserting *new* keys (or
accumulating delete tombstones) grows the compacted log without bound — one
permanent row per key that ever existed. Hot-key churn is collapsed; key *growth*
is not. For an append-heavy collection that is effectively unbounded.

The reset machinery to handle a too-stale reconnect already exists and is
**dormant**: on reconnect a client re-subscribes carrying `since = appliedCursor`;
the server serves a windowed catch-up if the log still reaches back that far,
else sends `reset` → the client drops its view and takes a fresh snapshot. But
because compaction always retains every key's latest op, the floor always reaches
back far enough, so `reset` effectively never fires. ADR-0002 C5 flagged exactly
this — and proposed (option a) gating GC by the minimum *connected* delivered
cursor, or (option b) resetting laggards before advancing the floor past them —
but left it unimplemented, noting "tombstone pruning + liveness-aware GC … would
activate the fallback."

This ADR activates it. The goal is purely to **bound `_sync_changes` by age**:
we do not need years of history. A client reconnecting from beyond the retention
horizon should download a full snapshot rather than an incremental delta.

### Vocabulary (to keep the reasoning honest)

- **`seq`** — `_sync_changes.seq`, an `INTEGER PRIMARY KEY AUTOINCREMENT` on the
  changelog table. A **server-assigned monotonic ordinal**, stamped when the CDC
  trigger fires. Unrelated to the collection's key.
- **`key`** — the collection's **client-supplied PK** (ULID / UUIDv7 / any random
  TEXT). Never timestamp-derived; never the basis of retention ordering.
- **`ts`** — server wall-clock (`unixepoch()*1000`) stamped by the trigger. The
  source of truth for a change's *age*.
- **floor** — `minChangeSeq`, the lowest `seq` still in the log.
- **retention horizon** — `now − changelogRetentionMs`; changes older are pruned.

## Decision

### 1. Prune the changelog by age

Add `pruneChanges(sql, olderThanMs, nowMs)` →
`DELETE FROM _sync_changes WHERE ts < (nowMs − olderThanMs)`. It mirrors the
existing time-bound `sweepDedup`. It runs inside `maybeCompact` (the
opportunistic post-burst housekeeping path), **after** `compactChanges` and
alongside `sweepDedup`:

```
compactChanges(sql)
pruneChanges(sql, changelogRetentionMs, now)   // no-op when retention is null
sweepDedup(sql, dedupRetentionMs, now)
```

`pruneChanges` owns the `null` (disable) check itself — `olderThanMs === null`
returns early — so the call site has no branch and the disable path is unit-
testable in isolation (matching how `compactChanges`/`sweepDedup` are tested
directly, never through the private `maybeCompact`/`waitUntil`).

`ts` is **server-stamped and trusted** — workerd's clock is authoritative and
non-decreasing within a DO, so we delete by `ts` directly rather than deriving a
`seq` boundary. (A seq-prefix delete was considered to remove the clock
assumption; it buys nothing here — see Alternatives.)

**Two verbs on the changelog, distinct axes:** `compactChanges` bounds by
key-cardinality (collapse churn); `pruneChanges` bounds by age (drop old).
Complementary — a workload with many *distinct, recent* keys is bounded by
neither, which is correct (those rows are live).

### 2. Reset a reconnect that is older than the floor

The reconnect gate keys off `since` vs the live floor — **no persisted
watermark** (we add no state we can derive). The only change from today is to
**flip the empty-log branch to `reset`**:

```
if (since > 0) {
  floor = minChangeSeq(sql)
  if (floor === 0)          reset      // empty + since>0 ⟹ history pruned, sub-sec
                                       //   retention, or storage-reset ⟹ resync
  else if (since >= floor-1) catchUp
  else                       reset
}
```

This is **correct with no extra state**, because of two facts:

- **Prune removes a `seq`-prefix below the floor — under one assumption.** `ts`
  is non-decreasing in `seq`, and we delete `ts < cutoff`, so every pruned row
  has `seq < floor`. Thus a client with `since ≥ floor−1` is missing nothing (all
  pruned changes are `≤ since`); a client with `since < floor−1` *might* be
  missing a pruned change → reset. (Over-eager at worst — it can reset a client
  just below the floor when catch-up was possible. Reset is always safe.) **The
  load-bearing assumption is `ts` monotonic in `seq`** — we trust workerd's
  server clock (non-decreasing within a DO); the code does *not* enforce it. A
  regressing clock would let prune delete a non-prefix and catch-up miss a row;
  the seq-prefix alternative (see Alternatives) would remove this dependency. We
  accept it deliberately: the platform is authoritative.
- **The log effectively cannot empty with live history — except at tiny
  retention, where the flip saves us.** `maybeCompact` runs only from
  `drainAndBroadcast`, which early-returns unless fresh rows were just drained.
  At any realistic retention the just-drained rows (`ts ≈ now`) survive, so the
  log stays non-empty. **The exception (caught in adversarial review):** the
  trigger stamps `ts = unixepoch()*1000` (*second*-granular) while prune uses
  millisecond `Date.now()`, so with `changelogRetentionMs` at or below ~1 s
  (notably `0`), the just-drained row's `ts` can fall below the cutoff and be
  pruned — emptying the log with a live table. This is exactly why the
  **empty→reset flip is required, not merely defensive**: an empty log with
  `since > 0` (pruned-away history, sub-second retention, *or* a DO
  storage-reset) resets the client — a needless-but-safe full re-download —
  rather than a silent, now-incorrect "up to date". So correctness never depends
  on the "can't empty" argument.

### 3. One knob, no validation

`changelogRetentionMs: number | null`, default `172_800_000` (2 days), a
`protected readonly` field — sibling to `dedupRetentionMs`. `null` **disables**
pruning entirely (the log reverts to compaction-only, unbounded-by-age — the
pre-0009 behavior). `null` is the honest disable signal: `0` is a valid
(aggressive) setting and `Infinity` would mean binding `-Infinity` into SQLite.
This intentionally diverges from `dedupRetentionMs` (plain `number`): disabling
*dedup* retention is nonsensical (its table would grow for no benefit), while
keeping *full changelog history* is a legitimate choice for a small bounded DO.

No clamping/validation: correctness holds at any retention including 0 (worst
case = over-eager full downloads). "Fail loud" guards silent incorrectness, not a
user knowingly choosing aggressive retention. A negative value is on the caller.

**Cadence:** pruning reuses `compactionEvery` — no new cadence knob. Compaction,
prune, and dedup-sweep are three distinct *jobs* sharing one *trigger*; they are
all cheap post-burst `DELETE`s with no reason to stagger. A prune that runs with
nothing old enough is a no-op.

### Why no per-connection cursor (the C5 question)

C5 worried a `MIN(seq)` floor would strand a connected-but-slow client. In the
as-built architecture it cannot, because the changelog has exactly **two
readers**: `drainAndBroadcast` (the recent tail, pushed once to connected subs)
and the reconnect catch-up. Live deltas are **push-once and self-contained** —
the broadcaster's `PendingDelta` carries the row `cols` inline, never a log
reference — so a connected client never re-reads the log, and pruning is
invisible to it. A client can fall behind the floor only by **disconnecting**;
when it reconnects below the floor, the gate resets it. That **is** C5 option
(b) — reset laggards before serving them stale deltas — realized lazily at
reconnect rather than proactively per connection. So per-connection cursor
tracking is unnecessary here, and C5's dedup-independence (correction #2) is
untouched (`dedupRetentionMs` stays separate).

## Alternatives considered

- **Persisted history-floor watermark in `_sync_meta`.** Rejected: hand-computed
  that `minChangeSeq` + emptiness + the client's `since` are sufficient (§2). The
  watermark would store state we can derive — against the "no state we don't
  need" bar.
- **Seq-prefix prune** —
  `DELETE … WHERE seq < (SELECT MIN(seq) FROM _sync_changes WHERE ts >= cutoff)`.
  Removes the `ts`-monotonic-in-`seq` assumption *by construction*. Rejected: the
  clock is trusted (server-authoritative), the log can't empty regardless (prune
  fires only post-fresh-drain), and compaction punches holes so neither variant
  yields a *contiguous* run — the only honest gain was the clock assumption,
  which we waive. `ts < cutoff` also states the intent ("older than retention")
  more legibly.
- **Per-connection delivered cursor / gate GC by min active cursor** (C5 option
  a). Rejected for the push-once reason above.

## Consequences

- `_sync_changes` is bounded by **age** (default 2 days), not just key
  cardinality — the unbounded-growth class is closed for the default config.
- **Behavior change:** a reconnect older than the horizon now gets `reset` +
  full snapshot instead of a silent (and, post-prune, incorrect) "up to date".
  The `reset` path is exercised for the first time in anger; previously dormant.
- Set `changelogRetentionMs: null` to restore pre-0009 (compaction-only)
  behavior.

## Out of scope (known, orthogonal)

- **Mid-tick eviction dropping pending deltas.** If the DO is evicted (crash /
  redeploy, *not* idle — the armed flush timer blocks idle hibernation within its
  ≤`tickMs` window) with deltas still buffered, and the hibernatable WebSocket
  survives so no reconnect fires, a connected client can silently miss that
  batch (`appliedSeq` advances on the next delivery without a contiguity check).
  This exists today, independent of retention; pruning neither causes nor worsens
  it. Not addressed here.

## Amendment — 2026-09-29: the dedup sweep rides receipts, not compaction

§1 put `sweepDedup` inside `maybeCompact`, which runs only when a drain finds
CDC rows. But `_sync_seen_tx` rows come from answered receipts, not drains. A DO
that only rejects mutations, replays receipts, or runs commands that write
nothing adds a row per frame and never sweeps (bug bash 2026-09-29, F7).

`sweepDedup` moves out of `maybeCompact`. It now runs after every answered
`mut`/`call` receipt (new outcome or replay), after the reply is sent, behind
one gate held in memory: at most one sweep per `dedupRetentionMs`. The gate
starts at 0, so the first receipt after construction or a hibernation wake
always sweeps. No new knob, no idle timer, no alarm, and no write per receipt
beyond `recordTx`.

- **Why in memory.** A persisted counter would add an UPSERT to every receipt.
  An in-memory counter would reset on each wake, so a quiet DO that hibernates
  between receipts would never reach its threshold. A time gate that reopens on
  wake has neither problem.
- **Why time, not a count.** `DELETE … WHERE ts < ?` scans the whole table
  (there is no index on `ts`). Under a count cadence the scans per hour grow with
  traffic, and so does each scan. Under a time gate a DO that stays awake scans
  once per window, whatever the traffic. A DO that wakes per receipt scans once
  per wake, but it is quiet, so its table is small.
- **Why the window is `dedupRetentionMs`.** Retention is a lower bound: a retry
  inside the window is deduped (ADR-0002 C5, ADR-0021). Sweeping less often only
  keeps rows longer. An expired receipt now lives at most one more window, so the
  table holds at most about two windows of receipts.
- **Server-originated writes** (`runSyncedWrite`) no longer sweep dedup. They add
  no receipts, so they add no rows to sweep.
- A DO that receives nothing sweeps nothing. Its table does not grow either.

Tests: `tests/dedup-sweep.test.ts` drives each receipt path through real
sockets and polls the table. It also pins the gate: closed within the window,
reopened by a wake (`evictDurableObject`) or by elapsed time.
