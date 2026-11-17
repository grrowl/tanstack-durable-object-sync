# 0003 — Cursor load-more is one atomic fetch, not two

**Status:** Accepted. Extends [ADR-0001](./0001-sync-architecture.md)'s subset
shaping; a deliberate, reasoned departure from Electric's adapter. The atomic
single-frame decision stands; the wire field *naming* described here (`ties` /
`where`) was superseded by [ADR-0005](./0005-fetch-frame-mirrors-loadsubsetoptions.md)
after 0.1.0.

## Context

On-demand windowed pagination loads the initial window bounded by `orderBy` +
`limit`, then serves scroll-back via TanStack DB's cursor double-read: a
`whereCurrent` (all rows equal to the boundary order value — the ties) plus a
`whereFrom` (the next page, strictly after the cursor). TanStack hands both to
`loadSubset` as one `cursor` object; the sync layer combines each with the
query's base `where` and fetches the older rows.

Our first implementation mirrored Electric's `electric-db-collection` adapter
exactly: **two** requests, run concurrently —
`Promise.all([fetch(ties), fetch(next)])` — then a single merge of both results
into the collection (`electric.ts:519-525` in `_reference/`).

## Decision

Issue the cursor double-read as **one** `fetch` frame carrying both predicates
(`ties` unbounded, `where` bounded by `limit`). The server runs the two SELECTs
in one handler turn — synchronous SQLite, no `await` between them — so both
observe the database at a single `seq`, and answers with one `page`.

The two halves cannot collapse into a single `where`: the ties must be unbounded
while the next page is limited, an asymmetry no single `(where, limit)` pair
expresses. So the frame carries a second predicate (`ties`); see
[`wire/frames.ts`](../../src/wire/frames.ts).

## Why we diverge from Electric

Electric's two-request split is correct *for Electric* because its snapshots and
live changes ride one ordered shape log, and each snapshot row carries Postgres
MVCC visibility metadata (`xmin`/`xmax`/`xip_list`, `isVisibleInSnapshot`). The
client can therefore tell a deleted row from a live one and reconcile pages read
at different LSNs.

Our page fetch is **out of band** from the seq-ordered delta stream and has no
per-row visibility metadata — so we cannot lean on that reconciliation. Two
frames let a live delta interleave between the ties read and the deferred merge.

### The bug the divergence fixes (hand-computed, then reproduced)

Window shows `ts 100..81`; `row81` is the boundary, already loaded. Client A
scrolls back while client B deletes `row81`. DO frame order:
`F_ties`, `MUT(delete row81)`, `F_next`.

```
DO:  F_ties @500 → [row81]               → send page_ties
     MUT delete row81; seq→501           → send d(delete row81), uptodate
     F_next @501 → [row80,…]             → send page_next

A:   page_ties      → fetch_ties resolves; Promise.all still waiting (no merge)
     d-delete row81 → buffered delete
     uptodate       → commit → row81 gone from the collection
     page_next      → Promise.all resolves → MERGE: row81 absent → re-inserted ⚠
```

`row81` is deleted server-side, so no future delta ever corrects it — a durable
phantom. `insert-if-absent` does not help: at merge time the row is genuinely
absent. The root cause is **read-early / apply-late**: `ties` is read at seq 500
but applied after a delete delta from seq 501.

One atomic fetch removes the inversion. The page is a single macrotask whose
merge (the microtask after it) completes before any later delta macrotask is
processed, so the delete always wins. Reproduced as a failing test against the
two-frame code, now green under the single frame
(`tests/on-demand.test.ts` — "does NOT resurrect a row deleted concurrently").

This is the kind of departure the rejected-options discipline exists for: we
reach past a battle-tested reference only with a concrete reason the reference's
safety net (MVCC visibility) is one we don't have, and a substrate (single
synchronous SQLite read) that gives us an equivalent guarantee more cheaply.

## Consequences

- **Stronger than the reference here:** one consistent read instead of two
  possibly-skewed ones, and one round-trip instead of two.
- Rows are still written **insert-if-absent** — a boundary tie already in the
  window must not be re-inserted (a differing value throws
  `DuplicateKeySyncError` and aborts the sync transaction; see
  [ADR-0002](./0002-adversarial-review-corrections.md) verification notes), and
  a key the live sub already holds keeps its fresher value. *(2026-11-17)* The
  rule also holds inside one page: a key written earlier in the same page is
  skipped. A page from the server never repeats a key, but a server render reads
  the two halves at two positions, where one row can be in both
  ([ADR-0011](./0011-ssr-dehydrate-hydrate.md) amendment 2026-11-17).
- The live `where` subscription remains the source of truth for anything
  currently in the collection; `fetch` only backfills older rows.
- **Still deferred:** an unbounded `whereCurrent` tie-set over-fetches if the
  lead order column is low-cardinality. Mitigation is a high-cardinality lead
  order key (include the pk) and, if needed, a fail-loud server ceiling — not a
  silent cap, which would reintroduce the skip-a-tie bug. Recorded, not done.
