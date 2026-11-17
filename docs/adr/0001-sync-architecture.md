# 0001 — Sync architecture: single-ordered-stream over a Durable Object

**Status:** Accepted, **amended by [ADR-0002](./0002-adversarial-review-corrections.md)**
(ordering barrier, on-demand shaping, retention/liveness, before-image dropped).
Read 0002 alongside this document. **D13's "base class" is reframed as a mixin by
[ADR-0015](./0015-syncable-mixin.md)**: the sync core is now `Syncable(Base)` and
`SyncDurableObject` is its trivial application over `DurableObject` — same
hibernation/`acceptWebSocket`/`serializeAttachment` mechanics, now composable over
any DO base.

## Context

We want to sync a [TanStack DB](https://tanstack.com/db) collection to a
Cloudflare Durable Object: optimistic mutations from the browser, live deltas
back, reconnect catch-up, and the full client-side reactive layer (live
queries, incremental view maintenance, optimistic rollback) for free.

The design is grounded in a close reading of three bodies of prior art:

- **[ElectricSQL](https://github.com/electric-sql/electric)** — its HTTP shape
  log is best-in-class server-authoritative sync. The key idea we take is the
  *single ordered stream*: a client tracks one position (`offset`), and a
  write is confirmed by watching its transaction id pass that same position —
  there is no second acknowledgement channel.
- **[TanStack DB](https://github.com/TanStack/db)** — its collection
  `SyncConfig` (`begin`/`write`/`commit`/`markReady`/`truncate`), optimistic
  overlay with reconcile-by-key, predicate IR + evaluator, and `db-ivm`
  differential dataflow are the entire client-side reactive layer. We build a
  collection adapter against this contract and run none of it on the server.
- **A prior bespoke spike** — proved the good
  primitives (CDC via SQLite triggers into one change table; hibernating
  WebSockets; client-supplied keys; an egress coalescer) but accreted
  *accidental complexity*: it split write confirmation across two channels
  (a delta stream and a separate ack), then needed a second client cursor and
  a "flush-before-ack" server invariant to reconcile them. We delete that
  split by design.

### The inversion

A Durable Object is a **single authoritative writer**. Its change log is
therefore *totally ordered and contiguous* — unlike Postgres, where
transaction ids are non-contiguous and Electric must carry visibility
metadata (`xmin`/`xmax`/`xip_list`) to reason about what a client has seen.

Because our log is contiguous, **write confirmation is a single `seq >= X`
comparison.** The whole apparatus Electric needs for non-contiguous ids — and
the two-cursor reconciliation the spike grew — collapses to one cursor. This
makes the model *simpler* on a DO than it is on Postgres, and is the central
reason this library exists.

## Decision

### Foundational choices

1. **WebSocket transport, one per DO.** No HTTP/SSE fallback, no CDN — a DO is
   a persistent endpoint with nothing to cache in front of it. Hibernation via
   `acceptWebSocket` + `setWebSocketAutoResponse` ping/pong.
2. **Server-authoritative, single writer per DO.** No CRDTs. No MVCC horizon —
   one monotonic `seq` is the high-water mark.
3. **No multi-DO transactions.** A transaction touches collections in one DO;
   cross-DO is rejected client-side before any I/O.
4. **Bidirectional.** Unlike read-only Electric, this library owns the write
   path: mutations are applied authoritatively in the DO inside a single
   `transactionSync`, and confirmed back on the one stream.
5. **Plain-TypeScript core**, no Effect/Drizzle dependency. Optional adapters
   may layer those on; the core imposes neither.

### The single-ordered-stream confirmation model

- The client tracks exactly **one** cursor, `appliedSeq` — the `seq` of the
  last delta it has applied. There is no second ("acked") cursor.
- A mutation applied in the DO's `transactionSync` fires AFTER triggers that
  append to the change log in the *same* SAVEPOINT, so the committed `seq` is
  the mutation's receipt. The DO returns that `commitSeq` to the originating
  client.
- Confirmation is `awaitSeq(commitSeq)`: resolve once `appliedSeq >= commitSeq`.
  At that instant TanStack DB's core has already retired the optimistic row by
  key. This is Electric's `awaitTxId`, reduced to a comparison.
- **The hard case — a write whose row matches no active subscription** — is
  handled by a payload-free `committed` control frame stamped with `commitSeq`
  on the *same* stream. The client advances `appliedSeq` from any frame
  carrying a `seq`. One stream, one cursor; no parallel ack channel.

### Retention: state-convergence, compacted

The change log compacts to **latest-op-per-key** beyond a horizon (alarm-
driven), with deletes surviving as tombstones until pruned below the floor.
The retention floor *is* the reconnect snap-fallback boundary: a client
resuming from `since >= floor` gets a windowed delta; from before it, a `reset`
+ fresh snapshot.

This is a **state-convergence log, not an event log** — it permanently
forecloses replaying every intermediate state to a catching-up client. That is
an accepted, deliberate trade ([steering decision](#steering-decisions)); an
event-sourced consumer, should one ever be needed, is a separate opt-in
retention class, not a reason to leave the log uncompacted.

### Decision register

Condensed; see `git log` and subsequent ADRs for elaboration as each lands.

| # | Decision | Choice |
|---|---|---|
| D1 | Wire encoding | Binary (MessagePack) default + explicit 1-byte tag; JSON behind a debug flag. |
| D2 | Frame batching | Egress coalescer (last-write-wins per `(sub,key)`, tunable tick); bypass for `committed`/`snap`/`reset`. |
| D3 | HTTP/SSE fallback | None. WebSocket only. |
| D4 | Shapes | No server-named shapes; a subscription is `(collection, where, orderBy, limit, offset, cursor)`. The predicate is the identity. |
| D5 | Subset shaping | Lower `where` to SQL; push `orderBy`/`limit`/`offset` into SQLite; cursor via double-request. |
| D6 | Predicate ceiling | Lower the comparison + logical floor (`eq ne gt gte lt lte in like and or not`); reject un-lowerable predicates with a clear error. |
| D7 | Subset dedup | `DeduplicatedLoadSubset` on the client; `isWhereSubset` on the server; limited-superset needs WHERE-equality. |
| D8 | On-demand sync | Support eager + on-demand (`loadSubset`→`sub`, `unloadSubset`→`unsub` with server refcount). |
| D9 | Keys | Client-supplied stable keys (ULID/UUIDv7), enforced at `defineCollection`. No server-assigned keys. |
| D10 | Move-in/out | Capture the OLD row (before-image) in the UPDATE trigger; the DO computes per-sub enter→insert / leave→delete / stay→update exactly. |
| D11 | Registry API | `defineCollection` / `defineMutation` / `defineCommand`. `mut` execute is **sync** (inside `transactionSync`); `authorize` and `call` execute are async. |
| D12 | CDC | AFTER triggers → one per-DO change log table (`_sync_changes`), with a before-image column. |
| D13 | Hibernation | `acceptWebSocket` + auto-response ping/pong + on-demand timers; identity via `serializeAttachment`. |
| D14 | Multiplexing | One WS multiplexes all collections in a DO; one `seq` space per DO. |
| D15 | IVM | Client-side via `@tanstack/db` db-ivm; the DO never joins/aggregates/IVMs. |
| D16 | Confirmation granularity | Per-mutation `committed` frame; `awaitSeq` resolves on it. |
| D17 | Value codec | A tagged codec (bigint/Date/NaN/±Inf) on the wire and at rest; same codec for IR literals. |
| D18 | rowUpdateMode | `partial` for live deltas; snapshots are always full-row inserts. |
| D19 | Mutation payload | Field-level `getChanges()` diffs. |
| D20 | Catch-up threshold | Configurable changed-key threshold; forced snapshot when `since < retentionFloor`. |

### Steering decisions

- **Changelog semantics:** state-convergence (compacted). We will not serve
  every intermediate state to a catching-up client.
- **Predicate floor (v1):** `eq ne gt gte lt lte in like and or not`.
  Un-lowerable predicates are rejected for server-side shaping; a client may
  still evaluate them over already-loaded rows.

## Enforced invariants

From the first end-to-end milestone onward:

1. **Exactly one client cursor** (`appliedSeq`). Any reintroduction of a
   second ("acked") cursor or a "flush-before-ack" server rule is a
   regression and is rejected on sight.
2. **The DO stores and emits; it never joins, aggregates, or runs IVM.**
   Only single-collection, single-row predicate evaluation happens server-side.

## Build sequence

Each milestone is a coherent, testable commit-group; server and client
interleave so every milestone is end-to-end demonstrable.

- **M0** — Project foundation (this commit): scaffold, license, README, ADRs,
  tooling, package layout.
- **M1** — CDC substrate + single cursor (server): `defineCollection` with
  client-key enforcement; AFTER triggers → `_sync_changes` with before-image;
  `seq` cursor; tagged value codec at rest.
- **M2** — Hibernating WS transport + binary frame codec (both).
- **M3** — Full-collection sync + single-stream confirmation (both). *The core
  inversion.* Includes the no-subscription-match `committed` path.
- **M4** — Egress coalescer (server).
- **M5** — Filtered subscriptions + IR predicate engine (both); move-in/out via
  before-image.
- **M6** — Subset shaping (both). Static subset shaping —
  `where`/`orderBy`/`limit`/`offset` lowered into SQLite on the `sub` frame,
  with un-lowerable predicates rejected (D6).
- **M11** — Dynamic **on-demand** subsets (client, where-based). `doCollection
  Options({ syncMode: 'on-demand' })`: the collection loads only the subsets its
  live-query `where` clauses request (each distinct `where` is one refcounted
  server sub; ordering/limit applied client-side by IVM), with the empty-commit
  no-subset-match clearing wired (ADR-0002 C2). **Deferred post-v1:** windowed
  pagination (`orderBy`/`limit`/cursor double-request with server-side window
  maintenance under churn) and `isWhereSubset` containment dedup — recorded as a
  deliberate v1 boundary, not silently dropped.
- **M7** — Compaction-defined retention + reconnect (both).
- **M8** — Multi-collection multiplexing + client IVM integration (both).
- **M9** — Hardening, publish build, and OSS polish.

## Consequences

- The two-channel confirmation complexity of the prior spike never exists in
  this codebase.
- Storage is bounded by compaction; the reconnect snap-fallback path is
  exercised in normal operation rather than being dead code.
- We inherit the entire client reactive layer from TanStack DB and the
  proven single-stream discipline from Electric, while keeping full DO
  sovereignty over data.

## Clarification — 2026-09-29: D17 and SQLite bigint precision

D17 describes the tagged codec, not arbitrary SQLite column storage. It
preserves `bigint` on the wire and in library-encoded TEXT (such as persisted
predicates and dedup results); workerd `SqlStorage` cannot bind a JS `bigint`,
and reading INTEGER values above `Number.MAX_SAFE_INTEGER` rounds them before
the codec sees them ([workerd#4195](https://github.com/cloudflare/workerd/issues/4195),
[issue #10](https://github.com/grrowl/tanstack-durable-object-sync/issues/10)).
Use TEXT columns for exact large integers and convert at the application edge.
