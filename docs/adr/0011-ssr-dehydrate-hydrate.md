# 0011 — SSR: dehydrate on the worker, hydrate to the cursor

**Status:** Accepted (experimental — designed against TanStack DB draft PR
[#1564](https://github.com/TanStack/db/pull/1564), lifted onto the PR **as
merged** in `@tanstack/db` 0.8.0; see *Amendments* below). Generalizes
ADR-0002 C1's flush-before-`committed` barrier to *all* cursor-advancing
emissions (C1′ below). **D5's peer-range floor is superseded by
[ADR-0022](./0022-db-version-support-policy.md)** (0.8.5 → 0.8.6).

## Context

TanStack DB's draft SSR PR adds `DbClient` with row-level
`dehydrate()`/`hydrate()` and three opaque sync-config hooks —
`exportSyncMeta(): unknown`, `importSyncMeta(meta)`,
`mergeSyncMeta(current, incoming)`. Facts about the upstream design that
constrain ours (verified against the PR, not its docs — it has none for
adapter authors):

- Hydration applies rows as **synced upserts** (`committed: true,
  immediate: true`) **before** `importSyncMeta` runs. An adapter cannot veto
  row application; correctness must come from post-connect catch-up.
- Dehydrated state has **no tombstones**. It is a snapshot *at* the exported
  cursor, never a delta. All delete-correctness is the adapter's problem.
- Hydration does **not** mark the collection ready; readiness stays under the
  adapter's `markReady()`.
- The hooks live on the (potentially module-scoped, request-shared) sync
  config. Our options creator takes the transport as an argument, so on the
  server our options are **per-request by construction** — we sidestep the
  upstream cross-request-leak hazard, and document per-request creation as a
  requirement.
- TanStack sync-write semantics: a sync `insert` for an existing key throws
  `DuplicateKeySyncError` unless values deep-equal; a sync `update` for a
  missing key upserts (our move-in path already relies on this).

Our model is one ordered stream per DO and a **single client cursor**
(`appliedSeq`) advanced only at commit boundaries (ADR-0001/0002). The server
already serves `since` on *any* sub — windowed catch-up within the retention
floor, honest `reset` below it (ADR-0009). SSR support is therefore mostly:
get a snapshot + cursor out of the DO without a WebSocket, round-trip the
cursor through the dehydrated state, and make the first sub carry `since`.

## Decision

### D1 — Socketless snapshot read: `readSyncSnapshot` RPC

`SyncDurableObject` gains a public RPC method:

```ts
readSyncSnapshot(
  req: { collection: string; where?: unknown; orderBy?: unknown; limit?: number },
  request: Request, // REQUIRED — runs through parseAttachment, the one auth gate
): Promise<{ rows: Array<Record<string, SqlStorageValue>>; cursor: string }>
```

Same compile path as the `fetch` frame (`compileSubsetQuery`); the gate awaits
*before* the reads, so rows and cursor are still taken at one position
(synchronous SQLite between them). Throws on unknown collection or unsupported
predicate — fail loud; RPC propagates.

Trust model: the binding limits callers to first-party workers, and the
REQUIRED `request` argument runs through **`parseAttachment` — the same gate
as the WS upgrade**. The worker passes the claims-bearing Request it already
forges (or forwards) for the socket path; a rejecting `parseAttachment`
rejects the read. Two paths, one gate: an author's tenant check cannot be
silently bypassed by the snapshot read (grill-session finding — an earlier
draft had no gate here, inverting the WS path's safe-by-default shape). The
minted claims are also the seam where uniform read-scoping would land, on
subs and snapshots alike — note that today *neither* path filters rows by
identity; `parseAttachment` is connection/read-level gating, and the
client-supplied `where` is shaping, not security.

**The exported cursor is a durable high-water mark** — `max(MAX(_sync_changes
.seq), drain_cursor)` — *not* bare `currentSeq()`, because retention can prune
the changelog empty while the table has rows, and a bogus cursor `0` against
live rows would let a delete that lands between render and hydration strand a
stale row forever (adversarial-review finding). Cursor `"0"` therefore honestly
means "no resume point": the client omits `since` and reconciles (D4).

### D2 — `SsrSnapshotTransport`, and `Transport` as an interface

What `doCollectionOptions` consumes becomes a structural `Transport` interface
(satisfied by `WebSocketTransport` unchanged). `SsrSnapshotTransport` implements
it for server rendering: constructor takes `read: (req) => Promise<{rows,
cursor}>` (the author passes `(req) => stub.readSyncSnapshot(req, request)`,
closing over the request's claims; no Cloudflare
types in the client build). `subscribe` performs one read and synthesizes
`onSnap*`/`onSnapEnd`; `connect()` resolves immediately (so on-demand
`loadSubset` during a server `preload()` works unchanged); its cursor is the
**min** across reads; `sendMut`/`sendCall`/`fetch` throw `SsrReadOnlyError`.
SSR is read-only.

Min is not merely the *safe* joint resume point (replay is idempotent;
skipping is not) — it is *self-consistent-making*: a render's reads land at
slightly different positions (milliseconds of DO time apart), and the first
catch-up from min replays exactly that skew window, converging every
dehydrated row to one position. Because the changelog `seq` is one stream
across all collections on the DO, the min is also a coherent position for
every collection sharing the transport — no per-collection reset risk.
Per-table cursor tracking (`cursorFor(table)`) was considered and rejected:
permanent interface surface to avoid a transient milliseconds-wide replay.

### D3 — syncMeta carries the cursor; the first sub carries `since`

`doCollectionOptions` implements the hooks:

- `exportSyncMeta → { v: 1, cursor: transport.appliedCursor, where? }` —
  `where` is a fingerprint (the codec envelope) of the eager filter the rows
  were dehydrated under. A cursor is only a sound resume point *for that
  filter*: catch-up emits changed keys only, so an **unchanged** out-of-filter
  hydrated row would never be reconciled away (second-review finding).
- `importSyncMeta` — validate (`v` unknown / malformed cursor → throw); a
  fingerprint mismatch (deploy skew) refuses the cursor and downgrades to the
  always-sound snapshot-reconcile path (`hydratedCursor = "0"`, transport
  unseeded); otherwise stash `hydratedCursor` and `transport.seedCursor(c)`.
- `mergeSyncMeta → min(cursor)` — min is self-healing: a late/stale chunk's
  rows are applied upstream before we're consulted, and a min cursor makes the
  next catch-up replay exactly the clobbered window.

`seedCursor(c)` may **regress** `appliedSeq` (claiming a *shorter* applied
prefix is always safe). A regress while LIVE cannot replay on the same socket:
boundary frames the server already sent (full duplex) would dispatch after the
regress and re-advance the cursor past the repair window (second-review
blocker). It therefore **forces a reconnect** — the old socket's queued
boundaries stop counting (`advance` suppressed; their data still applies,
idempotently) and the fresh socket resubscribes from the seed. One mechanism
for early and late hydration; no second cursor, no ack channel.

With a `hydratedCursor` (consumed once at sync start; cleared in the sync
cleanup fn — after a collection GC the rows are wiped, so a retained cursor
would resume over an empty store and silently lose data):

- **Eager**: the first sub carries `since`; `markReady()` immediately (rows are
  present; catch-up arrives as `d`+`uptodate`, which never fires `snap-end`).
  Be explicit about what this changes: **hydration redefines `ready` as
  "renderable", not "synced"** — `isReady` is true on the server pass (no
  socket will ever exist) and stays true offline with stale rows. That is the
  stale-while-revalidate contract, deliberately. An app that wants a
  "catching up → live" signal (a SyncIndicator) doesn't need new API: the
  transport already exposes it — `awaitSeq(String(BigInt(dehydratedCursor) +
  1n))` resolves at the first post-hydration boundary, i.e. caught up. Not
  README material (sharp-edged); recorded here for when someone asks.
  Below the retention floor the server answers `reset` → truncate + fresh
  snapshot — which DOES flash empty between the truncate's commit and the
  snapshot's (unlike the cursor-`"0"` reconcile path). Accepted, not fixed:
  the dehydrated cursor is seconds old, so falling below the floor requires
  `changelogRetentionMs` (default 2 days) shorter than the HTML's flight time
  — pathological config, not a reachable state. Unifying it would need the
  client to skip the truncate and let snapshot set-semantics reconcile, but a
  `reset` is also the only terminal for a REJECTED sub (no snapshot follows),
  where skipping the truncate keeps stale rows forever — the one outcome
  ranked worst throughout this design. The reset-cause ambiguity is harmless
  today (rejection is dev-loud; below-floor is pathological) and becomes
  worth a wire-level distinction — likely alongside the incarnation epoch —
  when client-side persistence (an LRU'd local db) makes days-old cursors
  routine. Future scope, deliberately not now.
- **On-demand**: **one transient unfiltered catch-up sub**
  (`since = hydratedCursor`, no `where`) that unsubscribes at *its own*
  sub-scoped terminal — never at a broadcast boundary, which can precede its
  frames. The dehydrated rows are the union of the server-loaded subsets;
  per-subset `since` is unsound for any subset the dehydrated state didn't
  cover, and subset-tracking still leaves overlapping-`where` stale-delete
  holes. One unfiltered catch-up covers every changed key (always-emit ⇒
  synthetic deletes included) in the seconds-wide render→hydrate window.
  **Semantic cost, accepted and documented**: changes to rows outside any
  hydrated subset land in the collection during that window (bounded by
  change volume). The leaked rows' staleness is **unobservable**: a live
  query whose predicate matches one has a server sub with that same
  predicate, whose snapshot/deltas converge it at observation time
  (update-if-exists) — stale only while nothing looks, fresh by the time
  anything does. Eager rendering of stale/leaked data is acceptable against
  the snappy client-first UI it buys; the residual cost is memory, bounded
  by seconds of change volume. `markReady()` **gates on the catch-up sub frame being
  sent** (not completed): `loadSubset` subs fire only after ready, so on the
  single ordered socket the catch-up always precedes subset snapshots
  (second-review finding — `connect().then(markReady)` alone races).
  Wire note: the transient's teardown depends on the server scoping the
  catch-up terminal (`uptodate.sub`); against a pre-0011 server the terminal
  arrives unscoped and the transient sub never tears down (an unfiltered
  live sub leaks until the socket drops). Matter-of-fact, not mitigated:
  pre-1.0, client/server version skew is not a supported configuration —
  the worker ships the bundle and the DO from one deploy.
  When the hydrated rows are **unresumable** — cursor `"0"`, or the server
  `reset`s the catch-up below the floor — on-demand **truncates** them
  (the reset path also unsubscribes immediately so the trailing unfiltered
  resnapshot is dropped unhandled). A full-table snapshot was rejected here —
  and the principled line between this and the tolerated catch-up leak above
  (both are "stale while unobserved, fresh when observed") is **on-demand's
  memory contract**: memory proportional to what you observe. A seconds-wide
  window of changed keys respects that contract asymptotically; a full-table
  snapshot breaks it categorically — unbounded in table size, on the mode
  whose purpose is not loading the table. The truncate refuses to convert
  on-demand into accidental-eager. Eager keeps the no-flash reconcile;
  on-demand keeps honesty.

### D4 — Snapshot reconciliation (and two pre-existing bugs fixed)

Adversarial review (gpt-5.5) rejected the obvious "insert-if-absent" guard for
snapshots — `snap-end` advances the cursor, so *skipping* a fresher snapshot
value and then dropping the socket loses that write forever. Instead:

- **C1′ (server)**: `broadcaster.flushOne(ws)` before any synchronous
  cursor-advancing emission (`handleSub` snapshot, `emitCatchUp`). C1 said
  "deltas flush before `committed`"; C1′ says **a socket's pending coalesced
  deltas always precede any cursor boundary on that socket**. This fixes a
  pre-existing bug independent of SSR: a multi-collection reconnect's catch-up
  `uptodate` could advance the cursor past another collection's still-buffered
  delta (drop before the tick ⇒ lost write).
- **`onSnap` writes update-if-exists** (snapshot value wins). With C1′ a
  snapshot value is never staler than the held row, so this converges; it also
  absorbs `DuplicateKeySyncError` when a subset snapshot lands over hydrated
  rows that changed since dehydration. (`loadMore`'s page path keeps its
  insert-if-absent: `page` frames never advance the cursor, and a page *can*
  be staler than a held row.)
- **Key-reconcile is ALWAYS armed for eager subs** (grill-session
  generalization; never for on-demand subset subs, whose snapshot must not
  delete other subsets' rows): an eager snapshot is authoritative set
  semantics over synced rows, period — at `snap-end`, held synced keys
  absent from the snapshot are deleted. For the normal empty-at-first-
  snapshot flow it is a no-op (and boundary-free: `begin` opens only when a
  delete is due); for ANY path where synced rows precede a snapshot —
  hydration with no resume point, a refused foreign-filter cursor, meta that
  failed validation — it is what prevents a server-deleted held row from
  being stale forever. An EMPTY snapshot still reconciles (zero keys is an
  authoritative set — second-review blocker). Honest set semantics without a
  truncate's flash-to-empty (SSR exists for first paint). Presence checks
  steer by `syncedData`, never the combined view — optimistic overlays are
  invisible to sync writes by design.
- **The syncMeta hooks fail loud but SAFE** (grill-session finding): upstream
  applies a chunk's rows BEFORE `mergeSyncMeta`/`importSyncMeta` run — a
  validation throw cannot veto them, so throwing alone would leave applied
  rows with no reconcile intent (and, on-demand, no truncate): stale
  forever. Both hooks set `hydratedCursor = "0"` (the always-sound
  snapshot-reconcile / truncate route) BEFORE throwing — the version skew
  still surfaces to the app, and the state left behind converges. This is
  also the gradual-upgrade path: a future `v: 2` payload degrades old
  clients safely and loudly; no per-version fallback logic.
- **`onDelta` maps `insert` → `update` when the key exists** — catch-up emits
  the latest CDC op per key, so a delete-then-reinsert since the cursor arrives
  as `insert` against a held key and would throw. Pre-existing on reconnect
  catch-up too; fixed for both.

### D5 — Packaging

Peer dependency `@tanstack/db >= 0.8.5`: the syncMeta hooks shipped in 0.8.0
(unchanged from the draft), and 0.8.5 carries the `commit()`-receipt contract
and descriptor-reuse-by-id this adapter adopts (see Amendments). No
self-branding via `Symbol.for` — the returned config carries upstream's own
`withCollectionConfigFactory` marker, so `collectionOptions(id, () =>
doCollectionOptions(...))` descriptors materialize fresh adapter state per
`DbClient`. (The draft era vendored PR-branch tarballs; those are gone —
everything builds against released packages.) Everything lands as
**experimental** in the changelog.

## Known limitations

- **No incarnation epoch.** A cursor from a pre-storage-reset DO whose new
  changelog already reaches past it would catch up silently-wrong. The exposure
  window for SSR is seconds and requires a storage reset inside it; fixing it
  properly is a protocol rev (an epoch in `_sync_meta` + a hello/epoch frame),
  deliberately deferred. Pre-existing for in-page reconnects too.
- ~~**Upstream is a draft.**~~ Resolved: PR #1564 merged 2026-08-17 and
  shipped in `@tanstack/db` 0.8.0 with the three hook signatures **byte-for-
  byte unchanged**. The semantics *around* them did change — see Amendments.

## Consequences

- SSR first paint with no WebSocket from the render path, no idle timers, no
  hibernation impact (the RPC is a plain request).
- The single-cursor inversion survives intact: `since` at first sub is a
  bootstrap parameter, `seedCursor` only ever claims a shorter prefix, and
  confirmation still rides the one stream.
- C1′ and the `onDelta` normalization harden reconnect for all clients, SSR or
  not.

## Amendments — 2026-08 lift onto merged upstream (`@tanstack/db` 0.8.5)

The design above was written against the draft PR. The PR merged (0.8.0,
2026-08-17) with the hook names/signatures intact but reshaped semantics
around them, and this repo's `main` moved 0.4.0 → 0.6.0 (ADR-0015..0019)
underneath the branch. The lift changed the following — each a deliberate
decision, not drift:

- **`exportSyncMeta` can return `undefined`, and the `Transport` seam gains
  `hasPosition`.** Merged upstream consults `exportSyncMeta()` for the
  *current* meta on **every** hydrated chunk and routes the incoming meta
  through `mergeSyncMeta` when current exists (draft: merge only across
  chunks). A fresh browser transport exporting `{cursor:"0"}` would win the
  MIN-merge against every real dehydrated cursor — silently downgrading all
  hydration to the snapshot-reconcile path, making D3's cursor resume dead
  code. The claim basis is now: the transport's position if it has one
  (`hasPosition`), else the unconsumed `hydratedCursor`, else **no meta at
  all**. `"0"` stays a *real* claim exactly where it is one — an SSR read
  against a DO with no history (`SsrSnapshotTransport.hasPosition` is true
  after any read, even at 0; a live `WebSocketTransport` can never claim 0).
- **`readSyncSnapshot` lives on the `Syncable` mixin as a public method** —
  DO RPC dispatches only on public instance members, so this is one
  deliberate addition to ADR-0015's four-method collision surface. The gate
  is the mixin's configured `parseAttachment` hook (same contract). D1's
  hand-rolled ordering note is obsolete: `compileSubsetQuery` now defaults
  `ORDER BY rowid` for every subset read (ADR-0015 era). RPC rows also
  normalize BLOB `ArrayBuffer → Uint8Array` — structured clone would leak the
  bare buffer where the wire codec normalizes it (ADR-0017 parity).
- **D3's forced regress-reconnect is re-derived against ADR-0016.** It is
  *voluntary* — not a network failure — so it bypasses the backoff policy
  entirely: no attempt consumed, no delay, and a custom `reconnectDelay`
  policy cannot declare it terminal. A failed open falls back into the normal
  policy-driven retry. The draft-era `suppressAdvance` flag is **replaced by a
  socket-identity guard on message dispatch** (only the current socket speaks
  for the STREAM): the flag protected the cursor but still let an abandoned
  socket's queued frames dispatch data, and its reset-at-install left a race
  window. Stream frames (`snap`/`snap-end`/`d`/`uptodate`/`reset`) from a
  stale socket are dropped — the resubscribe catch-up re-covers them
  idempotently. ID-scoped receipts (`committed`/`rejected`/`page`) are NOT
  re-covered by any replay, so a stale socket may still settle those waiters
  — it just never advances the cursor (codex adversary: a committed mutation
  must not be reported as timed out because a late hydration chunk forced a
  reconnect first). Main's own ADR-0016 machinery already carried the
  scheduling-time `reconnecting` flag and the stale-close guard this branch
  originally invented, so those SSR commits dropped out.
- **New upstream contracts adopted** (released after the draft): `commit()`
  receipts (`SyncAppliedReceipt`, 0.8.5) — snapshot terminals and cursor
  load-more settle `loadSubset` only once rows are visible; `markError`
  (0.8.2) — a failed ready-gate/first-subscribe fails `preload()` loud with
  the cause, and a retried `preload()` recovers (error → ready); subset-load
  failures reject that subset's promise (0.8.4 `loadSubset:error`), not the
  collection; `withCollectionConfigFactory` (0.8.0) as in D5. Not adopted
  (documented follow-up): `LoadSubsetOptions.signal` — cooperative
  cancellation of a shared refcounted sub needs its own design; loads
  complete correctly without it.
- **A second codex adversary round on the lift itself** hardened five more
  edges, each pinned by a test: `parseSyncMeta` rejects a NEGATIVE cursor (it
  would ride `since` to the server, draw a full snapshot the on-demand
  catch-up handler discards, and leak the transient sub forever);
  `mergeSyncMeta` with mismatched fingerprints yields the honest `"0"` under
  our fingerprint (MIN alone could let the matching side's cursor smuggle
  foreign-filter rows past import's check); `exportSyncMeta` is
  settlement-gated (while any commit receipt is unsettled it claims the last
  fully-settled position — the boundary cursor is not yet proof of applied
  rows, and a dehydrate in that window must under-claim); a REJECTED receipt
  fails its subset load / readiness rather than resolving it; and the
  on-demand ready-gate failure path heals — the catch-up terminal and every
  completed subset also `markReady()` (idempotent), so error → ready recovery
  actually happens. Plus one pre-existing transport fix: `unsubscribe` during
  an in-flight `subscribe`'s connect no longer sends a ghost sub the server
  would persist (ADR-0019) with no local consumer.
- **Upstream now natively reconciles hydration-seeded keys** (a later adapter
  `insert` of a seeded key is applied as an update — `hydrationSeedKeys`,
  0.8.0). D4's held-key upsert conversion stays: it is belt-and-braces on the
  hydration path and load-bearing for the mid-session reconnect catch-up,
  which upstream's seed tracking does not cover.
- **Not lifted:** the upstream live-query layer (`preloadLiveQuery`,
  `HydrationBoundary`, `@tanstack/react-router-with-db`) needs nothing from
  the adapter — dehydrated live-query results ride upstream's own state, and
  source-collection hydration is exactly the D3 path. The example app is the
  right home for that surface.

## Amendment 2026-11-17: a render refuses writes, never reads

**What changed underneath D2.** D2 says `fetch` throws `SsrReadOnlyError`. When
it was written, every fetch was a cursor fetch for load more (ADR-0003). Since
then an on-demand collection fetches in more cases. A request that an open or
pending watch covers loads its rows with a fetch that has no cursor (ADR-0023
D2). And `@tanstack/db` 0.9 and later send more plain subset requests on a first
load: a boundary tie request, a full-source request, and, for an underfilled
window with an index and an order it can express as a cursor, a request with a
cursor. The adapter answers the first two with a covered fetch and the third
with a cursor fetch. The snapshot transport refused every fetch, so a render
failed for ordinary pages: a list and a detail row of one table (0.8.6, 0.9.2
and 1.0.0-rc.1.5), and on 0.9 and later a single query with `orderBy` and
`limit`. The browser answered the same requests and was `ready`.

**Decision.** A render refuses writes, never reads. `sendMut` still throws
`SsrReadOnlyError`. `SsrSnapshotTransport.fetch` answers every fetch through the
same `read` it uses for a subscribe, with no change to the `read` request and no
server change:

- A fetch without a cursor is one read of its collection, `where`, `orderBy`
  and `limit`. On the server that is the read the WebSocket path makes for it:
  `readSyncSnapshot` and the `fetch` handler both compile those fields with
  `compileSubsetQuery`.
- A fetch with a cursor is the server's two reads (ADR-0003): first
  `and(where, whereCurrent)` without a limit (the boundary ties), then
  `and(where, whereFrom)` with the limit (the next page), both in the request's
  order. The rows are returned ties first, as the server returns them. A
  cursor that lacks either half is refused, as the server refuses it (the ties
  read would be an unbounded scan); the render rejects where the socket
  answers an empty page, the same difference as for every refusal (below).

Every read's position joins the render's MIN cursor (D2).

**Two positions for one cursor fetch.** ADR-0003 reads both halves at one
position because a page lands in a live delta stream: a delete applied between
the two reads could be undone by the later merge. A render has no delta stream.
Its rows go only into the dehydrated state, both reads join the MIN cursor, and
the hydration catch-up replays every change after the earlier one. Three
effects remain, all inside the render, all repaired on hydration:

- A row that moves past the boundary between the two reads is returned by both.
  The adapter writes a key once per page (ADR-0003, dated note): the first copy,
  from the earlier read, wins, and the catch-up brings the newer value. Without
  that, `@tanstack/db` 1.0 fails the load with a duplicate sync insert; 0.9.2
  keeps the later copy without an error.
- A row that moves onto the boundary value, or joins at it, between the two
  reads is returned by neither. The render does not show it, and the catch-up
  brings it on hydration.
- A row an earlier read returned and that is deleted, or leaves the predicate,
  before a later read is still rendered, and the catch-up removes or updates
  it. This is the skew between reads that D2 already accepts across subsets.

On the WebSocket path the skip changes nothing. The server reads both halves in
one synchronous turn. Core's cursors put `eq` in `whereCurrent` and `gt` or
`lt`, alone or `OR isNull OR isUndefined`, in `whereFrom`; these compile to
`col = ?` against `col > ?`, `col < ?` or `col IS NULL` on the same column and
value, which no row matches together, so such a page never repeats a key.
Core's cursor for a Date boundary is a 1 ms range (`gte` and `lt`) that would
overlap `gt`, but it cannot occur here: rows read from SQLite never hold a
Date, so no boundary value is one. Only an app
whose `getKey` does not return the primary key, which is outside the contract
(ADR-0001 D9, ADR-0007), could get one; such a page used to fail on 1.0 and now
keeps the first row.

**Rows a render may read.** A render now reads what the browser reads, so it
also reads what core asks for. On 0.9 and later, an order core cannot express
as a cursor makes it ask for the full matching subset after the first page:
any string order under the default `locale` sort (the primary key is always
TEXT, so `orderBy(id)` is one), and a boundary value that is NULL. In a render
those rows are read, held and dehydrated into the page's HTML, and the browser
reads them again over the socket. Measured with the real DO for
`where(body = "keep").orderBy(id).limit(10)` over 100 000 matching rows (rows
read per request, then rows dehydrated; 0.9.2 and 1.0.0-rc.1.5 gave the same
numbers):

| order | 0.9.2 and 1.0.0-rc.1.5 | 0.8.6 |
| --- | --- | --- |
| default sort, with or without an index | 10, then 100 000; 100 000 dehydrated | 10; 10 dehydrated |
| `stringSort: "lexical"`, without an index | 10, then a 1 row tie; 10 dehydrated | 10; 10 dehydrated |
| `stringSort: "lexical"`, with an index | 10, then a 1 row tie; 10 dehydrated | 10; 10 dehydrated |

Growing that window to 20 during the render read 20 rows and a 1 row tie
without an index, and a 1 row tie, a 10 row cursor page and a 1 row tie with
one (0.9.2 and 1.0.0-rc.1.5). On 0.8.6 it read a 1 row tie and a 10 row cursor
page with an index, and nothing without one (0.8.x does not grow an unindexed
window). Before this amendment the default sort case on 0.9 and later read 10
rows and then failed. The browser holds the same full subset, so a cap in the render would
show a different result from the browser; the cost is documented, not capped.
`stringSort: "lexical"` is what keeps it bounded; the index is not needed for
that. A tie group is read whole, as ADR-0003 already records.

**A refused predicate.** `readSyncSnapshot` throws when the DO refuses a
predicate (outside the floor, ADR-0013) or SQLite refuses the statement for
one of its limits, where the WebSocket path answers an empty page or a refused
sub. An app sees different results on the two sides. The render fails:
`preload()` rejects with a plain `Error` naming the refusal, and the query is
in `error`. The browser shows rows: a covered request's empty page resolves the
load, and the query evaluates its predicate over the rows its cover holds; an
uncovered request settles with no rows. This was so for an uncovered request
before this amendment, and is now so for a covered one. (Verified by the
adversarial review on 0.8.6, 0.9.2 and 1.0.0-rc.1.5 for an unsupported
predicate; the SQLite limit case follows the same code path, by reading.)

**The D3 cursor rule for rows a render adds by fetching.** The adapter installs
fetched rows insert-if-absent and attributes them to the covering watch
(ADR-0023 D2, D3), and they dehydrate with every other row. MIN keeps the
exported cursor at the earliest read of the render, whatever collection made
it (one transport, one cursor), and the on-demand catch-up (D3) replays every
change from there. Replay is idempotent, so nothing is skipped or doubled.

**Pinned.** `tests/ssr-transport.test.ts`: a covered query in both mount
orders, whose row only its fetch can bring; a covered fetch's own `orderBy`; a
default sort window that only the full-source request corrects; a tie request;
a cursor request in a first load; the row in both halves; a window that moves
during the render; two collections on one transport; and the reads each fetch
shape makes. `tests/ssr-hydration.test.ts`: a covered query's row written
between two reads, and a cursor request whose two reads have writes between
them, each through dehydrate and hydrate to convergence.

**Rejected.**

- *The adapter does not treat a request as covered when the transport cannot
  fetch.* This needs a new capability member on `Transport` (or the adapter
  knowing the SSR class), makes the render take a different path from the
  browser for the same query, and does nothing for a cursor request. D2 rests
  on one adapter code path swapped at the transport seam.
- *The adapter falls back to its own watch when the fetch rejects with
  `SsrReadOnlyError`.* The adapter would learn an SSR error type, every covered
  request would pay a rejected fetch first, and the rule for when a covered
  fetch falls back on an error would be decided here as a side effect of SSR.
- *Refuse a cursor fetch as load more.* Core sends one inside a first load, so
  the refusal failed renders the browser serves.
- *Read a cursor fetch at one position by adding the cursor to the `read`
  request.* Most faithful to ADR-0003, but an app whose `read` rebuilds the
  request, or a DO deployed before the client, would drop the cursor and return
  the first page again without any error.
- *The snapshot transport removes repeated keys.* It does not know the key, so
  it would need `getKey` or the primary key as new public surface, for one race.
- *Count the ties, then read `and(where, or(whereCurrent, whereFrom))` once with
  the ties added to the limit.* One position for the rows, but subtle: ties
  that join between the two reads shorten the page, which core may take as an
  underfilled window.
- *Accept the race as a limit.* It leaves a failed render the app cannot avoid.
