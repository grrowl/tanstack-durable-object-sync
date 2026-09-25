# Changelog

All notable changes to `tanstack-durable-object-sync` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While pre-1.0, the public API may change between 0.x releases.

## [Unreleased]

### Added

- **SSR support (experimental, ADR-0011)** — built on TanStack DB's merged
  SSR API (`DbClient` `dehydrate()`/`hydrate()`, PR
  [#1564](https://github.com/TanStack/db/pull/1564), shipped in
  `@tanstack/db` 0.8.0).
  - **Server:** `readSyncSnapshot(req, request)` — one consistent
    `{rows, cursor}` read over the DO binding, no WebSocket. The required
    `request` runs through `parseAttachment`: one auth gate for the socket
    and the read path. The cursor is a durable high-water mark
    (`max(currentSeq, drainCursor)` — robust to retention pruning); `"0"`
    honestly means "no resume point". RPC rows normalize BLOB
    `ArrayBuffer → Uint8Array` for wire-codec parity (ADR-0017).
  - **Client:** `SsrSnapshotTransport` (read-only; per-request; swapped at
    the new structural `Transport<Api>` seam), syncMeta cursor round-trip
    (`{v, cursor, where-fingerprint}`; fail-loud-but-safe import/merge),
    `since` on the first sub, `seedCursor` (a late chunk regresses and
    replays via a forced reconnect), always-armed eager snapshot reconcile
    (authoritative set semantics — no flash-to-empty, no stranded deletes),
    on-demand transient catch-up with honest truncate for unresumable rows.
  - **Wire (additive):** `uptodate` gains optional `sub` (a catch-up's
    terminal is sub-scoped); the first `sub` may carry `since`.
- **New upstream contracts adopted:** `commit()` receipts
  (`SyncAppliedReceipt`, 0.8.5) — subset loads settle only once rows are
  visible; `markError` (0.8.2) — a failed first connect fails `preload()`
  loud with the cause instead of hanging (a retried `preload()` recovers);
  per-subset load failures reject that subset's promise (0.8.4);
  `withCollectionConfigFactory` (0.8.0) — `doCollectionOptions` configs work
  as `collectionOptions(id, …)` descriptors with fresh adapter state per
  `DbClient`.

### Changed

- **Peer dependency: `@tanstack/db >= 0.8.6`** (was `>= 0.6.0`) — the SSR
  hooks shipped in 0.8.0; 0.8.5 carries the `commit()`-receipt contract and
  descriptor reuse this adapter adopts; 0.8.6 is the first release whose
  module evaluation is Worker-safe (0.8.5 calls `crypto.getRandomValues()` at
  module scope, so a Worker rendering SSR fails to start). The range stays
  uncapped (ADR-0022). The 0.6-era API is otherwise unchanged: the full
  pre-lift suite passed on 0.8.5 without modification.
- The transport ignores STREAM frames from an abandoned socket
  (identity-guarded message dispatch): only the current socket speaks for the
  stream; dropped frames are re-covered by the resubscribe catch-up from the
  applied cursor. ID-scoped receipts (`committed`/`rejected`/`page`) still
  settle their waiters from a stale socket — they are not re-covered by any
  replay — but never advance the cursor.
- **`WebSocketTransport.close()` is now revivable (ADR-0020).** A later
  `connect()` clears the intentional-close latch, restoring auto-reconnect and
  `onClosed` delivery on a reused transport. Previously `close()` was permanent
  across a `connect()`: a revived transport (connection pools reuse instances)
  silently lost both. Code that relied on `close()` being permanently terminal
  must not re-`connect()` the same instance.
- **The injectable `open` gains an optional `AbortSignal` (ADR-0020).**
  Additive and backward-compatible — existing openers compile and run
  unchanged — but a custom opener should honour it (close its socket, reject)
  so `close()` during an in-flight handshake frees a still-CONNECTING socket.
- **New export `ConnectionLostError` (ADR-0021, issue #39).** Settles an
  in-flight `mut`/`call` whose socket dropped unexpectedly before its receipt
  arrived, when no reconnect+replay can resolve the true outcome — catchable and
  `instanceof`-distinct from `MutationRejectedError`, `TransportClosedError`, and
  the generic confirmation timeout. The type is the contract: an app catches it
  to hold its optimistic overlay (the outcome is unknown) rather than roll back.
- Examples updated to `@tanstack/db` 0.9 / `@tanstack/react-db` 0.4, each bundling exactly one `@tanstack/db`; CI now typechecks, builds and boots every example (ADR-0024).

### Fixed

- `#send` no longer throws an uncaught `Can't call send() after close()` when a
  client subscribes then closes before the snapshot finishes streaming (normal
  churn: dispose, navigate-away, StrictMode teardown, forced reconnect). The
  server now skips sends on a non-`OPEN` socket and treats a post-close send as
  a benign no-op — the socket's `webSocketClose` already tore down its subs.
  Covers the `Broadcaster` egress path too (it routes through the same `#send`).
  Outbound-only; no state impact, no behavior change for OPEN sockets (issue #40).
- **`connect()` never resolves disconnected (ADR-0020, issue #37).** The
  close-epoch discard previously `return`ed, resolving `connect()` with no
  socket adopted; the awaiting `subscribe`/`sendMut`/`fetch` then sent on a
  null socket and threw, floating an unhandled rejection and leaving the
  collection silently empty. `connect()` now re-dials a revived transport or
  rejects the new typed `TransportClosedError`; a racing `subscribe` either
  lands its frame on the next connection or rejects typed (routed to
  `markError`, so `preload()` fails loud and recovers on retry).
- **`close()` can abort a socket whose `open()` is still in flight (ADR-0020,
  issue #38).** Previously `close()` disposed through `this.ws`, which is
  `null` during the handshake, so a slow or never-completing handshake leaked a
  CONNECTING socket forever. `close()` now aborts the in-flight `open()` via the
  `AbortSignal`; the default browser opener honours it, and the close-epoch
  discard remains the backstop for signal-ignoring openers.
- `unsubscribe` during an in-flight `subscribe`'s connect no longer sends the
  sub after the socket opens — previously the server persisted a ghost
  subscription (ADR-0019) with no local consumer until the socket dropped.
  Pre-existing; surfaced by the SSR lift's adversary review.
- **An in-flight `mut`/`call` is no longer abandoned to its generic timeout when
  the socket drops unexpectedly (ADR-0021, issue #39).** Server-side, the durable
  commit + broadcast run *before* the `committed` send, so a drop in that window
  used to time out and roll back a write that had already succeeded. Now, on an
  unexpected drop with subscriptions active, the transport HOLDS each in-flight
  `mut`/`call` and REPLAYS it on reconnect; the server's dedup table answers the
  replayed `txId` with its true recorded outcome — a committed write resolves
  `committed` (never a timeout rollback) within the dedup window, a rejected one
  rejects with its real `MutationRejectedError`. Where no reconnect can resolve it
  (a terminal 4xxx close, or no active subscriptions), or the replay does not
  answer within `timeoutMs` of the drop, the mutation settles promptly with the
  new typed `ConnectionLostError` instead of the generic timeout. Timeout
  semantics for a socket that stays open are unchanged.
- **Docs audit against `@tanstack/db` 0.9 / `@tanstack/react-db` 0.4.** Every
  README and recipe snippet now typechecks. The zod recipe imported `defineSync`
  from `tanstack-durable-object-sync/server`, a subpath the package does not
  export; it now imports from the package root. Corrected claims: an `authorize`
  throw surfaces its reason without the `VALIDATION` code (only schema failures
  carry it); without a range index an on-demand window can't page lazily;
  on-demand pages can include every row tied at the boundary, and an unlimited
  query loads every matching row; the static `where` filter is eager-mode only;
  `HydrationBoundary` needs a `DbProvider`; and the mixin also adds the public
  `readSyncSnapshot`.
- Shipped source maps now resolve: the package includes `src/`, which every
  `.js.map`/`.d.ts.map` references, so go-to-definition lands on the source
  instead of a missing file.
- **A `mut` op with an empty key (`""`) is now rejected (ADR-0025).** The pk is
  client-supplied TEXT, so `""` is never a real row identity. ADR-0014 said the
  wire layer already refused it, but it did not. The server now answers with
  `rejected` (`code: "VALIDATION"`) before authorize or any row write, so the client
  gets a prompt `MutationRejectedError` and rolls back. **Behaviour change:** a
  write that used `""` as a pk used to be accepted and is now refused.
- **A replayed `mut` is no longer rejected by a lowered op limit (ADR-0025
  amendment).** `maxOpsPerMutation` was checked before the dedup lookup. If the
  limit was lowered (for example, by a deploy) while a hold-and-replay
  (ADR-0021) was pending, the replayed frame got `LIMIT_EXCEEDED` and the client
  rolled back a write that had committed. The check now runs after the lookup,
  so a resent `mut` gets its stored outcome while that receipt is retained.
- **A `mut`/`call` replayed while the original is still running no longer runs
  twice (ADR-0025 amendment).** `authorize`, and a command's `execute`, can
  await I/O, and a second frame for the same `txId` could slip in and run its
  own attempt. Under ADR-0021's hold-and-replay (the socket drops mid-authorize,
  the client replays on reconnect), the replayed mutation failed on its own pk
  and the client rolled back a write that had committed. A replayed command ran
  its side effect a second time. A duplicate now waits for the running attempt
  and replays its outcome. A replayed `committed` also flushes the socket's
  pending deltas first, so it can't overtake a buffered delta (ADR-0002 C1).

### Internal

- CI tests both ends of the `@tanstack/db` peer range (ADR-0022): a `locked`
  leg (the lockfile's version; the devDependency moves to `^0.9.2`) and a
  `floor` leg that installs the version derived from `peerDependencies`.
- A pack smoke test (`npm run smoke:pack`, run in CI) installs the packed
  tarball into a throwaway consumer at the floor and locked versions, checks
  the tarball ships everything its manifest and source maps reference,
  type-checks the shipped declarations with `skipLibCheck` off, and boots a
  consumer Worker in workerd that renders SSR from a Durable Object.
- A weekly (and manually dispatchable) workflow runs the suite and the pack
  smoke test against `@tanstack/db@latest`.

## [0.6.0] — 2026-07-27

### Added

- **Typed oversize-frame handling (ADR-0018; part of
  [#28](https://github.com/grrowl/tanstack-durable-object-sync/issues/28)).**
  An oversize client mutation used to be dropped silently server-side
  (ADR-0012's `maxFrameBytes` guard), surfacing only as a confirmation
  timeout — and in production Cloudflare's ~1 MiB edge cap on inbound
  WebSocket messages means the frame may never reach the DO at all. The
  transport now guards **before sending**: a `mut`/`call` whose encoded size
  exceeds the 1 MiB edge cap rejects immediately with
  `MutationRejectedError` (code `"FRAME_TOO_LARGE"`), so the optimistic
  overlay rolls back promptly. The server's silent drop stays as defense in
  depth. Outbound gains a warn-only fixed 1 MiB threshold: a larger encoded
  frame logs a `console.warn` with size and collection but is still sent
  whole — column projection (#28a) remains the real fix for oversized
  full-row re-sends. The limits are infrastructure facts (Cloudflare's edge
  cap), not application preferences, so they are constants, not options.

- **Transport reconnect policy (ADR-0016; fixes #25, #26).** One option,
  `reconnectDelay?: number | ((attempt, closeCode?, closeReason?) => number | null)`
  (`null` = stop), replaces `reconnectDelayMs` (**breaking**, pre-1.0: a
  number keeps the old meaning — the default policy's base delay). The
  default (`defaultReconnectDelay`) replaces the fixed interval with capped
  exponential backoff + full jitter (base 250 ms; cap 30 s; attempt counter
  resets on a successful open) and treats application close codes 4000-4999
  as terminal, so an accept-then-close auth rejection (e.g. 4403) no longer
  retries forever. A terminal stop surfaces through the new
  `onClosed(code, reason)` hook, making auth closes distinguishable from
  transient drops.
- **Cohosting docs moved to `recipes/cohosting.md` and re-grounded.** The
  README's cohosting section shrinks to the pitch, the code sample, and a
  pointer; the recipe carries the rules, an honest account of what is verified
  and how (CI fake-host tests vs. source audit at pinned versions vs. the
  untested wake restore), and a reframed `@cloudflare/actors` story. Verified
  against `@cloudflare/actors@0.0.1-beta.6`: the `Actor` class hibernates fine
  on its own but won't cohost out of the box (its `Sockets` helper takes over
  socket connections completely and `Actor` claims the DO-wide auto-response
  slot), while the Actors helpers (`Alarms`, `Storage`) compose cleanly with
  sync over a plain `DurableObject` base — which Actors' own examples document.
  Supersedes the 0.5.0 "host defect" phrasing.

### Fixed

- **Subscriptions survive hibernation wake (ADR-0019; field report against
  0.5.1).** Hibernatable sockets survive a DO eviction by design, but the
  subscription registry was instance memory: a wake restored the socket set
  and nothing else, so an idle client's live queries went silently dead on a
  still-open socket — its own mutations still confirmed while deltas fanned
  out to nobody, and the client's only re-subscribe trigger (the close path)
  never fired. Subscriptions are now written through to a durable
  `_sync_subs` table keyed by a per-socket id tag stamped at accept, and
  restored onto surviving sockets during `registerSync` on every wake —
  before anything can dispatch or drain. No wire or client change: an
  unmodified 0.5.1 client against a fixed server recovers fully. Orphaned
  rows (a socket that dies without `webSocketClose`) are swept during the
  existing compaction housekeeping; no idle timers (ADR-0006 invariant
  intact). Pinned by `tests/hibernation.test.ts` under **real evictions**
  (`evictDurableObject`, unlocked by the vitest 4 migration — the
  eviction-based wake test issue #29 asked for), in five shapes: eviction
  after successful broadcasts, eviction before the first-ever broadcast,
  eviction of a cohosted base (tagged-restore branch, host socket untouched),
  a restored sub whose collection left the schema (reconciled: `reset` +
  row dropped, healthy subs untouched), and a restored predicate that no
  longer compiles (socket closed with a non-terminal code so reconnect
  re-subscribes — a `reset` would strand the query, adversarial review).
  Sockets accepted by a **pre-fix** build that survive an in-place upgrade
  carry no id tag and keep the old behavior (dead until reconnect) — a
  one-release sharp edge, documented in ADR-0019.
- **Rejection `code` now survives tx-dedup replay (#21).** The dedup record
  persisted only the rejection message, so a client retrying the same `txId`
  got the reason with no machine-readable `code` — breaking code-based error
  handling on exactly the retry path it exists for. `_sync_seen_tx` gains an
  `error_code` column (added in place on wake for already-deployed DOs), and
  the replayed `rejected` frame is now shaped identically to the original:
  `{ code, message }` when a code was recorded, `{ message }` otherwise.
- **BLOB columns no longer corrupt to `{}` over the wire (ADR-0017,
  [#27](https://github.com/grrowl/tanstack-durable-object-sync/issues/27)).**
  workerd's `SqlStorage` returns BLOB values as bare `ArrayBuffer`, which the
  msgpack encoder fell through to `encodeMap` on (and the JSON debug codec
  stringified as `{}`) — clients silently received an empty object, for
  snapshots and deltas alike. Both codecs now normalize `ArrayBuffer` at
  emission, so BLOB columns arrive on the client as a `Uint8Array` with the
  exact bytes.

## [0.5.1] — 2026-07-03

### Fixed

- **`registerSync` now rejects tables with no usable internal `rowid` (ADR-0015).**
  The cold-snapshot/fetch reader defaults to `ORDER BY rowid` when the client
  sends no `orderBy`. A `WITHOUT ROWID` table has no rowid, so that read threw
  `no such column: rowid` and hung the subscriber; a table with a declared
  `rowid` column shadows the internal one, so the read would silently sort by
  that arbitrary column. `assertSyncCompatible` now rejects both loudly at
  `registerSync`, alongside the existing `INTEGER PRIMARY KEY` guard. Ordinary
  rowid tables (the documented `id TEXT PRIMARY KEY` pattern) are unaffected.
- **`SyncMixin`'s declared `webSocketClose`/`webSocketError` now match their
  implementation.** Both were typed as returning `Promise<void>` while the
  mixin's overrides return `void` synchronously (there is nothing to await —
  both just drop bookkeeping for the closed socket). Retyped to
  `void | Promise<void>`, mirroring `@cloudflare/workers-types`'s own
  `DurableObject` interface. `webSocketMessage` is genuinely `async` and is
  unaffected. Cosmetic: no runtime behavior changed, only the emitted `.d.ts`.

## [0.5.0] — 2026-07-02

### Added

- **`Syncable(Base)` mixin — cohost sync on any Durable Object base (ADR-0015).**
  The sync machinery is now a curried mixin factory,
  `Syncable<Env, TUser>()(Base)`, so one DO can be both its framework's host (the
  Agents SDK `Agent`, `@cloudflare/think`'s `Think`, a bare `DurableObject`) and a
  tddc sync source — no dedicated sync DO, no mirror write. Exposed from the root
  and a `./server/mixin` subpath. Sync sockets carry a reserved tag and a plain
  attachment and claim only the `/_sync` path; all other traffic delegates to the
  host base, so the two protocols never cross (proof: partyserver's `__pk`
  filtering). `Actor` (`@cloudflare/actors`) is documented as unsupported because
  its `Sockets` helper adopts foreign sockets on wake. See the README "Cohosting"
  section.

- **Optional Standard Schema validation (ADR-0014).** A collection's
  `insert.schema` (the row schema, which also infers the collection's Row) and
  `update.schema` (a partial patch schema), and a command's schema, are checked
  at runtime and rejected loudly on failure. Any `~standard` library works (zod,
  valibot, arktype) and the framework adds no validator dependency. It is a gate,
  not a parser: the original value flows to handlers, so schemas must not rely on
  transforms, defaults, or coercion. See
  `recipes/zod-standard-schema-collections.md`.

### Changed

- **`SyncDurableObject` is now `Syncable()(DurableObject)`** — zero API change.
  All existing `extends SyncDurableObject<Env, Claims>` code, including
  `this.sql`, `this.registerSync`, `this.runSyncedWrite`, and an overridable
  `parseAttachment`, keeps compiling and behaving identically to 0.4.0 (the two
  DO-global side effects — `ping/pong` auto-response and `PRAGMA
  case_sensitive_like = ON` — stay ON for this base; they default OFF over any
  other base, opt in with `this.sync.configure`). The internal `sql` getter was
  removed from the mixin because it shadowed the host's `sql` tagged-template
  method; reach `this.ctx.storage.sql` directly on a non-`DurableObject` base.

- **Rejection reasons are surfaced uniformly for mutations and commands (revises
  ADR-0012 D3).** An `authorize` throw or a schema validation failure now reaches
  the client with its reason (validation failures carry a `VALIDATION` code);
  only `execute` errors stay sanitized. Previously a command's `authorize` error
  was sanitized like its `execute`, unlike a mutation's.

## [0.4.0] — 2026-07-01

### Changed

- **Authoring is now an object schema, not a builder (ADR-0014).**
  `new SyncRegistry().defineCollection().defineMutation().defineCommand()` is
  replaced by `defineSync<User, Env>()`, which binds identity/env once and
  returns `{ collection, command, schema }`. Mutations are a **closed
  insert/update/delete trio** co-located on the collection (mirroring
  `@tanstack/db`'s `onInsert/onUpdate/onDelete` — a custom mutation type is now
  structurally unrepresentable), superseding ADR-0001 D11 and absorbing
  ADR-0010's row manifest: the row type lives on the collection
  (`sync.collection<Message>({ pk })`) instead of a third `SyncRegistry`
  generic. The DO registers the schema value with `this.registerSync(schema)`.
  Breaking, no compat shim.

### Added

- **Typed commands, end to end (ADR-0014).** `export type Api = typeof schema`
  is the whole client contract: `new WebSocketTransport<Api>()` exposes a typed
  `transport.call.<command>(args)` proxy plus a typed low-level
  `sendCall(name, args)` (txId generated internally via `crypto.randomUUID()`),
  and `doCollectionOptions<Api, "table">({ … })` infers the row type from the
  schema — one source of truth across server and client.
- **`examples/multi-do`** — a two-Durable-Object example: one transport per DO, a
  React `SyncProvider`/`useSync` keyed by DO so command namespaces never
  collide, and a client-side cross-DO feed.

## [0.3.3] — 2026-06-13

### Fixed

- **A filtered subscription's membership no longer depends on which path
  decided it (ADR-0013).** The SQL snapshot and the JS delta/catch-up evaluators
  disagreed on two operators, so two clients could see different rows for the
  same `where` depending on connection timing:
  - **`ne` crashed the delta path and hung the client.** The SQL floor accepted
    `ne` but `@tanstack/db`'s evaluator has no `ne` (not-equal is `not(eq(...))`);
    its compile error escaped `handleSub` uncaught, so no `reset` was sent. `ne`
    is now off the floor and rejected with `reset` like any unsupported operator,
    and a defensive guard turns any predicate-compile failure into a `reset`
    rather than a hang. Use `not(eq(...))` for not-equal (unchanged for real
    clients).
  - **`like` was case-insensitive in the snapshot but case-sensitive in deltas.**
    The DO now sets `PRAGMA case_sensitive_like = ON`, making SQLite `LIKE` match
    `@tanstack/db`'s case-sensitive `like` on every path. (#17)

### Changed

- **Operator floor for server-side filtering dropped `ne`** — it is now
  `eq, gt, gte, lt, lte, like, in, and, or, not`, exactly the set the SQL and JS
  evaluators agree on row-for-row. `like` is now case-sensitive. (#17)

## [0.3.2] — 2026-06-13

### Added

- **Wire-input hardening at the server boundary (ADR-0012).** Frame-shape
  guards drop malformed frames — the socket survives and answers the next
  valid frame — and inbound limits bound frame size and per-connection
  subscription count. (#15)

### Fixed

- **A failed initial connect no longer wedges the client transport.** A
  WebSocket that never `open()`s fires no `close` event, so the auto-reconnect
  couldn't run and the cached rejected `connectPromise` wedged the transport
  permanently — every later `connect()` returned the same rejection. On a
  failed open the transport now clears the promise and re-arms the reconnect
  while subscriptions are live. (#12)

### Changed

- **Reconnect catch-up no longer issues an N+1.** Delta hydration batches
  keyed reads in chunks (≤64) and the per-table changelog read uses the
  `(tbl, seq)` composite index, so a 500-key catch-up issues ~8 queries
  instead of 500. Behavior is unchanged. (#14)

### Security

- **Mutation `execute` errors are sanitized.** A failed `execute` now returns a
  generic "mutation failed" rather than leaking SQLite/internal detail to the
  client; authorize errors still pass through. (#15)

### Internal

- Server error-path and wire-level test coverage expanded (#11); dead
  `snapshotAll` removed and CDC trigger DDL identifiers quoted (#13).

## [0.3.1] — 2026-06-11

### Fixed

- **Catch-up reinsert no longer wedges the client.** A key deleted-and-
  reinserted while a client was away arrives in the catch-up as op=`insert`
  for a key the client still holds; TanStack's sync write throws
  `DuplicateKeySyncError` on that, aborting the whole catch-up transaction.
  The adapter now applies a held-key insert as the upsert it semantically is
  (the move-in update-upsert contract, ADR-0002 C4).
- **Cursor barrier (C1′).** A snapshot or catch-up served on a socket
  with still-buffered coalesced deltas could advance the client's cursor past
  an undelivered write (multi-collection reconnect; drop before the tick lost
  the write). The server now flushes the socket's pending deltas before any
  synchronous cursor-advancing emission — ADR-0002 C1 generalized from
  `committed` to all cursor boundaries.
- **Reconnect window no longer kills subscriptions.** The `reconnecting` flag
  was set inside the reconnect timer, so a mutation fired within the reconnect
  delay of a drop established the fresh socket before the timer ran — with no
  resubscribe, leaving every subscription silently dead on the new socket (and
  the late timer wedged the flag). The flag is now set when the reconnect is
  scheduled, so whichever connect wins — timer- or demand-driven — resubscribes
  from the cursor.

## [0.3.0] — 2026-06-09

### Added

- **Typed mutations** (ADR-0010). `SyncRegistry` takes a third generic — a
  collection-row manifest — so handlers are fully typed without casts:
  `new SyncRegistry<TUser, Env, { messages: Message }>()` types `pk` (must be a
  column) and each handler's `op.cols` per op (`insert`→`Message`,
  `update`→`Partial<Message>`, `delete`→none). Purely type-level; the untyped
  two-generic form still works (`op.cols` falls back to `unknown`).
- **Changelog time-based retention** (ADR-0009). A new `changelogRetentionMs`
  knob (default 2 days) prunes `_sync_changes` rows older than the window, so the
  log is bounded by age, not just key-cardinality. A client reconnecting from
  beyond the surviving floor now receives a `reset` + full snapshot instead of an
  incremental delta. Set `changelogRetentionMs: null` to disable retention
  (compaction-only, the prior behavior).

### Changed

- **Renamed `Registry` → `SyncRegistry`.** Breaking, no compat shim. The old
  name was too generic and clashy; the public surface is uniformly `Sync*`
  (`SyncDurableObject`, `runSyncedWrite`, `registerSync`). Update your import and
  `new SyncRegistry(...)`.
- `registerSync` now reconciles CDC triggers to the registry instead of only
  adding them: triggers for a collection you've removed from the registry are
  dropped on the next `registerSync`, so an orphaned table stops firing capture
  triggers into `_sync_changes`. Trigger reap only — existing change rows are
  left untouched. (ADR-0008)

## [0.2.0] — 2026-05-31

### Changed

- **Author-owned schema; `registerSync` wires the sync.** Breaking. Collection
  definitions are now `{ table, pk }` — `ddl` is removed. You create your tables
  yourself (raw DDL, Drizzle, any migrator), then call `this.registerSync(registry)`
  — typically in your constructor's `blockConcurrencyWhile`, after migrating. It
  validates each table (one `PRAGMA`: the pk is a sole `TEXT` client key — D9) and
  installs the CDC triggers. Lazy `initRegistry`-on-`fetch` is gone; schema exists
  before the first event. Forget the call → `this.registry` throws loud. This also
  retires `runSyncedWrite`'s "caller ensures init" caveat. See
  [ADR-0007](docs/adr/0007-author-owned-schema-register-sync.md).

- **The cursor `fetch` frame now mirrors `@tanstack/db`'s `LoadSubsetOptions`.**
  Breaking wire change (client and server ship together). The frame carries a
  base `where` plus a raw `cursor: { whereFrom, whereCurrent }` — TanStack's own
  `CursorExpressions` names and semantics (the cursor expressions exclude the
  base `where`) — replacing the previous private `where`/`ties` fields that
  combined the predicates client-side. The server now composes `base AND
  whereCurrent` (ties, unbounded) and `base AND whereFrom` (next page, bounded by
  `limit`). Behaviour is unchanged (identical compiled SQL); the win is
  traceability to upstream. A malformed cursor (a missing half) is now rejected
  loudly instead of degrading to an unbounded scan. See
  [ADR-0005](docs/adr/0005-fetch-frame-mirrors-loadsubsetoptions.md).

- **Collection pk validation accepts TEXT *affinity*, not the literal `"TEXT"`.**
  `registerSync` now admits any TEXT-affinity pk (`TEXT`, `VARCHAR`, `CHAR`,
  `NVARCHAR`, …) so ORM/migrator-generated DDL composes; `INTEGER` (rowid alias)
  and other non-TEXT affinities are still rejected loudly (they'd break optimistic
  id parity). Part of ADR-0007.

### Added

- **`runSyncedWrite(fn)`** — a `protected` `SyncDurableObject` primitive for
  **server-originated writes** (an agent inserting a row, a webhook, a cron/
  `alarm` job, an admin edit, a bulk seed): apply a raw synchronous SQL closure
  in a transaction, then broadcast the resulting CDC to connected clients.
  Outside the client mutation flow — no `txId`, no receipt, no dedup (idempotency
  rides the collection's mandated stable keys).
  See [ADR-0006](docs/adr/0006-server-originated-writes.md).
- **`examples/board`** — an at-scale stress example: 5,000 tasks on one DO,
  bounded window load, `useLiveInfiniteQuery` cursor scroll-back, and a mutable
  order key (`updated_at`) so voting/starring bumps a task to the top (move-in
  via the always-emit upsert). A server-side firehose makes the deferred
  bounded-window-under-churn limitation visible: `loaded` climbs past `window`.
- A real-tie integration test (unbounded boundary ties + limited next page + base
  predicate composed into both halves) covering the cursor `fetch` against the DO.
- A move-in integration test: a cold row bumped server-side arrives via the
  no-`where` live sub and upserts into the collection (ADR-0002 C4).

## [0.1.0] — 2026-05-30

### Added

- **`afterCommit` post-commit hook + `env` in handler context.** Mutations gain
  an optional `afterCommit(ctx)` that runs fire-and-forget via `waitUntil` after
  the commit and receipt — the sanctioned home for external side effects a
  synchronous `execute` can't do (delete an R2 object, enqueue a job). It's
  isolated (a throw is logged, never affects the committed mutation) and owns its
  own idempotency. Handler contexts (`authorize`/`execute`/`afterCommit` and
  command `execute`) now receive the DO's `env`, typed via a new `Env` generic on
  `Registry` that defaults to `unknown` (existing `Registry<TUser>` unchanged).
  See [ADR-0004](docs/adr/0004-after-commit-hook.md).
- **Bounded initial load for on-demand windows.** A live query's `orderBy` and
  `limit` are forwarded to the server so the initial snapshot is the bounded
  window (e.g. the most recent N rows) rather than the whole `where` subset. The
  live subscription's predicate stays the `where` clause, so entering rows are
  still delivered.
- **Cursor load-more (scroll-back).** Extending a windowed query past its first
  page issues one one-shot paginated `fetch` (new `fetch`/`page` wire frames)
  carrying both halves of the cursor double-read — boundary ties (`ties`,
  unbounded) and the next page (`where`, bounded by `limit`), each combined with
  the base `where`. The server reads both at a single `seq` (atomic), and no new
  live subscription is taken — the window's deltas already flow over the existing
  `where` sub. See [ADR-0003](docs/adr/0003-atomic-cursor-fetch.md).

### Fixed

- **Cursor load-more no longer resurrects a concurrently-deleted row.** The
  earlier two-frame double request read the ties at one `seq` but applied them
  after a deferred merge; a live delete landing in between let the stale tie
  re-insert the deleted row, with no future delta to correct it. The double-read
  is now one atomic `fetch`, so the page applies in stream order before any later
  delta. See [ADR-0003](docs/adr/0003-atomic-cursor-fetch.md).
- **Cursor load-more no longer throws on overlapping boundary rows.** Page rows
  are written insert-if-absent, so a boundary tie already in the window (or a row
  a concurrent live delta already refreshed) is skipped rather than re-inserted —
  which would otherwise throw `DuplicateKeySyncError` and abort the open sync
  transaction. The live `where` subscription stays the source of truth for rows
  currently in the collection.
- **Rejected subscriptions no longer hang `preload()`.** A `reset` with no
  `snap-end` (the server's response to an unsupported predicate or unknown
  collection) now resolves the subset's load promise instead of leaving the live
  query waiting indefinitely.
- **On-demand `orderBy` IR shape.** `orderBy` clauses from real live queries
  (`{ expression, compareOptions }`) were not recognised by the SQL compiler, so
  server-side ordering was silently dropped and the wrong rows were returned for
  a bounded window. The compiler now accepts the live-query clause shape.

## [0.0.1]

Initial release — sync a [TanStack DB](https://tanstack.com/db) collection to a
[Cloudflare Durable Object](https://developers.cloudflare.com/durable-objects/)
over a single WebSocket. Server-authoritative, single-writer, no CRDTs.

### Added

- **Single-ordered-stream sync.** Change-data-capture via SQLite triggers into
  one per-DO change log; one monotonic `seq` cursor drives live deltas,
  reconnect catch-up, and write confirmation. The client tracks a single
  `appliedSeq` — no second acknowledgement channel.
- **Optimistic mutations** with single-stream confirmation (`awaitSeq`):
  `mut` (atomic row transactions) and `call` (named commands), exactly-once via
  `txId` dedup. Mutation `execute` runs inside `transactionSync`; `authorize`
  runs before it.
- **Client-supplied keys** enforced at `defineCollection` (ULID/UUIDv7); the
  optimistic id equals the confirmed id.
- **Filtered subscriptions.** A `where` predicate (a `@tanstack/db`
  `BasicExpression`) is evaluated server-side with `@tanstack/db`'s own
  compiler, so operators match the client exactly; move-in/move-out handled
  without a before-image. Client-side write-outside-filter preflight
  (`WriteOutsideSubError`).
- **Subset shaping** — `where`/`orderBy`/`limit`/`offset` lowered into SQLite
  (operator floor: eq, ne, gt, gte, lt, lte, like, in, and, or, not);
  un-lowerable predicates are rejected, never silently full-scanned.
- **On-demand subsets** (`syncMode: 'on-demand'`) — load only the subsets your
  live-query `where` clauses request; each distinct `where` is one refcounted
  server subscription, released on the last unload. Writes outside every loaded
  subset are confirmed without stranding an optimistic row.
- **Egress coalescer** — per-`(sub, key)` last-write-wins on a tunable tick,
  collapsing high-rate writes (e.g. streaming tokens) to one delta per tick;
  hibernation-native (no timer when idle).
- **Reconnect catch-up** — auto-reconnect resubscribes from the applied cursor;
  the server serves a windowed delta or, past the retention floor, a reset +
  snapshot.
- **Compaction-defined retention** — opportunistic (every N writes, off the
  response path via `waitUntil`; no idle-DO wakeups): the change log collapses
  to latest-op-per-key, with independent time-based dedup GC.
- **Multiplexing** — many collections on one DO share a single WebSocket.
- **Client IVM** — live queries (joins, filters, aggregates) run client-side
  via `@tanstack/db`; the DO stores and emits, never runs IVM.
- **Binary wire** — MessagePack frame codec (JSON debug fallback) + a tagged
  value codec preserving bigint/Date/NaN/±Infinity/-0/undefined/Uint8Array.
- **Hibernating WebSockets** — `acceptWebSocket` + auto-response ping/pong;
  per-socket identity via `serializeAttachment`.
- **Examples** — `examples/chat` (eager, multi-tab live sync) and
  `examples/on-demand` (on-demand subsets), both verified in a real browser.

### Deferred (post-1.0)

- Windowed pagination (`orderBy`/`limit`/cursor double-request with
  server-side window maintenance under churn).
- `isWhereSubset` containment dedup of overlapping subsets.
