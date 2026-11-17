# 0023 — On-demand: watches, exact acquisitions, and row holds

**Status:** Accepted. Replaces the on-demand subset model of ADR-0001 D8 /
M11 ("one refcounted server subscription per distinct `where`"). Builds on
ADR-0002 C4 (always-emit move-out), ADR-0003/0005 (the atomic `fetch` page),
ADR-0011 D3 (on-demand hydration catch-up), ADR-0016 (reconnect) and ADR-0022
(tested at the floor and at current). **Amends** ADR-0016's resubscribe: D7
below decides which subs may resume from the cursor. **Narrows** ADR-0011's
stale-socket receipts: a `page` from an abandoned socket now fails its fetch
(D8); mutation receipts are unchanged.

## Context

`@tanstack/db` 0.9 changed what core asks an on-demand adapter for, and
tightened the contract it asks under (0.9.0 CHANGELOG):

- **Exact results.** "A result describes only the exact `options` passed to
  this call." Core's new ordered loader
  (`query/live/ordered-source-loader.ts`) issues requests of new shapes: a
  first page `{orderBy, limit}`; boundary **ties** as a separate
  `{where: and(subscriptionWhere, eq(orderCol, v))}` with no cursor, once per
  page boundary; a **prefix** `{orderBy, limit: n}` for orders it cannot
  express as a cursor (a locale-collated string, several columns); and a
  **full source** `{where}` — no order, no limit — when an order is not
  expressible, or after a published row is deleted or re-ordered
  (`onSourceChanges` → `invalidateSourceOrdering` → `requireFullSourceRecovery`,
  L70/L83/L269/L484 at the 0.9.2 tag).
- **Release-then-reload.** Replays release the prior acquisition before
  loading its replacement. "Custom adapters must support a release/load gap
  and preserve resources still held by other owners."
- **Unload** must be idempotent and must not throw.

Our adapter identified a subset by `where` alone and kept each subset's rows
after release. Auditing it against those contracts, through real live queries
on 0.8.6 and 0.9.2, found (all reproduced; `tests/on-demand-contracts.test.ts`
pins each):

1. **Identity ignored orderBy/limit.** Requests with one `where` and different
   orders or limits shared the first request's bounded snapshot (opposite
   orders showed the wrong rows); a grown unindexed window never reached the
   DO; on 0.9 the tie, prefix and full-source requests were answered with the
   first page's already-resolved promise.
2. **A move-out clobbered other subsets.** Always-emit (ADR-0002 C4) sends every
   sub a frame for every changed key: the row if it matches, else a synthetic
   delete. With two subscriptions, one sub's synthetic delete hard-deleted a row
   the other still matched.
3. **Ghosts across the release/load gap.** Released rows stayed in the
   collection with nothing keeping them live; a row deleted meanwhile came back
   after the reload, whose snapshot carries no tombstones.
4. **A refused sub truncated the collection.** A refusal (unsupported
   predicate, sub cap, unknown collection) arrives as `reset` with no snapshot,
   and the subset handler truncated everything. On 0.8 that wiped every other
   subset for good (0.8's load-then-release replay was answered from the
   refcount); on 0.9 the truncate replay re-subscribed the refused predicate
   and looped (hundreds of `sub` frames a second).
5. **subId reuse.** `${table}#${whereJSON}` was reused by a reload, so a released
   sub's in-flight snapshot dispatched into the new handler.
6. **Cleanup closed the shared transport**, silencing every other collection on
   it (0.9.1's GC reclaim runs cleanup more often).
7. **A reconnect before a sub's first snapshot** resubscribed it from the shared
   cursor: the DO answered a catch-up, its rows never loaded, its load hung.

Together, 1 and 2 broke `examples/board` on 0.9: the tie request became a
second live sub; on the first insert it sent a move-out delete for the new row
right after the main sub's insert; `committed` retired the overlay and the row
was lost; that delete made the loader request the full source, which (1)
answered with the first page's promise, so paging stopped for good.

## Decision

D1–D6 and D8 are on-demand only. D7 (reconnect) applies to every sub, eager
included. No server or wire change.

### D1 — A watch keeps a predicate live; an acquisition is one request

A **watch** is one server sub: every delta for a row its `where` matches
arrives on it. Its subId is fresh (`${table}#${++seq}`), so a released
watch's late frames find no handler (5).

An **acquisition** is one `loadSubset` request, identified by a stable encoding
of `[where, orderBy, limit]` (the wire value codec, so Dates, bytes and bigints
neither collide nor throw). Identical requests share one acquisition
(refcounted). Cursor requests stay one-shot `fetch`es with no acquisition
(ADR-0003). A non-cursor request with a non-zero `offset` is rejected loudly:
neither a snapshot nor a fetch can honour it, and upstream pairs an offset with
a cursor.

### D2 — A covered request is a fetch that pins its watch

A request whose `where` is kept live by an accepted watch — the watch is
unfiltered, or every top-level conjunct of its `where` is a conjunct of the
request's (core's tie shape is `and(subscriptionWhere, tie)`, and a
subscription `where` may itself be an `and`) — needs no new subscription. It is answered by one `fetch` frame shaped by its own
where/orderBy/limit (the DO already serves a cursor-less fetch), written
insert-if-absent like a cursor page (ADR-0003), and it **pins** the watch until
released. Any other request opens its own watch whose snapshot is shaped by the
request. A watch the DO has not accepted yet covers too: the request waits for
the watch's first `snap-end` before it fetches, opens its own watch if the DO
refuses that one, and fails with it if its subscribe fails. So each request loads exactly its rows (1), compatible
requests mounted together share one subscription, and 0.9's ties never become
subscriptions: one server sub per predicate.

### D3 — Holds: a row goes when no watch holds it

The adapter records which watches hold each row: a snapshot row, an upsert
delta, or a fetched row (attributed to its covering watch). A `delete` from a
watch drops that watch's hold; the row is deleted only when no watch holds it
(2). A real delete reaches every watch (always-emit), so the last hold removes
it; the order of frames inside a batch does not matter. Rows no watch has ever
held (SSR-hydrated ones) keep today's behaviour: any delete removes them. The
first watch to deliver such a row adopts it.

Cost: per loaded row, one map entry and a small set of watches (usually one),
plus one entry in each holding watch's row set. Every frame does O(1) work; a
release is O(rows that watch held).

### D4 — Release removes what nothing else holds

When a watch's last pin goes, it is unsubscribed and the rows only it held are
deleted in one sync commit (3). A reload snapshots them fresh. This is the
memory contract of on-demand (ADR-0011: memory proportional to what is
observed) and what upstream's own on-demand adapters do.

Two interactions (the first pinned by a test; the second follows from D3):

- **Optimistic overlays.** A released row with a pending optimistic mutation
  stays visible through its overlay; the write still commits on the DO. The
  release's deletes are sync writes, which core applies after the persisting
  transaction settles; then the post-mutation empty commit (ADR-0002 C2)
  retires the overlay and the row leaves. Nothing is lost on the DO.
- **SSR-hydrated rows** a watch adopted leave with that watch, like any other.

### D5 — A reset before bootstrap is a refusal; after, a truncate

With D7, a sub that never reached its first terminal never resubscribes with
`since`, so a `reset` before its first `snap-end` can only be a refusal. It
holds no rows: the load settles as before and nothing else is touched (4).

A `reset` after bootstrap means the sub fell below the retention floor on a
reconnect. Its rows may be stale, and so may rows covered fetches and cursor
pages attributed to it, which its bounded resnapshot does not restore. Only
core's truncate replay knows every demand, so this keeps today's truncate, and
the truncate starts a new **generation**. Every current watch is retired at
once (unsubscribed), so the old subs' trailing reset/resnapshot frames find no
handler even when core applies the truncate later, queued behind a persisting
transaction: one truncate per reconnect, not one per sub. Acquisitions from
before it no longer share, cover or install pages, so core's replay reloads
every demand with fresh subs under both upstream orders (0.8
load-then-release, 0.9 release-then-reload). A load still waiting on a retired
watch is settled once the truncate is applied (core's replay barrier exists
from then on) — resolved, not rejected: core reports a rejected load as its
query's error, and its replay reloads the demand anyway; if the truncate itself
fails, so do they. A load released before its rows arrived is settled at once.

### D6 — Cleanup unsubscribes; it never closes the transport

Cleanup unsubscribes every watch and the transient hydration catch-up sub. The
transport is the caller's and may serve other collections (6). Eager already
worked this way.

### D7 — A sub resumes from the cursor only after it bootstrapped

The transport marks a sub bootstrapped at its first `snap-end` or own
catch-up `uptodate`. Only then may it resume from the shared cursor, which
other subs may have advanced past changes it never saw. On reconnect a
bootstrapped sub resumes with `since = cursor`; a sub given an explicit `since`
(SSR hydration) resumes from that `since` until its own terminal (or from a
lower cursor, if a late hydration chunk regressed it); any other restarts from
a snapshot, and its handler first undoes what a partial snapshot delivered,
inside the still-open sync transaction (7). On-demand drops the watch's holds; eager
deletes the partial snapshot's keys. Either way a row deleted meanwhile is not
carried over, and rows that still exist are re-upserted before the commit.

### D8 — Contract hygiene

Unload is idempotent per options object and never throws: core releases each
acquisition with the object it loaded, so a repeat release, or an object that
was never loaded, is a no-op. Dropping a last hold always queues the delete,
even for a row that is still only an insert in the open transaction or in a
queued commit. A page installs nothing when its session was cleaned up, a
truncate intervened, or (covered fetch) every owner released it. A covered
fetch can be shared, so one owner's abort does not cancel it for the others;
a cursor page, never shared, also honours its request's `signal`. A `page` that
arrives on an abandoned socket (a late hydration chunk forced a reconnect)
fails its fetch instead: the fresh socket's replay may already have deleted a
row it carries, and nothing would mention that row again.

## Upstream behaviour, not ours: the one-time full-subset reload on 0.9

On 0.9 the first delete of a published window row, or the first update that
changes a published row's order value (a board vote), makes core request the
**full matching subset** once; the adapter must return it. After that the
loader holds the full source, so later changes load nothing more. An
upstream-only repro (a minimal correct in-memory source, no adapter code)
shows it: 0.9.2 makes `loadSubset({})` on the first re-order and nothing after;
0.8.6 makes none and keeps a stale local top-k (it showed a row moved out of
the window). The repro travels with the upstream report. The same happens at
mount for orders 0.9 cannot express as a cursor.

## Consequences

- Each on-demand request loads exactly what it asks for, on both 0.8.6 and
  0.9.2, with one server sub per predicate. The board works on 0.9.
- A released query's rows leave the collection unless another loaded query
  holds them (documented in README and the on-demand recipe).
- A covered full-subset request is one `page` frame; outbound frames are not
  edge-capped (ADR-0018 D3), so a large one warns rather than fails.
- Cleanup no longer closes the transport. An app that owned a transport only
  for one on-demand collection closes it itself (`transport.close()`).
- A request whose predicate is equivalent to a watch's but written differently
  (`and(b, a)` vs `and(a, b)`) is not recognised as covered and opens its own
  watch: correct, one extra sub.

## Rejected alternatives

- **A live sub per distinct request.** Correct with D3, but 0.9 issues a tie
  request per page boundary; each would be a live sub counted against
  `maxSubsPerSocket` and fanned every change.
- **Resolving deletes per batch** (drop a delete if another sub upserted the key
  in the same batch). Fails for per-sub catch-ups, which arrive in separate
  batches.
- **Dropping only the reset sub's holds** instead of truncating. It deletes
  covered and cursor rows that the bounded resnapshot never restores, and core
  never learns to reload them.
- **`commit(signal)` for pages.** A page is consistent at its stream position;
  the contract allows settling once the rows are visible.

## Follow-ups (not in this change)

- The single cursor is unsound across a drop *during* a resubscribe round: one
  sub's catch-up terminal advances it before another's catch-up ran, and the
  second then resumes past changes it never saw. Needs its own ADR (it changes
  ADR-0002's single-cursor invariant).
- `#handleFetch` does not flush the coalescer before reading, so a page can
  overtake a buffered delta (it converges when the delta lands).
- A refused fetch or sub is indistinguishable from an empty one on the wire
  (`page(rows=[])`, `reset`); `page-error`/`sub-error` frames would let loads
  fail loud.
- SQL `ORDER BY` ignores `nulls` and locale collation, so a bounded snapshot
  can choose a different top-k than the client's comparator.

## Amendment — 2026-09-29: fetch flush barrier

The fetch/coalescer follow-up above is resolved: `#handleFetch` now flushes the
requesting socket's buffered deltas before reading its page, as the subscription
snapshot path already does. A covered load therefore cannot settle with an old
held row while its newer delta remains buffered. The page remains an atomic
read and does not advance the client cursor; the preceding delta boundary
commits the update first. `tests/fetch-flush-barrier.test.ts` pins both the
wire order and the held row's value at load settlement through the real DO,
transport, and on-demand collection.

## Amendment — 2026-09-29: bounded snapshot NULL placement

The `nulls` half of the ORDER BY follow-up is fixed: each SQLite sort term
explicitly uses the requested `NULLS FIRST` or `NULLS LAST`, and `NULLS FIRST`
when the clause names none. `@tanstack/db` 0.8.6 and 0.9.2 both default to
`nulls: "first"` in either direction, while SQLite's default puts NULLs last
in a `DESC` sort, so a descending window used to drop its NULL rows.
`tests/order-by-nulls.test.ts` compares the real DO's bounded snapshot with
TanStack's own answer. The locale-collation half remains open (pinned as
`it.fails`), as does top-k selection among NULL ties crossing the limit, which
SQLite does not break by row key as TanStack does.

Cursor pages are client-built `where` expressions, so this fix does not touch
them. 0.8.6's `buildCursor` ignores `nulls` (plain `gt`/`lt`, `eq` for ties).
0.9.2 builds no cursor at a NULL boundary: a page request becomes a prefix load
(`orderBy` + `limit`, no cursor), and a boundary-tie request loads the full
subset. At a non-NULL boundary with `nulls: "last"`, its `whereFrom` is
`gt(v)` (`lt(v)` descending) `OR isNull OR isUndefined`, and `isNull` is
outside the predicate floor (ADR-0013): the fetch is refused as an empty page
and the window stops growing. Pinned as `it.fails`.

## Amendment — 2026-09-29: row-key tie-break and the nulls-last cursor

Two of the open items above are closed.

- **Ties.** Every bounded read ends its `ORDER BY` with the collection's pk,
  ascending whatever the direction. `@tanstack/db` breaks value ties by row key
  ascending (`createKeyedComparator` → `compareKeys` in db-ivm; identical in
  0.8.6's db-ivm 0.1.19 and 0.9.2's 0.1.22), comparing strings with `<`. The
  term is `COLLATE BINARY` explicitly, since a pk may declare another collation
  (NOCASE would order `a` before `B`). BINARY matches JS code-unit order except
  for astral versus U+E000–U+FFFF characters (JS compares UTF-16 code units,
  SQLite UTF-8 bytes): a known residual divergence. The snapshot, RPC snapshot, and fetch paths all build their
  SQL in `compileSubsetQuery`, so one change covers them.
  `tests/order-by-ties.test.ts` drives the sub and fetch paths.
- **Nulls-last cursor.** `isNull` and `isUndefined` joined the predicate floor
  (ADR-0013 amendment, same date), so 0.9.2's `gt(v) OR isNull OR isUndefined`
  cursor compiles and the window grows.

## Amendment 2026-11-17: text ordering by locale is an accepted limit

The locale half of the ORDER BY follow-up is not fixed, and we decided not to
fix it. This records the limit, the reasons, and what each client version does.

**The limit.** The server orders text by bytes (SQLite `BINARY`). `@tanstack/db`
compares strings with `localeCompare` unless the query sets `stringSort`, and
`locale` is the default. A bounded read (a `sub` snapshot or a `fetch` page with
`orderBy` and `limit`) over a text column can therefore return a different first
window than the client would pick. With `apple, Banana, cherry, Date, eel, Fig`
and a limit of 3, ascending, the server returns `Banana, Date, Fig` and the
client wants `apple, Banana, cherry`. With `stringSort: "lexical"` the two orders
agree (the client compares code units with `<`), apart from the astral-character
residual described in the row-key tie-break amendment above.

**Why it is not fixed.**

- SQLite in a Durable Object has no locale collation. We tried it in workerd:
  `COLLATE icu_en`, `COLLATE unicode` and `COLLATE en_US` each fail with "no such
  collation sequence", `load_extension` is not authorized, and `SqlStorage` (in
  `@cloudflare/workers-types`) has no call that registers a collation. `NOCASE`
  works, but it is not a locale order.
- The only fix that gives the right rows is to leave the limit off the server
  query when the order may be a locale order over text. The request does not say
  whether the column holds text, and `locale` is the default for every query. A
  rule that drops the bound for every default sort would make the common case of
  ordering by a date string, or by a number, load every row. A rule that looks at
  the column's type needs its own design and ADR.
- It would only help one client version, because the others already correct the
  window themselves (below).

**What each supported version does when `stringSort` is not set.** The
published source of each version was read, and the files are named below.

- `@tanstack/db` 0.8.6. `query/live/collection-subscriber.js` (lines 215 to 228)
  sends the bounded request with `limit: offset + limit` and the order, and
  nothing checks the string order. Later pages use a plain `gt` or `lt` cursor
  (`utils/cursor.js`, lines 3 to 11). The first window is kept, so it can be
  wrong. The independent check of 2026-11-17 measured it with a limit of 3 over
  8 rows: window `Banana, Date, Fig`, wanted
  `apple, Banana, cherry`, 3 of 8 rows loaded.
- `@tanstack/db` 0.9.2. The same bounded request goes out first
  (`query/live/ordered-source-loader.js`, `loadPage`, line 210). When it settles,
  `loadBoundary` (lines 303 to 311) calls `canExpressCursorOrder`
  (`utils/cursor.js`, lines 37 to 46), which is false for a string unless
  `stringSort` is `lexical`, and then calls `loadFullSource`. The final window is
  right and the cost is the whole subset. The same check measured: right window,
  8 of 8 rows loaded. With `lexical`: right window, 3 of 8 rows.
- `@tanstack/db` 1.0.0-rc.1.5 (the version this repo develops against). The same
  steps as 0.9.2: `query/live/ordered-source-loader.js` (lines 371 and 437) and
  `utils/cursor.js` (line 35). `tests/order-by-nulls.test.ts` passes on it with
  the final window right. The number of rows loaded was not measured on it.

Not measured on any version: how long the wrong first window shows on 0.9.2 and
later before the full load replaces it, and text outside ASCII or with a named
`locale`.

**What stays.** The eight `it.fails` locale pins in `tests/order-by-nulls.test.ts`
stay. They now document this limit and will fail the build, as they should, if
the server ever matches the client. The README tells readers to set
`stringSort: "lexical"` on a limited query over a text column.

**Revisit if** the supported floor moves to 0.9 or later (then no supported
version shows a wrong final window and only the cost remains), or if upstream
starts sending a bounded request for a locale order (then nothing corrects the
first window and we would need a fix).

## Amendment 2026-11-17: the adapter sends a request the server can read

**Context.** The server now reads a column ref only in the one shape
`{ type: "ref", path: [name] }` and refuses any other (ADR-0013 amendment
2026-11-17, column references). The library's client sends other shapes:

- every version sends a nested field as a longer path (`orderBy(m.meta.x)` sends
  `["meta", "x"]`), in an `orderBy`, a cursor, and a `where`;
- on 0.9 and later a limited query ordered by a nested field also asks, on its
  first load and again when its window grows, for the rows that tie with the
  boundary, as `and(where, eq(order expression, v))`: a `where` that holds the
  longer path;
- 0.8.6 pushes an `orderBy` and its `limit` through a subquery or a join with the
  outer alias kept (`["sub", "body"]`, `["sub", "title"]`, and `["f", "name"]`
  for a join ordered by clauses on two of its collections).

The server refuses all of them. On the socket a refusal reads as no rows and logs
an error line; in a server render a refused read throws, so `preload()` rejects
and the query ends in `error`. On 0.8.6 a query ordered by `sub.body` with a limit
worked on main and showed nothing after the server change. The server cannot
read these refs correctly in general, so it must keep refusing them.

**Decision.** The adapter sends a request the server can read that contains the
one core asked for. The client filters, sorts and cuts the rows it holds, so a
request that returns more rows gives the same query. The rule below drops whole
top-level conjuncts. It does not look for the tightest such request: for
`or(and(a, x), and(a, y))` with `x` and `y` unreadable it sends no `where`,
though `a` is implied by both branches.

- **`where`.** `readableWhere` drops each top-level conjunct (flattened through
  nested `and`s) that holds a ref the server cannot read, and sends the rest. Any
  other node that holds one (an `or`, a `not`, a comparison that is the whole
  `where`) is dropped whole, as the conjunct it is part of; when it is the root,
  the request has no `where`. Nothing is widened inside an `or` or a `not`: a
  `not` over a wider predicate would be narrower. When nothing is dropped the
  `where` is core's own object. A rebuilt `and` keeps the original node's keys
  in order, so its request key equals core's for the same conjuncts.
- **`orderBy`.** When any term is not a column ref the server reads, the order is
  dropped.
- **Both together.** When either is rewritten, the request is for the whole
  readable `where` subset: no `orderBy`, `limit` or `offset`. A limit is never
  kept over a wider `where`, because the top rows of the wider set can miss rows
  of the narrower one.
- **"Cannot read"** is `columnRefProblem` in `src/wire/column-ref.ts` and nothing
  else, the same function the compiler refuses with. An operator off the floor
  (ADR-0013 D2) is not touched: the server still refuses it, and it still reads
  as no rows.

1. **Where it is decided.** Once, at the top of `loadSubset`, by `sendable`. The
   request key (D1), the cover check and the watch it may open (D2), the covered
   fetch, and the resubscribe after a reconnect all use the sent request. The
   SSR transport serves the same `subscribe` and `fetch` calls, so a server
   render reads the same request. The bookkeeping that core releases by
   (`outstanding`, `acquisitionOf`, D8) stays keyed by the object core passed.
2. **The watch.** A rewritten request that no watch covers opens a watch on the
   sent `where`, with no `orderBy` or `limit`. Its snapshot is the whole readable
   subset, and its deltas cover that superset, so a row that enters or leaves
   the narrower `where` arrives on it; the client's own filter decides. Such a
   watch covers every later request whose conjuncts include its own (D2),
   bounded or not.
3. **Key and cover.** The rewritten request's key is that of the sent request,
   so it shares an acquisition with any request that is sent the same: a 0.9
   full-source load on the same `where`, a second query ordered differently by a
   nested field, and the tie request, which widens to the base `where`. On the
   first load of a nested-field query that shares the acquisition already held
   and sends no frame. A rewritten request that a watch already covers is a
   covered fetch of the whole readable subset and pins that watch.
4. **A cursor request** (load more) is decided in `loadMore` by the same rules.
   Its cursor is built from the same order, so when the request is rewritten the
   fetch is for the readable `where` alone, with no cursor, order or limit,
   written insert-if-absent (ADR-0003) and held by the covering watch. When the
   covering watch was opened with no limit and has its snapshot, it already
   holds every row of its `where`, and so every row the cursor asks for: nothing
   is fetched. This applies to any cursor request under such a watch, rewritten
   or not.
5. **Cost.** A rewritten query loads every row its readable `where` matches, and
   its watch keeps all of them live, however small its `limit`. A `where` on a
   nested field alone loads the whole collection. This is the only way such a
   query can be answered correctly, but it is not visible in the query; the
   README says it next to the other ordering notes and in the limits.

**What the rule cannot see.** On 0.8.6, a subquery whose alias equals the inner
alias and whose `select` renames a field (`q.from({ f: q.from({ f: files })
.select(({ f }) => ({ id: f.id, name: f.id })) }).orderBy(({ f }) => f.name)
.limit(2)`) has its alias stripped and sends `orderBy ["name"]` with the limit.
The path is one element and names a real column, so it is sent, and the server
sorts by the table's `name` while the query orders by the selected `name`. The
first window can then be wrong, with no refusal and no log line. It is the same
on main. 0.9 and later load the whole source for it. Measured by the review on
0.8.6.

**Evidence.** `tests/client-ref-shapes.test.ts` runs a fixed list of live
queries through the real adapter and Durable Object: the three 0.8.6 `orderBy`
shapes, a nested-field order with and without an index, a nested-field `where`
alone and inside an `and`, an `or` and a `not` (with and without an order), and
ordinary queries. Each must end `ready` with the rows and order @tanstack/db
gives over the same data, before and after its window grows, and no ref the
server refuses may reach the wire. Each is also rendered through
`SsrSnapshotTransport` reading `readSyncSnapshot`, and must end `ready` with the
same rows. The refs core asks for are written down per version line, so a later
@tanstack/db that sends another shape fails there first. Further tests pin that
a rewritten request is released by core's object (the collection empties and the
server holds no sub) and shared when it is sent the same (one `sub`, no
`fetch`), and that a cursor request under a whole-subset watch sends nothing; each
was checked by breaking the code it pins. CI runs the locked version and the
floor; 0.9.2 was run by hand. Before this change, on 0.8.6 the three 0.8.6
shapes and the nested-field order ended `ready` with no rows, on 0.9.2 and
1.0.0-rc.1.5 the nested-field order did, and on 1.0.0-rc.1.5 the nested-field
`where` shapes did (the run before this change was made on that version only;
the server refuses such a `where` on every version). On 0.9.2 and 1.0.0-rc.1.5
a server render of a nested-field order ended in `error`.
