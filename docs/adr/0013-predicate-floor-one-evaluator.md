# ADR-0013: Filtered-subscription membership — one evaluator is the source of truth; the floor is the verified-agreeing set

**Status**: Accepted
**Date**: 2026-06-13
**Characterization**: `tests/predicate-parity.test.ts` (originally plan 003)

## Context

A filtered subscription's membership is decided by **two different evaluators**
depending on the path:

- The **initial snapshot** filters in SQLite: the `where` IR is lowered to SQL
  by `src/server/sql-compiler.ts` (`"col" = ?`, `"col" LIKE ?`, …).
- **Live deltas and reconnect catch-up** filter in JavaScript: the same IR is
  compiled by `@tanstack/db`'s `compileSingleRowExpression` + `toBooleanPredicate`
  in `src/server/subscriptions.ts` (`sync-do.ts` delta/catch-up paths). The client's
  eager-write preflight (`do-collection.ts`) uses that same JS evaluator, and so do
  the client's own live queries.

If the two disagree on any row, a client's view of a filtered subset depends on
*when it connected* — a row excluded by the snapshot can be pushed in by a later
delta, and two clients silently diverge. Characterization found two real divergences:

1. **`ne` — crash + hang.** `sql-compiler.ts` accepted `ne` (lowered to `!=`), but
   `@tanstack/db`'s evaluator has no `ne` in its registry (not-equal is expressed as
   `not(eq(...))`). `compileSingleRowExpression` threw a `QueryCompilationError` from
   inside `subs.add`, which runs in `handleSub` **after** the `try/catch` that wraps
   `compileSubsetQuery`. The throw escaped uncaught: no `reset` was sent and the
   subscriber hung until timeout.
2. **`like` — case divergence.** SQLite `LIKE` is ASCII case-*insensitive* by default;
   `@tanstack/db`'s `like` is case-*sensitive* (its case-insensitive variant is
   `ilike`, which is off-floor). A row `"HELLO"` matched `like "hello%"` in the SQL
   snapshot but not in the JS deltas.

## Decisions

### D1: `@tanstack/db`'s evaluator is the source of truth for membership

It already decides membership on three of the four sites (delta, catch-up, client
preflight), and the client's live queries use the same library. The SQL snapshot is
an *optimization* that must reproduce exactly the rows that evaluator accepts. So when
the two disagree, **SQL conforms to `@tanstack/db`** — never the reverse.

### D2: The operator floor is the verified-agreeing set

Floor = `{ eq, gt, gte, lt, lte, like, in, and, or, not }`. **`ne` is removed** — it
is not in `@tanstack/db`'s evaluator, and a real client emits `not(eq(...))` (which
both paths handle identically). Anything outside the floor is rejected with
`UnsupportedPredicateError` → `reset`, as before. `tests/predicate-parity.test.ts`
pins row-for-row agreement for each floor operator across both paths; **any operator
added to `COMPARATORS` must add a parity case** (enforced by review).

### D3: `LIKE` is made case-sensitive to match

The DO sets `PRAGMA case_sensitive_like = ON` in the `SyncDurableObject` constructor.
The pragma is connection-scoped, and the constructor runs on every instantiation —
including a hibernation wake — so it is always in force before any query (the same
lifecycle the existing `setWebSocketAutoResponse` registration relies on). It is
contained: the IR→SQL compiler is the only producer of `LIKE` in the codebase.
(`case_sensitive_like` is a *setter-only* pragma — there is no getter — so it is
verified behaviorally in tests, not by readback.) Case-*insensitive* matching is
`ilike`, which stays off-floor (see deferred).

The `like` **pattern must be a string literal**: `@tanstack/db`'s `evaluateLike`
returns `false` unless both operands are strings, whereas SQLite `LIKE` would
coerce a non-string pattern (`123` → `'123'`) and match rows the JS path rejects.
The compiler rejects a non-string `like` pattern with `UnsupportedPredicateError`
(→ `reset`), keeping the floor *verified*-agreeing rather than merely usually so.

### D4: A defensive fail-loud guard at predicate compile

`compilePredicate` (`subscriptions.ts`) now wraps the `@tanstack/db` compile and
re-throws any failure as `UnsupportedPredicateError`; `handleSub` catches it around
`subs.add` and answers with `reset`. With the floors aligned this is belt-and-
suspenders, but it guarantees fail-loud (a `reset`, never a hang) for any future
operator that lands in one floor and not the other.

## Consequences

- **Behavior change (observable):** a `ne` subscription is now rejected with `reset`
  (was: an indefinite hang). `like` on a filtered subscription is now case-sensitive
  on every path (was: case-insensitive in the snapshot only, and divergent from
  deltas) — `"HELLO"` no longer matches `like "hello%"`. This aligns snapshot, delta,
  catch-up, and client-side semantics. Real `@tanstack/db` clients are unaffected by
  the `ne` removal — they emit `not(eq(...))`, which is fully supported.
- **No idle timers / hibernation impact:** the pragma is set synchronously in the
  constructor; no polling introduced.
- **Deferred:** `ilike` (case-insensitive `LIKE`) remains off-floor. Adding it would
  lower to `lower("col") LIKE lower(?)` (or equivalent) plus a parity case — a
  separate decision.
- **Test coverage:** `tests/predicate-parity.test.ts` (6 cases, both paths) pins
  floor-operator agreement; `tests/subscriptions.test.ts` pins the JS-floor guard at
  the unit level; `tests/sql-compiler.test.ts` pins `ne` rejection and `not(eq)`
  lowering.

## Amendment 2026-09-29: `isNull` and `isUndefined` join the floor (bugbash F4-cursor)

On @tanstack/db 0.9, an indexed window ordered `nulls: "last"` scrolls with a
cursor whose `whereFrom` is `gt(v) OR isNull(col) OR isUndefined(col)` (`lt` for
descending). Both null checks were off-floor, so the cursor fetch was refused as
an empty page and the window never grew.

D2's rule still holds: each addition is a verified-agreeing operator. Checked
against `evaluators.ts` in both 0.8.6 and 0.9.2 (identical):

- `isNull(x)` is `x === null`. SQL: `"col" IS NULL`. The delta and catch-up paths
  evaluate rows hydrated from SQLite, where a NULL column is always `null`
  (whether the write omitted it or sent `null`), so the two agree.
- `isUndefined(x)` is `x === undefined`. A stored column is never undefined (SQLite
  has no such value; hydration yields `null`), so it is false for every stored row.
  SQL: the constant `0`, like the empty `in`. The operand is still validated as a
  plain column reference. Residual: `=== undefined` is TRUE for a property the row
  lacks, i.e. a column the table does not have; there SQL says false. Unknown
  columns are out of contract (the schema is author-owned and shared with the
  client, and SQLite reads an unresolvable quoted name as a string literal, so
  no operator fails loud on one).

The floor is now `{ eq, gt, gte, lt, lte, like, in, isNull, isUndefined, and, or,
not }`. `tests/predicate-parity.test.ts` pins both operators, and the composed
cursor shape, across the snapshot and delta paths, per D2.

## Amendment 2026-11-17: long `in` lists, and `in` compares by kind (bug bash)

**Context.** The compiler bound one SQL parameter per `in` value. SQLite in a
Durable Object accepts at most 100 bound parameters per statement, so an `in`
over 101 values threw from SQLite: the `sub` got no reply and its live query
stayed loading, and a `fetch` timed out. An app reaches this without writing
`inArray`: @tanstack/db's join loader sends one `inArray` over every join key
(`query/live/subset-demand-controller.js`: `requestSegment` in 0.8.6 and 0.9.2,
`createSegment` in 1.0.0-rc.1.5). Every path that runs the compiled SQL was
affected: the snapshot, both halves of a cursor fetch, the reset-and-resnapshot
below the retention floor, and `readSyncSnapshot`. The windowed catch-up and the
wake restore use the JS evaluator only, so they were not.

**Where 100 comes from.** workerd's `SqliteDatabase::setupSecurity`
(`src/workerd/util/sqlite.c++`) calls
`sqlite3_limit(db, SQLITE_LIMIT_VARIABLE_NUMBER, 100)`, and Cloudflare's Durable
Objects limits page lists "Maximum bound parameters per query: 100". In workerd,
100 parameters run and 101 fail with `too many SQL variables`
(`tests/long-in-lists.test.ts`, "the platform limit"). The same function sets
`SQLITE_LIMIT_SQL_LENGTH` to 100 000 bytes, and the expression tree depth is
capped at 100 (`Expression tree is too large (maximum depth 100)`, observed).

**What was measured in workerd** (vitest-pool-workers 0.18.7):

- Inlined literals hit the statement-length cap: 3 000 UUIDs make a 117 KB
  statement and fail with `SQLITE_TOOBIG`. Rejected.
- Running the list in chunks across statements breaks `ORDER BY`/`LIMIT`/
  `OFFSET` and any composition (`or`, `not`, two lists). Rejected.
- `x IN (SELECT value FROM json_each(?))` takes one parameter per list and the
  plan still searches the pk index (`SEARCH … USING INDEX sqlite_autoindex_…
  (id=?)`). `json_each` is on workerd's SQL function allowlist. Strings come back
  exact, including NUL, astral characters and JSON-looking text. Safe integers
  come back exact. Doubles do not always: 3 196 of 25 008 random bit patterns
  parsed to a neighbouring double, all of them at extreme magnitudes (none
  between 1e-20 and 1e20).
- The bound form already disagreed with the evaluator. workerd binds every JS
  number as REAL and booleans and Dates as text, and SQLite converts by column
  affinity, while @tanstack/db's `in` compares by kind. So `in(text_col, [1])`
  matched the text "1.0", `in(int_col, ["5"])` matched 5, a `null` in a list made
  `not(in(…))` drop every row, and `not(in(col, []))` kept a NULL column that the
  evaluator leaves out (`in` is unknown there, and so is its `not`).

**Decisions.**

1. **`in` compiles by kind, with one parameter per kind instead of per value.**
   Strings go in one JSON array read with `json_each`, guarded by
   `typeof(col) IN ('text', 'null')`. Safe integers go in one JSON array, and
   other numbers bind one each (JSON text cannot carry every double exactly);
   both are guarded by `typeof(col) IN ('integer', 'real', 'null')`. The guard
   makes a value match only stored values of its own kind, as the evaluator
   does, and allowing `'null'` keeps a NULL column unknown, so `not` leaves it
   out as the evaluator does. `null`, `undefined` and `NaN` never match in the
   evaluator (0.8.6 to 1.0.0-rc.1.5) and are dropped. When nothing is left, the
   result is `CASE WHEN col IS NULL THEN NULL ELSE 0 END`: false, or unknown for
   a NULL column.
2. **Other kinds in an `in` list are refused** (`UnsupportedPredicateError`, so
   `reset` or an empty page): boolean, bigint, Date, bytes, objects and arrays.
   SQLite has no boolean, bigint or date, and a server row never holds a
   `Uint8Array` (BLOBs hydrate as `ArrayBuffer`), so no SQL form agrees with the
   evaluator on them. Before, they bound as text (`'true'`, a Date's
   `toString()`) or a blob and disagreed, or threw (bigint) and hung. This is D3's
   rule for `like` patterns applied to `in`. A Date could later be admitted by
   sending its `getTime()`, which is what the evaluator compares; that is a
   separate decision.
3. **The parameter rule covers the whole statement.** `compileSubsetQuery`
   refuses a statement that would bind more than 100 values in total, counting
   every list, comparison literal, `limit` and `offset`. What is still refused:
   an `or` of more than about 98 comparisons, or more than about 98 distinct
   non-integer numbers across lists. The other SQLite caps (expression depth
   100, statement length 100 KB) are not checked at compile time; a statement
   over them throws from SQLite and gets the refusal reply (ADR-0012 amendment
   of the same date).

**Not changed, recorded here.** The comparators (`eq`, `gt`, …) still bind one
value with SQLite's affinity conversion, so `eq(text_col, 1)` still matches
"1.0". Other divergences that apply to both SQL forms and to `eq` alike: a column
declared `COLLATE NOCASE` compares case-insensitively; a lone surrogate is
stored as U+FFFD, which the evaluator does not equate with the surrogate; an
integer beyond 2^53 hydrates rounded. On the delta path, a BLOB never matches
bytes, because the server hydrates it as `ArrayBuffer`.

**Evidence.** `tests/in-list-parity.test.ts` pins SQL and evaluator agreement for
every admitted kind against columns of each affinity, with and without `not`,
at 99, 100, 101 and 3 000 values, inside larger predicates, and through the DO's
snapshot and delta paths. It also pins the refusals. It passes on @tanstack/db
0.8.6, 0.9.2 and 1.0.0-rc.1.5. `tests/long-in-lists.test.ts` drives every path
that runs the compiled SQL.
