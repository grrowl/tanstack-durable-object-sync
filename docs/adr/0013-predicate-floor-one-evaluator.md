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

## Amendment 2026-11-17: long `in` lists, `in` compares by kind, and SQLite's limits are refused at compile time (bug bash)

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

**SQLite's limits in a Durable Object.** workerd's `SqliteDatabase::setupSecurity`
(`src/workerd/util/sqlite.c++`) sets them; Cloudflare's Durable Objects limits page
lists the same numbers. The ones a compiled query can reach:

| Limit | Value | How it fails |
| --- | --- | --- |
| `SQLITE_LIMIT_VARIABLE_NUMBER` | 100 bound parameters | `too many SQL variables` |
| `SQLITE_LIMIT_EXPR_DEPTH` | an expression tree 100 deep | `Expression tree is too large` |
| `SQLITE_LIMIT_SQL_LENGTH` | 100 000 bytes of SQL | `statement too long` |
| `SQLITE_LIMIT_LENGTH` | one string value | `string or blob too big` |
| `SQLITE_LIMIT_LIKE_PATTERN_LENGTH` | 50 bytes | `LIKE or GLOB pattern too complex`, only when the LIKE is evaluated on a row |

For `SQLITE_LIMIT_LENGTH`, workerd's main branch sets 8 MiB + 34 (`MAX_ROW_LENGTH`
in `sqlite.h`), but the workerd this package is tested on (1.20260721.1) rejects a
plain string over 2 200 000 bytes and a `json_each` list of exactly 2 200 000
bytes. The other limits in that function cannot be reached by a compiled query: it
uses no compound select, no function with more than one argument, and no more
than 100 columns. A query is at most 100 000 bytes of SQL, so its program stays far
below `SQLITE_LIMIT_VDBE_OP` (250 000 instructions).

**What was measured in workerd** (vitest-pool-workers 0.18.7):

- Inlined literals hit the statement-length cap: 3 000 UUIDs make a 117 KB
  statement and fail with `SQLITE_TOOBIG`. Rejected.
- Running the list in chunks across statements breaks `ORDER BY`/`LIMIT`/
  `OFFSET` and any composition (`or`, `not`, two lists). Rejected.
- `x IN (SELECT value FROM json_each(?))` takes one parameter per list and the
  plan still searches the pk index. `json_each` is on workerd's SQL function
  allowlist. Strings come back exact, including NUL, astral characters and
  JSON-looking text. Safe integers come back exact. Doubles do not always: 3 196
  of 25 008 random bit patterns parsed to a neighbouring double, all of them at
  extreme magnitudes (none between 1e-20 and 1e20).
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
   both are guarded by `typeof(col) IN ('integer', 'real', 'null')`. Each kind is
   its own `OR` term, which keeps the pk index for every mix of kinds (SQLite's
   `MULTI-INDEX OR`; one term mixing kinds made it scan). The guard makes a value
   match only stored values of its own kind, as the evaluator does, and allowing
   `'null'` keeps a NULL column unknown, so `not` leaves it out as the evaluator
   does. When no value is left, the result is
   `CASE WHEN col IS NULL THEN NULL ELSE 0 END`: false, or unknown for a NULL
   column.
2. **A value that can equal nothing a stored row holds is dropped; a Date matches
   by its time value.** The evaluator compares a Date by `getTime()`
   (`normalizeValue`, the same in 0.8.6, 0.9.2 and 1.0.0-rc.1.5), so a Date
   travels as that integer and matches a numeric row with that value. Dropped:
   `null` and `undefined` (never members); `NaN` and an invalid Date (the
   evaluator does match `NaN` to `NaN`, but SQLite stores `NaN` as NULL, so no row
   holds it); booleans, bigints, objects and arrays (a row from SQLite holds none
   of them); and `Uint8Array` (BLOBs hydrate as `ArrayBuffer`, which the
   evaluator never equates with a `Uint8Array`). Dropping agrees with the
   evaluator and refuses nothing, which matters while a refusal still reads as an
   empty result on the client. Before this change these values bound as text
   (`'true'`, a Date's `toString()`) or a blob and disagreed, or threw (bigint).
3. **Every limit in the table is refused when the query is compiled.**
   `compileSubsetQuery` refuses (`UnsupportedPredicateError`, so `reset`, an
   empty page, or a rejected `readSyncSnapshot`) a statement that binds more than
   100 values in total (every list, comparison literal, `limit` and `offset`
   counts), whose `where` is more than 100 levels deep, whose SQL is over 100 000
   bytes, or that binds a string of 2 200 000 bytes or more; and a `like` pattern
   over 50 bytes. The depth is computed with SQLite's own rule (a leaf is 1, any
   other node 1 + its tallest child, `a OR b OR c` parsed left-deep), except for
   the `in` terms, whose heights were measured: a `json_each` term is 5, one level
   more than the rule gives, and a bound-list term is 4. The value edge is one
   byte early for a plain string, because a `json_each` list of exactly
   2 200 000 bytes already fails. A LIKE pattern over 50 bytes is refused even
   where SQLite would never evaluate it.

   What this refuses that ran before: an `or` of 100 or more comparisons (depth,
   or the parameter count past 100), and an `or` of 97 to 99 one-value `in` terms
   (each guarded `in` term is two levels deeper than the old `col IN (?)`; 96 still
   runs). Everything else it refuses used to throw from SQLite.

**Not changed, recorded here.** The comparators (`eq`, `gt`, …) still bind one
value with SQLite's affinity conversion, so `eq(text_col, 1)` still matches "1.0",
and `in` and `eq` now disagree on mixed kinds. Other divergences that apply to
`in` and `eq` alike: a column declared `COLLATE NOCASE` compares
case-insensitively; a lone surrogate is stored as U+FFFD, which the evaluator does
not equate with the surrogate; an integer beyond 2^53 hydrates rounded (and a
bound string such as `"9007199254740993"` stores that exact integer in an INTEGER
column). On the delta path, a BLOB never matches bytes. On 0.8.6 only, the
evaluator turns a `Uint8Array` into the string `__u8__1,2,…`, which would equal a
text row holding exactly that string; dropping the bytes misses that row.

**Evidence.** `tests/in-list-parity.test.ts` pins SQL and evaluator agreement for
every kind of list value against columns of each affinity, with and without
`not`, at 99, 100, 101 and 3 000 values, inside larger predicates, and through the
DO's snapshot and delta paths, and that every mix of kinds keeps the pk index. It
passes on @tanstack/db 0.8.6 and 1.0.0-rc.1.5. `tests/sqlite-limits.test.ts`
compiles queries on both sides of each limit, across shapes, and requires the
compiler to accept a query exactly when SQLite runs it.
`tests/long-in-lists.test.ts` drives every path that runs the compiled SQL.
