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
lists the same numbers. The ones a query can reach:

| Limit | Value | How it fails |
| --- | --- | --- |
| `SQLITE_LIMIT_VARIABLE_NUMBER` | 100 bound parameters | `too many SQL variables` |
| `SQLITE_LIMIT_EXPR_DEPTH` | an expression tree 100 deep | `Expression tree is too large` |
| `SQLITE_LIMIT_SQL_LENGTH` | 100 000 bytes of SQL | `statement too long` |
| `SQLITE_LIMIT_LENGTH` | one string or blob value, and one row | `string or blob too big` |
| `SQLITE_LIMIT_COLUMN` | 100, which also caps ORDER BY terms | `too many terms in ORDER BY clause` |
| `SQLITE_LIMIT_LIKE_PATTERN_LENGTH` | 50 bytes | `LIKE or GLOB pattern too complex`, only when the LIKE is evaluated on a row |

For `SQLITE_LIMIT_LENGTH`, workerd's main branch sets 8 MiB + 34 (`MAX_ROW_LENGTH`
in `sqlite.h`), but the workerd this package is tested on (1.20260721.1) rejects a
string over 2 200 000 bytes. A `json_each` list whose one value is nearly the whole
list already fails at exactly 2 200 000 bytes; a list of that size made of small
values still runs. The other limits in that function cannot be reached: a query
uses no compound select and no function with more than one argument, and at most
100 000 bytes of SQL keeps its program far below `SQLITE_LIMIT_VDBE_OP` (250 000
instructions; argued, not tested). Two more throws a query could reach are not
SQLite limits: workerd cannot bind a bigint, and a `limit` or `offset` of 2^63 or
more binds as a REAL SQLite will not take (`datatype mismatch`).

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
3. **Every limit in the table is refused when the query is compiled, on a safe
   bound.** The checks are one-sided: they may refuse a little early, and on top-
   level conjunctions they accept no statement that SQLite rejects for a limit.
   A conjunction nested inside an `or` can still plan deeper than the depth
   bound counts; the limit-error backstop (ADR-0012 amendment) refuses that
   hand-built shape. `compileSubsetQuery`
   refuses (`UnsupportedPredicateError`, so `reset`, an empty page, or a rejected
   `readSyncSnapshot`) a statement that binds more than 100 values in total (every
   list, comparison literal, `limit` and `offset` counts); whose SQL is over 100 000
   bytes; that binds a string or bytes of 2 200 000 bytes or more; that has more
   than 99 `orderBy` terms (the pk tie-break makes 100); whose `limit` or `offset`
   is 2^63 or more; or that compares with a bigint (not dropped as `in` drops one:
   `gt` and `lt` do compare a bigint with a number in the evaluator). A `like`
   pattern over 50 bytes is refused even where SQLite would never evaluate it. A
   predicate whose stored form (`_sync_subs.where_ir`) is over the value cap is
   refused too, before the sub is added (ADR-0012 amendment).

   Depth cannot be computed exactly, for two reasons. SQLite adds one level to a
   whole `where` that holds a subquery anywhere (here, a `json_each` term), not
   only on the path through it. And a MULTI-INDEX OR plan rebuilds the other
   top-level conjuncts as one left-deep AND and wraps it again, so the depth
   depends on the table's own indexes. So the compiler refuses when a bound
   exceeds 100: the tallest top-level conjunct (conjuncts flattened through nested
   `and`s), plus the number of conjuncts, plus one, plus one more when the `where`
   holds a `json_each` term. Conjunct heights use SQLite's rule (a leaf is 1, any
   other node 1 + its tallest child, `a OR b OR c` parsed left-deep); a guarded
   `in` term is 4. The tree's own height never exceeds tallest + count - 1, and
   the review measured the rebuilt plan at tallest + count. Measured in workerd
   (`tests/sqlite-limits.test.ts`, on tables with and without secondary indexes):
   no accepted statement in 665 280 fuzzed predicates is rejected, and most shapes
   are refused 2 or 3 levels early. The worst case is one tall conjunct beside
   many short ones, where the bound adds what SQLite needs only the larger of: an
   `or` of 30 `eq` terms beside an `or(…)` and k `isNull` terms is accepted up to
   k = 66 and runs in SQLite up to k = 96, 30 levels early. Real predicates are
   nowhere near 100 levels.

   What this refuses that ran before: an `or` of 98 or 99 comparisons (97 still
   runs), an `or` of 95 to 99 one-value `in` terms (94 still runs; the guarded
   term is deeper than the old `col IN (?)`), other shapes within a few levels of
   100, a flat `and` that is tall and wide together (as above), and a LIKE pattern
   over 50 bytes that SQLite would never have evaluated. Everything else it
   refuses used to throw from SQLite.

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
takes each limit across its edge in families of shapes, on tables with and
without secondary indexes, and requires that no accepted statement is rejected
by SQLite, that every family crosses its own edge, and that early refusal stays
within a stated slack; it also runs a deterministic fuzz of 89 376 predicates
(about 13 seconds). `tests/long-in-lists.test.ts` drives every path that runs the
compiled SQL.

## Amendment 2026-11-17: a query that names a column the table does not have is refused

**Context.** The compiler writes a column as a double quoted name. When the table
has no such column, SQLite does not fail: it reads the name as a string literal
and runs the statement. The evaluator, which decides the deltas, reads the
missing column as undefined. Measured on the test table `messages (id, body)`
with raw `sub` and `fetch` frames: `eq`, `gt`, `in`, `like` and `not(isNull)` on
`nope` snapshot every row; `eq(nope, "other")` and `isNull(nope)` snapshot none;
`orderBy nope limit 2` returns two rows in no meaningful order; later inserts
send synthetic deletes; and `fetch` returns the same wrong rows. There was no
refusal and no log line, so a typo in a column name gave an app a wrong list
that looked right. The 2026-09-29 amendment called unknown columns out of
contract. That is replaced here: they are refused.

**What SQLite does.** Measured in workerd 1.20260721.1:

- A double quoted name that matches no column is a string literal in `WHERE`
  and in `ORDER BY` alike, so `"nope" = 'nope'` is true on every row,
  `"nope" IS NULL` is false, and `ORDER BY "nope"` sorts by a constant. The same
  holds on a rowid table, a `WITHOUT ROWID` table and a view.
- A name in backticks, or one qualified with the table (`"messages"."nope"`), is
  never read as a string. Both clauses fail with `no such column`.
- SQLite matches a name in any case (`"BODY"` finds `body`) and resolves
  `rowid` and `_rowid_` on a rowid table. A row from `SELECT *` has
  neither key: its keys are the declared names, and the rowid is not one of
  them. So the evaluator reads `row.BODY` and `row.rowid` as undefined while the
  SQL reads a real value.
- A generated column, virtual or stored, is in `SELECT *` and in
  `pragma_table_xinfo` (with `hidden` 2 or 3), but not in `pragma_table_info`.
- A view and a `WITHOUT ROWID` table cannot be registered (`assertSyncCompatible`
  finds no primary key on a view and no rowid on the other), and a virtual table
  cannot carry the capture triggers. So none of them is a synced table, and none
  changes the answer below.

**Options.**

1. Read the table's columns when a query is compiled, and refuse a name that is
   not one of them.
2. Quote names so SQLite never reads them as strings (backticks, or the table
   name in front), and treat `no such column` as a refusal.
3. Both.

Option 2 is not correct on its own. It still accepts `BODY` and `rowid`, which
SQLite resolves and the evaluator does not see. It only fails when the
statement is prepared, but the catch-up path of a reconnecting `sub` never runs
the compiled SQL, and a sub restored on wake (ADR-0019) has no SQL at all, so
both would still accept the query. It would also have to pick `no such column`
out of other errors by the message text. Option 3 refuses nothing that option 1
does not already refuse. Option 1 is the simplest one that is correct.

**Decision.**

- `compileSubsetQuery` returns, beside the SQL, the names its `where` and
  `orderBy` refer to (`columns`). It stays free of SQLite.
- The mixin refuses a query that names a column not in
  `SELECT name FROM pragma_table_xinfo(?)`. For any table that can be synced,
  that is the list of names `SELECT *` returns. The names are compared exactly,
  as a row's keys are. The refusal is an `UnsupportedPredicateError`, so it takes
  the same path as an operator off the floor: a `sub` gets `reset` and nothing of
  it stays registered, a `fetch` gets the empty page, and `readSyncSnapshot`
  throws. The class does not survive the RPC: a caller of `readSyncSnapshot`
  through a stub gets a plain `Error` whose message starts with
  `UnsupportedPredicateError:`. The log line is the existing one, for example
  `sub 's1' on 'messages' rejected: column 'nope' is not in table 'messages'`,
  clipped like every client string.
- The pk is a column of the table, so it passes. The library adds no column to
  a synced table: its triggers write to `_sync_changes`, and its own tables are
  all `_sync_*`. The `ORDER BY rowid` default and the pk tie break are written by
  the compiler and are not checked. A `rowid` named by the client is refused.
- For a ref as the library's client sends it, the SQL and the evaluator now
  read the same column, so the residual in the 2026-09-29 amendment
  (`isUndefined` is true in the evaluator for a column the table lacks) can no
  longer be reached by such a ref. This says nothing about a ref built by hand
  in a shape the client does not send.
- A table dropped while the object is alive gives an empty list, so a query
  that names a column is now refused with `column 'x' is not in table 't'`,
  where before it threw `no such table`. A query that names no column still
  throws `no such table`.

**Cost.** One read of the pragma for each `sub`, `fetch` and `readSyncSnapshot`
whose query names a column, and none for one that names no column (an eager
collection's `sub`). A cursor `fetch` compiles two statements and reads the list
once for both. A restore reads it once per table. Each read counts as rows read,
one per column of the table (measured: `rowsRead` 2 and `rowsWritten` 0 on
`messages (id, body)`), so each such `sub`, `fetch` and snapshot read adds that
many rows read. Whether Cloudflare bills rows read from a pragma like rows read
from a table was not verified. The read is a synchronous call into SQLite's in
memory schema; it was not timed, because workerd does not advance its clock
inside a request.
Nothing is kept between queries, so a column added while the object is alive
(by `ALTER TABLE` in a migration, with or without a later `registerSync`) is
accepted by the next query. There is no timer and nothing runs while the object
is idle.

**Restore (ADR-0019).** A persisted sub carries only its `where` (its `orderBy`
shaped a snapshot that was already sent). On restore, that `where` now runs
through the whole SQL compiler, not only the evaluator, and its columns are
checked, reading each table's columns once per restore. Any compile refusal
closes the socket on wake. Within one build only a missing column can do that:
the statement compiled on restore is the sub's own, with the same `where` and no
`orderBy`, `limit` or `offset`, so it has no more parameters, bytes or depth
than the statement accepted at sub time. Any other refusal can only come from a
row written by an older build, which is what the 1011 close was written for.

If a migration dropped a column a persisted `where` names, the sub is handled as
one whose predicate no longer compiles: the socket closes with 1011 and its
persisted subs are deleted. What the client then goes through, checked by
reading `src/client/transport.ts` and `src/client/do-collection.ts` and not by
a run:

- The transport reconnects and sends every sub again. The sub on the dropped
  column had bootstrapped, so it goes with `since`. The server refuses it with
  `reset`, and persists nothing of it.
- In on-demand mode, a `reset` on a bootstrapped watch reads as "below the
  retention floor". The collection truncates (`truncateAll`), and the truncate
  replay loads every subset of that collection on that client again from
  fresh snapshots. The replay sends a fresh sub for the bad query, which is
  refused as a watch that never bootstrapped: its load settles and nothing else
  is touched, so it does not loop.
- In eager mode, the collection's one sub truncates on that `reset` and the
  collection stays empty, as for any refused eager sub.
- Subs of other collections on the same socket catch up from `since` as usual.
- A refused sub is not persisted, so the next wake has nothing to close.

So the cost is one reconnect and one full reload of that collection for each
client that held such a sub. Restore runs inside every `registerSync`, so a
column dropped while the object is awake is caught the same way when
`registerSync` runs after it. One dropped with no `registerSync` after it is not
caught: until the next wake or reconnect, live subs on it read the column as
undefined.

**Not changed.** A refused query still reads as no results on the client, as
every refusal does today. The kinds of values a column is compared with are not
checked.

**Evidence.** `tests/unknown-columns.test.ts` drives every shape above through a
`sub` (including the catch-up path), a `fetch` (including a cursor) and
`readSyncSnapshot` on a real Durable Object (one of them through the RPC stub),
pins each half of a cursor `fetch` alone, and checks that nothing of a
refused sub is registered or persisted, that the log line names the column and
is clipped, that the pk and a column added after the first query (plain or
generated) are accepted, and the restore after a column is dropped.

## Amendment 2026-11-17: a column reference must have the one shape the server reads

**Context.** The rule behind the previous amendment is that the SQL that builds
a snapshot and the evaluator that decides the deltas must read the same column.
The review of that change found two more ways a frame could break it, both in
how the compiler read a ref. Both were measured in workerd 1.20260721.1 with hand
built frames.

1. In `where` the compiler read `path[0]` and ignored every other field of the
   ref. The evaluator reads the whole ref. On 1.0.0-rc.1.5,
   `compileSingleRowRef` (`query/compiler/evaluators.js`) reads the path through
   `getPropRefPropertyPath` (`query/ir.js`), which drops the first element when it
   equals `sourceAlias`. So `{ type: "ref", path: ["body"], sourceAlias: "body" }`
   was `body` in SQL and the whole row in the evaluator: the snapshot sent the
   matching row, and a later insert of a matching row sent a synthetic delete.
   0.8.6 and 0.9.2 have no `sourceAlias` and walk `path` whole.
2. In `orderBy` the compiler read the last element of the path, and in `where` it
   required exactly one. An `orderBy` on `["nope", "body"]` sorted by `body`,
   while the client's comparator reads `row.nope.body`, which is undefined, so it
   orders by key. The last element rule came in with commit b7f4e2b, which gives
   no reason for it.

**What the client sends.** Measured on 0.8.6, 0.9.2 and 1.0.0-rc.1.5 through the
real adapter (`tests/client-ref-shapes.test.ts` keeps the list and what each
version sends). After the frame codec no version sent `sourceAlias` or any field
on a ref other than `type` and `path`. Most paths have one element. These do not:

- every version sends a nested field as a longer path (`orderBy(m.meta.x)` sends
  `["meta", "x"]`), in `orderBy`, in a cursor, and in a `where`; on 0.9 and later
  a limited query ordered by one also sends it in the `where` of its tie
  request, on its first load;
- 0.8.6 pushes an `orderBy` and its `limit` through a subquery or a join to the
  source collection, keeping the outer alias: a subquery `sub` with
  `select({ body: m.body })` ordered by `sub.body` sends `["sub", "body"]`; with
  `select({ title: m.body })` it sends `["sub", "title"]`; a join ordered by
  `m.body` then `f.name` (clauses on two of its collections) sends `["body"]`
  and `["f", "name"]` to `messages`
  (0.8.6 `normalizeExpressionPaths` strips only the subscriber's own alias, and
  `processOrderBy` pushes the order to the source). 0.9.2 and 1.0.0-rc.1.5 load
  the whole source with no `orderBy` for these three.

For `["sub", "body"]` sorting by the last element is right, and on main that
query worked. For `["sub", "title"]`, `["f", "name"]` and `["meta", "x"]` it is
wrong. The server cannot tell them apart.

**Decision.** A ref is read only when it is exactly `{ type: "ref", path: [name] }`:
no field other than `type` and `path`, a `path` that is an array of one element,
and that element a string that passes the identifier check. The rule is
`columnRefProblem` in `src/wire/column-ref.ts`. `column()` in `sql-compiler.ts`
refuses what it rejects, and the expression of an `orderBy` term now goes
through `column()` too, so `where`, `orderBy` and both halves of a cursor follow
one rule. Anything else is an `UnsupportedPredicateError`, so a `sub` gets
`reset`, a `fetch` gets the empty page, and `readSyncSnapshot` throws. The message
and the log line name the cause, for example
`a column reference carries only 'type' and 'path'; got 'sourceAlias'`.

The on-demand adapter uses the same function to send a request the server can
read that contains the one core asked for: it drops an `orderBy` the
server would refuse, drops the conjuncts of a `where` that hold such a ref, and
then asks for the whole readable `where` subset with no `limit` (ADR-0023
amendment 2026-11-17). The client filters, sorts and cuts the rows it holds. So
the client queries above, and a `where` on a nested field, never reach the
server in a shape it refuses, and give the right rows on the socket and in a
server render, at the cost of loading every row the readable `where` matches.
This holds for on-demand queries only. An eager collection sends its own static
`where` as given, because it has no query that could filter a wider set, so an
eager collection's `where` on a nested field is still refused.
A frame built by hand that carries such a ref is still refused.

The shape is strict, not only "no `sourceAlias`", because a field the compiler
does not read is a field the evaluator may read, in this release of
@tanstack/db or a later one, and the server cannot tell which. A path that is not
an array shows the same thing: the compiler read `path.length` and `path[0]`,
while the evaluator iterates `path`, which throws for a plain object on 0.8.6
and 0.9.2. A `val` node and a `func` node are not made strict about extra
fields: the evaluator reads only `value`, and `name` and `args`, on all three
versions. But a `func` must now have a string `name` and an array `args` (the
evaluator calls `args.map`, so a map with `length` compiled here and threw
there), and its name is looked up in the operator table by own key only.

**What the rule refuses beyond the two examples.**

- A ref with any other field: `sourceAlias` with any value, `bindingId`, or any
  other key. On 1.0 `bindingId` is not enumerable, so the client never encodes it.
- A path that is not an array, or whose one element is not a string. Before, an
  element such as `["body"]` passed the identifier check by string conversion and
  was refused only by the column check, with a misleading message.
- An `orderBy` on a path of more than one element: the nested field and the
  0.8.6 paths above, when a frame carries them. The adapter no longer sends
  them.
- An `orderBy` expression that is not a ref (a function) is still refused, now
  with `expected a column reference` instead of `unsupported orderBy clause`.

The simple `{ col, dir }` orderBy shape is unchanged; the client never sends it,
and tests use it.

**Malformed frames.** Six more frames the compiler did not refuse are now an
`UnsupportedPredicateError`:

- three threw from the compiler, so a `sub` or a `fetch` got no reply and stayed
  loading: an `orderBy` term of `null`, a `func` with no `args`, and an `orderBy`
  term that holds a bigint and no usable `expression` or `col` (`JSON.stringify`
  in the error message threw; MessagePack decodes a 64 bit integer as a bigint);
- a `func` named after an `Object.prototype` member: `name in` the operator table
  followed the prototype chain and wrote broken SQL, so a `fetch` got no reply
  and a `sub` got `reset` from the evaluator guard;
- a `func` whose `args` is not an array: it compiled, and the evaluator calls
  `args.map` and throws, so a `fetch` returned rows and a `sub` got `reset`;
- an `orderBy` that is not an array: it was ignored without a word, and with a
  `limit` returned the first rows by `rowid`.

The library's client sends none of them.

**Restore (ADR-0019).** A persisted `where` holding a ref of another shape now
fails to compile on restore, so its socket closes with 1011, as for any refusal
of a row written by an older build. An older build could only have persisted
such a `where` from a hand built frame.

**Evidence.** `tests/column-refs.test.ts` drives each shape above, and the six
malformed frames, through a `sub` (checking that nothing is persisted), a
`fetch` and `readSyncSnapshot` on a real Durable Object, checks that a cursor
whose `whereCurrent` alone carries an aliased ref is refused, and checks that
the client's shape still works in a `where`, an `orderBy` and a cursor.
`tests/client-ref-shapes.test.ts` runs a fixed list of query shapes through the
real adapter, on the socket and in a server render, records the refs core asks
for per version line, checks that no refused ref reaches the wire or a read, and pins that `columnRefProblem` is what the
compiler refuses in an `orderBy`. The tables that cannot be synced correctly,
which the same review found, are refused at registration (ADR-0007 amendment
2026-11-17).
