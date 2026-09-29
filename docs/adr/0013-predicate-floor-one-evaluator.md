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
  plain column reference.

The floor is now `{ eq, gt, gte, lt, lte, like, in, isNull, isUndefined, and, or,
not }`. `tests/predicate-parity.test.ts` pins both operators, and the composed
cursor shape, across the snapshot and delta paths, per D2.
