# 0007 — Author-owned schema; `registerSync` wires the sync

**Status:** Accepted. Supersedes ADR-0001's collection `ddl` + lazy schema init,
moves the [ADR-0001](./0001-sync-architecture.md) D9 enforcement point, and
retires [ADR-0006](./0006-server-originated-writes.md)'s init caveat. Hard
breaking change to the DO authoring API (pre-1.0).
[ADR-0015](./0015-syncable-mixin.md) leans on this decision: because
`registerSync` is author-driven (called in the constructor's
`blockConcurrencyWhile` after the tables exist), the mixin needs no
base-constructor magic and composes cleanly with a host base's own constructor —
the author creates tables, then calls `this.sync.registerSync`, in that order.

## Context

A collection definition carried a `ddl` string. The framework parsed it
(`assertValidCollection` — `splitColumns`, column-walking) to enforce D9 (the
pk is a client-supplied `TEXT` key, no `AUTOINCREMENT`), ran it to `CREATE TABLE`,
and installed CDC triggers — all lazily, on the first `fetch`, guarded by a
`schemaReady` flag. `initRegistry()` did this.

Two problems surfaced:

1. **Owning the schema is a liability.** Subclasses want to control their own
   schema — evolve it with versioned migrations, use Drizzle, `ALTER TABLE` over
   time. A single `ddl` string the framework parses and runs can't express that,
   and the parser is fragile.
2. **Lazy init is a footgun.** It runs on the WebSocket upgrade, so a
   server-originated write to a never-connected DO (an agent, a cron job) hits a
   DO with no tables/triggers — the caveat ADR-0006 had to document. Moving init
   to a base-class constructor runs into JS field-ordering: the base constructor
   can't read the subclass's `registry` field (`undefined` during `super()`).

## Decision

Invert control. The framework does **not** create tables and does **not**
orchestrate a lifecycle. The author owns migration entirely and **pushes** the
registry into the framework by calling one method:

```ts
protected registerSync(registry: Registry<TUser, Env>): void
//  initSchema → for each collection: assertSyncCompatible (PRAGMA) + installTriggers → store
```

`registerSync` validates each collection's table is sync-compatible (one
`PRAGMA table_info`: exactly one `pk` column, it is the declared `pk`, type
`TEXT`) and installs CDC triggers. It is idempotent. The author calls it **after**
their tables exist — typically in their own constructor's `blockConcurrencyWhile`,
after migrating:

```ts
class BoardDO extends SyncDurableObject<Claims, Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      await this.runMigrations()              // any tool: raw DDL, Drizzle, Actors' SQLSchemaMigrations
      this.registerSync(
        new Registry<Claims>()
          .defineCollection({ table: "tasks", pk: "id" })   // no `ddl`
          .defineMutation({ ... }),
      )
    })
  }
}
```

Collection definitions are now `{ table, pk }` — `ddl` is removed.

## Why

- **The author pushes; the base reads nothing off the subclass.** This dissolves
  the field-ordering problem entirely — no abstract member, no factory method, no
  `await null`, no framework constructor magic. `registerSync` runs in the
  author's own code, where their tables and fields already exist.
- **Standard Cloudflare ergonomics, kept.** The author writes a normal
  constructor with `blockConcurrencyWhile`. We follow Cloudflare Actors'
  lesson — hand the author a tool (`runAll`/`registerSync`) and let them call it
  at the right moment — rather than fighting JS construction order.
- **Composability.** Migration is a separate concern from sync registration, so
  *any* migration strategy (raw `CREATE TABLE IF NOT EXISTS`, a versioned Drizzle
  migrator, Actors' `SQLSchemaMigrations`) composes with `registerSync`. We don't
  reinvent migration versioning.
- **`registerSync` over `migrate(registry)`.** Migration creates the author's
  tables and doesn't need the registry; conflating the two muddles two jobs.
- **D9 still enforced, just relocated.** The pk-is-`TEXT`-client-key invariant is
  load-bearing for optimistic id parity — drop it and an `INTEGER PRIMARY KEY`
  table yields silent un-retiring optimistic phantoms. It moves from `ddl`-parsing
  at `defineCollection` to one `PRAGMA` check in `registerSync`, which works
  uniformly however the table was created. Identifier + `_sync_`-prefix checks
  (injection/collision safety on the interpolated `{table, pk}` strings) stay at
  `defineCollection`.
- **`assertSyncCompatible` / `installTriggers` are `#private`** — framework
  internals, never author-facing nouns.

## Consequences

- **`initRegistry()` lazy-on-`fetch` is removed.** Schema + triggers exist before
  the first event because the author's `blockConcurrencyWhile` completes first.
- **ADR-0006's "caller ensures init" caveat is retired** — `runSyncedWrite` to a
  constructed DO Just Works; triggers are guaranteed installed.
- **The `ddl` parser is deleted** — `assertValidCollection` shrinks to the
  identifier/prefix string checks; the column-walking goes.
- **Fail-loud, author's responsibility.** Forget `registerSync` → `this.registry`
  throws on the first sync op. Call it before the tables exist → the `PRAGMA`
  check throws ("table missing / pk not a TEXT key — create it before
  registerSync"). Both are loud and actionable.
- **Cost:** a hard break for every DO subclass — `protected registry = …(ddl)`
  field → a constructor that migrates then `registerSync(registry)`. All three
  examples and the test-worker move to the new shape.

## Amendment 2026-11-17: a generated `rowid` column and a `__proto__` column are refused

**Context.** The review of the unknown columns change (ADR-0013 amendment
2026-11-17) found two kinds of table that passed `assertSyncCompatible` but
cannot be synced correctly. Both were measured in workerd 1.20260721.1.

1. **A generated column named `rowid`.** `assertSyncCompatible` read the columns
   with `pragma_table_info`, which does not list generated columns, so its check
   for a column named `rowid` (ADR-0015) did not see one. Its probe
   `SELECT rowid` succeeds, because `rowid` then names the generated column. So
   `CREATE TABLE p4 (id TEXT PRIMARY KEY, body TEXT, rowid TEXT GENERATED ALWAYS
   AS (upper(body)) VIRTUAL)` registered, and the default `ORDER BY rowid` sorted
   by the generated value instead of by insertion order. A stored generated
   column does the same.
2. **A column named `__proto__`.** The name passes the identifier check. workerd
   builds each row of `SELECT *` by setting each column on a plain object, so this
   name calls the `__proto__` setter: the row has no own `__proto__` key, a text
   value is dropped, and a NULL sets the row's prototype to null. No snapshot and
   no delta carries the column, and a `where` on it matched rows in SQL while the
   evaluator, which reads `row.__proto__`, read the prototype and matched none.

**Decision.**

- `assertSyncCompatible` reads `pragma_table_xinfo` instead of
  `pragma_table_info`. It lists the same columns and the generated ones, which
  `SELECT *` returns. A generated column cannot be a primary key, so the pk checks
  give the same answers. The existing check for a column named `rowid`, in any
  letter case, now sees a generated one, virtual or stored.
- It refuses a column named exactly `__proto__`, plain or generated, with a
  message that names the column. The rule is "a name a row object cannot carry".
  `__proto__` is the only accessor that `Object.prototype` defines, so it is the
  only such name: setting `constructor`, `toString` or `hasOwnProperty` makes an
  own key, and the test pins that. The name is compared exactly, as row keys are,
  so `__PROTO__` is an ordinary column.

Both are refused at registration and not per query, because the fault is in the
table: no query on it can be answered correctly (the default order) or no row
of it can be carried (the `__proto__` column). That is the kind of fault the
other `assertSyncCompatible` checks refuse.

**Consequences.**

- **Breaking.** `registerSync` now throws for a table with a generated column
  named `rowid` or a column named `__proto__`, where it used to succeed. An app
  with such a table must rename the column before it upgrades.
- Like every check in `assertSyncCompatible`, these run in `registerSync`. A
  migration that adds such a column while the object is alive, with no
  `registerSync` after it, is caught only at the next `registerSync`, at the latest
  on the next wake. Until then the column check of ADR-0013 reads the live table,
  so it accepts a query that names a `__proto__` column added that way.
- No cost at query time. `registerSync` reads the same pragma family as before,
  once per collection.

**Evidence.** `tests/assert-sync-compatible.test.ts` pins both refusals (virtual
and stored `rowid`, plain and generated `__proto__`), what SQLite and workerd do
that makes each table wrong, and that `constructor`, `toString` and
`hasOwnProperty` are still accepted.
