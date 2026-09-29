// CDC substrate (ADR-0001 D12): one per-DO change log fed by AFTER triggers,
// plus the single monotonic `seq` cursor.
//
// `_sync_changes` is the one ordered stream — the source of truth for live
// deltas, reconnect catch-up, and write confirmation. A trigger's INSERT runs
// inside the same `transactionSync` SAVEPOINT as the user write, so the change
// row commits or rolls back atomically with it (no orphaned log rows).
//
// No before-image column: move-in/move-out is computed at emit time from the
// current row + the subscription predicate (ADR-0002 C4).

export const SYNC_PREFIX = "_sync_"

export type ChangeOp = "insert" | "update" | "delete"

export interface ChangeRow {
  seq: number
  tbl: string
  key: string
  op: ChangeOp
  ts: number
  // Index signature lets ChangeRow satisfy sql.exec<T>()'s constraint.
  [k: string]: string | number
}

/** Create the framework-owned tables. Idempotent. */
export function initSchema(sql: SqlStorage): void {
  // seq INTEGER PRIMARY KEY AUTOINCREMENT is the monotonic cursor; SQLite
  // indexes the PK already. The composite index serves per-table catch-up.
  sql.exec(`CREATE TABLE IF NOT EXISTS _sync_changes (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              tbl TEXT NOT NULL,
              key TEXT NOT NULL,
              op  TEXT NOT NULL,
              ts  INTEGER NOT NULL
            )`)
  sql.exec(`CREATE INDEX IF NOT EXISTS _sync_changes_tbl_seq ON _sync_changes(tbl, seq)`)
  sql.exec(`CREATE TABLE IF NOT EXISTS _sync_meta (k TEXT PRIMARY KEY, v TEXT)`)
  // Mutation/command dedup (exactly-once under retry). See dedup.ts.
  sql.exec(`CREATE TABLE IF NOT EXISTS _sync_seen_tx (
              tx_id      TEXT PRIMARY KEY,
              ok         INTEGER NOT NULL,
              cursor     TEXT,
              error      TEXT,
              error_code TEXT,
              result     TEXT,
              ts         INTEGER NOT NULL
            )`)
  // `error_code` was added after tables shipped without it (issue #21): a DO
  // created earlier wakes with the old shape, and CREATE IF NOT EXISTS won't
  // touch it. ADD COLUMN isn't idempotent on its own, so gate on table_info.
  const seenTxCols = Array.from(sql.exec<{ name: string }>("SELECT name FROM pragma_table_info('_sync_seen_tx')"))
  if (!seenTxCols.some((c) => c.name === "error_code")) {
    sql.exec("ALTER TABLE _sync_seen_tx ADD COLUMN error_code TEXT")
  }
  // Durable per-socket subscriptions (ADR-0019): hibernatable sockets survive
  // eviction while the in-memory registry does not; these rows let the wake
  // restore re-associate subscriptions with surviving sockets. SQL lives in
  // subscriptions.ts.
  sql.exec(`CREATE TABLE IF NOT EXISTS _sync_subs (
              socket_id  TEXT NOT NULL,
              sub_id     TEXT NOT NULL,
              collection TEXT NOT NULL,
              where_ir   TEXT,
              PRIMARY KEY (socket_id, sub_id)
            )`)
}

/**
 * Install AFTER INSERT/UPDATE/DELETE triggers copying change events into
 * `_sync_changes`. Idempotent. `tbl`/`pk` MUST be validated identifiers
 * (`compileSchema` enforces this via `assertValidCollection`) — they are
 * interpolated into DDL. Identifiers are double-quoted for consistency with the
 * rest of the codebase (`sql-compiler.ts`, `ensureTriggers`' DROP), though the
 * regex gate remains the real safety net. The `'${tbl}'` string literal (the
 * value inserted into the `tbl` column) stays single-quoted as a value, not an
 * identifier.
 *
 * Each statement is passed whole: splitting on `;` would sever the inner
 * `INSERT ...;` from its `END`.
 */
export function installTriggers(sql: SqlStorage, tbl: string, pk: string): void {
  sql.exec(
    `CREATE TRIGGER IF NOT EXISTS "_sync_changes_${tbl}_ai" AFTER INSERT ON "${tbl}" BEGIN
       INSERT INTO _sync_changes(tbl,key,op,ts) VALUES ('${tbl}', CAST(NEW."${pk}" AS TEXT), 'insert', unixepoch()*1000);
     END`,
  )
  sql.exec(
    `CREATE TRIGGER IF NOT EXISTS "_sync_changes_${tbl}_au" AFTER UPDATE ON "${tbl}" BEGIN
       INSERT INTO _sync_changes(tbl,key,op,ts) VALUES ('${tbl}', CAST(NEW."${pk}" AS TEXT), 'update', unixepoch()*1000);
     END`,
  )
  sql.exec(
    `CREATE TRIGGER IF NOT EXISTS "_sync_changes_${tbl}_ad" AFTER DELETE ON "${tbl}" BEGIN
       INSERT INTO _sync_changes(tbl,key,op,ts) VALUES ('${tbl}', CAST(OLD."${pk}" AS TEXT), 'delete', unixepoch()*1000);
     END`,
  )
}

/**
 * Make the `_sync_changes_*` triggers exactly reflect the registered
 * collections (ADR-0008): validate + install each registered table's triggers,
 * then DROP any of OUR triggers whose collection is gone (set-diff against the
 * expected names). Idempotent — a steady registry installs nothing new and
 * reaps nothing — so `registerSync` can call it on every wake to reconcile the
 * whole trigger state. Removing a collection from the registry thus stops its
 * orphaned table from firing capture triggers into `_sync_changes`.
 *
 * Takes a structural `{table, pk}` rather than registry.ts's `CollectionDef` so
 * changes.ts stays free of any import from registry.ts (which already imports
 * `SYNC_PREFIX` from here — the reverse would be a cycle).
 */
export function ensureTriggers(
  sql: SqlStorage,
  collections: Iterable<{ table: string; pk: string }>,
): void {
  const want = new Set<string>()
  for (const { table, pk } of collections) {
    assertSyncCompatible(sql, table, pk)
    installTriggers(sql, table, pk)
    want.add(`_sync_changes_${table}_ai`)
    want.add(`_sync_changes_${table}_au`)
    want.add(`_sync_changes_${table}_ad`)
  }
  // GLOB, not LIKE: `_` is a LIKE wildcard but a GLOB literal, so this matches
  // exactly our `_sync_changes_` namespace and never an author trigger.
  const existing = sql.exec<{ name: string }>(
    "SELECT name FROM sqlite_master WHERE type='trigger' AND name GLOB '_sync_changes_*'",
  )
  for (const { name } of existing) {
    if (!want.has(name)) sql.exec(`DROP TRIGGER IF EXISTS "${name}"`)
  }
}

/**
 * Validate that an author-created table is sync-compatible (ADR-0007, D9): the
 * declared `pk` must be the table's SOLE primary key and have TEXT affinity (a
 * client-supplied stable key: TEXT/VARCHAR/CHAR/…). Rejects a missing table, a
 * composite/wrong pk, an `INTEGER PRIMARY KEY` (the rowid alias — server-assigned,
 * which breaks optimistic id parity), and any table that lacks a usable internal
 * `rowid` — a `WITHOUT ROWID` table (which has none) or one with a declared
 * `rowid` column (which shadows it) — because the cold-snapshot/fetch reader
 * orders by `rowid` when the client sends no orderBy (ADR-0015). Introspects the
 * real table, so it works however the author created it. `pragma_table_info` is
 * the table-valued form, so the table name binds as a parameter.
 */
export function assertSyncCompatible(sql: SqlStorage, tbl: string, pk: string): void {
  const cols = Array.from(
    sql.exec<{ name: string; type: string; is_pk: number }>(
      "SELECT name, type, pk AS is_pk FROM pragma_table_info(?)",
      tbl,
    ),
  )
  if (cols.length === 0) {
    throw new Error(`collection '${tbl}': table not found — create it (migrate) before registerSync`)
  }
  const pks = cols.filter((c) => c.is_pk > 0)
  if (pks.length !== 1 || pks[0]!.name !== pk) {
    throw new Error(
      `collection '${tbl}': '${pk}' must be the sole PRIMARY KEY (D9) — got [${pks.map((c) => c.name).join(", ") || "none"}]`,
    )
  }
  if (!hasTextAffinity(pks[0]!.type)) {
    throw new Error(
      `collection '${tbl}': pk '${pk}' must have TEXT affinity (TEXT, VARCHAR, CHAR, …) so it ` +
        `stores the client-supplied id verbatim; got '${pks[0]!.type || "(no type)"}'. An INTEGER ` +
        `key aliases rowid (server-assigned) and breaks optimistic id parity (D9).`,
    )
  }
  // The cold-snapshot/fetch reader defaults to `ORDER BY rowid` when the client
  // sends no orderBy (ADR-0015), so a synced table must expose SQLite's internal
  // rowid — the insertion-order key that default is built on. Two ways an author
  // can defeat that, both rejected here at registerSync rather than left to
  // surface at read time:
  //
  //   1. A declared column named `rowid` SHADOWS the internal rowid, so
  //      `ORDER BY rowid` would silently sort by that arbitrary user column
  //      (and lose determinism on null/duplicate values). Check the introspected
  //      columns directly.
  if (cols.some((c) => c.name.toLowerCase() === "rowid")) {
    throw new Error(
      `collection '${tbl}': a column named 'rowid' shadows SQLite's internal rowid, which the ` +
        `snapshot/fetch reader orders by when the client sends no orderBy (ADR-0015). Rename that column.`,
    )
  }
  //   2. A `WITHOUT ROWID` table has NO rowid at all, so `ORDER BY rowid` throws
  //      `no such column: rowid` and hangs the subscriber. With a shadowing
  //      column already ruled out, probing the exact property the reader relies
  //      on is unambiguous — a compile-time "no such column: rowid" is the
  //      signal; anything else is an unrelated failure and rethrows unchanged.
  try {
    sql.exec(`SELECT rowid FROM "${tbl}" LIMIT 0`)
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    if (!/rowid/i.test(msg)) throw e
    throw new Error(
      `collection '${tbl}': table is WITHOUT ROWID — sync requires a rowid table so snapshot/fetch ` +
        `reads have a deterministic default order (ORDER BY rowid, ADR-0015). Recreate '${tbl}' ` +
        `without the WITHOUT ROWID clause.`,
    )
  }
}

/**
 * SQLite TEXT affinity, per the type-affinity rules (in order): a type
 * containing "INT" is INTEGER affinity; otherwise one containing CHAR/CLOB/TEXT
 * is TEXT. So this accepts `TEXT`, `VARCHAR(n)`, `NVARCHAR`, `CHARACTER`,
 * `CLOB`, … (however a migrator or ORM spells a string key) and rejects
 * `INTEGER` (the rowid/AUTOINCREMENT footgun), `REAL`/`NUMERIC`, and untyped
 * (BLOB-affinity) columns. We require TEXT affinity because the key must
 * preserve the client string exactly — a non-TEXT affinity could coerce it.
 */
function hasTextAffinity(declaredType: string): boolean {
  const t = (declaredType || "").toUpperCase()
  if (t.includes("INT")) return false
  return t.includes("CHAR") || t.includes("CLOB") || t.includes("TEXT")
}

/** Highest `seq` in the log — the current cursor / write-confirmation receipt. */
export function currentSeq(sql: SqlStorage): number {
  const rows = Array.from(
    sql.exec<{ s: number | null }>("SELECT MAX(seq) AS s FROM _sync_changes"),
  )
  return Number(rows[0]?.s ?? 0)
}

/**
 * Durable high-water mark — the latest position the stream has reached, robust
 * to retention pruning the changelog empty (`currentSeq` alone reads 0 then,
 * which would hand SSR a bogus "no history" cursor for live rows; ADR-0011 D1).
 * The drain cursor lives in `_sync_meta` and survives pruning; an undrained
 * tail is covered by the MAX over the log itself.
 */
export function highWaterSeq(sql: SqlStorage): number {
  return Math.max(currentSeq(sql), getDrainCursor(sql))
}

/** Lowest `seq` still in the log — the retention floor for reconnect catch-up. */
export function minChangeSeq(sql: SqlStorage): number {
  const rows = Array.from(
    sql.exec<{ s: number | null }>("SELECT MIN(seq) AS s FROM _sync_changes"),
  )
  return Number(rows[0]?.s ?? 0)
}

/**
 * Compact the change log to the latest op per (tbl,key): superseded rows are
 * deleted. Bounds the log to ~distinct changed keys. Each key's LATEST op —
 * including a delete tombstone — is retained, so reconnect catch-up
 * (latest-op-per-key) stays correct for any cursor and the snap-fallback stays
 * dormant. (Tombstone pruning + liveness-aware GC — which would activate the
 * fallback — are deferred post-v1.)
 */
export function compactChanges(sql: SqlStorage): void {
  sql.exec(
    "DELETE FROM _sync_changes WHERE seq NOT IN (SELECT MAX(seq) FROM _sync_changes GROUP BY tbl, key)",
  )
}

/**
 * Age-based changelog retention (ADR-0009): drop every change older than
 * `olderThanMs`. Bounds `_sync_changes` by AGE — complementing compaction, which
 * bounds it by key-cardinality. `ts` is server-stamped and trusted, so we delete
 * by age directly. Mirrors the time-bound `sweepDedup`.
 *
 * `olderThanMs === null` DISABLES retention (no-op) — the log reverts to
 * compaction-only. A reconnect older than the surviving floor is handled by the
 * `reset` gate in `handleSub`; this only reclaims storage.
 */
export function pruneChanges(sql: SqlStorage, olderThanMs: number | null, nowMs: number): void {
  if (olderThanMs === null) return
  sql.exec("DELETE FROM _sync_changes WHERE ts < ?", nowMs - olderThanMs)
}

/** The "highest seq we've broadcast" watermark. */
export function getDrainCursor(sql: SqlStorage): number {
  const rows = Array.from(
    sql.exec<{ v: string }>("SELECT v FROM _sync_meta WHERE k='drain_cursor'"),
  )
  return rows.length === 0 ? 0 : Number(rows[0]!.v ?? 0)
}

export function setDrainCursor(sql: SqlStorage, seq: number): void {
  sql.exec(
    "INSERT INTO _sync_meta(k,v) VALUES('drain_cursor', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
    String(seq),
  )
}

/** All change rows with seq > cursor, in seq order. */
export function readChangesSince(sql: SqlStorage, cursor: number): Array<ChangeRow> {
  return Array.from(
    sql.exec<ChangeRow>(
      "SELECT seq, tbl, key, op, ts FROM _sync_changes WHERE seq > ? ORDER BY seq",
      cursor,
    ),
  )
}

/** One table's change rows with seq > cursor, in seq order. Uses the
 *  `_sync_changes_tbl_seq` composite index — this is the per-table catch-up
 *  read that index exists for. */
export function readChangesSinceFor(
  sql: SqlStorage,
  tbl: string,
  cursor: number,
): Array<ChangeRow> {
  return Array.from(
    sql.exec<ChangeRow>(
      "SELECT seq, tbl, key, op, ts FROM _sync_changes WHERE tbl = ? AND seq > ? ORDER BY seq",
      tbl,
      cursor,
    ),
  )
}

/** Current rows for a set of keys, for hydrating deltas. `tbl`/`pk` are
 *  validated identifiers (`compileSchema` enforces this). Queries in chunks
 *  of 64 to avoid SQLite bound-parameter limits and eliminate the N+1 pattern:
 *  a reconnect catch-up over 500 keys now issues ⌈500/64⌉ = 8 queries instead
 *  of 500. Identifiers are quoted; values are bound parameters. */
export function hydrateRows(
  sql: SqlStorage,
  tbl: string,
  pk: string,
  keys: Array<string>,
): Map<string, Record<string, SqlStorageValue>> {
  const out = new Map<string, Record<string, SqlStorageValue>>()
  const CHUNK = 64 // stay far below any SQLite bound-parameter limit
  for (let i = 0; i < keys.length; i += CHUNK) {
    const chunk = keys.slice(i, i + CHUNK)
    const placeholders = chunk.map(() => "?").join(", ")
    for (const row of sql.exec<Record<string, SqlStorageValue>>(
      `SELECT * FROM "${tbl}" WHERE "${pk}" IN (${placeholders})`,
      ...chunk,
    )) {
      out.set(String(row[pk]), row)
    }
  }
  return out
}
