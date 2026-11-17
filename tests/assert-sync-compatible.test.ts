import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { assertSyncCompatible } from "../src/server/changes.ts"

// WHY: D9 (the pk is a sole TEXT, client-supplied key) is load-bearing for
// optimistic id parity — a server-assigned INTEGER/AUTOINCREMENT key yields
// optimistic rows that never retire. Since the author now owns table creation
// (ADR-0007), the rule is enforced against the ACTUAL table at registerSync time
// via PRAGMA. Each case pins the *reason* a table is rejected, not just that it is.

// A fresh DO per call gives an isolated SQLite database to define a table in.
function withSql<T>(fn: (sql: SqlStorage) => T): Promise<T> {
  return runInDurableObject(env.TEST_DO.get(env.TEST_DO.newUniqueId()), (_i, s) => fn(s.storage.sql))
}

describe("assertSyncCompatible (ADR-0007, D9) — real-table introspection", () => {
  it("accepts a column-level TEXT PRIMARY KEY", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, body TEXT)")
      expect(() => assertSyncCompatible(sql, "t", "id")).not.toThrow()
    })
  })

  it("accepts a table-level PRIMARY KEY on a TEXT column", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT, body TEXT, PRIMARY KEY (id))")
      expect(() => assertSyncCompatible(sql, "t", "id")).not.toThrow()
    })
  })

  it("rejects a missing table", async () => {
    await withSql((sql) => {
      expect(() => assertSyncCompatible(sql, "nope", "id")).toThrow(/not found/)
    })
  })

  it("rejects an INTEGER rowid-alias primary key", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/TEXT affinity/)
    })
  })

  it("rejects AUTOINCREMENT (server-assigned key breaks id parity)", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/TEXT affinity/)
    })
  })

  it("rejects a non-TEXT pk type", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id BLOB PRIMARY KEY)")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/TEXT affinity/)
    })
  })

  it("rejects a table with no primary key", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT, body TEXT)")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/sole PRIMARY KEY/)
    })
  })

  it("rejects a pk-name mismatch (the PK is some other column)", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (other TEXT PRIMARY KEY, id TEXT)")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/sole PRIMARY KEY/)
    })
  })

  it("rejects a composite primary key", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT, k TEXT, PRIMARY KEY (id, k))")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/sole PRIMARY KEY/)
    })
  })

  it("rejects a WITHOUT ROWID table (no rowid for the default snapshot order)", async () => {
    // The pk is a valid sole TEXT key, so it clears every D9 check — but a
    // WITHOUT ROWID table has no rowid, and the cold-snapshot/fetch reader
    // defaults to `ORDER BY rowid` (ADR-0015). Without this guard the table
    // registers, then the first orderBy-less sub throws `no such column: rowid`
    // at read time and hangs the subscriber. Reject loudly at registerSync.
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, body TEXT) WITHOUT ROWID")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/WITHOUT ROWID/)
    })
  })

  it("rejects a declared `rowid` column that shadows SQLite's internal rowid", async () => {
    // A user column named `rowid` clears every D9 check AND makes `SELECT rowid`
    // resolve — but it shadows the internal insertion-order rowid, so the
    // reader's default `ORDER BY rowid` (ADR-0015) would sort by this arbitrary
    // column and lose determinism on null/duplicate values. Probing alone can't
    // see the shadow; reject the declared column explicitly.
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, rowid TEXT)")
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/shadows/)
    })
  })

  // ADR-0007 amendment 2026-11-17. A generated column is not in `pragma_table_info`,
  // and `SELECT rowid` resolves to it, so neither the column check nor the probe
  // saw it, and the default `ORDER BY rowid` sorted by the generated value.
  for (const [kind, ddl] of [
    ["virtual", "rowid TEXT GENERATED ALWAYS AS (upper(body)) VIRTUAL"],
    ["stored", "ROWID TEXT GENERATED ALWAYS AS (upper(body)) STORED"],
  ] as const) {
    it(`rejects a ${kind} generated column named rowid`, async () => {
      await withSql((sql) => {
        sql.exec(`CREATE TABLE t (id TEXT PRIMARY KEY, body TEXT, ${ddl})`)
        sql.exec("INSERT INTO t (id, body) VALUES ('a', 'z'), ('b', 'a')")
        // Why the probe alone cannot see it: `rowid` reads the generated value.
        expect(Array.from(sql.exec("SELECT rowid AS r FROM t ORDER BY rowid"), (x) => x.r)).toEqual(["A", "Z"])
        expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/shadows/)
      })
    })
  }

  // ADR-0007 amendment 2026-11-17. workerd builds a row of `SELECT *` by setting
  // each column on a plain object, so a column named `__proto__` is never an own
  // key of the row, and a NULL in it sets the row's prototype to null. Neither
  // the snapshot nor a delta can carry that column, and SQL can match on it where
  // the evaluator reads the row's prototype.
  it("rejects a column named __proto__, plain or generated", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, __proto__ TEXT)")
      sql.exec("INSERT INTO t (id, __proto__) VALUES ('a', 'x'), ('b', NULL)")
      const [a, b] = Array.from(sql.exec("SELECT * FROM t ORDER BY id"))
      expect(Object.keys(a!)).toEqual(["id"])
      expect(Object.getPrototypeOf(b!)).toBe(null)
      expect(() => assertSyncCompatible(sql, "t", "id")).toThrow(/__proto__/)

      sql.exec("CREATE TABLE g (id TEXT PRIMARY KEY, body TEXT, __proto__ TEXT GENERATED ALWAYS AS (body) VIRTUAL)")
      expect(() => assertSyncCompatible(sql, "g", "id")).toThrow(/__proto__/)
    })
  })

  // The fix ADR-0007 gives an app: rename the column in a migration before
  // registerSync. RENAME COLUMN works for a generated column too, with rows; drop
  // and add does not for a STORED one ("cannot add a STORED column").
  it("a refused column renamed with RENAME COLUMN, rows and all, then registers", async () => {
    await withSql((sql) => {
      for (const [t, ddl, col] of [
        ["v", "rowid TEXT GENERATED ALWAYS AS (upper(body)) VIRTUAL", "rowid"],
        ["s", "rowid TEXT GENERATED ALWAYS AS (upper(body)) STORED", "rowid"],
        ["p", "__proto__ TEXT", "__proto__"],
        ["g", "__proto__ TEXT GENERATED ALWAYS AS (body) STORED", "__proto__"],
      ] as const) {
        sql.exec(`CREATE TABLE ${t} (id TEXT PRIMARY KEY, body TEXT, ${ddl})`)
        sql.exec(`INSERT INTO ${t} (id, body) VALUES ('a', 'x'), ('b', 'y')`)
        expect(() => assertSyncCompatible(sql, t, "id")).toThrow()
        sql.exec(`ALTER TABLE ${t} RENAME COLUMN ${col} TO renamed`)
        expect(() => assertSyncCompatible(sql, t, "id")).not.toThrow()
        expect(Array.from(sql.exec(`SELECT count(*) AS c FROM ${t}`))[0]!.c).toBe(2)
      }
      // Why not drop and add: on a table with rows a STORED column cannot be added.
      sql.exec("CREATE TABLE d (id TEXT PRIMARY KEY, body TEXT, rowid TEXT GENERATED ALWAYS AS (upper(body)) STORED)")
      sql.exec("INSERT INTO d (id, body) VALUES ('a', 'x')")
      sql.exec("ALTER TABLE d DROP COLUMN rowid")
      expect(() => sql.exec("ALTER TABLE d ADD COLUMN loud TEXT GENERATED ALWAYS AS (upper(body)) STORED")).toThrow(/STORED/)
    })
  })

  it("accepts __PROTO__: the name is compared exactly, as a row's keys are", async () => {
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, __PROTO__ TEXT)")
      sql.exec("INSERT INTO t VALUES ('a', 'x')")
      const [a] = Array.from(sql.exec("SELECT * FROM t"))
      expect(Object.keys(a!)).toEqual(["id", "__PROTO__"])
      expect(() => assertSyncCompatible(sql, "t", "id")).not.toThrow()
    })
  })

  it("accepts columns named after other Object.prototype members", async () => {
    // Only `__proto__` is an accessor on a plain object; these are set as own keys.
    await withSql((sql) => {
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, constructor TEXT, toString TEXT, hasOwnProperty TEXT)")
      sql.exec("INSERT INTO t VALUES ('a', NULL, 'x', 'y')")
      const [a] = Array.from(sql.exec("SELECT * FROM t"))
      expect(Object.keys(a!)).toEqual(["id", "constructor", "toString", "hasOwnProperty"])
      expect(() => assertSyncCompatible(sql, "t", "id")).not.toThrow()
    })
  })
})
