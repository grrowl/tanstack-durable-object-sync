import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { compileSubsetQuery } from "../src/server/sql-compiler.ts"

// WHY (codex review of the F4-ties fix): the edge runs through real SQLite, since
// the bug is in what SQLite does with the compiled text.

const inDo = (run: (sql: SqlStorage) => void): Promise<void> =>
  runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(`edges-${crypto.randomUUID()}`)), (_i, s) => run(s.storage.sql))

describe("compiled subset SQL edges", () => {
  it("the pk tie-break is BINARY even when the pk column declares another collation", async () => {
    // @tanstack/db compares row keys with `<` (code-unit order): "B" < "a". A pk
    // declared COLLATE NOCASE would otherwise order a, B.
    await inDo((sql) => {
      sql.exec("CREATE TABLE ci(id TEXT PRIMARY KEY COLLATE NOCASE, v TEXT)")
      for (const id of ["a", "B", "c"]) sql.exec("INSERT INTO ci(id,v) VALUES(?,NULL)", id)
      const q = compileSubsetQuery("ci", { pk: "id", orderBy: [{ col: "v", dir: "asc" }], limit: 2 })
      expect(sql.exec<{ id: string }>(q.sql, ...q.params).toArray().map((r) => r.id)).toEqual(["B", "a"])
    })
  })
})
