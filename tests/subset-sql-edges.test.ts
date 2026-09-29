import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { compileSubsetQuery, compileWhere } from "../src/server/sql-compiler.ts"

// WHY (codex review of the F4-cursor/F4-ties fixes): both edges run through real
// SQLite, since the bug is in what SQLite does with the compiled text.

const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })
const inDo = (run: (sql: SqlStorage) => void): Promise<void> =>
  runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(`edges-${crypto.randomUUID()}`)), (_i, s) => run(s.storage.sql))

describe("compiled subset SQL edges", () => {
  it("isUndefined on a column the table lacks fails loud, like every other operator", async () => {
    // @tanstack/db's `=== undefined` is TRUE for a property absent from a hydrated row,
    // so a constant-false SQL would silently diverge from the delta path for a
    // column that does not exist. Referencing the column makes SQLite reject it.
    await inDo((sql) => {
      const ok = compileWhere(fn("isUndefined", ref("body")))
      expect(sql.exec(`SELECT * FROM messages WHERE ${ok.sql}`).toArray()).toEqual([])
      const bad = compileWhere(fn("isUndefined", ref("ghost")))
      expect(() => sql.exec(`SELECT * FROM messages WHERE ${bad.sql}`)).toThrow(/ghost/)
    })
  })

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
