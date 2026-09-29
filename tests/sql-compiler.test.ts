import { describe, expect, it } from "vitest"
import { compileSubsetQuery, compileWhere, UnsupportedPredicateError } from "../src/server/sql-compiler.ts"

// WHY: this lowers client predicates into SQLite so subsets are filtered at the
// source, not scanned in memory. It must produce correct parameterised SQL for
// the operator floor, quote identifiers, and REJECT anything outside the floor
// (D6: fail loud, never a silent full scan). Tests assert both the SQL shape
// and the rejections.

const ref = (col: string) => ({ type: "ref", path: [col] })
const val = (value: unknown) => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>) => ({ type: "func", name, args })

describe("IR -> SQL compiler (M6)", () => {
  it("compiles comparison operators with bound params", () => {
    expect(compileWhere(fn("eq", ref("body"), val("x")))).toEqual({ sql: `"body" = ?`, params: ["x"] })
    expect(compileWhere(fn("gte", ref("n"), val(5)))).toEqual({ sql: `"n" >= ?`, params: [5] })
    expect(compileWhere(fn("like", ref("body"), val("h%")))).toEqual({ sql: `"body" LIKE ?`, params: ["h%"] })
  })

  it("rejects a non-string `like` pattern (SQL would coerce, @tanstack/db would not — ADR-0013)", () => {
    // SQLite LIKE coerces 123 → '123' and could match; the JS evaluator returns
    // false for a non-string pattern. Reject so the two paths can't diverge.
    expect(() => compileWhere(fn("like", ref("body"), val(123)))).toThrow(/pattern must be a string/)
    expect(() => compileWhere(fn("like", ref("body"), val(null)))).toThrow(UnsupportedPredicateError)
  })

  it("compiles and/or/not with grouping", () => {
    const e = fn("and", fn("gt", ref("n"), val(5)), fn("not", fn("eq", ref("k"), val("z"))))
    expect(compileWhere(e)).toEqual({ sql: `("n" > ? AND (NOT "k" = ?))`, params: [5, "z"] })
  })

  it("compiles `in` with one placeholder per element", () => {
    expect(compileWhere(fn("in", ref("id"), val(["a", "b", "c"])))).toEqual({
      sql: `"id" IN (?, ?, ?)`,
      params: ["a", "b", "c"],
    })
  })

  it("compiles an empty `in` to a constant-false", () => {
    expect(compileWhere(fn("in", ref("id"), val([])))).toEqual({ sql: `0`, params: [] })
  })

  it("rejects operators outside the floor (ne, ilike, isNull, functions)", () => {
    // `ne` was removed from the floor (ADR-0013): @tanstack/db's evaluator has no
    // `ne` (only `not(eq(...))`), so accepting it in SQL desynced the snapshot and
    // delta paths. It must now be rejected like any other off-floor operator.
    expect(() => compileWhere(fn("ne", ref("body"), val("x")))).toThrow(UnsupportedPredicateError)
    expect(() => compileWhere(fn("ilike", ref("body"), val("x")))).toThrow(UnsupportedPredicateError)
    expect(() => compileWhere(fn("isNull", ref("body")))).toThrow(UnsupportedPredicateError)
    expect(() => compileWhere(fn("upper", ref("body")))).toThrow(/not supported/)
  })

  it("the supported 'not equal' is not(eq(...)), which lowers correctly", () => {
    // The canonical not-equal a @tanstack/db client emits — both floors agree on it.
    expect(compileWhere(fn("not", fn("eq", ref("body"), val("x"))))).toEqual({
      sql: `(NOT "body" = ?)`,
      params: ["x"],
    })
  })

  it("rejects nested/aliased column paths", () => {
    expect(() => compileWhere(fn("eq", { type: "ref", path: ["a", "b"] }, val(1)))).toThrow(/nested/)
  })

  it("rejects a non-literal comparison RHS and a non-ref LHS", () => {
    expect(() => compileWhere(fn("eq", ref("a"), ref("b")))).toThrow(/literal/)
    expect(() => compileWhere(fn("eq", val(1), val(2)))).toThrow(/column reference/)
  })

  it("builds a full subset SELECT with where/orderBy/limit/offset", () => {
    const q = compileSubsetQuery("messages", {
      where: fn("eq", ref("body"), val("x")),
      orderBy: [{ col: "created_at", dir: "desc" }],
      limit: 10,
      offset: 20,
    })
    expect(q.sql).toBe(`SELECT * FROM "messages" WHERE "body" = ? ORDER BY "created_at" DESC NULLS FIRST LIMIT ? OFFSET ?`)
    expect(q.params).toEqual(["x", 10, 20])
  })

  it("emits LIMIT -1 when offset is given without a limit (SQLite requirement)", () => {
    const q = compileSubsetQuery("t", { offset: 5 })
    expect(q.sql).toBe(`SELECT * FROM "t" ORDER BY rowid LIMIT -1 OFFSET ?`)
    expect(q.params).toEqual([5])
  })

  it("preserves each orderBy clause's null placement", () => {
    const q = compileSubsetQuery("t", {
      orderBy: [
        { expression: ref("body"), compareOptions: { direction: "desc", nulls: "last" } },
        { expression: ref("id"), compareOptions: { direction: "asc" } },
      ],
    })
    expect(q.sql).toBe(`SELECT * FROM "t" ORDER BY "body" DESC NULLS LAST, "id" ASC NULLS FIRST`)
  })

  it("defaults to ORDER BY rowid when the client sends no orderBy (deterministic snapshot order)", () => {
    // Field-verified regression: a bare SELECT with no ORDER BY leaves row order
    // as an accident of SQLite's query plan. `rowid` is always available (D9
    // forbids an INTEGER PRIMARY KEY pk) and matches insertion order among
    // currently-live rows.
    const q = compileSubsetQuery("t", {})
    expect(q.sql).toBe(`SELECT * FROM "t" ORDER BY rowid`)
  })

  it("rejects a negative limit/offset and an invalid orderBy column", () => {
    expect(() => compileSubsetQuery("t", { limit: -1 })).toThrow(/non-negative/)
    expect(() => compileSubsetQuery("t", { orderBy: [{ col: "a; DROP" }] })).toThrow(/orderBy column/)
  })
})
