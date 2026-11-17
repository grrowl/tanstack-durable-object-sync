import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { compileSubsetQuery, compileWhere } from "../src/server/sql-compiler.ts"

// WHY: SQLite in a Durable Object runs with reduced limits (workerd's
// `SqliteDatabase::setupSecurity`): 100 bound parameters, an expression tree
// 100 levels deep, 100 000 bytes of SQL, 2 200 000 bytes in one bound string
// (on the workerd this package is tested with), and 50 bytes in a LIKE pattern.
// A query over one throws from SQLite at run time, and a sub or fetch that
// throws gets no reply. So the compiler refuses every query over a limit, and
// nothing under it (ADR-0013 amendment 2026-11-17). Each case below compiles a
// query, runs the same SQL in SQLite, and requires the compiler to accept it
// exactly when SQLite runs it, on both sides of each limit. If workerd changes
// a limit, a case here fails.

const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })

const eqId = (i: number): unknown => fn("eq", ref("id"), val(`k${i}`))
const isNull = (): unknown => fn("isNull", ref("body"))
const nest = (k: number, inner: unknown): unknown => (k === 0 ? inner : fn("not", nest(k - 1, inner)))
const range = (from: number, to: number): Array<number> => Array.from({ length: to - from + 1 }, (_, i) => from + i)
/** A balanced `or` tree over `n` leaves: shallow, so only its length grows. */
const balanced = (n: number): unknown =>
  n === 1 ? isNull() : fn("or", balanced(Math.ceil(n / 2)), balanced(Math.floor(n / 2)))

/** `raw`: the WHERE SQL and params to run, where `compileWhere` itself refuses. */
type Case = { label: string; where: unknown; limit?: number; offset?: number; raw?: { sql: string; params: Array<unknown> } }

/** For each case: does the compiler accept it, and does SQLite run it? */
async function verdicts(cases: Array<Case>): Promise<Array<{ label: string; accepted: boolean; runs: string }>> {
  const stub = env.TEST_DO.get(env.TEST_DO.idFromName(`limits-${crypto.randomUUID()}`))
  return runInDurableObject(stub, (_i, s) => {
    const sql = s.storage.sql
    sql.exec("CREATE TABLE m (id TEXT PRIMARY KEY, body TEXT, n INTEGER)")
    // One row, so a LIKE is evaluated (SQLite checks the pattern length only then).
    sql.exec("INSERT INTO m VALUES ('a', 'x', 1)")
    return cases.map(({ label, where, limit, offset, raw }) => {
      let accepted = true
      try {
        compileSubsetQuery("m", { pk: "id", where, limit, offset })
      } catch {
        accepted = false
      }
      // The same statement, built without the compiler's checks.
      const w = raw ?? compileWhere(where)
      let text = `SELECT * FROM "m" WHERE ${w.sql} ORDER BY rowid`
      const params = [...w.params]
      if (limit != null) (text += " LIMIT ?"), params.push(limit)
      if (offset != null) (text += " OFFSET ?"), params.push(offset)
      let runs = "runs"
      try {
        Array.from(sql.exec(text, ...params))
      } catch (e) {
        runs = String(e)
      }
      return { label, accepted, runs }
    })
  })
}

/** Every case's verdict must match, and the family must cross its limit. */
async function expectEdge(cases: Array<Case>): Promise<void> {
  const out = await verdicts(cases)
  const wrong = out.filter((v) => v.accepted !== (v.runs === "runs"))
  expect(wrong, "compiler and SQLite disagree").toEqual([])
  expect(out.some((v) => v.accepted), "premise: some case runs").toBe(true)
  expect(out.some((v) => !v.accepted), "premise: some case is over the limit").toBe(true)
}

describe("the compiler refuses exactly what Durable Object SQLite rejects", () => {
  it("bound parameters (100)", async () => {
    const reals = (n: number): unknown => fn("in", ref("n"), val(Array.from({ length: n }, (_, i) => i + 0.5)))
    await expectEdge([
      ...[99, 100, 101].map((n) => ({ label: `in of ${n} non-integers`, where: reals(n) })),
      { label: "98 + limit + offset", where: reals(98), limit: 5, offset: 0 },
      { label: "99 + limit + offset", where: reals(99), limit: 5, offset: 0 },
    ])
  })

  it("expression depth (100), across shapes", async () => {
    await expectEdge([
      ...range(97, 101).map((n) => ({ label: `or of ${n} eq`, where: fn("or", ...range(1, n).map(eqId)) })),
      ...range(97, 101).map((n) => ({ label: `or of ${n} isNull`, where: fn("or", ...range(1, n).map(isNull)) })),
      ...range(95, 99).map((n) => ({
        label: `or of ${n} one-string in`,
        where: fn("or", ...range(1, n).map((i) => fn("in", ref("id"), val([`k${i}`])))),
      })),
      ...range(96, 100).map((k) => ({ label: `not^${k} eq`, where: nest(k, eqId(0)) })),
      ...range(96, 100).map((k) => ({ label: `not^${k} like`, where: nest(k, fn("like", ref("body"), val("x%"))) })),
      ...range(95, 99).map((k) => ({ label: `not^${k} empty in`, where: nest(k, fn("in", ref("body"), val([]))) })),
      ...range(92, 96).map((k) => ({
        label: `not^${k} in of three kinds`,
        where: nest(k, fn("in", ref("n"), val(["a", 1, 1.5]))),
      })),
      ...range(94, 97).map((n) => ({
        label: `or of ${n} eq, then a one-string in`,
        where: fn("or", ...range(1, n).map(eqId), fn("in", ref("id"), val(["z"]))),
      })),
      ...range(93, 97).map((k) => ({
        label: `not^${k} and(in, in of numbers)`,
        where: nest(k, fn("and", fn("in", ref("id"), val(["a", "b"])), fn("in", ref("n"), val([1, 2.5])))),
      })),
      ...range(95, 98).map((k) => ({ label: `not^${k} in of non-integers`, where: nest(k, fn("in", ref("n"), val([1.5]))) })),
      ...range(94, 98).map((k) => ({
        label: `not^${k} and(or(…))`,
        where: nest(k, fn("and", fn("or", isNull(), eqId(1), eqId(2)), eqId(3))),
      })),
    ])
  })

  it("statement length (100 000 bytes)", async () => {
    // Find the leaf count whose SQL first passes 100 000 bytes, then try both sides.
    const length = (n: number): number => `SELECT * FROM "m" WHERE ${compileWhere(balanced(n)).sql} ORDER BY rowid`.length
    let n = 1000
    while (length(n) <= 100_000) n += 1
    await expectEdge([n - 2, n - 1, n, n + 1].map((k) => ({ label: `balanced or of ${k} (${length(k)} bytes)`, where: balanced(k) })))
  })

  it("one bound value (under 2 200 000 bytes), as a JSON list or a plain string", async () => {
    // `["…"]` adds 4 bytes around the string; "é" is 2 bytes of UTF-8. A
    // `json_each` list of exactly 2 200 000 bytes fails, so that is the edge for
    // every bound string; a plain string of exactly that size would still run.
    await expectEdge([
      { label: "in list of 2 199 999 bytes", where: fn("in", ref("id"), val(["x".repeat(2_199_999 - 4)])) },
      { label: "in list of 2 200 000 bytes", where: fn("in", ref("id"), val(["x".repeat(2_200_000 - 4)])) },
      { label: "in list of 274 999 integers", where: fn("in", ref("n"), val(Array(274_999).fill(1234567))) },
      { label: "in list of 275 000 integers", where: fn("in", ref("n"), val(Array(275_000).fill(1234567))) },
      { label: "eq of 2 199 998 bytes", where: fn("eq", ref("body"), val("é".repeat(1_099_999))) },
      { label: "eq of 2 200 002 bytes", where: fn("eq", ref("body"), val("é".repeat(1_100_001))) },
    ])
  })

  it("LIKE pattern (50 bytes)", async () => {
    const like = (label: string, pattern: string): Case => ({
      label,
      where: fn("like", ref("body"), val(pattern)),
      raw: { sql: `"body" LIKE ?`, params: [pattern] },
    })
    await expectEdge([
      like("50 bytes", "%".repeat(50)),
      like("51 bytes", "%".repeat(51)),
      like("50 bytes of é", "é".repeat(25)),
      like("52 bytes of é", "é".repeat(26)),
    ])
  })
})
