import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { compileSubsetQuery, compileWhere, isSqliteLimitError } from "../src/server/sql-compiler.ts"

// WHY: SQLite in a Durable Object runs with reduced limits (workerd's
// `SqliteDatabase::setupSecurity`). A statement over one throws from SQLite at
// run time, and a sub that throws gets no reply and stays registered. So the
// compiler refuses every statement over a limit (ADR-0013 amendment
// 2026-11-17). The property is one-sided: the compiler never accepts a
// statement SQLite rejects; it may refuse a little early. Exactness is not
// possible for depth: SQLite adds a level to a `where` holding a subquery, and a
// MULTI-INDEX OR plan adds depth that depends on the table's own indexes.
//
// Each case is compiled; an accepted one is run as compiled, and a refused one
// is run as the same SQL built without the checks, to see whether SQLite would
// have run it. Every family must cross its own edge (some case SQLite rejects),
// accept below it, and refuse at most a few cases early.

const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })
const range = (from: number, to: number): Array<number> => Array.from({ length: Math.max(0, to - from + 1) }, (_, i) => from + i)
const eqc = (col: string, v: unknown): unknown => fn("eq", ref(col), val(v))
const isNull = (col = "body"): unknown => fn("isNull", ref(col))
const orEq = (n: number, col = "body"): unknown => fn("or", ...range(1, n).map((i) => eqc(col, `k${i}`)))
const nest = (k: number, inner: unknown): unknown => (k <= 0 ? inner : fn("not", nest(k - 1, inner)))
/** A balanced `or` tree over `n` leaves: shallow, so only its length grows. */
const balanced = (n: number): unknown => (n === 1 ? isNull() : fn("or", balanced(Math.ceil(n / 2)), balanced(Math.floor(n / 2))))
/** `and(a, and(b, and(c, …)))`: conjuncts nested to the right. */
const rightAnd = (parts: Array<unknown>): unknown => parts.reduceRight((acc, p) => fn("and", p, acc))

/** `orderBy` lists column names; `raw` is the WHERE to run where `compileWhere` itself refuses. */
interface Q {
  where?: unknown
  orderBy?: Array<string>
  limit?: number
  offset?: number
  raw?: { sql: string; params: Array<unknown> }
}
interface Verdict {
  accepted: boolean
  /** Whether SQLite runs the statement (as compiled, or built without checks). */
  runs: boolean
  error?: string
}

/** The statement `compileSubsetQuery` would build, without its limit checks. */
function unchecked(tbl: string, q: Q): { sql: string; params: Array<unknown> } {
  const params: Array<unknown> = []
  let sql = `SELECT * FROM "${tbl}"`
  if (q.where != null) {
    const w = q.raw ?? compileWhere(q.where)
    sql += ` WHERE ${w.sql}`
    params.push(...w.params)
  }
  sql += q.orderBy
    ? ` ORDER BY ${q.orderBy.map((c) => `"${c}" ASC NULLS FIRST`).join(", ")}, "id" COLLATE BINARY ASC`
    : " ORDER BY rowid"
  if (q.limit != null) (sql += " LIMIT ?"), params.push(q.limit)
  else if (q.offset != null) sql += " LIMIT -1"
  if (q.offset != null) (sql += " OFFSET ?"), params.push(q.offset)
  return { sql, params }
}

/** Run `each(verdict)` in a DO over three tables: `m` (pk only), `mi`
 *  (secondary indexes on `n` and `body`), and `mn` (500 rows, an index on `n`
 *  only). The indexes put MULTI-INDEX OR plans in play. */
async function inTables<T>(each: (verdict: (tbl: string, q: Q) => Verdict) => T): Promise<T> {
  const stub = env.TEST_DO.get(env.TEST_DO.idFromName(`limits-${crypto.randomUUID()}`))
  return runInDurableObject(stub, (_i, s) => {
    const sql = s.storage.sql
    for (const t of ["m", "mi", "mn"]) {
      sql.exec(`CREATE TABLE ${t} (id TEXT PRIMARY KEY, body TEXT, n INTEGER, z TEXT)`)
      // A LIKE is checked against its length cap only when evaluated on a row.
      sql.exec(`INSERT INTO ${t} VALUES ('a', 'x', 1, NULL), ('b', NULL, NULL, NULL), ('c', 'k1', 2, 'z')`)
    }
    sql.exec("CREATE INDEX mi_n ON mi(n)")
    sql.exec("CREATE INDEX mi_body ON mi(body)")
    sql.exec("CREATE INDEX mn_n ON mn(n)")
    for (let i = 0; i < 500; i++) sql.exec("INSERT INTO mn VALUES (?, ?, ?, ?)", `r${i}`, `b${i}`, i, `z${i}`)
    const run = (stmt: { sql: string; params: Array<unknown> }): string | undefined => {
      try {
        Array.from(sql.exec(stmt.sql, ...stmt.params))
        return undefined
      } catch (e) {
        return String(e instanceof Error ? e.message : e)
      }
    }
    return each((tbl, q) => {
      const orderBy = q.orderBy?.map((col) => ({ col, dir: "asc" }))
      let compiled: { sql: string; params: Array<unknown> } | undefined
      try {
        compiled = compileSubsetQuery(tbl, { pk: "id", where: q.where, orderBy, limit: q.limit, offset: q.offset })
      } catch {
        compiled = undefined
      }
      const error = run(compiled ?? unchecked(tbl, q))
      return { accepted: compiled !== undefined, runs: error === undefined, error }
    })
  })
}

/** A family is a sequence of cases growing towards and past one limit. */
type Family = { label: string; cases: Array<Q>; tables?: Array<string>; slack?: number }

/** Per family and table: no accepted case SQLite rejects; some case SQLite
 *  rejects (the family crosses its own edge); some case accepted; and the
 *  compiler stops accepting at most `slack` cases before SQLite stops running. */
async function expectSafeEdges(families: Array<Family>): Promise<void> {
  const { problems, notes } = await inTables((verdict) => {
    const out: Array<string> = []
    const notes: Array<string> = []
    for (const { label, cases, tables = ["m", "mi"], slack = 3 } of families) {
      for (const tbl of tables) {
        const vs = cases.map((q) => verdict(tbl, q))
        const at = `${label} on ${tbl}`
        vs.forEach((v, i) => {
          if (v.accepted && !v.runs) out.push(`${at}: case ${i} accepted, SQLite rejects it (${v.error})`)
        })
        if (!vs.some((v) => !v.runs)) out.push(`${at}: never crosses its edge`)
        if (!vs.some((v) => v.accepted)) out.push(`${at}: nothing accepted`)
        const early = vs.map((v) => v.runs).lastIndexOf(true) - vs.map((v) => v.accepted).lastIndexOf(true)
        if (early > slack) out.push(`${at}: refuses ${early} cases early`)
        if (early > 3) notes.push(`${at}: refuses ${early} cases early`)
      }
    }
    return { problems: out, notes }
  })
  for (const note of notes) console.log(note)
  expect(problems).toEqual([])
}

describe("the compiler never accepts a statement SQLite rejects for a limit", () => {
  it("bound parameters (100)", async () => {
    const reals = (n: number): unknown => fn("in", ref("n"), val(range(1, n).map((i) => i + 0.5)))
    await expectSafeEdges([
      { label: "in of n non-integers", cases: range(97, 101).map((n) => ({ where: reals(n) })), slack: 0 },
      { label: "non-integers + limit + offset", cases: range(96, 100).map((n) => ({ where: reals(n), limit: 5, offset: 0 })), slack: 0 },
    ])
  })

  it("expression depth (100), across shapes, with and without secondary indexes", async () => {
    const inS = fn("in", ref("id"), val(["a"]))
    const inMixed = fn("in", ref("id"), val(["a", 1]))
    const k = (from: number, to: number, mk: (k: number) => unknown): Array<Q> => range(from, to).map((i) => ({ where: mk(i) }))
    const orIdN = fn("or", eqc("id", "a"), eqc("n", 1))
    await expectSafeEdges([
      { label: "or of k eq", cases: k(94, 101, (i) => orEq(i)) },
      { label: "or of k isNull", cases: k(94, 101, (i) => fn("or", ...range(1, i).map(() => isNull()))) },
      { label: "or of k one-string in", cases: k(92, 100, (i) => fn("or", ...range(1, i).map((j) => fn("in", ref("id"), val([`k${j}`]))))) },
      { label: "not^k eq", cases: k(94, 101, (i) => nest(i, eqc("body", "x"))) },
      { label: "not^k like", cases: k(94, 101, (i) => nest(i, fn("like", ref("body"), val("x%")))) },
      { label: "not^k empty in", cases: k(93, 100, (i) => nest(i, fn("in", ref("body"), val([])))) },
      { label: "not^k in of three kinds", cases: k(90, 97, (i) => nest(i, fn("in", ref("n"), val(["a", 1, 1.5])))) },
      // Cause A: a subquery anywhere in the `where` adds a level to all of it.
      { label: "and(in, not^k eq)", cases: k(93, 100, (i) => fn("and", inS, nest(i, eqc("body", "x")))) },
      { label: "or(eq × k, in)", cases: k(93, 100, (i) => fn("or", ...range(1, i).map((j) => eqc("body", `k${j}`)), inS)) },
      { label: "and(in mixed, isNull, or of k eq)", cases: k(92, 99, (i) => fn("and", inMixed, isNull("n"), orEq(i))) },
      // Cause B: a MULTI-INDEX OR plan rebuilds the other conjuncts deeper.
      { label: "and(or(eq id, eq n), isNull, or of k eq)", cases: k(92, 99, (i) => fn("and", orIdN, isNull("n"), orEq(i))) },
      // Right-nested conjuncts: the plan rebuilds them left-deep, so they count
      // one level each. On `mn` (500 rows, index on `n`) the plan is chosen.
      {
        label: "and(or of 70 eq, right-nested k isNull(z), or(eq id, eq n))",
        cases: k(20, 34, (i) => fn("and", orEq(70), rightAnd([...range(1, i).map(() => isNull("z")), orIdN]))),
        tables: ["mn"],
      },
      {
        label: "and(or of 40 eq, right-nested k isNull(z), or(eq id, eq n))",
        cases: k(50, 66, (i) => fn("and", orEq(40), rightAnd([...range(1, i).map(() => isNull("z")), orIdN]))),
        tables: ["mn"],
      },
      {
        label: "and(or of 20 eq, right-nested k isNull(z), or(eq id, eq n))",
        cases: k(70, 88, (i) => fn("and", orEq(20), rightAnd([...range(1, i).map(() => isNull("z")), orIdN]))),
        tables: ["mn"],
      },
      // The bound's worst case: one tall conjunct beside many short ones. It adds
      // the tallest conjunct and the conjunct count; SQLite needs about the larger.
      // Measured: accepted up to 66 short conjuncts, SQLite runs up to 96.
      {
        label: "and(or(eq id, eq n), k isNull(z), or of 30 eq)",
        cases: k(55, 100, (i) => fn("and", orIdN, ...range(1, i).map(() => isNull("z")), orEq(30))),
        tables: ["m", "mi", "mn"],
        slack: 30,
      },
    ])
  })

  it("statement length (100 000 bytes)", async () => {
    const length = (n: number): number => unchecked("mi", { where: balanced(n) }).sql.length
    let n = 1000
    while (length(n) <= 100_000) n += 1
    await expectSafeEdges([{ label: "balanced or", cases: range(n - 3, n + 1).map((c) => ({ where: balanced(c) })), slack: 1 }])
  })

  it("one bound value (2 200 000 bytes): a JSON list, a string, bytes", async () => {
    await expectSafeEdges([
      // `["…"]` adds 4 bytes around the string.
      { label: "in list of one long string", cases: [2_199_998, 2_199_999, 2_200_000, 2_200_001].map((b) => ({ where: fn("in", ref("id"), val(["x".repeat(b - 4)])) })), slack: 1 },
      { label: "in list of 7-digit integers", cases: [274_998, 274_999, 275_000, 275_001].map((c) => ({ where: fn("in", ref("n"), val(Array(c).fill(1234567))) })), slack: 1 },
      // "é" is 2 bytes of UTF-8.
      { label: "eq string", cases: [1_099_999, 1_100_000, 1_100_001].map((c) => ({ where: eqc("body", "é".repeat(c)) })), slack: 1 },
      { label: "eq bytes", cases: [2_199_999, 2_200_000, 2_200_001].map((b) => ({ where: eqc("body", new Uint8Array(b)) })), slack: 1 },
    ])
  })

  it("LIKE pattern (50 bytes)", async () => {
    const like = (pattern: string): Q => ({ where: fn("like", ref("body"), val(pattern)), raw: { sql: `"body" LIKE ?`, params: [pattern] } })
    await expectSafeEdges([
      { label: "ASCII pattern", cases: [49, 50, 51].map((b) => like("%".repeat(b))), slack: 0 },
      { label: "2-byte pattern", cases: [24, 25, 26].map((c) => like("é".repeat(c))), slack: 0 },
    ])
  })

  it("ORDER BY terms (100, with the pk tie-break)", async () => {
    await expectSafeEdges([{ label: "orderBy of k columns", cases: range(97, 101).map((c) => ({ orderBy: Array(c).fill("body") })), slack: 0 }])
  })

  it("limit and offset under 2^63", async () => {
    await expectSafeEdges([
      { label: "limit", cases: [2 ** 62, 2 ** 63, 1e300].map((limit) => ({ limit })), slack: 0 },
      { label: "offset", cases: [2 ** 62, 2 ** 63, 1e300].map((offset) => ({ limit: 1, offset })), slack: 0 },
    ])
  })

  it("a bigint comparison value (workerd cannot bind one)", async () => {
    const cmp = (op: string, sqlOp: string, v: unknown): Q => ({ where: fn(op, ref("n"), val(v)), raw: { sql: `"n" ${sqlOp} ?`, params: [v] } })
    await expectSafeEdges([
      { label: "eq", cases: [cmp("eq", "=", 10), cmp("eq", "=", 10n)], slack: 0 },
      { label: "gt", cases: [cmp("gt", ">", 10), cmp("gt", ">", 10n)], slack: 0 },
    ])
  })
})

describe("fuzz: no accepted statement is rejected by SQLite", () => {
  // Deterministic, after the review's fuzz: a tall chain (k levels, crossing
  // 100) placed in each context beside each kind of sibling term, with and
  // without orderBy/limit/offset, on a table with and without secondary indexes.
  it("across contexts, siblings and tables", async () => {
    const leaves: Record<string, unknown> = {
      eq: eqc("body", "x"),
      eqId: eqc("id", "a"),
      isNull: isNull(),
      isUndef: fn("isUndefined", ref("body")),
      like: fn("like", ref("body"), val("x%")),
      inJson: fn("in", ref("id"), val(["a", "b"])),
      inInts: fn("in", ref("n"), val([1, 2])),
      inBound: fn("in", ref("n"), val([1.5])),
      inTwo: fn("in", ref("id"), val(["a", 1])),
      inThree: fn("in", ref("n"), val(["a", 1, 1.5])),
      inEmpty: fn("in", ref("body"), val([])),
      or2: fn("or", eqc("id", "a"), eqc("n", 1)),
      and2: fn("and", eqc("id", "a"), fn("in", ref("n"), val([1]))),
      notIn: fn("not", fn("in", ref("id"), val(["a"]))),
    }
    const shapes: Record<string, (h: unknown, s: unknown, s2: unknown) => unknown> = {
      "hole": (h) => h,
      "and(S,hole)": (h, s) => fn("and", s, h),
      "and(hole,S)": (h, s) => fn("and", h, s),
      "or(S,hole)": (h, s) => fn("or", s, h),
      "or(hole,S)": (h, s) => fn("or", h, s),
      "and(S,S2,hole)": (h, s, s2) => fn("and", s, s2, h),
      "and(hole,S,S2)": (h, s, s2) => fn("and", h, s, s2),
      "and(S,and(S2,hole))": (h, s, s2) => fn("and", s, fn("and", s2, h)),
      "or(S,S2,hole)": (h, s, s2) => fn("or", s, s2, h),
      "or(and(S,hole),S2)": (h, s, s2) => fn("or", fn("and", s, h), s2),
      "not(and(S,or(S2,hole)))": (h, s, s2) => fn("not", fn("and", s, fn("or", s2, h))),
      "and(or(S,S2),isNull,hole)": (h, s, s2) => fn("and", fn("or", s, s2), isNull("n"), h),
    }
    const holes: Record<string, (k: number) => unknown> = {
      "not^k eq": (k) => nest(k, leaves.eq),
      "not^k in": (k) => nest(k, leaves.inThree),
      "or of k eq": (k) => orEq(k - 8),
      "not^k (or of 9 eq)": (k) => nest(k - 10, orEq(9)),
    }
    const extras: Array<Q> = [{}, { orderBy: ["body", "n"], limit: 5, offset: 2 }]
    const result = await inTables((verdict) => {
      let total = 0
      let crossed = 0
      const unsafe: Array<string> = []
      let earliest = 0
      for (const tbl of ["m", "mi", "mn"]) {
        for (const [shape, mk] of Object.entries(shapes)) {
          for (const sName of Object.keys(leaves)) {
            for (const s2Name of shape.includes("S2") ? ["inJson", "or2"] : ["eq"]) {
              for (const [holeName, hole] of Object.entries(holes)) {
                for (const extra of extras) {
                  let lastAccepted = -1
                  let lastRuns = -1
                  let failed = false
                  for (let k = 88; k <= 101; k++) {
                    const v = verdict(tbl, { ...extra, where: mk(hole(k), leaves[sName], leaves[s2Name]) })
                    total++
                    if (v.accepted && !v.runs) unsafe.push(`${tbl} ${shape} S=${sName} S2=${s2Name} ${holeName} k=${k}: ${v.error}`)
                    if (v.accepted) lastAccepted = k
                    if (v.runs) lastRuns = k
                    else failed = true
                  }
                  if (failed) crossed++
                  earliest = Math.max(earliest, lastRuns - lastAccepted)
                }
              }
            }
          }
        }
      }
      return { total, crossed, unsafe: unsafe.slice(0, 20), unsafeCount: unsafe.length, earliest }
    })
    console.log(
      `limits fuzz: ${result.total} statements, ${result.unsafeCount} accepted then rejected, ` +
        `${result.crossed} families crossed 100, refused at most ${result.earliest} levels early`,
    )
    expect(result.unsafe).toEqual([])
    expect(result.earliest).toBeLessThanOrEqual(6)
  }, 60_000)
})

describe("SQLite's limit errors are recognised (the backstop)", () => {
  // `isSqliteLimitError` matches SQLite's messages for a statement over a limit.
  // Each is provoked for real, so a change in wording fails here instead of
  // silently turning a refusal back into a hang.
  it("matches every limit error SQLite gives, and nothing else", async () => {
    const stub = env.TEST_DO.get(env.TEST_DO.idFromName(`limit-errors-${crypto.randomUUID()}`))
    const verdicts = await runInDurableObject(stub, (_i, s) => {
      const sql = s.storage.sql
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, body TEXT)")
      sql.exec("INSERT INTO t VALUES ('a', 'x')")
      const provoke = (stmt: string, ...params: Array<unknown>): string => {
        try {
          Array.from(sql.exec(stmt, ...params))
        } catch (e) {
          return e instanceof Error ? e.message : String(e)
        }
        return `did not fail: ${stmt.slice(0, 60)}`
      }
      const limitErrors = [
        provoke(`SELECT ${Array(101).fill("?").join(", ")}`, ...Array(101).fill(1)),
        provoke(`SELECT * FROM t WHERE ${Array(101).fill(`"body" IS NULL`).join(" OR ")}`),
        provoke(`SELECT 1${" ".repeat(100_000)}`),
        provoke("SELECT length(?)", "x".repeat(2_200_001)),
        provoke("INSERT INTO t VALUES ('b', ?)", "x".repeat(2_200_001)),
        provoke(`SELECT * FROM t ORDER BY ${Array(101).fill(`"body"`).join(", ")}`),
        provoke(`SELECT * FROM t WHERE "body" LIKE ?`, "%".repeat(51)),
      ]
      const otherErrors = [
        provoke("SELECT * FROM no_such_table"),
        provoke("INSERT INTO t VALUES ('a', 'dup')"),
        provoke("SELECT * FROM t LIMIT ?", 2 ** 63),
      ]
      return {
        limit: limitErrors.map((m) => `${isSqliteLimitError(new Error(m)) ? "limit" : "MISSED"}: ${m.slice(0, 70)}`),
        other: otherErrors.map((m) => `${isSqliteLimitError(new Error(m)) ? "MATCHED" : "other"}: ${m.slice(0, 70)}`),
      }
    })
    expect(verdicts.limit.filter((v) => !v.startsWith("limit"))).toEqual([])
    expect(verdicts.other.filter((v) => !v.startsWith("other"))).toEqual([])
  })
})
