import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { compileSubsetQuery, UnsupportedPredicateError } from "../src/server/sql-compiler.ts"
import { SubscriptionRegistry } from "../src/server/subscriptions.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: `in` decides membership on two paths — the snapshot in SQLite (the
// compiled SQL) and deltas/catch-up in @tanstack/db's evaluator — and they
// must pick the same rows (ADR-0013 D1). A long list cannot bind one parameter
// per value (Durable Object SQLite allows 100 per statement), so `in` compiles
// to a different SQL form; this pins that the form agrees with the evaluator
// for every kind of value the floor admits in a list, at sizes below, at, above
// the limit and at a few thousand, against columns of every affinity — and
// that the kinds it does not admit are refused rather than silently diverging.
// (ADR-0013 amendment 2026-11-17.)

const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })

// One column per SQLite affinity. `a` declares no type (BLOB/none affinity).
const COLS = { t: "TEXT", i: "INTEGER", r: "REAL", n: "NUMERIC", a: "" } as const
// Each value is written to every column, so affinity converts it per column
// exactly as a mutation's bound write would (e.g. 1 → "1" in TEXT, "5" → 5 in
// INTEGER).
const STORED: Array<unknown> = [
  "a", "1", "1.0", "5", "abc", "", " 5", "a\u0000b", "😀", "[1]", "null",
  1, 1.5, 5, 0, -7, 2 ** 53 - 1, 1e300, Infinity, -Infinity, null, new Uint8Array([1]),
]
const strings = (n: number): Array<string> => Array.from({ length: n }, (_, i) => `s${i}`)
const ints = (n: number): Array<number> => Array.from({ length: n }, (_, i) => i - 7)

const LISTS: Record<string, Array<unknown>> = {
  empty: [],
  "one string": ["a"],
  "numeric-looking strings": ["1", "5", " 5", "1.0"],
  "odd strings": ["a\u0000b", "😀", "", "[1]", "null", '"q"', "\\"],
  "duplicates": ["a", "a", 5, 5, "a"],
  "safe integers": [1, 5, 0, -7],
  "-0": [-0],
  "largest safe integer": [2 ** 53 - 1],
  "non-integer numbers": [1.5, 0.1, 1e300],
  "infinities": [Infinity, -Infinity],
  "only null": [null],
  "null, undefined and NaN beside values": [null, "a", undefined, NaN, 5],
  "mixed kinds": [1, "1", "abc", 1.5, 5],
  "99 strings": [...strings(97), "a", "1"],
  "100 strings": [...strings(98), "a", "1"],
  "101 strings": [...strings(99), "a", "1"],
  "3000 strings": [...strings(2998), "a", "1"],
  "101 integers": ints(101),
  "3000 integers": ints(3000),
  "3000 mixed": [...strings(1500), ...ints(1500), "abc", 1.5],
}

/** For each list × column × (in, not in): the ids SQLite selects through the
 *  compiled SQL vs the ids the delta path's evaluator accepts. Returns the
 *  disagreements. */
async function disagreements(lists: Record<string, Array<unknown>>): Promise<Array<string>> {
  const stub = env.TEST_DO.get(env.TEST_DO.idFromName(`ilp-${crypto.randomUUID()}`))
  return runInDurableObject(stub, (_i, s) => {
    const sql = s.storage.sql
    const cols = Object.keys(COLS)
    sql.exec(`CREATE TABLE m (id TEXT PRIMARY KEY, ${cols.map((c) => `${c} ${COLS[c as keyof typeof COLS]}`).join(", ")})`)
    STORED.forEach((v, k) => {
      sql.exec(`INSERT INTO m (id, ${cols.join(", ")}) VALUES (?, ${cols.map(() => "?").join(", ")})`, `k${k}`, ...cols.map(() => v))
    })
    // Rows exactly as hydrateRows hands them to the delta path.
    const rows = Array.from(sql.exec("SELECT * FROM m")) as Array<Record<string, unknown>>
    const registry = new SubscriptionRegistry()
    const out: Array<string> = []
    for (const [label, list] of Object.entries(lists)) {
      for (const col of cols) {
        for (const negate of [false, true]) {
          const inList = fn("in", ref(col), val(list))
          const where = negate ? fn("not", inList) : inList
          const { predicate } = registry.add({} as WebSocket, "s", "m", where)
          const js = rows.filter((row) => predicate(row)).map((row) => row.id as string).sort()
          const q = compileSubsetQuery("m", { pk: "id", where })
          const got = Array.from(sql.exec(q.sql, ...q.params)).map((row) => row.id as string).sort()
          if (JSON.stringify(got) !== JSON.stringify(js)) {
            const show = (ids: Array<string>): string =>
              ids.map((id) => {
                const v = rows.find((row) => row.id === id)![col]
                return `${id}=${v instanceof ArrayBuffer ? "blob" : JSON.stringify(v)}`
              }).join(" ")
            out.push(
              `${negate ? "not " : ""}in ${label} on ${col} (${COLS[col as keyof typeof COLS] || "no type"}): ` +
                `sql-only [${show(got.filter((x) => !js.includes(x)))}] js-only [${show(js.filter((x) => !got.includes(x)))}]`,
            )
          }
        }
      }
    }
    return out
  })
}

describe("`in` membership: compiled SQL agrees with the evaluator", () => {
  it("for every admitted kind, every column affinity, and every size", async () => {
    expect(await disagreements(LISTS)).toEqual([])
  })

  it("inside a larger predicate (and/or with other terms, two long lists)", async () => {
    const stub = env.TEST_DO.get(env.TEST_DO.idFromName(`ilp-${crypto.randomUUID()}`))
    const mismatches = await runInDurableObject(stub, (_i, s) => {
      const sql = s.storage.sql
      sql.exec("CREATE TABLE m (id TEXT PRIMARY KEY, body TEXT, n INTEGER)")
      for (let k = 0; k < 400; k++) sql.exec("INSERT INTO m VALUES (?, ?, ?)", `k${k}`, k % 3 === 0 ? null : `b${k % 7}`, k)
      const rows = Array.from(sql.exec("SELECT * FROM m")) as Array<Record<string, unknown>>
      const registry = new SubscriptionRegistry()
      const ids = (n: number, step: number): Array<string> => Array.from({ length: n }, (_, i) => `k${i * step}`)
      const cases = [
        fn("and", fn("in", ref("id"), val(ids(150, 2))), fn("in", ref("n"), val(ints(300)))),
        fn("or", fn("in", ref("id"), val(ids(60, 3))), fn("in", ref("id"), val(ids(60, 5)))),
        fn("and", fn("not", fn("in", ref("body"), val(["b1", "b2"]))), fn("in", ref("id"), val(ids(200, 1)))),
        fn("or", fn("eq", ref("body"), val("b3")), fn("not", fn("in", ref("n"), val([...ints(150), 1.5])))),
      ]
      return cases.flatMap((where, k) => {
        const { predicate } = registry.add({} as WebSocket, "s", "m", where)
        const js = rows.filter((row) => predicate(row)).map((row) => row.id).sort()
        const q = compileSubsetQuery("m", { pk: "id", where })
        const got = Array.from(sql.exec(q.sql, ...q.params)).map((row) => row.id).sort()
        return JSON.stringify(got) === JSON.stringify(js) ? [] : [`case ${k}: sql ${got.length} rows, js ${js.length} rows`]
      })
    })
    expect(mismatches).toEqual([])
  })
})

describe("`in` list values the floor does not admit are refused", () => {
  // SQLite has no boolean, bigint or date, and a server row never holds a
  // Uint8Array (BLOBs hydrate as ArrayBuffer), so no SQL form can agree with
  // the evaluator on these. They used to bind as text ('true', a Date's
  // toString()), a blob, or throw (bigint).
  for (const [label, item] of [
    ["boolean", true],
    ["bigint", 10n],
    ["Date", new Date(5)],
    ["Uint8Array", new Uint8Array([1])],
    ["object", { a: 1 }],
    ["array", [1]],
  ] as const) {
    it(`${label}`, () => {
      expect(() => compileSubsetQuery("m", { pk: "id", where: fn("in", ref("id"), val(["a", item])) })).toThrow(
        UnsupportedPredicateError,
      )
    })
  }

  // Non-integer numbers bind one parameter each (JSON text does not carry
  // every double exactly), so a statement with more than 100 of them is refused.
  it("more than 100 non-integer numbers in one statement", () => {
    const where = fn("in", ref("n"), val(Array.from({ length: 101 }, (_, i) => i + 0.5)))
    expect(() => compileSubsetQuery("m", { pk: "id", where })).toThrow(UnsupportedPredicateError)
  })
})

// The real snapshot and delta paths, through the DO, on the realistic shapes.
const codec = createFrameCodec()
async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}
const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))
function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 3000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

// m0 and m1 are named by every id list below; "one" holds the text "1" and
// "onedot" the text "1.0" (what a bound number 1 turns into in a TEXT column);
// "nul" has a NULL body.
const SEEDS: Array<{ id: string; body: string | null }> = [
  { id: "m0", body: "x" },
  { id: "m1", body: "y" },
  { id: "one", body: "1" },
  { id: "onedot", body: "1.0" },
  { id: "nul", body: null },
]

async function snapshotMembers(where: unknown): Promise<Array<string>> {
  const room = `ilp-snap-${crypto.randomUUID()}`
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const seed of SEEDS) s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", seed.id, seed.body)
  })
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  const frames = await collectUntil(ws, (f) => f.t === "snap-end" || f.t === "reset")
  ws.close()
  if (frames.at(-1)!.t !== "snap-end") throw new Error("sub refused")
  return frames.flatMap((f) => (f.t === "snap" ? [f.key as string] : [])).sort()
}

async function deltaMembers(where: unknown): Promise<Array<string>> {
  const ws = await openWs(`ilp-delta-${crypto.randomUUID()}`)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  await collectUntil(ws, (f) => f.t === "snap-end")
  send(ws, {
    t: "mut",
    txId: "seed",
    collection: "messages",
    ops: SEEDS.map((seed) => ({ type: "insert" as const, key: seed.id, cols: { id: seed.id, body: seed.body } })),
  })
  const frames = await collectUntil(ws, (f) => f.t === "committed")
  ws.close()
  return frames.flatMap((f) => (f.t === "d" && f.op !== "delete" ? [f.key as string] : [])).sort()
}

describe("`in` through the DO: snapshot and deltas agree", () => {
  const ids = (n: number): Array<string> => Array.from({ length: n }, (_, i) => `m${i}`)
  const cases: Array<[string, unknown, Array<string>]> = [
    ["in(id, 99 ids)", fn("in", ref("id"), val(ids(99))), ["m0", "m1"]],
    ["in(id, 100 ids)", fn("in", ref("id"), val(ids(100))), ["m0", "m1"]],
    ["in(id, 101 ids)", fn("in", ref("id"), val(ids(101))), ["m0", "m1"]],
    ["in(id, 3000 ids)", fn("in", ref("id"), val(ids(3000))), ["m0", "m1"]],
    ["in(body, [1]) on a TEXT column", fn("in", ref("body"), val([1])), []],
    ["not in(body, []) beside a NULL body", fn("not", fn("in", ref("body"), val([]))), ["m0", "m1", "one", "onedot"]],
    ["not in(body, [null])", fn("not", fn("in", ref("body"), val([null]))), ["m0", "m1", "one", "onedot"]],
    ["not in(body, [null, 'x'])", fn("not", fn("in", ref("body"), val([null, "x"]))), ["m1", "one", "onedot"]],
  ]
  for (const [label, where, expected] of cases) {
    it(label, async () => {
      const snap = await snapshotMembers(where)
      const delta = await deltaMembers(where)
      expect(delta, "the evaluator's answer").toEqual(expected)
      expect(snap, "the snapshot must match the evaluator").toEqual(delta)
    })
  }
})
